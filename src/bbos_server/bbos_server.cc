/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

/*
 * bbos_server.cc  a mercury-based server for the BuddyStore
 * 10-Mar-2017
 */

#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <map>
#include <string>

#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_proc.h>
#include <mercury_proc_string.h>

#ifndef _WIN32
#undef _GNU_SOURCE /* XXX: stop warning: mercury_thread.h redefs this */
#endif
#include <mercury_thread_pool.h>

#include "../libbbos/bbos.h"
#include "../libbbos/bbos_rpc.h"

/* defaults */
#define DEF_NUM_WORKERS   32       /* size of thread pool */

namespace pdlfs {
namespace bb {

/*
 * thread_args: for hg_thread_pool we need a source for hg_thread_work.
 * we keep a free list of them in the BuddyServer (grows on demand).
 * note that the thread pool expects the hg_thread_work struct to be
 * embedded in something that we manage (it does not alloc or free
 * hg_thread_work).
 */
struct thread_args {
  struct hg_thread_work work;  /* contains func and args for thread */
  hg_handle_t handle;          /* current handle */
  class BuddyServer *srvr;     /* server that owns us */
  struct thread_args *nxtfree; /* next free item when on free list */
};

/*
 * append_state: state we keep for an append (a multi-step operation
 * due to the bulk xfers).
 */
struct append_state {
  struct thread_args *targs;   /* args for this thread */
  bbos_append_in_t in;         /* decoded input (must free when done) */
  hg_bulk_t local_bulkhand;    /* local bulk handle for buffer */
  void *buffer;                /* local bulk buffer */
  hg_size_t size;              /* size of local bulk buffer */
};

/*
 * read_state: state we keep for a read (a multi-step operation
 * due to the bulk xfers).  this is used in the same way as append_state,
 * but it has a different "in" structure...
 */
struct read_state {
  struct thread_args *targs;   /* args for this thread */
  bbos_read_in_t in;           /* decoded input (must free when done) */
  hg_bulk_t local_bulkhand;    /* local bulk handle for buffer */
  void *buffer;                /* local bulk buffer */
  hg_size_t size;              /* size of local bulk buffer */
};

/*
 * BuddyServer: main server object, keeps all server state.
 */
class BuddyServer {
 private:
  class BuddyStore *store_;        /* backend */
  char *server_url_;               /* mercury url to listen to (strdup'd) */
  hg_class_t *hg_class_;           /* mercury class */
  hg_context_t *hg_context_;       /* mercury context (queues, etc.) */
  int num_worker_threads_;         /* number of worker threads */
  hg_thread_pool_t *thread_pool_;  /* worker thread pool */
  int made_progress_thread_;       /* != if we forked a progress thread */
  pthread_t progress_thread_;      /* for HG process/trigger */

  /* ID#s for our 4 RPCs */
  hg_id_t mkobj_rpc_id_;
  hg_id_t append_rpc_id_;
  hg_id_t read_rpc_id_;
  hg_id_t get_size_rpc_id_;

  pthread_mutex_t lock_;           /* lock (for multithreading) */
  pthread_cond_t cond_;            /* for waiting for shutdown */
  int running_;                    /* progress thread running */
  int shutdown_;                   /* set to trigger shutdown */
  int refcnt_;                     /* # of active thread requests out */
  int n_targs_;                    /* # of thread args allocated */
  struct thread_args *ta_free_;    /* free list of thread args */

  static void *progress(void *arg);

  /* generic handler gets all RPC requests and relays to thread pool */
  static hg_return_t generic_handler(hg_handle_t handle);

  /* per-rpc handlers run in the thread pool */
  static HG_THREAD_RETURN_TYPE mkobj_handler(void *args);
  static HG_THREAD_RETURN_TYPE append_handler(void *args);
  static HG_THREAD_RETURN_TYPE read_handler(void *args);
  static HG_THREAD_RETURN_TYPE get_size_handler(void *args);

  /* for two-part bulk RPCs (append/read) we provide finish callbacks */
  static hg_return_t append_finish(const struct hg_cb_info *info);
  static hg_return_t read_finish(const struct hg_cb_info *info);

  /* allocate/malloc thread_args, gain reference */
  struct thread_args *gettargs(hg_thread_func_t nxtfun, hg_handle_t handle) {
    struct thread_args *ta;

    pthread_mutex_lock(&lock_);
    ta = ta_free_;
    if (ta) {
      ta_free_ = ta->nxtfree;
      ta->nxtfree = NULL;      /* just to be safe */
    } else {
      ta = (struct thread_args *)malloc(sizeof(*ta));  /*malloc new one*/
      if (ta) {
        n_targs_++;
        ta->nxtfree = NULL;
      }
    }

    if (ta) {    /* fill it in and gain a reference */
      ta->work.func = nxtfun;
      ta->work.args = ta;
      ta->handle = handle;
      ta->srvr = this;
      refcnt_++;
    }

    pthread_mutex_unlock(&lock_);
    return(ta);
  }

  /* free thread_args for reuse, destroys handle if non-null */
  void freetargs_handle(struct thread_args *ta) {
    pthread_mutex_lock(&lock_);
    if (ta->handle) HG_Destroy(ta->handle);
    ta->nxtfree = ta_free_;
    ta_free_ = ta;
    if (refcnt_ > 0) refcnt_--;
    pthread_mutex_unlock(&lock_);
  }

  /* dealloc all resources we are holding */
  void dealloc() {
    struct thread_args *ta;

    if (made_progress_thread_)
      pthread_join(progress_thread_, NULL);
    made_progress_thread_ = 0;
    if (thread_pool_ && hg_thread_pool_destroy(thread_pool_) < 0)
      fprintf(stderr, "BuddyServer::dealloc: thread pool dest failed\n");
    thread_pool_ = NULL;
    if (hg_context_ && HG_Context_destroy(hg_context_) != HG_SUCCESS)
      fprintf(stderr, "BuddyServer::dealloc: hg ctx dest failed\n");
    hg_context_ = NULL;
    if (hg_class_ && HG_Finalize(hg_class_) != HG_SUCCESS)
      fprintf(stderr, "BuddyServer::dealloc: hg finalize failed\n");
    hg_class_ = NULL;
    if (server_url_) free(server_url_);
    server_url_ = NULL;

    while ((ta = ta_free_) != NULL) {
      ta_free_ = ta->nxtfree;
      free(ta);
    }

  }

 public:
  BuddyServer() : server_url_(NULL), hg_class_(NULL), hg_context_(NULL),
                  thread_pool_(NULL), made_progress_thread_(0), running_(0),
                  shutdown_(0), refcnt_(0), n_targs_(0), ta_free_(NULL) {
    if (pthread_mutex_init(&lock_, NULL) != 0 ||
        pthread_cond_init(&cond_, NULL) != 0) {
        fprintf(stderr, "BuddyServer: mutex/cond init failure?\n");
        abort();   /* this should never happen */
    }
  }
  ~BuddyServer();

  int run(char *url, int numworkers, BuddyStore *store);
  void shutdown() { shutdown_ = 1; }   /* called from signal handler */
  void wait();
  /*
   * total number of thread_args allocated tells us the max number of
   * threads from the thread pool we used at any one time...
   */
  int max_concurrent_reqs() { return n_targs_; }

};

/*
 * BuddyServer::~BuddyServer() - dtor frees resources
 */
BuddyServer::~BuddyServer() {

  /* first ensure it is really shutdown */
  this->shutdown();
  this->wait();

  /* dump any allocated resources */
  this->dealloc();
}

/*
 * BuddyServer::progress: mercury progress function.  static class fn.
 * thread forked off by BuddyServer::run().  trigger runs the first
 * level callback (generic_handler).
 */
void *BuddyServer::progress(void *arg) {
  class BuddyServer *bs = reinterpret_cast<class BuddyServer *>(arg);
  hg_return_t ret;
  unsigned int actual_count;

  pthread_mutex_lock(&bs->lock_);
  bs->running_ = 1;
  pthread_mutex_unlock(&bs->lock_);

  while (!bs->shutdown_ || bs->refcnt_) {
    do {
      ret = HG_Trigger(bs->hg_context_, 0, 1, &actual_count);
    } while (ret == HG_SUCCESS && actual_count && !bs->shutdown_);

    if (!bs->shutdown_) HG_Progress(bs->hg_context_, 100);
  }

  pthread_mutex_lock(&bs->lock_);
  bs->running_ = 0;
  pthread_mutex_unlock(&bs->lock_);
  pthread_cond_broadcast(&bs->cond_);   /* let them know we are done */

  return(NULL);
}

/*
 * BuddyServer::run: main routine to run the buddy server.
 * returns -1 on init error, 0 on success.
 */
int BuddyServer::run(char *url, int numworkers, BuddyStore *store) {

  if (shutdown_) {
    fprintf(stderr, "BuddyServer::run: can't rerun a used server\n");
    return(-1);
  }

  store_ = store;
  server_url_ = strdup(url);
  if (!server_url_) {
    fprintf(stderr, "BuddyServer: server_url_ malloc failed!\n");
    goto failure;
  }

  hg_class_ = HG_Init(url, HG_TRUE);
  if (hg_class_ == NULL) {
    fprintf(stderr, "BuddyServer: HG_Init of %s failed!\n", url);
    goto failure;
  }
  hg_context_ = HG_Context_create(hg_class_);
  if (hg_context_ == NULL) {
    fprintf(stderr, "BuddyServer: HG_Context_create failed!\n");
    goto failure;
  }
  /* XXX: register can't fail (but it must malloc?) */
  mkobj_rpc_id_ = MERCURY_REGISTER(hg_class_, "bbos_mkobj_rpc",
                  bbos_mkobj_in_t, bbos_mkobj_out_t,
                  BuddyServer::generic_handler);
  append_rpc_id_ = MERCURY_REGISTER(hg_class_, "bbos_append_rpc",
                   bbos_append_in_t, bbos_append_out_t,
                   BuddyServer::generic_handler);
  read_rpc_id_ = MERCURY_REGISTER(hg_class_, "bbos_read_rpc",
                 bbos_read_in_t, bbos_read_out_t, BuddyServer::generic_handler);
  get_size_rpc_id_ = MERCURY_REGISTER(hg_class_, "bbos_get_size_rpc",
                     bbos_get_size_in_t, bbos_get_size_out_t,
                     BuddyServer::generic_handler);
  /*
   * set the registered data for these to the BuddyServer.  we'll recover
   * it later in the handler (after the thread pool).
   */
  if (HG_Register_data(hg_class_, mkobj_rpc_id_, this, NULL) != HG_SUCCESS ||
      HG_Register_data(hg_class_, append_rpc_id_, this, NULL) != HG_SUCCESS ||
      HG_Register_data(hg_class_, read_rpc_id_, this, NULL) != HG_SUCCESS ||
      HG_Register_data(hg_class_, get_size_rpc_id_, this, NULL) != HG_SUCCESS) {
    fprintf(stderr, "BuddyServer: unable to register RPC data?!?\n");
    goto failure;
  }

  num_worker_threads_ = numworkers;
  if (hg_thread_pool_init(numworkers, &thread_pool_) < 0) {
    fprintf(stderr, "BuddyServer: threadpool %d failed!\n", numworkers);
    goto failure;
  }
  if (pthread_create(&progress_thread_, NULL,
                     BuddyServer::progress, this) != 0) {
    fprintf(stderr, "BuddyServer: progress thread create failed!\n");
    goto failure;
  }
  made_progress_thread_ = 1;

  /* we are ready to serve!! */

  return(0);

failure:
  this->dealloc();
  return(-1);
}

/*
 * BuddyServer::wait: wait for buddy server to exit
 */
void BuddyServer::wait() {
  int cnt = 0;
  pthread_mutex_lock(&lock_);
  while (shutdown_ == 0 || running_ == 1) {
    if (pthread_cond_wait(&cond_, &lock_) != 0) {
        if (cnt++ < 10)
          fprintf(stderr, "BuddySever::wait: *** cond wait failed? ***\n");
    }
  }
  pthread_mutex_unlock(&lock_);
}

/*
 * BuddyServer::generic_handler: this gets all our RPC calls and
 * kicks them off to thread pool for execution (thus keeping the
 * progress/trigger thread free to continue to drive the network).
 * we track the number operations out in the thread pool via refcnt_.
 * (static class fn)
 */
hg_return_t BuddyServer::generic_handler(hg_handle_t handle) {
  struct hg_info *hgi;
  class BuddyServer *srvr;
  struct thread_args *myta;
  hg_thread_func_t nextfunc;

  /*
   * note: no matter what we do, we must HG_Destroy() the handle when
   * we are done with it or it never get freed (i.e. memory leak).
   */

  /* recover the BuddyServer class from the registered data */
  hgi = HG_Get_info(handle);
  srvr = (hgi) ? reinterpret_cast<class BuddyServer *>
                 (HG_Registered_data(hgi->hg_class, hgi->id)) : NULL;
  if (!srvr) {
    HG_Destroy(handle);
    fprintf(stderr, "BuddyServer::generic: can't get srvr!\n");
    return(HG_INVALID_PARAM);   /* highly unlikely */
  }

  if (hgi->id == srvr->mkobj_rpc_id_) {
    nextfunc = BuddyServer::mkobj_handler;
  } else if (hgi->id == srvr->append_rpc_id_) {
    nextfunc = BuddyServer::append_handler;
  } else if (hgi->id == srvr->read_rpc_id_) {
    nextfunc = BuddyServer::read_handler;
  } else if (hgi->id == srvr->get_size_rpc_id_) {
    nextfunc = BuddyServer::get_size_handler;
  } else {
    HG_Destroy(handle);
    fprintf(stderr, "BuddyServer::generic: unknown rpc id?!\n");
    return(HG_INVALID_PARAM);   /* highly unlikely */
  }

  /* build up a thread args so we can pass req to thread pool. */
  myta = srvr->gettargs(nextfunc, handle);
  if (myta == NULL) {
    HG_Destroy(handle);
    fprintf(stderr, "BuddyServer::generic: gettargs alloc failed\n");
    return(HG_NOMEM_ERROR);     /* could happen, hopefully not! */
  }

  /*
   * now we are holding a reference to the BuddySever via myta.
   * we must drop it at some point with freetargs_handle (which also
   * destroys the handle).
   */

  /* post it to the thread pool */
  if (hg_thread_pool_post(srvr->thread_pool_, &myta->work) < 0) {
    fprintf(stderr, "BuddyServer::generic: thread pool post failed!\n");
    srvr->freetargs_handle(myta);
    return(HG_NOMEM_ERROR);     /* highly unlikely */
  }

  /*
   * success, processing will resume from thread pool calling
   * myta->work.func(myta->work.args);
   */
  return(HG_SUCCESS);
}

/*
 * BuddyServer::mkobj_handler: called from the thread pool
 */
HG_THREAD_RETURN_TYPE BuddyServer::mkobj_handler(void *args) {
  struct thread_args *ta;
  bbos_mkobj_in_t in;
  mkobj_flag_t flag;
  bbos_mkobj_out_t out;
  hg_return_t hgret;

  /* extract handle and BuddyServer from mercury data */
  ta = reinterpret_cast<struct thread_args *>(args);

  /* get input data */
  if (HG_Get_input(ta->handle, &in) == HG_SUCCESS) {
    flag = (in.readopt) ? READ_OPTIMIZED : WRITE_OPTIMIZED;
    out.status = ta->srvr->store_->mkobj(in.name, flag);
    HG_Free_input(ta->handle, &in);
  } else {
    fprintf(stderr, "BuddyServer::mkobj_handler get input failed?\n");
    out.status = BB_FAILED;   /* unlikely */
  }

  /* send reply */
  hgret = HG_Respond(ta->handle, NULL, NULL, &out);
  if (hgret != HG_SUCCESS) {
    fprintf(stderr, "BuddyServer::mkobj_handler HG_Respond failed %d?\n",
                     hgret);
  }

  /* and we are done! */
  ta->srvr->freetargs_handle(ta);    /* HG_Destroy()'s handle too */
  return (hg_thread_ret_t)NULL;
}

/*
 * BuddyServer::append_handler: called from the thread pool
 */
HG_THREAD_RETURN_TYPE BuddyServer::append_handler(void *args) {
  struct thread_args *ta;
  struct hg_info *hgi;
  struct append_state *as;
  bbos_append_out_t out;
  hg_return_t hgret;

  /* extract handle and BuddyServer from mercury data */
  ta = reinterpret_cast<struct thread_args *>(args);
  hgi = HG_Get_info(ta->handle);
  if (!hgi) {
    out.size = BB_FAILED;
    fprintf(stderr, "BuddyServer::append_handler get info failed\n");
    goto failed;   /* very unlikely */
  }

  /* get state structure for bulk xfr */
  as = (struct append_state *)malloc(sizeof(*as));
  if (!as) {
    out.size = BB_FAILED;  /* XXX: no memory */
    fprintf(stderr, "BuddyServer::append_handler alloc state failed\n");
    goto failed;
  }
  as->targs = ta;
  as->local_bulkhand = NULL;
  as->buffer = NULL;

  if (HG_Get_input(ta->handle, &as->in) != HG_SUCCESS) {
    fprintf(stderr, "BuddyServer::append_handler get input failed?\n");
    out.size = BB_FAILED;
    free(as);
    goto failed;        /* unlikely */
  }
  /* from here failures need to go to state_failed (to free input, etc.) */

  as->size = HG_Bulk_get_size(as->in.bulk_handle);
  if (as->size < 1) {
    out.size = BB_FAILED;
    fprintf(stderr, "BuddyServer::append_handler bad input size!\n");
    goto state_failed;   /* shouldn't happen */
  }
  as->buffer = malloc(as->size);
  if (!as->buffer) {
    out.size = BB_FAILED;
    fprintf(stderr, "BuddyServer::append_handler local alloc failed (%d)!\n",
                     (int)as->size);
    goto state_failed;   /* shouldn't happen */
  }
  hgret = HG_Bulk_create(hgi->hg_class, 1, &(as->buffer), &as->size,
                         HG_BULK_WRITE_ONLY, &as->local_bulkhand);
  if (hgret != HG_SUCCESS) {
    out.size = BB_FAILED;
    fprintf(stderr, "BuddyServer::append_handler bulkcreate failed (%d)!\n",
                     hgret);
    goto state_failed;   /* shouldn't happen */
  }

  /* start bulk transfer */
  hgret = HG_Bulk_transfer(hgi->context, BuddyServer::append_finish, as,
                           HG_BULK_PULL, hgi->addr, as->in.bulk_handle, 0,
                           as->local_bulkhand, 0, as->size, HG_OP_ID_IGNORE);
  if (hgret != HG_SUCCESS) {
    out.size = BB_FAILED;
    fprintf(stderr, "BuddyServer::append_handler bulk xfer failed (%d)!\n",
                     hgret);
    goto state_failed;   /* shouldn't happen */
  }

  /* bulk started, we'll resume processing in append_finish() */
  return (hg_thread_ret_t)NULL;

state_failed:   /* failed after allocating state */

  if (as->local_bulkhand) HG_Bulk_free(as->local_bulkhand);
  if (as->buffer) free(as->buffer);
  HG_Free_input(ta->handle, &as->in);
  free(as);

  /*FALLTHROUGH*/
failed:         /* failed (no state allocated) */
  hgret = HG_Respond(ta->handle, NULL, NULL, &out);
  if (hgret != HG_SUCCESS) {
    fprintf(stderr, "BuddyServer::append_handler HG_Respond failed %d?\n",
                     hgret);
  }

  ta->srvr->freetargs_handle(ta);    /* HG_Destroy()'s handle too */
  return (hg_thread_ret_t) NULL;
}

/*
 * BuddyServer::append_finish: called at the end of an append op
 * (after the bulk transfer completes...)
 */
hg_return_t BuddyServer::append_finish(const struct hg_cb_info *info) {
  struct append_state *as = (struct append_state *)info->arg;
  bbos_append_out_t out;
  hg_return_t hgret;

  /* pass data to store */
  /* XXX: append bcopy()s the data, but it could take over the as->buffer */
  out.size = as->targs->srvr->store_->append(as->in.name, as->buffer,
                                             as->size);
  /* don't need a callback */
  hgret = HG_Respond(as->targs->handle, NULL, NULL, &out);
  if (hgret != HG_SUCCESS) {
    fprintf(stderr, "BuddyServer::append_finish: reply failed (%d)?\n", hgret);
    /* not much else we can do */
  }
  HG_Bulk_free(as->local_bulkhand);
  free(as->buffer);
  HG_Free_input(as->targs->handle, &as->in);
  as->targs->srvr->freetargs_handle(as->targs);
  free(as);
  return(HG_SUCCESS);
}

/*
 * BuddyServer::read_handler: called from the thread pool
 */
HG_THREAD_RETURN_TYPE BuddyServer::read_handler(void *args) {
  struct thread_args *ta;
  struct hg_info *hgi;
  struct read_state *rs;
  size_t got;    /* XXX: from BuddyStore, ssize_t? */
  hg_size_t hggot;
  bbos_read_out_t out;
  hg_return_t hgret;

  /* extract handle and BuddyServer from mercury data */
  ta = reinterpret_cast<struct thread_args *>(args);
  hgi = HG_Get_info(ta->handle);
  if (!hgi) {
    out.size = BB_FAILED;
    fprintf(stderr, "BuddyServer::read_handler get info failed\n");
    goto failed;   /* very unlikely */
  }

  /* get state structure for bulk xfr */
  rs = (struct read_state *)malloc(sizeof(*rs));
  if (!rs) {
    out.size = BB_FAILED;  /* XXX: no memory */
    fprintf(stderr, "BuddyServer::read_handler alloc state failed\n");
    goto failed;
  }
  rs->targs = ta;
  rs->local_bulkhand = NULL;
  rs->buffer = NULL;

  if (HG_Get_input(ta->handle, &rs->in) != HG_SUCCESS) {
    fprintf(stderr, "BuddyServer::read_handler get input failed?\n");
    out.size = BB_FAILED;
    free(rs);
    goto failed;        /* unlikely */
  }
  /* from here failures need to go to state_failed (to free input, etc.) */

  rs->size = HG_Bulk_get_size(rs->in.bulk_handle);
  /* XXX: we also have in.size */
  if (rs->size < 1 || rs->in.size < 1) {
    out.size = BB_FAILED;
    fprintf(stderr, "BuddyServer::read_handler bad input size!\n");
    goto state_failed;   /* shouldn't happen */
  }
  if (rs->in.size < rs->size)  /* use the smallest of the two */
    rs->size = rs->in.size;

  /*
   * XXX: store read() bcopy()s data into our buffer.  we could
   * bulk xfer directly from the store's buffer if store could
   * loan a static copy to us...
   */
  rs->buffer = malloc(rs->size);
  if (!rs->buffer) {
    out.size = BB_FAILED;
    fprintf(stderr, "BuddyServer::read_handler local alloc failed (%d)!\n",
                     (int)rs->size);
    goto state_failed;   /* shouldn't happen */
  }

  /* load the data into our local buffer from the store */
  got = ta->srvr->store_->read(rs->in.name, rs->buffer,
                               rs->in.offset, rs->size);
  if (got < 1 || got > rs->size) {
    /* XXX: got of 0 is ok */
    if (got == 0) {
      out.size = 0;     /* EOF */
    } else {
      out.size = BB_FAILED;
      fprintf(stderr, "BuddyServer::read_handler store read fail!\n");
    }
    goto state_failed;   /* XXX EOF isn't really a failure */
  }
  if (got < rs->size) {
    fprintf(stderr, "BuddyServer::read_handler: short read (fyi)\n"); /*XXX*/
    rs->size = got;       /* short read */
  }

  /* ok, no we can bulk send it back - create bulk handle */
  hggot = got;     /* 32->64 bit */
  hgret = HG_Bulk_create(hgi->hg_class, 1, &(rs->buffer), &hggot,
                         HG_BULK_READ_ONLY, &rs->local_bulkhand);
  if (hgret != HG_SUCCESS) {
    out.size = BB_FAILED;
    fprintf(stderr, "BuddyServer::read_handler bulkcreate failed (%d)!\n",
                     hgret);
    goto state_failed;   /* shouldn't happen */
  }

  /* start bulk transfer */
  hgret = HG_Bulk_transfer(hgi->context, BuddyServer::read_finish, rs,
                           HG_BULK_PUSH, hgi->addr, rs->in.bulk_handle, 0,
                           rs->local_bulkhand, 0, hggot, HG_OP_ID_IGNORE);
  if (hgret != HG_SUCCESS) {
    out.size = BB_FAILED;
    fprintf(stderr, "BuddyServer::read_handler bulk xfer failed (%d)!\n",
                     hgret);
    goto state_failed;   /* shouldn't happen */
  }

  /* bulk started, we'll resume processing in read_finish() */
  return (hg_thread_ret_t)NULL;

state_failed:   /* failed after allocating state */

  if (rs->local_bulkhand) HG_Bulk_free(rs->local_bulkhand);
  if (rs->buffer) free(rs->buffer);
  HG_Free_input(ta->handle, &rs->in);
  free(rs);

  /*FALLTHROUGH*/
failed:         /* failed (no state allocated) */
  hgret = HG_Respond(ta->handle, NULL, NULL, &out);
  if (hgret != HG_SUCCESS) {
    fprintf(stderr, "BuddyServer::read_handler HG_Respond failed %d?\n",
                     hgret);
  }

  ta->srvr->freetargs_handle(ta);    /* HG_Destroy()'s handle too */
  return (hg_thread_ret_t) NULL;
}

/*
 * BuddyServer::read_finish: called at the end of a read op
 * (after the bulk transfer completes...)
 */
hg_return_t BuddyServer::read_finish(const struct hg_cb_info *info) {
  struct read_state *rs = (struct read_state *)info->arg;
  bbos_read_out_t out;
  hg_return_t hgret;

  out.size = rs->size;
  /* don't need a callback */
  hgret = HG_Respond(rs->targs->handle, NULL, NULL, &out);
  if (hgret != HG_SUCCESS) {
    fprintf(stderr, "BuddyServer::read_finish: reply failed (%d)?\n", hgret);
    /* not much else we can do */
  }
  HG_Bulk_free(rs->local_bulkhand);
  free(rs->buffer);
  HG_Free_input(rs->targs->handle, &rs->in);
  rs->targs->srvr->freetargs_handle(rs->targs);
  free(rs);
  return(HG_SUCCESS);
}

/*
 * BuddyServer::get_size_handler: called from the thread pool
 */
HG_THREAD_RETURN_TYPE BuddyServer::get_size_handler(void *args) {
  struct thread_args *ta;
  bbos_get_size_in_t in;
  bbos_get_size_out_t out;
  hg_return_t hgret;

  /* extract handle and BuddyServer from mercury data */
  ta = reinterpret_cast<struct thread_args *>(args);

  /* get input data */
  if (HG_Get_input(ta->handle, &in) == HG_SUCCESS) {
    out.size = ta->srvr->store_->get_size(in.name);
    HG_Free_input(ta->handle, &in);
  } else {
    fprintf(stderr, "BuddyServer::get_size_handler get input failed?\n");
    out.size = BB_FAILED;   /* unlikely */
  }

  /* send reply */
  hgret = HG_Respond(ta->handle, NULL, NULL, &out);
  if (hgret != HG_SUCCESS) {
    fprintf(stderr, "BuddyServer::get_size_handler HG_Respond failed %d?\n",
                     hgret);
  }

  /* and we are done! */
  ta->srvr->freetargs_handle(ta);    /* HG_Destroy()'s handle too */
  return (hg_thread_ret_t)NULL;
}

}  // namespace bb
}  // namespace pdlfs

/*
 * globals
 */
char *argv0;
pdlfs::bb::BuddyServer *g_mysrvr = NULL;   /* so signal handler can access */

/*
 * signal_shutdown: signal handler that triggers a shutdown
 */
static void signal_shutdown(int) {
  if (g_mysrvr)
    g_mysrvr->shutdown();
}

/*
 * usage
 * idea for env var mappings:
 * "BB_Lustre_chunk_size",         -l <lustre sisze>
 * "BB_Mercury_transfer_size",     -m <merc xfer>
 * "BB_Binpacking_threshold",      -b <binpack threshhold>
 * "BB_Binpacking_policy",         -p <policy>
 * "BB_Object_dirty_threshold",    -t <dirty threshold>
 * "BB_Max_container_size",        -c <max container size>
 * "BB_Read_phase"                 -r <read phase>
 *
 * "BB_Output_dir"                 -d <output-dir>
 *
 * "BB_Server_port",               # XXX: use BB_Server (argv)
 * "BB_Server_IP_address",
 * "BB_Num_workers",               -w <number of workers>
 */
static void usage(const char *msg) {
    if (msg) fprintf(stderr, "%s: %s\n", argv0, msg);
    fprintf(stderr, "usage: %s [options] [server-url]\n", argv0);
    fprintf(stderr, "\noptions:\n");
    fprintf(stderr, "\t-w workers  number of worker threads (def=%d)\n",
            DEF_NUM_WORKERS);
    fprintf(stderr, "\n");
    fprintf(stderr, "if server URL not spec'd on the command line\n");
    fprintf(stderr, "it must be provided via env vars:\n");
    fprintf(stderr, "BB_Server or BB_Server_port/BB_Server_IP_address\n");
    fprintf(stderr, "(server port/addr vars are for backward compat only\n");
    exit(1);
}


/*
 * main: main program for server
 */
int main(int argc, char **argv) {
  struct sigaction sa;
  class pdlfs::bb::BuddyStore *store;
  class pdlfs::bb::BuddyServer *server;
  int ch, num_workers;
  char *srvr_url;
  char s_url[256];        /* XXX: for backward compat */
  const char *port;       /* XXX: for backward compat */
  argv0 = argv[0];

  num_workers = DEF_NUM_WORKERS;
  while ((ch = getopt(argc, argv, "w:")) != -1) {
    switch (ch) {
      case 'w':
        num_workers = atoi(optarg);
        if (num_workers < 1) usage("bad number of workers");
        break;
      default:
        usage(NULL);
    }
  }
  argc -= optind;
  argv += optind;

  if (argc > 1)
    usage(NULL);

  if (argc == 1) {
    srvr_url = argv[0];
  } else {
    srvr_url = getenv("BB_Server");
  }

  /* backward compat */
  if (srvr_url == NULL) {
    srvr_url = getenv("BB_Server_IP_address");
    port = getenv("BB_Server_port");
    if (!port)
      port = "19900";
    if (!srvr_url) usage("need a server url");
    snprintf(s_url, sizeof(s_url), "tcp://%s:%s", srvr_url, port);
    srvr_url = s_url;
  }

  printf("\n%s options:\n", argv0);
  printf("\tserver-url = %s\n", srvr_url);
  printf("\tnworkers   = %d\n", num_workers);
  printf("\n");

  /* map SIGINT to shutdown */
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = signal_shutdown;
  sigfillset(&sa.sa_mask);
  if (sigaction(SIGINT, &sa, NULL) < 0) {
    perror("sigaction");
    exit(1);
  }

  /* BuddyStore current reads config from environment */
  store = new pdlfs::bb::BuddyStore();
  store->print_config(stdout);

  /* allocate blank server */
  server = new pdlfs::bb::BuddyServer();

  /* run server */
  fprintf(stderr, "bbos_server: running server object\n");
  if (server->run(srvr_url, num_workers, store) < 0) {
    fprintf(stderr, "bbos_server: server run failed!\n");
    exit(1);
  }

  /* plug into global g_mysrvr to start catching signals */
  g_mysrvr = server;

  /* wait for it to exit */
  server->wait();

  printf("bbos_server: server finished!\n");
  printf("bbos_server: max-concurrent-reqs: %d\n",
         server->max_concurrent_reqs());
  delete server;

  printf("bbos_server: closing down store\n");
  delete store;
  printf("bbos_server: store closed.  exiting...\n");

  exit(0);
}
