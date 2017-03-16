/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_proc_string.h>

#ifndef _WIN32
#undef _GNU_SOURCE /* XXX: stop warning: mercury_thread.h redefs this */
#endif
#include <mercury_thread.h>

#include "bbos/bbos_api.h"
#include "bbos_rpc.h"

namespace pdlfs {
namespace bb {

/*
 * operation_details: tracking state for a single active RPC.
 * typically allocated on stack of caller.
 */
struct operation_details {
  pthread_mutex_t olock;      /* lock the data structure */
  pthread_cond_t ocond;       /* caller waits on this */
  int opdone;                 /* callback sets to 1 when done */
  hg_handle_t handle;         /* mercury handle for the RPC */
  /* both in and out are on caller's stack */
  void *in;                   /* input structure */
  void *out;                  /* output structure (must HG_Free_output) */
  hg_return_t outrv;          /* state of output */
};

/* this inits olock, ocond, and opdone */
#define OD_INIT { PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0 }

/*
 * BuddyClient: main object for client RPC stubs
 */
class BuddyClient {
 private:
  char *server_url_;          /* strdup'd copy of server URL */
  hg_addr_t server_addr_;     /* looked up server address */
  hg_class_t *hg_class_;      /* mercury class */
  hg_context_t *hg_context_;  /* mercury context (queues, etc.) */
  hg_thread_t prog_thread_;   /* our progress thread */
  int made_thread_;           /* set if we need to join thread @ shudown */
  int extmode_;               /* XXX: non-zero if EXTernal hg mode */

  /* ID#s for our 4 RPCs */
  hg_id_t mkobj_rpc_id_;
  hg_id_t append_rpc_id_;
  hg_id_t read_rpc_id_;
  hg_id_t get_size_rpc_id_;

  pthread_mutex_t lock_;      /* lock (for multithreading) */
  pthread_cond_t cond_;       /* for waiting for shutdown */
  int running_;               /* progress thread running */
  int shutdown_;              /* set to trigger shutdown */
  int refcnt_;                /* number of active requests */

  /* caller gains reference (fails if shutting down) */
  int gainref() {
    int rv;
    pthread_mutex_lock(&lock_);
    if (shutdown_) {
      rv = BB_FAILED;
    } else {
      refcnt_++;
      rv = BB_SUCCESS;
    }
    pthread_mutex_unlock(&lock_);
    return(rv);
  }

  /* caller drops reference when done */
  void dropref() {
    pthread_mutex_lock(&lock_);
    if (refcnt_ > 0) refcnt_--;

    /* XXX: EXT mode send signal when done (since no local prog. thread) */
    if (extmode_ && shutdown_ && refcnt_ == 0)
       pthread_cond_broadcast(&cond_);
    /* XXX: EXT mode */

    pthread_mutex_unlock(&lock_);
  }

  /* static class functions (mainly for C-style callbacks) */
  static void *progress_main(void *args);
  static hg_return_t lookup_cb(const struct hg_cb_info *cbi);

 public:
  BuddyClient() : server_url_(NULL), server_addr_(HG_ADDR_NULL),
                  hg_class_(NULL), hg_context_(NULL), made_thread_(0),
                  extmode_(0), running_(0), shutdown_(0), refcnt_(0) {
    if (pthread_mutex_init(&lock_, NULL) != 0 ||
        pthread_cond_init(&cond_, NULL) != 0) {
        fprintf(stderr, "BuddyClient: mutex/cond init failure?\n");
        abort();   /* this should never happen */
    }
  };
  ~BuddyClient();             /* dtor: triggers shutdown process */

  /*
   * XXX: enable EXTernal mode for working around mercury issues.
   * the idea is to run this after new'ing, but before calling attach.
   * we assume the mercury instance we are handed is already fully up
   * and running with its own progress thread (so we can add our callbacks
   * and lookup the server address)...
   */
  void set_extmode(hg_class_t *cls, hg_context_t *ctx) {
    if (running_ || shutdown_ || refcnt_ || !cls || !ctx) {
      fprintf(stderr, "BuddyClient::set_extmode usage error!\n");
      abort();
    }
    hg_class_ = cls;
    hg_context_ = ctx;
    extmode_ = 1;
  };
  /* XXX: EXT mode */

  /* attach a freshly allocated BuddyClient to a server */
  int attach(const char *local, const char *srvrurl);

  /* our operations */
  int mkobj(const char *name, bbos_mkobj_flag_t flag);
  ssize_t append(const char *name, void *buf, size_t len);
  ssize_t read(const char *name, void *buf, off_t offset, size_t len);
  off_t get_size(const char *name);
};

/*
 * C-style functions
 */

/*
 * operation_details_wait: wait for op to complete
 */
void operation_details_wait(struct operation_details *odp) {
  pthread_mutex_lock(&odp->olock);
  while (!odp->opdone) {
    if (pthread_cond_wait(&odp->ocond, &odp->olock) != 0) { /* WAITS HERE */
      fprintf(stderr, "BuddyClient:: op details cond wait failed?\n");
      abort();   /* should never happen */
    }
  }
  pthread_mutex_unlock(&odp->olock);
}

/* dummy RPC handler (we don't recv inbound RPCs) */
static hg_return_t bbos_rpc_handler(hg_handle_t handle) { return HG_SUCCESS; }

/*
 * bbos_opdet_cb: generic operation details callback function
 */
static hg_return_t bbos_opdet_cb(const struct hg_cb_info *cbi) {
  struct operation_details *odp;

  odp = reinterpret_cast<struct operation_details *>(cbi->arg);
  odp->outrv = HG_Get_output(odp->handle, odp->out);
  if (odp->outrv != HG_SUCCESS) {
    fprintf(stderr, "bbos_opdet_cb: get output failed?\n");
  }

  pthread_mutex_lock(&odp->olock);
  odp->opdone = 1;
  pthread_mutex_unlock(&odp->olock);
  pthread_cond_signal(&odp->ocond);
  return(HG_SUCCESS);
}

/*
 * BuddyClient::progress_main: main function for our progress look
 * (static class fn...)
 */
void *BuddyClient::progress_main(void *args) {
  class BuddyClient *mybc = reinterpret_cast<class BuddyClient *>(args);
  hg_return_t ret;
  unsigned int actual_count;

  pthread_mutex_lock(&mybc->lock_);
  mybc->running_ = 1;
  pthread_mutex_unlock(&mybc->lock_);

  while (!mybc->shutdown_ || mybc->refcnt_) {
    do {
      ret = HG_Trigger(mybc->hg_context_, 0, 1, &actual_count);
    } while (ret == HG_SUCCESS && actual_count && !mybc->shutdown_);

    if (!mybc->shutdown_) HG_Progress(mybc->hg_context_, 100);
  }
  pthread_mutex_lock(&mybc->lock_);
  mybc->running_ = 0;
  pthread_mutex_unlock(&mybc->lock_);
  pthread_cond_broadcast(&mybc->cond_);   /* let them know we are done */

  return (NULL);
}

/*
 * BuddyClient::lookup_cb: callback function when lookup completes.
 * this only happens at attach time...
 */
hg_return_t BuddyClient::lookup_cb(const struct hg_cb_info *cbi) {
  struct operation_details *op;
  class BuddyClient *bcptr;

  op = reinterpret_cast<struct operation_details *>(cbi->arg);
  bcptr = reinterpret_cast<class BuddyClient *>(op->in);

  if (cbi->ret != HG_SUCCESS) {
    fprintf(stderr, "lookup_cb(%s): failed (%d)\n",
            bcptr->server_url_, cbi->ret);
  } else {
    bcptr->server_addr_ = cbi->info.lookup.addr;
  }

  pthread_mutex_lock(&op->olock);
  op->opdone = 1;
  pthread_mutex_unlock(&op->olock);
  pthread_cond_broadcast(&op->ocond);  /* wake up waiter */

  return(HG_SUCCESS);
}


/*
 * BuddyClient::attach: attach a freshly allocated client to a server.
 * this is pulled out of the ctor so it can return error information.
 * assumes caller will delete this object on failure (so dtor will
 * handle freeing any resources allocated here).
 */
int BuddyClient::attach(const char *local, const char *srvrurl) {
  void *bcptr = reinterpret_cast<void *>(this);
  hg_return_t ret;
  struct operation_details od = OD_INIT;
  hg_op_id_t lookupop;

  server_url_ = strdup(srvrurl);
  if (!server_url_) {
    fprintf(stderr, "BuddyClient::attach: out of memory\n");
    return(BB_FAILED);
  }

  /* XXX: EXT mode uses previously provided hg_class/context */
  if (extmode_) goto extmode_skip;

  hg_class_ = HG_Init(local, HG_FALSE);
  if (!hg_class_) {
    fprintf(stderr, "BuddyClient::attach: HG_Init(%s) failed\n", local);
    return(BB_FAILED);
  }
  hg_context_ = HG_Context_create(hg_class_);
  if (!hg_context_) {
    fprintf(stderr, "BuddyClient::attach: HG_Context_create failed!\n");
    return(BB_FAILED);
  }

  if (hg_thread_create(&prog_thread_, BuddyClient::progress_main,
                       bcptr) < 0) {
    fprintf(stderr, "BuddyClient::attach: hg_thread_create failed!\n");
    return(BB_FAILED);
  }
  made_thread_ = 1;  /* so we join it at shutdown time */

extmode_skip:     /* XXX: EXT mode */

  /* XXX: register can't fail (but it must malloc?) */
  mkobj_rpc_id_ = MERCURY_REGISTER(hg_class_, "bbos_mkobj_rpc",
                  bbos_mkobj_in_t, bbos_mkobj_out_t, bbos_rpc_handler);
  append_rpc_id_ = MERCURY_REGISTER(hg_class_, "bbos_append_rpc",
                   bbos_append_in_t, bbos_append_out_t, bbos_rpc_handler);
  read_rpc_id_ = MERCURY_REGISTER(hg_class_, "bbos_read_rpc",
                 bbos_read_in_t, bbos_read_out_t, bbos_rpc_handler);
  get_size_rpc_id_ = MERCURY_REGISTER(hg_class_, "bbos_get_size_rpc",
                     bbos_get_size_in_t, bbos_get_size_out_t,
                     bbos_rpc_handler);

  /* now we need to lookup the server address */
  od.handle = NULL;
  od.in = bcptr;
  ret = HG_Addr_lookup(hg_context_, BuddyClient::lookup_cb, &od,
                       server_url_, &lookupop);
  if (ret != HG_SUCCESS) {
    fprintf(stderr, "BuddyClient::attach: lookup failed (%d)\n", ret);
    return(BB_FAILED);
  }

  /* wait for address to resolve */
  operation_details_wait(&od);

  if (server_addr_ == HG_ADDR_NULL) {
    fprintf(stderr, "BuddyClient::attach: lookup op failed!\n");
    return(BB_FAILED);
  }

  /* ready to roll! */
  return(BB_SUCCESS);
}

/*
 * ~BuddyClient::BuddyClient(): shutdown
 */
BuddyClient::~BuddyClient() {

  pthread_mutex_lock(&lock_);
  shutdown_ = 1;
  while (running_) {
    if (pthread_cond_wait(&cond_, &lock_) != 0) { /* WAITS HERE */
      fprintf(stderr, "~BuddyClient: cond wait failed?\n");
      abort();   /* should never happen */
    }
  }
  if (made_thread_)
    hg_thread_join(prog_thread_);

  if (extmode_) { /* XXX EXT mode, no prog thread, wait for refcnt_ == 0 */
    while (refcnt_ > 0) {
      if (pthread_cond_wait(&cond_, &lock_) != 0) { /* WAITS HERE */
        fprintf(stderr, "~BuddyClient: ext cond wait failed?\n");
        abort();   /* should never happen */
      }
    }
  }  /* XXX: EXT mode */

  pthread_mutex_unlock(&lock_);

  if (server_addr_) {
    HG_Addr_free(hg_class_, server_addr_);
    server_addr_ = HG_ADDR_NULL;
  }

  if (extmode_) {     /* XXX EXT mode: don't free external mercury structs */
    hg_context_ = NULL;
    hg_class_ = NULL;
  }

  /* ignore return values on these guys since we are going away */
  if (hg_context_) {
    HG_Context_destroy(hg_context_);
    hg_context_ = NULL;
  }
  if (hg_class_) {
    HG_Finalize(hg_class_);
    hg_class_ = NULL;
  }

  pthread_mutex_destroy(&lock_);
  pthread_cond_destroy(&cond_);
  if (server_url_) {
    free(server_url_);
    server_url_ = NULL;
  }
}

/*
 * BuddyClient::mkobj: make an object
 */
int BuddyClient::mkobj(const char *name, bbos_mkobj_flag_t flag) {
  struct operation_details od = OD_INIT;
  bbos_mkobj_in_t in;
  bbos_mkobj_out_t out;
  hg_return_t hgret;
  int ret = this->gainref();

  if (ret != BB_SUCCESS)
    return(ret);

  /* now holding reference */
  in.name = name;
  in.readopt = (flag == READ_OPTIMIZED) ? HG_TRUE : HG_FALSE;

  hgret = HG_Create(hg_context_, server_addr_, mkobj_rpc_id_, &od.handle);
  if (hgret != HG_SUCCESS) {
    fprintf(stderr, "BuddyClient::mkobj: HG_Create failed (%d)?\n", hgret);
    ret = BB_FAILED;
    goto done;
  }
  od.in = &in;
  od.out = &out;
  hgret = HG_Forward(od.handle, bbos_opdet_cb, &od, &in);

  if (hgret == HG_SUCCESS)
    operation_details_wait(&od);

  if (hgret == HG_SUCCESS && od.outrv == HG_SUCCESS) {
    ret = out.status;
    HG_Free_output(od.handle, &out);
  } else {
    ret = BB_FAILED;
  }

  HG_Destroy(od.handle);

done:
  this->dropref();
  return(ret);
}


/*
 * BuddyClient::append: append data to an object
 */
ssize_t BuddyClient::append(const char *name, void *buf, size_t len) {
  struct operation_details od = OD_INIT;
  bbos_append_in_t in;
  bbos_append_out_t out;
  hg_return_t hgret;
  hg_size_t hlen = len;   /* hg_size_t is 64 bits, size_t 32 */
  ssize_t ret = this->gainref();

  if (ret != BB_SUCCESS)
    return(ret);

  /* now holding reference */
  in.name = name;
  if ((hgret = HG_Bulk_create(hg_class_, 1, &buf, &hlen, HG_BULK_READ_ONLY,
                              &in.bulk_handle)) != HG_SUCCESS) {
    fprintf(stderr, "BuddyClient::append: bulk create failed (%d)\n", hgret);
    ret = BB_FAILED;
    goto done;
  }
  hgret = HG_Create(hg_context_, server_addr_, append_rpc_id_, &od.handle);
  if (hgret != HG_SUCCESS) {
    fprintf(stderr, "BuddyClient::append: HG_Create failed (%d)?\n", hgret);
    HG_Bulk_free(in.bulk_handle);
    ret = BB_FAILED;
    goto done;
  }

  od.in = &in;
  od.out = &out;
  hgret = HG_Forward(od.handle, bbos_opdet_cb, &od, &in);

  if (hgret == HG_SUCCESS)
    operation_details_wait(&od);

  if (hgret == HG_SUCCESS && od.outrv == HG_SUCCESS) {
    ret = out.size;
    HG_Free_output(od.handle, &out);
  } else {
    ret = BB_FAILED;
  }

  HG_Bulk_free(in.bulk_handle);
  HG_Destroy(od.handle);

done:
  this->dropref();
  return(ret);
}


/*
 * BuddyClient::read: read an object
 */
ssize_t BuddyClient::read(const char *name, void *buf,
                          off_t offset, size_t len) {
  struct operation_details od = OD_INIT;
  bbos_read_in_t in;
  bbos_read_out_t out;
  hg_return_t hgret;
  hg_size_t hlen = len;   /* hg_size_t is 64 bits, size_t 32 */
  ssize_t ret = this->gainref();

  if (ret != BB_SUCCESS)
    return(ret);

  /* now holding reference */
  in.name = name;
  in.offset = offset;
  in.size = len;
  if ((hgret = HG_Bulk_create(hg_class_, 1, &buf, &hlen, HG_BULK_WRITE_ONLY,
                              &in.bulk_handle)) != HG_SUCCESS) {
    fprintf(stderr, "BuddyClient::read: bulk create failed (%d)\n", hgret);
    ret = BB_FAILED;
    goto done;
  }

  hgret = HG_Create(hg_context_, server_addr_, read_rpc_id_, &od.handle);
  if (hgret != HG_SUCCESS) {
    fprintf(stderr, "BuddyClient::read: HG_Create failed (%d)?\n", hgret);
    HG_Bulk_free(in.bulk_handle);
    ret = BB_FAILED;
    goto done;
  }

  od.in = &in;
  od.out = &out;
  hgret = HG_Forward(od.handle, bbos_opdet_cb, &od, &in);

  if (hgret == HG_SUCCESS)
    operation_details_wait(&od);

  if (hgret == HG_SUCCESS && od.outrv == HG_SUCCESS) {
    ret = out.size;
    HG_Free_output(od.handle, &out);
  } else {
    ret = BB_FAILED;
  }

  HG_Bulk_free(in.bulk_handle);
  HG_Destroy(od.handle);

done:
  this->dropref();
  return(ret);
}

/*
 * BuddyClient::get_size: get size of an object
 */
off_t BuddyClient::get_size(const char *name) {
  struct operation_details od = OD_INIT;
  bbos_get_size_in_t in;
  bbos_get_size_out_t out;
  hg_return_t hgret;
  off_t ret = this->gainref();

  if (ret != BB_SUCCESS)
    return(ret);

  /* now holding reference */
  in.name = name;

  hgret = HG_Create(hg_context_, server_addr_, get_size_rpc_id_, &od.handle);
  if (hgret != HG_SUCCESS) {
    fprintf(stderr, "BuddyClient::get_size: HG_Create failed (%d)?\n", hgret);
    ret = BB_FAILED;
    goto done;
  }

  od.in = &in;
  od.out = &out;
  hgret = HG_Forward(od.handle, bbos_opdet_cb, &od, &in);

  if (hgret == HG_SUCCESS)
    operation_details_wait(&od);

  if (hgret == HG_SUCCESS && od.outrv == HG_SUCCESS) {
    ret = out.size;
    HG_Free_output(od.handle, &out);
  } else {
    ret = BB_FAILED;
  }

  HG_Destroy(od.handle);

done:
  this->dropref();
  return(ret);
}

}  // namespace bb
}  // namespace pdlfs

extern "C" {

/*
 * bbos_init: init function
 */
int bbos_init(const char *local, const char *server, bbos_handle_t *bbosp) {
  class pdlfs::bb::BuddyClient *bc = new pdlfs::bb::BuddyClient;
  int rv;

  rv = bc->attach(local, server);

  if (rv == BB_SUCCESS) {
    *bbosp = reinterpret_cast<bbos_handle_t>(bc);
  } else {
    delete bc;
  }

  return (rv);
}

/* XXX: EXT mode */
/*
 * bbos_init_ext: init function using external mercury mode
 */
int bbos_init_ext(const char *local, const char *server,
                  void *vhclass, void *vhctx, bbos_handle_t *bbosp) {
  class pdlfs::bb::BuddyClient *bc = new pdlfs::bb::BuddyClient;
  int rv;

  hg_class_t *hcls = reinterpret_cast<hg_class_t *>(vhclass);
  hg_context_t *hctx = reinterpret_cast<hg_context_t *>(vhctx);
  bc->set_extmode(hcls, hctx);

  rv = bc->attach(local, server);

  if (rv == BB_SUCCESS) {
    *bbosp = reinterpret_cast<bbos_handle_t>(bc);
  } else {
    delete bc;
  }

  return (rv);
}
/* XXX: EXT mode */

/*
 * bbos_finalize: shutdown the bbos (chains off to dtor to do the work)
 */
void bbos_finalize(bbos_handle_t bbos) {
  class pdlfs::bb::BuddyClient *bc =
      reinterpret_cast<pdlfs::bb::BuddyClient *>(bbos);
  delete bc;
}

/*
 * bbos_mkobj: make a bbos object
 */
int bbos_mkobj(bbos_handle_t bbos, const char *name,
               bbos_mkobj_flag_t flag) {
  class pdlfs::bb::BuddyClient *bc =
      reinterpret_cast<pdlfs::bb::BuddyClient *>(bbos);
  return (bc->mkobj(name, flag));
}

/*
 * bbos_append: append data to bbos object
 */
ssize_t bbos_append(bbos_handle_t bbos, const char *name, void *buf,
                    size_t len) {
  class pdlfs::bb::BuddyClient *bc =
      reinterpret_cast<pdlfs::bb::BuddyClient *>(bbos);
  return (bc->append(name, buf, len));
}

/*
 * bbos_read: read data from bbos object
 */
ssize_t bbos_read(bbos_handle_t bbos, const char *name, void *buf,
                  off_t offset, size_t len) {
  class pdlfs::bb::BuddyClient *bc =
      reinterpret_cast<pdlfs::bb::BuddyClient *>(bbos);
  return (bc->read(name, buf, offset, len));
}

/*
 * bbos_get_size: get current size of a bbos object
 */
off_t bbos_get_size(bbos_handle_t bbos, const char *name) {
  class pdlfs::bb::BuddyClient *bc =
      reinterpret_cast<pdlfs::bb::BuddyClient *>(bbos);
  return (bc->get_size(name));
}

}  // extern "C"
