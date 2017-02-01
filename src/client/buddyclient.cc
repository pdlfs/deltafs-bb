/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <string.h>
#include <sstream>
#include <fstream>
#include <unistd.h>
#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_macros.h>
#include <mercury_request.h>
#include <mercury_hl.h>
#include <mercury_hl_macros.h>
#include <mercury_proc_string.h>
#include <mercury_config.h>
#include "buddyclient.h"

namespace pdlfs {
namespace bb {

static int done = 0;
static pthread_cond_t done_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t done_mutex = PTHREAD_MUTEX_INITIALIZER;
static void run_my_rpc(int value);
static na_class_t *network_class;
static hg_context_t *hg_context;
static hg_class_t *hg_class;
static hg_addr_t server_addr;
static hg_id_t mkobj_rpc_id;
static hg_id_t append_rpc_id;
static hg_id_t read_rpc_id;
static hg_id_t get_size_rpc_id;
static hg_bool_t hg_progress_shutdown_flag;

struct operation_details {
  char name[256];
  hg_handle_t handle;
  enum ACTION action;
  union {
    bbos_mkobj_out_t mkobj_out;
    bbos_append_out_t append_out;
    bbos_read_out_t read_out;
    bbos_get_size_out_t get_size_out;
  } output;
  void *buf;
  size_t len;
  off_t offset;
};

static void* client_rpc_progress_fn(void *args) {
  hg_return_t ret;
  unsigned int actual_count;

  while(!hg_progress_shutdown_flag)
  {
      do {
          ret = HG_Trigger(hg_context, 0, 1, &actual_count);
      } while((ret == HG_SUCCESS) && actual_count && !hg_progress_shutdown_flag);

      if(!hg_progress_shutdown_flag)
          HG_Progress(hg_context, 100);
  }
}

static hg_return_t bbos_rpc_handler(hg_handle_t handle) {
  return HG_SUCCESS;
}

static hg_return_t bbos_mkobj_cb(const struct hg_cb_info *callback_info) {
  struct operation_details *op = (struct operation_details *)callback_info->arg;
  hg_return_t hg_ret;
  hg_ret = HG_Get_output(callback_info->info.forward.handle, &(op->output.mkobj_out));
  assert(hg_ret == HG_SUCCESS);
  pthread_mutex_lock(&done_mutex);
  done++;
  pthread_cond_signal(&done_cond);
  pthread_mutex_unlock(&done_mutex);
  /* Complete */
  hg_ret = HG_Destroy(callback_info->info.forward.handle);
  assert(hg_ret == HG_SUCCESS);
  return HG_SUCCESS;
}

static hg_return_t issue_mkobj_rpc(const struct hg_cb_info *callback_info) {
  bbos_mkobj_in_t in;
  hg_return_t hg_ret;
  struct operation_details *op = (struct operation_details *)callback_info->arg;
  server_addr = (hg_addr_t) callback_info->info.lookup.addr;
  hg_ret = HG_Create(hg_context, server_addr, mkobj_rpc_id, &(op->handle));
  assert(hg_ret == HG_SUCCESS);
  assert(op->handle);
  /* Fill input structure */
  in.name = (const char *)op->name;
  /* Forward call to remote addr and get a new request */
  hg_ret = HG_Forward(op->handle, bbos_mkobj_cb, op, &in);
  assert(hg_ret == HG_SUCCESS);
  return ((hg_return_t)NA_SUCCESS);
}

static hg_return_t bbos_append_cb(const struct hg_cb_info *callback_info) {
  struct operation_details *op = (struct operation_details *)callback_info->arg;
  hg_return_t hg_ret;
  hg_ret = HG_Get_output(callback_info->info.forward.handle, &(op->output.append_out));
  assert(hg_ret == HG_SUCCESS);
  pthread_mutex_lock(&done_mutex);
  done++;
  pthread_cond_signal(&done_cond);
  pthread_mutex_unlock(&done_mutex);
  /* Complete */
  hg_ret = HG_Destroy(callback_info->info.forward.handle);
  assert(hg_ret == HG_SUCCESS);
  return HG_SUCCESS;
}

static hg_return_t issue_append_rpc(const struct hg_cb_info *callback_info) {
  bbos_append_in_t in;
  hg_return_t hg_ret;
  struct operation_details *op = (struct operation_details *)callback_info->arg;
  server_addr = (hg_addr_t) callback_info->info.lookup.addr;
  hg_ret = HG_Create(hg_context, server_addr, append_rpc_id, &(op->handle));
  assert(hg_ret == HG_SUCCESS);
  assert(op->handle);

  /* Fill input structure */
  in.name = (const char *)op->name;
  hg_ret = HG_Bulk_create(hg_class, 1, &(op->buf), &(op->len), HG_BULK_READ_ONLY, &(in.bulk_handle));
  assert(hg_ret == HG_SUCCESS);

  /* Forward call to remote addr and get a new request */
  hg_ret = HG_Forward(op->handle, bbos_append_cb, op, &in);
  assert(hg_ret == HG_SUCCESS);
  return ((hg_return_t)NA_SUCCESS);
}

static hg_return_t bbos_read_cb(const struct hg_cb_info *callback_info) {
  struct operation_details *op = (struct operation_details *)callback_info->arg;
  hg_return_t hg_ret;
  hg_ret = HG_Get_output(callback_info->info.forward.handle, &(op->output.read_out));
  assert(hg_ret == HG_SUCCESS);
  pthread_mutex_lock(&done_mutex);
  done++;
  pthread_cond_signal(&done_cond);
  pthread_mutex_unlock(&done_mutex);
  /* Complete */
  hg_ret = HG_Destroy(callback_info->info.forward.handle);
  assert(hg_ret == HG_SUCCESS);
  return HG_SUCCESS;
}

static hg_return_t issue_read_rpc(const struct hg_cb_info *callback_info) {
  bbos_read_in_t in;
  hg_return_t hg_ret;
  struct operation_details *op = (struct operation_details *)callback_info->arg;
  server_addr = (hg_addr_t) callback_info->info.lookup.addr;
  hg_ret = HG_Create(hg_context, server_addr, read_rpc_id, &(op->handle));
  assert(hg_ret == HG_SUCCESS);
  assert(op->handle);

  /* Fill input structure */
  in.name = (const char *) op->name;
  in.offset = (off_t) op->offset;
  in.size = (size_t) op->len;

  op->buf = (void *) calloc (1, op->len);

  hg_ret = HG_Bulk_create(hg_class, 1, &(op->buf), &(op->len),
            HG_BULK_READWRITE, &(in.bulk_handle));
  assert(hg_ret == HG_SUCCESS);

  /* Forward call to remote addr and get a new request */
  hg_ret = HG_Forward(op->handle, bbos_read_cb, op, &in);
  assert(hg_ret == HG_SUCCESS);
  return ((hg_return_t)NA_SUCCESS);
}

static hg_return_t bbos_get_size_cb(const struct hg_cb_info *callback_info) {
  struct operation_details *op = (struct operation_details *)callback_info->arg;
  hg_return_t hg_ret;
  hg_ret = HG_Get_output(callback_info->info.forward.handle, &(op->output.get_size_out));
  assert(hg_ret == HG_SUCCESS);
  pthread_mutex_lock(&done_mutex);
  done++;
  pthread_cond_signal(&done_cond);
  pthread_mutex_unlock(&done_mutex);
  /* Complete */
  hg_ret = HG_Destroy(callback_info->info.forward.handle);
  assert(hg_ret == HG_SUCCESS);
  return HG_SUCCESS;
}

static hg_return_t issue_get_size_rpc(const struct hg_cb_info *callback_info) {
  bbos_mkobj_in_t in;
  hg_return_t hg_ret;
  struct operation_details *op = (struct operation_details *)callback_info->arg;
  server_addr = (hg_addr_t) callback_info->info.lookup.addr;
  hg_ret = HG_Create(hg_context, server_addr, get_size_rpc_id, &(op->handle));
  assert(hg_ret == HG_SUCCESS);
  assert(op->handle);
  /* Fill input structure */
  in.name = (const char *)op->name;
  /* Forward call to remote addr and get a new request */
  hg_ret = HG_Forward(op->handle, bbos_get_size_cb, op, &in);
  assert(hg_ret == HG_SUCCESS);
  return ((hg_return_t)NA_SUCCESS);
}

static void run_my_rpc(struct operation_details *op)
{
    na_return_t ret;
    switch (op->action) {
      case MKOBJ: ret = (na_return_t)HG_Addr_lookup(hg_context, issue_mkobj_rpc, op, "tcp://localhost:1240", HG_OP_ID_IGNORE);
                  break;
      case APPEND: ret = (na_return_t)HG_Addr_lookup(hg_context, issue_append_rpc, op, "tcp://localhost:1240", HG_OP_ID_IGNORE);
                   break;
      case READ: ret = (na_return_t)HG_Addr_lookup(hg_context, issue_read_rpc, op, "tcp://localhost:1240", HG_OP_ID_IGNORE);
                 break;
      case GET_SIZE: ret = (na_return_t)HG_Addr_lookup(hg_context, issue_get_size_rpc, op, "tcp://localhost:1240", HG_OP_ID_IGNORE);
                     break;
    }
    assert(ret == NA_SUCCESS);
    (void)ret;
    return;
}

class BuddyClient
{
  private:
    int port;
    hg_thread_t progress_thread;

  public:
    BuddyClient(int port=1234) {
      network_class = NULL;
      hg_context = NULL;
      hg_class = NULL;

      this->port = port;
      /* start mercury and register RPC */
      int ret;

      network_class = NA_Initialize("tcp", NA_FALSE);
      assert(network_class);

      hg_class = HG_Init_na(network_class);
      assert(hg_class);

      hg_context = HG_Context_create(hg_class);
      assert(hg_context);

      hg_progress_shutdown_flag = false;
      hg_thread_create(&progress_thread, client_rpc_progress_fn, hg_context);
      mkobj_rpc_id = MERCURY_REGISTER(hg_class, "bbos_mkobj_rpc", bbos_mkobj_in_t, bbos_mkobj_out_t, bbos_rpc_handler);
      append_rpc_id = MERCURY_REGISTER(hg_class, "bbos_append_rpc", bbos_append_in_t, bbos_append_out_t, bbos_rpc_handler);
      read_rpc_id = MERCURY_REGISTER(hg_class, "bbos_read_rpc", bbos_read_in_t, bbos_read_out_t, bbos_rpc_handler);
      get_size_rpc_id = MERCURY_REGISTER(hg_class, "bbos_get_size_rpc", bbos_get_size_in_t, bbos_get_size_out_t, bbos_rpc_handler);
    }

    ~BuddyClient() {
      hg_return_t ret = HG_SUCCESS;
      ret = HG_Hl_finalize();
      assert(ret == HG_SUCCESS);
    }

    int mkobj(const char *name) {
      struct operation_details *op = new operation_details;
      sprintf(op->name, "%s", name);
      int retval = -1;
      op->buf = NULL;
      op->len = 0;
      op->action = MKOBJ;
      run_my_rpc(op);
      pthread_mutex_lock(&done_mutex);
      while(done < 1)
        pthread_cond_wait(&done_cond, &done_mutex);
      done--;
      pthread_mutex_unlock(&done_mutex);
      retval = op->output.mkobj_out.status;
      free(op);
      return retval;
    }

    size_t append(const char *name, void *buf, size_t len) {
      struct operation_details *op = new operation_details;
      sprintf(op->name, "%s", name);
      size_t retval = 0;
      op->buf = (void *) malloc (len);
      memcpy(op->buf, buf, len);
      op->len = len;
      assert(op->buf);
      op->action = APPEND;
      run_my_rpc(op);
      pthread_mutex_lock(&done_mutex);
      while(done < 1)
        pthread_cond_wait(&done_cond, &done_mutex);
      done--;
      pthread_mutex_unlock(&done_mutex);
      retval = (size_t) op->output.append_out.size;
      free(op->buf);
      free(op);
      return retval;
    }

    size_t read(const char *name, void *buf, off_t offset, size_t len) {
      struct operation_details *op = new operation_details;
      sprintf(op->name, "%s", name);
      int retval;
      op->len = len;
      op->offset = offset;
      op->action = READ;
      run_my_rpc(op);
      pthread_mutex_lock(&done_mutex);
      while(done < 1)
        pthread_cond_wait(&done_cond, &done_mutex);
      done--;
      pthread_mutex_unlock(&done_mutex);
      retval = (size_t) op->output.read_out.size;
      memcpy(buf, op->buf, retval);
      free(op->buf);
      free(op);
      return retval;
    }

    int get_size(const char *name) {
      struct operation_details *op = new operation_details;
      sprintf(op->name, "%s", name);
      int retval = -1;
      op->buf = NULL;
      op->len = 0;
      op->action = GET_SIZE;
      run_my_rpc(op);
      pthread_mutex_lock(&done_mutex);
      while(done < 1)
        pthread_cond_wait(&done_cond, &done_mutex);
      done--;
      pthread_mutex_unlock(&done_mutex);
      retval = op->output.get_size_out.size;
      free(op);
      return retval;
    }
};

} // namespace bb
} // namespace pdlfs
