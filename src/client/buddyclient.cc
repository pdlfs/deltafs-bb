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
#include <map>
#include "buddyclient.h"

namespace pdlfs {
namespace bb {

static int done = 0;
static pthread_cond_t done_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t done_mutex = PTHREAD_MUTEX_INITIALIZER;
static na_class_t *network_class;
static hg_context_t *hg_context;
static hg_class_t *hg_class;
static hg_addr_t server_addr;
static hg_id_t mkobj_rpc_id;
static hg_id_t append_rpc_id;
static hg_id_t read_rpc_id;
static hg_id_t get_size_rpc_id;
static hg_bool_t hg_progress_shutdown_flag;
static std::map<std::string, uint64_t> pending_replies;

static char server_url[PATH_LEN];

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
  union {
    bbos_mkobj_in_t mkobj_in;
    bbos_append_in_t append_in;
    bbos_read_in_t read_in;
    bbos_get_size_in_t get_size_in;
  } input;
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

static hg_return_t lookup_address(const struct hg_cb_info *callback_info) {
  server_addr = (hg_addr_t) callback_info->info.lookup.addr;
  pthread_mutex_lock(&done_mutex);
  done++;
  pthread_cond_signal(&done_cond);
  pthread_mutex_unlock(&done_mutex);
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

static hg_return_t issue_mkobj_rpc(struct operation_details *op) {
  hg_return_t hg_ret;
  hg_ret = HG_Create(hg_context, server_addr, mkobj_rpc_id, &(op->handle));
  assert(hg_ret == HG_SUCCESS);
  assert(op->handle);

  /* Fill input structure */
  op->input.mkobj_in.name = (const char *)op->name;

  /* Forward call to remote addr and get a new request */
  hg_ret = HG_Forward(op->handle, bbos_mkobj_cb, op, &op->input.mkobj_in);
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

static hg_return_t issue_append_rpc(struct operation_details *op) {
  hg_return_t hg_ret;
  hg_ret = HG_Create(hg_context, server_addr, append_rpc_id, &(op->handle));
  assert(hg_ret == HG_SUCCESS);
  assert(op->handle);

  /* Fill input structure */
  op->input.append_in.name = (const char *)op->name;

  /* Forward call to remote addr and get a new request */
  hg_ret = HG_Forward(op->handle, bbos_append_cb, op, &op->input.append_in);
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

static hg_return_t issue_read_rpc(struct operation_details *op) {
  hg_return_t hg_ret;
  hg_ret = HG_Create(hg_context, server_addr, read_rpc_id, &(op->handle));
  assert(hg_ret == HG_SUCCESS);
  assert(op->handle);

  /* Fill input structure */
  op->input.read_in.name = (const char *) op->name;
  assert(hg_ret == HG_SUCCESS);

  /* Forward call to remote addr and get a new request */
  hg_ret = HG_Forward(op->handle, bbos_read_cb, op, &op->input.read_in);
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

static hg_return_t issue_get_size_rpc(struct operation_details *op) {
  hg_return_t hg_ret;
  hg_ret = HG_Create(hg_context, server_addr, get_size_rpc_id, &(op->handle));
  assert(hg_ret == HG_SUCCESS);
  assert(op->handle);

  /* Fill input structure */
  op->input.get_size_in.name = (const char *)op->name;

  /* Forward call to remote addr and get a new request */
  hg_ret = HG_Forward(op->handle, bbos_get_size_cb, op, &op->input.get_size_in);
  assert(hg_ret == HG_SUCCESS);
  return ((hg_return_t)NA_SUCCESS);
}

class BuddyClient
{
  private:
    hg_thread_t progress_thread;
    int port;

  public:
    BuddyClient(const char *config_file) {
      assert(config_file != NULL);
      na_return_t na_ret;
      std::ifstream configuration(config_file);
      if(!configuration) {
        exit(-BB_CONFIG_ERROR);
      }
      int i = 0;
      const char *out_manifest;
      char knob[PATH_LEN];
      while(configuration >> knob) {
        switch (i) {
          case 0: port = atoi(knob);
                  break;
          case 1: snprintf(server_url, PATH_LEN, "tcp://%s:%d", knob, port);
                  break;
        }
        i++;
      }
      network_class = NULL;
      hg_context = NULL;
      hg_class = NULL;

      /* start mercury and register RPC */
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

      /* lookup address only once */
      na_ret = (na_return_t)HG_Addr_lookup(hg_context, lookup_address, NULL, server_url, HG_OP_ID_IGNORE);
      assert(na_ret == NA_SUCCESS);

      pthread_mutex_lock(&done_mutex);
      while(done < 1)
        pthread_cond_wait(&done_cond, &done_mutex);
      done--;
      pthread_mutex_unlock(&done_mutex);
    }

    ~BuddyClient() {
      while (pending_replies.size() > 0) {
        sleep(1);
      }
      hg_return_t ret = HG_SUCCESS;
      ret = HG_Hl_finalize();
      assert(ret == HG_SUCCESS);
    }

    int mkobj(const char *name, mkobj_flag_t type=WRITE_OPTIMIZED) {
      struct operation_details *op = new operation_details;
      sprintf(op->name, "%s", name);
      int retval = -1;
      hg_return_t rpc_ret;
      op->action = MKOBJ;
      op->input.mkobj_in.name = (const char *) op->name;
      op->input.mkobj_in.type = (hg_bool_t) type;
      rpc_ret = issue_mkobj_rpc(op);
      assert(rpc_ret == HG_SUCCESS);
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
      hg_return_t hg_ret = HG_Bulk_create(hg_class, 1, &buf, &(len),
                            HG_BULK_READ_ONLY,
                            &(op->input.append_in.bulk_handle));
      assert(hg_ret == HG_SUCCESS);
      op->action = APPEND;
      hg_ret = issue_append_rpc(op);
      assert(hg_ret == HG_SUCCESS);
      pthread_mutex_lock(&done_mutex);
      while(done < 1)
        pthread_cond_wait(&done_cond, &done_mutex);
      done--;
      pthread_mutex_unlock(&done_mutex);
      retval = (size_t) op->output.append_out.size;
      HG_Bulk_free(op->input.append_in.bulk_handle);
      free(op);
      return retval;
    }

    size_t read(const char *name, void *buf, off_t offset, size_t len) {
      struct operation_details *op = new operation_details;
      sprintf(op->name, "%s", name);
      int retval;
      op->input.read_in.offset = (hg_size_t) offset;
      op->input.read_in.size = (hg_size_t) len;
      hg_return_t hg_ret = HG_Bulk_create(hg_class, 1, &buf,
                            &(op->input.read_in.size),
                            HG_BULK_READWRITE,
                            &(op->input.read_in.bulk_handle));
      assert(hg_ret == HG_SUCCESS);
      op->action = READ;
      hg_ret = issue_read_rpc(op);
      assert(hg_ret == HG_SUCCESS);
      pthread_mutex_lock(&done_mutex);
      while(done < 1)
        pthread_cond_wait(&done_cond, &done_mutex);
      done--;
      pthread_mutex_unlock(&done_mutex);
      retval = (size_t) op->output.read_out.size;
      HG_Bulk_free(op->input.read_in.bulk_handle);
      free(op);
      return retval;
    }

    int get_size(const char *name) {
      struct operation_details *op = new operation_details;
      sprintf(op->name, "%s", name);
      int retval = -1;
      hg_return_t ret_rpc;
      op->action = GET_SIZE;
      ret_rpc = issue_get_size_rpc(op);
      assert(ret_rpc == HG_SUCCESS);
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
