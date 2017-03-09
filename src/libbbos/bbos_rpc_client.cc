/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h> /* note: assert only enabled for debug builds */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>

#include <mercury_bulk.h>
#include <mercury_proc_string.h>

/* XXX: Avoid compilation warning - mercury_thread.h redefines _GNU_SOURCE */
#ifndef _WIN32
#undef _GNU_SOURCE
#endif
#include <mercury_thread.h>

#include "bbos/bbos_api.h"
#include "bbos_rpc.h"

#define PSTRLEN 256 /* XXX was PATH_LEN */

namespace pdlfs {
namespace bb {

/*
 * BuddyClient: main object for client RPC stubs
 */
class BuddyClient {
 private:
  hg_thread_t progress_thread;
  int port;

 public:
  BuddyClient();
  ~BuddyClient();
  int mkobj(const char *name, bbos_mkobj_flag_t type = WRITE_OPTIMIZED);
  size_t append(const char *name, void *buf, size_t len);
  size_t read(const char *name, void *buf, off_t offset, size_t len);
  int get_size(const char *name);
};

#define NUM_CLIENT_CONFIGS 2  // keep in sync with configs enum and config_names
static char config_names[NUM_CLIENT_CONFIGS][PSTRLEN] = {
    "BB_Server_port", "BB_Server_IP_address"};
enum client_configs { PORT };

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

static char server_url[PSTRLEN];

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

static void *client_rpc_progress_fn(void *args) {
  hg_return_t ret;
  unsigned int actual_count;

  while (!hg_progress_shutdown_flag) {
    do {
      ret = HG_Trigger(hg_context, 0, 1, &actual_count);
    } while ((ret == HG_SUCCESS) && actual_count && !hg_progress_shutdown_flag);

    if (!hg_progress_shutdown_flag) HG_Progress(hg_context, 100);
  }
  return (NULL);
}

static hg_return_t bbos_rpc_handler(hg_handle_t handle) { return HG_SUCCESS; }

static hg_return_t lookup_address(const struct hg_cb_info *callback_info) {
  server_addr = (hg_addr_t)callback_info->info.lookup.addr;
  pthread_mutex_lock(&done_mutex);
  done++;
  pthread_cond_signal(&done_cond);
  pthread_mutex_unlock(&done_mutex);
  return HG_SUCCESS;
}

static hg_return_t bbos_mkobj_cb(const struct hg_cb_info *callback_info) {
  struct operation_details *op = (struct operation_details *)callback_info->arg;
  hg_return_t hg_ret;
  hg_ret = HG_Get_output(callback_info->info.forward.handle,
                         &(op->output.mkobj_out));
  if (hg_ret != HG_SUCCESS) abort();
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
  if (hg_ret != HG_SUCCESS) abort();
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
  hg_ret = HG_Get_output(callback_info->info.forward.handle,
                         &(op->output.append_out));
  if (hg_ret != HG_SUCCESS) abort();

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
  if (hg_ret != HG_SUCCESS) abort();
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
  hg_ret =
      HG_Get_output(callback_info->info.forward.handle, &(op->output.read_out));
  if (hg_ret != HG_SUCCESS) abort();
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
  if (hg_ret != HG_SUCCESS) abort();
  assert(op->handle);

  /* Fill input structure */
  op->input.read_in.name = (const char *)op->name;
  assert(hg_ret == HG_SUCCESS);

  /* Forward call to remote addr and get a new request */
  hg_ret = HG_Forward(op->handle, bbos_read_cb, op, &op->input.read_in);
  assert(hg_ret == HG_SUCCESS);
  return ((hg_return_t)NA_SUCCESS);
}

static hg_return_t bbos_get_size_cb(const struct hg_cb_info *callback_info) {
  struct operation_details *op = (struct operation_details *)callback_info->arg;
  hg_return_t hg_ret;
  hg_ret = HG_Get_output(callback_info->info.forward.handle,
                         &(op->output.get_size_out));
  if (hg_ret != HG_SUCCESS) abort();
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
  if (hg_ret != HG_SUCCESS) abort();
  assert(op->handle);

  /* Fill input structure */
  op->input.get_size_in.name = (const char *)op->name;

  /* Forward call to remote addr and get a new request */
  hg_ret = HG_Forward(op->handle, bbos_get_size_cb, op, &op->input.get_size_in);
  assert(hg_ret == HG_SUCCESS);
  return ((hg_return_t)NA_SUCCESS);
}

BuddyClient::BuddyClient() {
  /* Default configs */
  port = 19900;

  /* Now scan the environment vars to find out what to override. */
  // enum client_configs config_overrides = (pdlfs::bb::client_configs) 0;
  int config_overrides = 0;
  while (config_overrides < NUM_CLIENT_CONFIGS) {
    const char *v = getenv(config_names[config_overrides]);
    if (v != NULL) {
      switch (config_overrides) {
        case 0:
          port = atoi(v);
          break;
        case 1:
          snprintf(server_url, PSTRLEN, "tcp://%s:%d", v, port);
          break;
      }
    }
    config_overrides++;
  }

  na_return_t na_ret;
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
  mkobj_rpc_id = MERCURY_REGISTER(hg_class, "bbos_mkobj_rpc", bbos_mkobj_in_t,
                                  bbos_mkobj_out_t, bbos_rpc_handler);
  append_rpc_id =
      MERCURY_REGISTER(hg_class, "bbos_append_rpc", bbos_append_in_t,
                       bbos_append_out_t, bbos_rpc_handler);
  read_rpc_id = MERCURY_REGISTER(hg_class, "bbos_read_rpc", bbos_read_in_t,
                                 bbos_read_out_t, bbos_rpc_handler);
  get_size_rpc_id =
      MERCURY_REGISTER(hg_class, "bbos_get_size_rpc", bbos_get_size_in_t,
                       bbos_get_size_out_t, bbos_rpc_handler);

  /* lookup address only once */
  na_ret = (na_return_t)HG_Addr_lookup(hg_context, lookup_address, NULL,
                                       server_url, HG_OP_ID_IGNORE);
  if (na_ret != NA_SUCCESS) abort();

  pthread_mutex_lock(&done_mutex);
  while (done < 1) pthread_cond_wait(&done_cond, &done_mutex);
  done--;
  pthread_mutex_unlock(&done_mutex);
}

BuddyClient::~BuddyClient() {
  while (pending_replies.size() > 0) {
    sleep(1);
  }
  hg_return_t ret = HG_SUCCESS;
  HG_Context_destroy(hg_context); /* XXX return value */
  HG_Finalize(hg_class);          /* XXX return value */
  if (ret != HG_SUCCESS) abort();
}

int BuddyClient::mkobj(const char *name, bbos_mkobj_flag_t type) {
  struct operation_details *op = new operation_details;
  sprintf(op->name, "%s", name);
  int retval = -1;
  hg_return_t rpc_ret;
  op->action = MKOBJ;
  op->input.mkobj_in.name = (const char *)op->name;
  op->input.mkobj_in.type = (hg_bool_t)type;
  rpc_ret = issue_mkobj_rpc(op);
  if (rpc_ret != HG_SUCCESS) abort();
  pthread_mutex_lock(&done_mutex);
  while (done < 1) pthread_cond_wait(&done_cond, &done_mutex);
  done--;
  pthread_mutex_unlock(&done_mutex);
  retval = op->output.mkobj_out.status;
  free(op);
  return retval;
}

size_t BuddyClient::append(const char *name, void *buf, size_t len) {
  struct operation_details *op = new operation_details;
  sprintf(op->name, "%s", name);
  size_t retval = 0;
  hg_size_t hlen = len;
  hg_return_t hg_ret =
      HG_Bulk_create(hg_class, 1, &buf, &hlen, HG_BULK_READ_ONLY,
                     &(op->input.append_in.bulk_handle));
  if (hg_ret != HG_SUCCESS) abort();
  op->action = APPEND;
  hg_ret = issue_append_rpc(op);
  assert(hg_ret == HG_SUCCESS);
  pthread_mutex_lock(&done_mutex);
  while (done < 1) pthread_cond_wait(&done_cond, &done_mutex);
  done--;
  pthread_mutex_unlock(&done_mutex);
  retval = (size_t)op->output.append_out.size;
  HG_Bulk_free(op->input.append_in.bulk_handle);
  free(op);
  return retval;
}

size_t BuddyClient::read(const char *name, void *buf, off_t offset,
                         size_t len) {
  struct operation_details *op = new operation_details;
  sprintf(op->name, "%s", name);
  int retval;
  op->input.read_in.offset = (hg_size_t)offset;
  op->input.read_in.size = (hg_size_t)len;
  hg_return_t hg_ret =
      HG_Bulk_create(hg_class, 1, &buf, &(op->input.read_in.size),
                     HG_BULK_WRITE_ONLY, &(op->input.read_in.bulk_handle));
  if (hg_ret != HG_SUCCESS) abort();
  op->action = READ;
  hg_ret = issue_read_rpc(op);
  assert(hg_ret == HG_SUCCESS);
  pthread_mutex_lock(&done_mutex);
  while (done < 1) pthread_cond_wait(&done_cond, &done_mutex);
  done--;
  pthread_mutex_unlock(&done_mutex);
  retval = (size_t)op->output.read_out.size;
  HG_Bulk_free(op->input.read_in.bulk_handle);
  free(op);
  return retval;
}

int BuddyClient::get_size(const char *name) {
  struct operation_details *op = new operation_details;
  sprintf(op->name, "%s", name);
  int retval = -1;
  hg_return_t ret_rpc;
  op->action = GET_SIZE;
  ret_rpc = issue_get_size_rpc(op);
  if (ret_rpc != HG_SUCCESS) abort();
  pthread_mutex_lock(&done_mutex);
  while (done < 1) pthread_cond_wait(&done_cond, &done_mutex);
  done--;
  pthread_mutex_unlock(&done_mutex);
  retval = op->output.get_size_out.size;
  free(op);
  return retval;
}

}  // namespace bb
}  // namespace pdlfs

extern "C" {

/*
 * bbos_init: init function
 */
void *bbos_init(char *server) {
  class pdlfs::bb::BuddyClient *bc = new pdlfs::bb::BuddyClient;

  /* XXX: ctor error possible? */

  if (server) {
    fprintf(stderr, "bbos_init: server currently init'd through env XXX\n");
  }

  return (reinterpret_cast<void *>(bc));
}

/*
 * bbos_finalize: shutdown the bbos
 */
void bbos_finalize(void *bbos) {
  class pdlfs::bb::BuddyClient *bc =
      reinterpret_cast<pdlfs::bb::BuddyClient *>(bbos);
  delete bc;
}

/*
 * bbos_mkobj: make a bbos object
 */
int bbos_mkobj(void *bbos, const char *name, bbos_mkobj_flag_t flag) {
  class pdlfs::bb::BuddyClient *bc =
      reinterpret_cast<pdlfs::bb::BuddyClient *>(bbos);
  return (bc->mkobj(name, flag));
}

/*
 * bbos_append: append data to bbos object
 */
size_t bbos_append(void *bbos, const char *name, void *buf, size_t len) {
  class pdlfs::bb::BuddyClient *bc =
      reinterpret_cast<pdlfs::bb::BuddyClient *>(bbos);
  return (bc->append(name, buf, len));
}

/*
 * bbos_read: read data from bbos object
 */
size_t bbos_read(void *bbos, const char *name, void *buf, off_t offset,
                 size_t len) {
  class pdlfs::bb::BuddyClient *bc =
      reinterpret_cast<pdlfs::bb::BuddyClient *>(bbos);
  return (bc->read(name, buf, offset, len));
}

/*
 * bbos_get_size: get current size of a bbos object
 */
off_t bbos_get_size(void *bbos, const char *name) {
  class pdlfs::bb::BuddyClient *bc =
      reinterpret_cast<pdlfs::bb::BuddyClient *>(bbos);
  return (bc->get_size(name));
}

}  // extern "C"
