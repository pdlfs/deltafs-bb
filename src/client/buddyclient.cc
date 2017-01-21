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
#include "buddyclient.h"
//#include "src/server/interface.h"

namespace pdlfs {
namespace bb {

static int done = 0;
static pthread_cond_t done_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t done_mutex = PTHREAD_MUTEX_INITIALIZER;
static hg_id_t my_rpc_id;

static hg_return_t my_rpc_cb(const struct hg_cb_info *info);
static void run_my_rpc(int value);
static hg_return_t lookup_cb(const struct hg_cb_info *callback_info);

static int rpc_retval = 0;
static void *rpc_retbuf = NULL;

/* struct used to carry state of overall operation across callbacks */
struct my_rpc_client_state
{
    hg_size_t size;
    hg_size_t output_size;
    int action;
    char name[PATH_LEN];
    void *input;
    void *output;
    hg_bulk_t input_bulk_handle;
    hg_bulk_t output_bulk_handle;
    hg_handle_t handle;
};

struct operation_details {
  char name[256];
  enum ACTION action;
  void *buf;
  size_t len;
  off_t offset;
};

static void run_my_rpc(struct operation_details *op)
{
    /* address lookup.  This is an async operation as well so we continue in
     * a callback from here.
     */
    hg_engine_addr_lookup("tcp://localhost:1234", lookup_cb, op);
    return;
}

static hg_return_t lookup_cb(const struct hg_cb_info *callback_info)
{
    na_addr_t svr_addr = callback_info->info.lookup.addr;
    my_rpc_in_t in;
    struct hg_info *hgi;
    int ret;
    size_t output_size;
    struct my_rpc_client_state *my_rpc_state_p;

    assert(callback_info->ret == 0);

    /* set up state structure */
    my_rpc_state_p = (pdlfs::bb::my_rpc_client_state*) malloc(sizeof(*my_rpc_state_p));
    struct operation_details *op = (struct operation_details *) callback_info->arg;
    my_rpc_state_p->action = op->action;
    snprintf(my_rpc_state_p->name, PATH_LEN, "%s", op->name);
    my_rpc_state_p->output_size = op->len;
    if(my_rpc_state_p->output_size == 0) {
      my_rpc_state_p->output_size = 1;
    }
    switch (my_rpc_state_p->action) {

      case MKOBJ: my_rpc_state_p->size = PATH_LEN;
                  /* This includes allocating a src buffer for bulk transfer */
                  my_rpc_state_p->input = calloc(1, my_rpc_state_p->size);
                  my_rpc_state_p->output = calloc(1, my_rpc_state_p->output_size);
                  assert(my_rpc_state_p->input != NULL);
                  assert(my_rpc_state_p->output != NULL);
                  snprintf((char*)my_rpc_state_p->input, PATH_LEN, "%s", op->name);
                  break;

      case APPEND:  my_rpc_state_p->size = PATH_LEN + op->len + sizeof(size_t);
                    /* This includes allocating a src buffer for bulk transfer */
                    my_rpc_state_p->input = calloc(1, my_rpc_state_p->size);
                    my_rpc_state_p->output = calloc(1, my_rpc_state_p->output_size);
                    assert(my_rpc_state_p->input != NULL);
                    assert(my_rpc_state_p->output != NULL);
                    snprintf((char*)my_rpc_state_p->input, PATH_LEN, "%s", op->name);
                    memcpy((void *)((char*)my_rpc_state_p->input + PATH_LEN), &op->len, sizeof(size_t));
                    memcpy((void *)((char *)my_rpc_state_p->input + PATH_LEN + sizeof(size_t)), op->buf, op->len);
                    free(op->buf);
                    break;

      case READ: my_rpc_state_p->size = PATH_LEN + sizeof(size_t) + sizeof(off_t);
                 my_rpc_state_p->output_size += PATH_LEN + sizeof(size_t) + sizeof(off_t);
                 /* This includes allocating a src buffer for bulk transfer */
                 my_rpc_state_p->input = calloc(1, my_rpc_state_p->size);
                 my_rpc_state_p->output = calloc(1, my_rpc_state_p->output_size);
                 assert(my_rpc_state_p->input != NULL);
                 assert(my_rpc_state_p->output != NULL);
                 snprintf((char*)my_rpc_state_p->input, PATH_LEN, "%s", op->name);
                 memcpy((void *)((char*)my_rpc_state_p->input + PATH_LEN), &op->len, sizeof(size_t));
                 memcpy((void *)((char*)my_rpc_state_p->input + PATH_LEN + sizeof(size_t)), &op->offset, sizeof(off_t));
                 free(op->buf);
                 break;
    }

    free(op);

    /* create create handle to represent this rpc operation */
    hg_engine_create_handle(svr_addr, my_rpc_id, &my_rpc_state_p->handle);

    /* register buffer for rdma/bulk access by server */
    hgi = HG_Get_info(my_rpc_state_p->handle);
    assert(hgi);
    ret = HG_Bulk_create(hgi->hg_class, 1, &(my_rpc_state_p->input), &(my_rpc_state_p->size),
        HG_BULK_READ_ONLY, &(in.input_bulk_handle));
    my_rpc_state_p->input_bulk_handle = in.input_bulk_handle;
    assert(ret == 0);

    ret = HG_Bulk_create(hgi->hg_class, 1, &(my_rpc_state_p->output), &(my_rpc_state_p->output_size),
        HG_BULK_READWRITE, &(in.output_bulk_handle));
    assert(ret == 0);

    /* Send rpc. Note that we are also transmitting the bulk handle in the
     * input struct.  It was set above.
     */
    in.action = my_rpc_state_p->action;
    ret = HG_Forward(my_rpc_state_p->handle, my_rpc_cb, my_rpc_state_p, &in);
    assert(ret == 0);
    (void)ret;

    return((hg_return_t)NA_SUCCESS);
}

/* callback triggered upon receipt of rpc response */
static hg_return_t my_rpc_cb(const struct hg_cb_info *info)
{
    my_rpc_out_t out;
    int ret;
    size_t out_length;
    struct my_rpc_client_state *my_rpc_state_p = (pdlfs::bb::my_rpc_client_state*)info->arg;

    assert(info->ret == HG_SUCCESS);

    /* decode response */
    ret = HG_Get_output(info->info.forward.handle, &out);
    assert(ret == 0);
    (void)ret;

    rpc_retval = out.ret;

    switch (my_rpc_state_p->action) {
      case 2: memcpy(rpc_retbuf, my_rpc_state_p->output, rpc_retval);
              break;
    }

    /* clean up resources consumed by this rpc */
    HG_Bulk_free(my_rpc_state_p->input_bulk_handle);
    HG_Bulk_free(my_rpc_state_p->output_bulk_handle);
    HG_Free_output(info->info.forward.handle, &out);
    HG_Destroy(info->info.forward.handle);
    free(my_rpc_state_p->input);
    free(my_rpc_state_p->output);
    free(my_rpc_state_p);

    /* signal to main() that we are done */
    pthread_mutex_lock(&done_mutex);
    done++;
    pthread_cond_signal(&done_cond);
    pthread_mutex_unlock(&done_mutex);

    return(HG_SUCCESS);
}

class BuddyClient//: public Client
{
  private:

  public:
    BuddyClient() {
      int i;

      /* start mercury and register RPC */
      hg_engine_init(NA_FALSE, "tcp");
      hg_class_t* hg_class;
      hg_class = hg_engine_get_class();

      my_rpc_id = MERCURY_REGISTER(hg_class, "my_rpc", my_rpc_in_t, my_rpc_out_t, my_rpc_handler);
    }

    int mkobj(char *name) {
      struct operation_details *op = new operation_details;
      sprintf(op->name, "%s", name);
      int retval;
      op->buf = NULL;
      op->len = 0;
      op->action = MKOBJ;
      run_my_rpc(op);
      pthread_mutex_lock(&done_mutex);
      while(done < 1)
        pthread_cond_wait(&done_cond, &done_mutex);
      done--;
      pthread_mutex_unlock(&done_mutex);
      retval = (int) rpc_retval;
      rpc_retval = 0;
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
      retval = (size_t) rpc_retval;
      rpc_retval = 0;
      return retval;
    }

    size_t read(const char *name, void *buf, off_t offset, size_t len) {
      struct operation_details *op = new operation_details;
      sprintf(op->name, "%s", name);
      int retval;
      rpc_retbuf = buf;
      op->len = len;
      op->offset = offset;
      op->action = READ;
      op->buf = NULL;
      run_my_rpc(op);
      pthread_mutex_lock(&done_mutex);
      while(done < 1)
        pthread_cond_wait(&done_cond, &done_mutex);
      done--;
      pthread_mutex_unlock(&done_mutex);
      retval = (int) rpc_retval;
      rpc_retval = 0;
      return retval;
    }

    ~BuddyClient() {
      /* shut down */
      hg_engine_finalize();
    }
};

} // namespace bb
} // namespace pdlfs
