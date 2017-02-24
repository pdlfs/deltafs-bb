/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <list>
#include <map>
#include <mercury_hl_macros.h>
#include <mercury_proc_string.h>
/* Avoid compilation warning */
#ifndef _WIN32
    #undef _GNU_SOURCE
#endif
#include <mercury_thread.h>
#include <mercury_config.h>
#include <mercury_thread_pool.h>

namespace pdlfs {
namespace bb {

MERCURY_GEN_PROC(bbos_mkobj_in_t,
    ((hg_const_string_t)(name))\
    ((hg_bool_t)(type)))
MERCURY_GEN_PROC(bbos_mkobj_out_t,
    ((hg_id_t)(status)))
MERCURY_GEN_PROC(bbos_append_in_t,
    ((hg_const_string_t)(name))\
    ((hg_bulk_t)(bulk_handle)))
MERCURY_GEN_PROC(bbos_append_out_t,
    ((hg_size_t)(size)))
MERCURY_GEN_PROC(bbos_read_in_t,
    ((hg_const_string_t)(name))\
    ((hg_size_t)(offset))\
    ((hg_size_t)(size))\
    ((hg_bulk_t)(bulk_handle)))
MERCURY_GEN_PROC(bbos_read_out_t,
    ((hg_size_t)(size)))
MERCURY_GEN_PROC(bbos_get_size_in_t,
    ((hg_const_string_t)(name)))
MERCURY_GEN_PROC(bbos_get_size_out_t,
    ((hg_size_t)(size)))

static hg_return_t bbos_rpc_handler(hg_handle_t handle);

typedef uint64_t oid_t;
typedef uint64_t pfsid_t;
typedef uint32_t chunkid_t;

#define PATH_LEN 256

#define BB_ENOSPC 1
#define BB_SYNC_ERROR 2
#define BB_CONFIG_ERROR 3
#define BB_ENOMEM 4
#define BB_INVALID_READ 5
#define BB_ENOMANIFEST 6
#define BB_ENOCONTAINER 7
#define BB_ERROBJ 8

enum mkobj_flag_t {READ_OPTIMIZED, WRITE_OPTIMIZED};
enum container_flag_t {COMBINED, INDIVIDUAL};
enum binpacking_policy_t { RR_WITH_CURSOR, ALL };
enum stage_out_policy { SEQ_OUT, PAR_OUT };
enum stage_in_policy { SEQ_IN, PAR_IN };

#define NUM_SERVER_CONFIGS 10 // keep in sync with configs enum and config_names
char config_names[NUM_SERVER_CONFIGS][PATH_LEN] = {
  "BB_Server_Port",
  "BB_Lustre_chunk_size",
  "BB_Mercury_transfer_size",
  "BB_Num_workers",
  "BB_Binpacking_threshold",
  "BB_Binpacking_policy",
  "BB_Object_dirty_threshold",
  "BB_Max_container_size",
  "BB_Server_IP_address",
  "BB_Output_dir"
};
enum server_configs {
  PORT,
  LUSTRE_CHUNK_SIZE,
  MERCURY_CHUNK_SIZE,
  WORKER_THREADS,
  BINPACKING_THRESHOLD_SIZE,
  BINPACKING_POLICY_NAME,
  OBJ_DIRTY_THRESHOLD_SIZE,
  MAX_CONTAINER_SIZE,
  SERVER_IP,
  OUTPUT_DIR
};

typedef struct {
  pthread_t thread;
  std::list<struct hg_cb_info *> queue;
} client_context_t;

typedef struct {
  chunkid_t id;
  size_t size; // size of chunk in bytes
  void *buf; // buffer pointer for chunk
} chunk_info_t;

typedef struct {
  //oid_t id;
  char name[PATH_LEN];
  size_t size;
  mkobj_flag_t type;
  std::list<chunk_info_t *> *lst_chunks; // list of chunks in BBOS object
  chunkid_t last_chunk_flushed;
  size_t dirty_size;
  pthread_mutex_t mutex;
  chunkid_t cursor;
  bool marked_for_packing;
  chunkid_t last_full_chunk;
} bbos_obj_t;

typedef struct {
  bbos_obj_t *obj;
  chunkid_t start_chunk;
  chunkid_t end_chunk;
} binpack_segment_t;

typedef struct {
  char container_name[PATH_LEN];
  chunkid_t start_chunk;
  chunkid_t end_chunk;
  off_t offset;
} container_segment_t;

} // namespace bb
} // namespace pdlfs
