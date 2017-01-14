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
//#include "src/server/interface.h"

namespace pdlfs {
namespace bb {

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

//#define PFS_CHUNK_SIZE 8388608
//#define OBJ_CHUNK_SIZE 2097152
enum binpacking_policy { GREEDY };
enum stage_out_policy { SEQ_OUT, PAR_OUT };
enum stage_in_policy { SEQ_IN, PAR_IN };

typedef struct {
  chunkid_t id;
  size_t size; // size of chunk in bytes
  void *buf; // buffer pointer for chunk
} chunk_info_t;

typedef struct {
  //oid_t id;
  char name[PATH_LEN];
  size_t size;
  std::list<chunk_info_t *> *lst_chunks; // list of chunks in BBOS object
  chunkid_t last_chunk_flushed;
  size_t dirty_size;
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
