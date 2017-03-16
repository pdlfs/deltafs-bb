/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <list>
#include <map>

#include "bbos/bbos_api.h"

#define PATH_LEN 256

namespace pdlfs {
namespace bb {

typedef uint32_t chunkid_t;

enum binpacking_policy_t { RR_WITH_CURSOR, ALL };
enum container_flag_t { COMBINED, INDIVIDUAL };

typedef struct {
  char container_name[PATH_LEN];
  chunkid_t start_chunk;
  chunkid_t end_chunk;
  off_t offset;
} container_segment_t;

typedef struct {
  chunkid_t id;
  size_t size;                 // size of chunk in bytes
  void *buf;                   // buffer pointer for chunk
  container_segment_t *c_seg;  // container segment holding chunk info
} chunk_info_t;

typedef struct {
  char name[PATH_LEN];
  size_t size;
  bbos_mkobj_flag_t type;
  std::list<chunk_info_t *> *lst_chunks;  // list of chunks in BBOS object
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
  container_segment_t *c_seg;
} binpack_segment_t;

class BuddyStore {
 private:
  std::map<std::string, bbos_obj_t *> *object_map;
  std::map<std::string, std::list<container_segment_t *> *>
      *object_container_map_;
  size_t dirty_bbos_size_;
  size_t dirty_individual_size_;
  size_t binpacking_threshold_;
  binpacking_policy_t binpacking_policy_;
  std::list<bbos_obj_t *> *lru_objects_;
  std::list<bbos_obj_t *> *individual_objects_;
  pthread_t binpacking_thread_;
  size_t OBJECT_DIRTY_THRESHOLD_;
  size_t CONTAINER_SIZE_;
  char output_dir_[PATH_LEN];
  pthread_mutex_t bbos_mutex_;
  int containers_built_;
  char output_manifest_[PATH_LEN];
  int read_phase_;

  size_t PFS_CHUNK_SIZE_;
  size_t OBJ_CHUNK_SIZE_;
  bool BINPACKING_SHUTDOWN_;
  double avg_chunk_response_time_;
  double avg_container_response_time_;
  double avg_append_latency_;
  double avg_binpack_time_;
  uint64_t num_chunks_written_;
  uint64_t num_containers_written_;
  uint64_t num_appends_;
  uint64_t num_binpacks_;

  chunk_info_t *make_chunk(chunkid_t id, int malloc_chunk = 1);
  size_t add_data(chunk_info_t *chunk, void *buf, size_t len);
  size_t get_data(chunk_info_t *chunk, void *buf, off_t offset, size_t len);
  std::list<binpack_segment_t> all_binpacking_policy();
  std::list<binpack_segment_t> rr_with_cursor_binpacking_policy();
  std::list<binpack_segment_t> get_all_segments();
  void build_global_manifest(const char *manifest_name);
  int build_object_container_map(const char *container_name);
  bbos_obj_t *create_bbos_cache_entry(const char *name, bbos_mkobj_flag_t type);
  bbos_obj_t *populate_object_metadata(const char *name,
                                    bbos_mkobj_flag_t type = WRITE_OPTIMIZED);
  void invoke_binpacking(container_flag_t type);
  std::list<binpack_segment_t> get_objects(container_flag_t type = COMBINED);
  int build_container(const char *c_name,
                      std::list<binpack_segment_t> lst_binpack_segments);
  int lock_server();
  int unlock_server();
  size_t get_dirty_size();
  uint32_t get_individual_obj_count();
  size_t get_binpacking_threshold();
  size_t get_binpacking_policy();
  const char *get_next_container_name(char *path, container_flag_t type);

  static void *binpacker_main(void *args);

 public:
  BuddyStore();
  ~BuddyStore();

  void print_config(FILE *fp);
  int mkobj(const char *name, bbos_mkobj_flag_t type = WRITE_OPTIMIZED);
  size_t append(const char *name, void *buf, size_t len);
  size_t read(const char *name, void *buf, off_t offset, size_t len);
  size_t get_size(const char *name);
};

}  // namespace bb
}  // namespace pdlfs
