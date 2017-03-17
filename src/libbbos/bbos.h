/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

/*
 * bbos - burst buffer object store
 *
 * the bbos moves data between memory, the burst buffer, and the
 * backing filesystem (i.e. lustre).   we support 4 main operations:
 *
 *  1. mkobj - create a new bbos object in memory
 *  2. append - append data to the named bbos object's memory
 *  3. read - read bbos object data (from memory, loaded in ram on demand)
 *  4. get_size - report current size of a bbos object
 *
 * when the amount of data appended to the bbos objects passes a threshold
 * we write it to the burst buffer in a container file... data from
 * multiple objects is combined into a container file and the BuddyStore
 * object maintains metadata that provides a mapping from bbos objects
 * to container files.   data is written to backing store by the binpacking
 * thread.
 */
#pragma once

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <list>
#include <map>
#include <string>

#include "bbos/bbos_api.h"

#define PATH_LEN 256

namespace pdlfs {
namespace bb {

/*
 * chunkid_t: bbos objects are divided into PFS_CHUNK_SIZE_ byte chunks.
 * chunkid is the chunk block number in the object...  a typical chunk
 * size is 8MB.
 */
typedef uint32_t chunkid_t;

/*
 * binpacking_policy_t: the policy that the binpacking thread uses to
 * find data to pack off to a container in backing store.
 */
enum binpacking_policy_t {
  RR_WITH_CURSOR,            /* round robbin with cursor */
  ALL                        /* take everything (e.g. for shutdown) */
};

/*
 * container_flag_t:
 */
enum container_flag_t {
  COMBINED,
  INDIVIDUAL
};

/*
 * container_segment_t:
 */
typedef struct {
  char container_name[PATH_LEN];     /* container filename on backing store */
  chunkid_t start_chunk;             /* starting chunk number */
  chunkid_t end_chunk;               /* ending chunk number */
  off_t offset;                      /* offset in container file */
} container_segment_t;

/*
 * chunk_info_t: describes one chunk inside a bbos object
 */
typedef struct {
  chunkid_t id;                      /* chunk's ID number */
  size_t size;                       /* current size (<= PFS_CHUNK_SIZE_) */
  void *buf;                         /* malloc'd buf with data */
  container_segment_t *c_seg;        /* container seg holding chunk info */
} chunk_info_t;

/*
 * bbos_obj_t: top-level bbos object structure.
 * XXX: locking protocol?
 */
typedef struct {
  char name[PATH_LEN];               /* name of object */
  size_t size;                       /* current size of object */
  bbos_mkobj_flag_t type;            /* read or write optimized */
  std::list<chunk_info_t *> *lst_chunks;  /* list of chunks for this obj */
  size_t dirty_size;                 /* #bytes of unflushed appended data */
  pthread_mutex_t objmutex;          /* XXX */
  chunkid_t cursor;                  /* chunk block to pack next */
  bool marked_for_packing;           /* if we are past obj dirty threshold */
  chunkid_t last_full_chunk;         /* last complete chunk in obj(?) */
} bbos_obj_t;

/*
 * binpack_segment_t: the binpack thread builds a list of segments from
 * bbos objects that it wants to pack into a container...
 */
typedef struct {
  bbos_obj_t *obj;                   /* source object */
  chunkid_t start_chunk;             /* starting chunk to pack */
  chunkid_t end_chunk;               /* ending chunk (exclusive?) */
} binpack_segment_t;

/*
 * BuddyStoreOptions: configuartion options for bbos store
 */
struct BuddyStoreOptions {
  size_t PFS_CHUNK_SIZE;                /* bbos object chunk size */
  size_t OBJ_CHUNK_SIZE;                /* input append size */
  size_t binpacking_threshold;          /* start binpack trigger */
  binpacking_policy_t binpacking_policy;   /* policy to use */
  size_t OBJECT_DIRTY_THRESHOLD;        /* threshold to start binpack */
  size_t CONTAINER_SIZE;                /* target backing container size */
  int read_phase;                       /* XXX: in read phase? */
  std::string output_dir;               /* output dir for container file */

  BuddyStoreOptions();                  /* establishes defaults */
};

/*
 * BuddyStore: burst buffer object store main object
 */
class BuddyStore {
 private:
  /* this block is const after we are open */
  BuddyStoreOptions o_;                 /* config options */
  char output_manifest_[PATH_LEN];      /* output manifest file name */
  pthread_t binpacking_thread_;         /* binpacking thread handle */
  int made_bp_thread_;                  /* non-zero if bp thread launched */

  /* binpacking thread hooks */
  int bp_running_;                      /* written by bp thread */
  int bp_shutdown_;                     /* written only at shutdown */

  /* variables below here must be protected with the global lock */
  pthread_mutex_t bbos_mutex_;          /* global lock */

  /* object_map is the "directory" of in memory objects we know about */
  std::map<std::string, bbos_obj_t *> *object_map_;  /* name => bbos_obj_t */

  /* object_comtainer_map_ maps object names to all their container segs */
  std::map<std::string, std::list<container_segment_t *> *>
      *object_container_map_;                        /* name => list of segs */

  size_t dirty_bbos_size_;              /* XXX total */
  size_t dirty_individual_size_;        /* XXX total */
  std::list<bbos_obj_t *> *lru_objects_;          /* LRU list of objects */
  std::list<bbos_obj_t *> *individual_objects_;   /* READ opt object list */
  int containers_built_;                /* # of containers built */

  /* statistics */
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
  BuddyStore() : made_bp_thread_(0), bp_running_(0), bp_shutdown_(0),
    dirty_bbos_size_(0), dirty_individual_size_(0), containers_built_(0),
    avg_chunk_response_time_(0.0), avg_container_response_time_(0.0),
     avg_append_latency_(0.0), avg_binpack_time_(0.0), num_chunks_written_(0),
    num_containers_written_(0), num_appends_(0), num_binpacks_(0) {

    object_map_ = new std::map<std::string, bbos_obj_t *>;
    object_container_map_ =
      new std::map<std::string, std::list<container_segment_t *> *>;
    lru_objects_ = new std::list<bbos_obj_t *>;
    individual_objects_ = new std::list<bbos_obj_t *>;
    if (pthread_mutex_init(&bbos_mutex_, NULL) != 0) {
      fprintf(stderr, "BuddyStore::BuddyStore(): mutex init failed\n");
      abort();
    }

  }
  ~BuddyStore();

  static int Open(struct BuddyStoreOptions &opts, class BuddyStore **bsp);
  void print_config(FILE *fp);
  int mkobj(const char *name, bbos_mkobj_flag_t type = WRITE_OPTIMIZED);
  size_t append(const char *name, void *buf, size_t len);
  size_t read(const char *name, void *buf, off_t offset, size_t len);
  size_t get_size(const char *name);
};

}  // namespace bb
}  // namespace pdlfs
