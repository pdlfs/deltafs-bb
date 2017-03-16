/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "bbos.h"

namespace pdlfs {
namespace bb {

/*XXXCDC: REMOVE BB_Server_port, BB_Server_IP_address, BB_Num_workers */

#define NUM_SERVER_CONFIGS 8  // keep in sync with configs enum and config_names
static char config_names[NUM_SERVER_CONFIGS][PATH_LEN] = {
    "BB_Lustre_chunk_size",
    "BB_Mercury_transfer_size",
    "BB_Binpacking_threshold",
    "BB_Binpacking_policy",
    "BB_Object_dirty_threshold",
    "BB_Max_container_size",
    "BB_Output_dir",
    "BB_Read_phase"};

#ifndef CLOCK_REALTIME /* tmp hack to make it compile on macosx */
#define CLOCK_REALTIME 0
static int clock_gettime(int id, struct timespec *tp) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  tp->tv_sec = tv.tv_sec;
  tp->tv_nsec = tv.tv_usec * 1000;
  return (0);
}
#endif

static void timespec_diff(struct timespec *start, struct timespec *stop,
                          struct timespec *result) {
  if ((stop->tv_nsec - start->tv_nsec) < 0) {
    result->tv_sec = stop->tv_sec - start->tv_sec - 1;
    result->tv_nsec = stop->tv_nsec - start->tv_nsec + 1000000000;
  } else {
    result->tv_sec = stop->tv_sec - start->tv_sec;
    result->tv_nsec = stop->tv_nsec - start->tv_nsec;
  }
  return;
}

/*
 * start of class functions
 */

/*
 * BuddyStore::binpacker_main: main routine for binpack thread
 */
void *BuddyStore::binpacker_main(void *args) {
  BuddyStore *bs = (BuddyStore *)args;
  printf("\nStarting binpacking thread...\n");
  do {
    if (bs->get_dirty_size() >= bs->get_binpacking_threshold()) {
      bs->invoke_binpacking(COMBINED);
    }
    sleep(1);
  } while (!bs->BINPACKING_SHUTDOWN_);
  /* pack whatever is remaining */
  bs->invoke_binpacking(COMBINED);
  while (bs->get_individual_obj_count() > 0) {
    bs->invoke_binpacking(INDIVIDUAL);
  }
  printf("Shutting down binpacking thread\n");
  bs->BINPACKING_SHUTDOWN_ = false;
  pthread_exit(NULL);
}

/*
 * BuddyStore::invoke_binpacking: called from binpacker main thread
 */
void BuddyStore::invoke_binpacking(container_flag_t type) {
  struct timespec ts_before, ts_after;
  std::list<binpack_segment_t> lst_binpack_segments;
  /* we need to binpack */
  this->lock_server();
  /* identify segments of objects to binpack. */
  timespec binpack_diff_ts;
  clock_gettime(CLOCK_REALTIME, &ts_before);
  lst_binpack_segments = this->get_objects(type);
  clock_gettime(CLOCK_REALTIME, &ts_after);
  num_binpacks_ += 1;
  timespec_diff(&ts_before, &ts_after, &binpack_diff_ts);
  avg_binpack_time_ *= (num_binpacks_ - 1);
  avg_binpack_time_ +=
      ((binpack_diff_ts.tv_sec * 1000000000) + binpack_diff_ts.tv_nsec);
  avg_binpack_time_ /= num_binpacks_;
  this->unlock_server();
  char path[PATH_LEN];
  if (lst_binpack_segments.size() > 0) {
    this->build_container(this->get_next_container_name(path, type),
                          lst_binpack_segments);
    // TODO: stage out DW file to lustre - refer https://github.com/hpc/libhio.
  }
}

chunk_info_t *BuddyStore::make_chunk(chunkid_t id, int malloc_chunk) {
  chunk_info_t *new_chunk = new chunk_info_t;
  new_chunk->id = id;
  if (malloc_chunk == 1) {
    new_chunk->buf = (void *)malloc(sizeof(char) * o_.PFS_CHUNK_SIZE);
  } else {
    new_chunk->buf = NULL;
  }
  if (new_chunk == NULL) {
    return NULL;
  }
  new_chunk->size = 0;
  return new_chunk;
}

size_t BuddyStore::add_data(chunk_info_t *chunk, void *buf, size_t len) {
  // Checking of whether data can fit into this chunk has to be done outside
  memcpy((void *)((char *)chunk->buf + chunk->size), buf, len);
  chunk->size += len;
  return len;
}

size_t BuddyStore::get_data(chunk_info_t *chunk, void *buf, off_t offset,
                            size_t len) {
  memcpy(buf, (void *)((char *)chunk->buf + offset), len);
  return len;
}

std::list<binpack_segment_t> BuddyStore::all_binpacking_policy() {
  std::list<binpack_segment_t> segments;
  // FIXME: hardcoded to all objects
  std::map<std::string, bbos_obj_t *>::iterator it_map = object_map_->begin();
  while (it_map != object_map_->end()) {
    binpack_segment_t seg;
    seg.obj = (*it_map).second;
    pthread_mutex_lock(&seg.obj->objmutex);
    if (seg.obj->type == READ_OPTIMIZED) {
      it_map++;
      continue;
    }
    seg.start_chunk = seg.obj->cursor;
    seg.end_chunk = seg.obj->lst_chunks->size();
    size_t last_chunk_size = seg.obj->lst_chunks->back()->size;
    seg.obj->dirty_size -=
        ((seg.end_chunk - 1) - seg.start_chunk) * o_.PFS_CHUNK_SIZE;
    seg.obj->dirty_size -= last_chunk_size;
    pthread_mutex_unlock(&seg.obj->objmutex);
    dirty_bbos_size_ -=
        ((seg.end_chunk - 1) - seg.start_chunk) * o_.PFS_CHUNK_SIZE;
    dirty_bbos_size_ -= last_chunk_size;
    segments.push_back(seg);
    it_map++;
  }
  return segments;
}

std::list<binpack_segment_t> BuddyStore::rr_with_cursor_binpacking_policy() {
  std::list<binpack_segment_t> segments;
  chunkid_t num_chunks = o_.CONTAINER_SIZE / o_.PFS_CHUNK_SIZE;
  std::list<bbos_obj_t *> packed_objects_list;
  while (num_chunks > 0 && lru_objects_->size() > 0) {
    bbos_obj_t *obj = lru_objects_->front();
    lru_objects_->pop_front();
    binpack_segment_t seg;
    pthread_mutex_lock(&obj->objmutex);
    seg.obj = obj;
    seg.start_chunk = obj->cursor;
    seg.end_chunk = seg.start_chunk;
    if ((obj->last_full_chunk - seg.start_chunk) > num_chunks) {
      seg.end_chunk += num_chunks;
      obj->cursor += num_chunks;
    } else {
      seg.end_chunk = obj->last_full_chunk;
      obj->cursor = obj->last_full_chunk;
    }
    obj->dirty_size -= (seg.end_chunk - seg.start_chunk) * o_.PFS_CHUNK_SIZE;
    dirty_bbos_size_ -= (seg.end_chunk - seg.start_chunk) * o_.PFS_CHUNK_SIZE;
    if (obj->dirty_size > o_.OBJECT_DIRTY_THRESHOLD) {
      packed_objects_list.push_back(obj);
    } else {
      obj->marked_for_packing = false;
    }
    pthread_mutex_unlock(&obj->objmutex);
    segments.push_back(seg);
    num_chunks -= (seg.end_chunk - seg.start_chunk);
  }
  while (packed_objects_list.size() > 0) {
    bbos_obj_t *obj = packed_objects_list.front();
    packed_objects_list.pop_front();
    lru_objects_->push_back(obj);
  }
  return segments;
}

std::list<binpack_segment_t> BuddyStore::get_all_segments() {
  std::list<binpack_segment_t> segments;
  bbos_obj_t *obj = individual_objects_->front();
  individual_objects_->pop_front();
  chunkid_t num_chunks = obj->dirty_size / o_.PFS_CHUNK_SIZE;
  while (num_chunks > 0) {
    binpack_segment_t seg;
    seg.obj = obj;
    seg.start_chunk = obj->cursor;
    seg.end_chunk = obj->lst_chunks->size();
    obj->dirty_size -= (seg.end_chunk - seg.start_chunk) * o_.PFS_CHUNK_SIZE;
    switch (obj->type) {
      case WRITE_OPTIMIZED:
        dirty_bbos_size_ -= (seg.end_chunk - seg.start_chunk) * o_.PFS_CHUNK_SIZE;
        break;
      case READ_OPTIMIZED:
        dirty_individual_size_ -=
            (seg.end_chunk - seg.start_chunk) * o_.PFS_CHUNK_SIZE;
        break;
    }
    segments.push_back(seg);
    num_chunks -= (seg.end_chunk - seg.start_chunk);
  }
  return segments;
}

/*
 * Build the global manifest file used to bootstrap BBOS from all the
 * containers and their contents.
 */
void BuddyStore::build_global_manifest(const char *manifest_name) {
  // we have to iterate through the object container map and write it to
  // a separate file.
  FILE *fp = fopen(manifest_name, "w+");
  std::map<std::string, uint32_t> container_map;
  std::map<std::string, std::list<container_segment_t *> *>::iterator it_map =
      object_container_map_->begin();
  while (it_map != object_container_map_->end()) {
    std::list<container_segment_t *>::iterator it_list =
        it_map->second->begin();
    while (it_list != it_map->second->end()) {
      std::map<std::string, uint32_t>::iterator it_c_map =
          container_map.find(std::string((*it_list)->container_name));
      if (it_c_map == container_map.end()) {
        container_map.insert(
            it_c_map,
            std::pair<std::string, uint32_t>(
                std::string((*it_list)->container_name), container_map.size()));
        fprintf(fp, "%s\n", (*it_list)->container_name);
      }
      it_list++;
    }
    it_map++;
  }
  fclose(fp);
}

/*
 * Build the object container mapping from the start of container files.
 */
int BuddyStore::build_object_container_map(const char *container_name) {
  std::ifstream container(container_name);
  if (!container) {
    return BB_ENOCONTAINER;
  }
  std::string line;
  std::string token;
  std::string bbos_name;

  container >> containers_built_;  // first line contains number of objects.
  int num_objs = containers_built_;
  std::getline(container, line);  // this is the empty line

  while (num_objs > 0) {
    std::getline(container, line);
    int i = 0;
    char *end;
    container_segment_t *c_seg = new container_segment_t;
    strcpy(c_seg->container_name, container_name);
    size_t pos = 0;
    std::string delimiter(":");
    while ((pos = line.find(delimiter)) != std::string::npos) {
      token = line.substr(0, pos);
      switch (i) {
        case 0:
          bbos_name = token;
          break;
        case 1:
          c_seg->start_chunk = strtoul(token.c_str(), &end, 10);
          break;
        case 2:
          c_seg->end_chunk = strtoul(token.c_str(), &end, 10);
          break;
      };
      line.erase(0, pos + delimiter.length());
      i++;
    }
    c_seg->offset = strtoul(line.c_str(), &end, 10);
    std::map<std::string, std::list<container_segment_t *> *>::iterator it_map =
        object_container_map_->find(bbos_name);
    if (it_map != object_container_map_->end()) {
      // entry exists. place segment in right position.
      std::list<container_segment_t *> *lst_segments = it_map->second;
      std::list<container_segment_t *>::iterator it_list =
          lst_segments->begin();
      while (it_list != lst_segments->end()) {
        if ((*it_list)->start_chunk < c_seg->start_chunk) {
          it_list++;
        } else {
          break;
          lst_segments->insert(it_list, c_seg);
        }
      }
      lst_segments->insert(it_list, c_seg);
    } else {
      std::list<container_segment_t *> *lst_segments =
          new std::list<container_segment_t *>;
      lst_segments->push_back(c_seg);
      object_container_map_->insert(
          it_map, std::pair<std::string, std::list<container_segment_t *> *>(
                      bbos_name, lst_segments));
    }
    num_objs--;
  }
  return (0);
}

bbos_obj_t *BuddyStore::create_bbos_cache_entry(const char *name,
                                                bbos_mkobj_flag_t type) {
  bbos_obj_t *obj = new bbos_obj_t;
  obj->lst_chunks = new std::list<chunk_info_t *>;
  obj->dirty_size = 0;
  obj->size = 0;
  obj->type = type;
  obj->cursor = 0;
  obj->marked_for_packing = false;
  obj->last_full_chunk = 0;
  pthread_mutex_init(&obj->objmutex, NULL);
  pthread_mutex_lock(&obj->objmutex);
  sprintf(obj->name, "%s", name);
  std::map<std::string, bbos_obj_t *>::iterator it_map = object_map_->begin();
  object_map_->insert(it_map, std::pair<std::string, bbos_obj_t *>(
                                 std::string(obj->name), obj));
  if (type == READ_OPTIMIZED) {
    individual_objects_->push_back(obj);
  }
  return obj;
}

bbos_obj_t *BuddyStore::populate_object_metadata(const char *name,
                                                 bbos_mkobj_flag_t type) {
  std::map<std::string, std::list<container_segment_t *> *>::iterator it_map =
      object_container_map_->find(name);
  if (it_map == object_container_map_->end()) {
    // return BB_ENOOBJ;
    return NULL;
  }
  // entry exists. place segment in right position.
  std::list<container_segment_t *> *lst_segments = it_map->second;
  std::list<container_segment_t *>::iterator it_list = lst_segments->begin();
  bbos_obj_t *obj = create_bbos_cache_entry(name, WRITE_OPTIMIZED);
  chunkid_t i = 0;
  while (it_list != lst_segments->end()) {
    for (i = (*it_list)->start_chunk; i < (*it_list)->end_chunk; i++) {
      chunk_info_t *chunk = make_chunk(i, 0);
      obj->lst_chunks->push_back(chunk);
      chunk->c_seg = (*it_list);
      obj->size += o_.PFS_CHUNK_SIZE;
    }
    it_list++;
  }
  return obj;
}

/*
 * BuddyStoreOptions ctor sets the default values
 */
BuddyStoreOptions::BuddyStoreOptions()
    : PFS_CHUNK_SIZE(8388608),           /* 8MB */
      OBJ_CHUNK_SIZE(2097152),           /* 2MB */
      binpacking_threshold(21474836480), /* 20GB before trigger binpack */
      binpacking_policy(RR_WITH_CURSOR),
      OBJECT_DIRTY_THRESHOLD(268435456), /* 256MB */
      CONTAINER_SIZE(10737418240),       /* 10GB */
      read_phase(0),
      output_dir("/tmp") {}


BuddyStore::BuddyStore() {
  /* Default configs */
  o_.PFS_CHUNK_SIZE = 8388608;            // 8 MB
  o_.OBJ_CHUNK_SIZE = 2097152;            // 2 MB
  o_.binpacking_threshold = 21474836480;  // 20 GB before triggering binpacking
  o_.binpacking_policy = RR_WITH_CURSOR;
  o_.OBJECT_DIRTY_THRESHOLD = 268435456;  // 256 MB
  o_.CONTAINER_SIZE = 10737418240;        // 10 GB
  o_.output_dir = "/tmp";
  o_.read_phase = 0;

  avg_chunk_response_time_ = 0.0;
  avg_container_response_time_ = 0.0;
  avg_append_latency_ = 0.0;
  avg_binpack_time_ = 0.0;
  num_chunks_written_ = 0;
  num_containers_written_ = 0;
  num_appends_ = 0;
  num_binpacks_ = 0;

  /* Now scan the environment vars to find out what to override. */
  int config_overrides = 0;
  while (config_overrides < NUM_SERVER_CONFIGS) {
    const char *v = getenv(config_names[config_overrides]);
    if (v != NULL) {
      switch (config_overrides) {
        case 0:
          o_.PFS_CHUNK_SIZE = strtoul(v, NULL, 0);
          break;
        case 1:
          o_.OBJ_CHUNK_SIZE = strtoul(v, NULL, 0);
          break;
        case 2:
          o_.binpacking_threshold = strtoul(v, NULL, 0);
          break;
        case 3:
          o_.binpacking_policy = (binpacking_policy_t)atoi(v);
          break;
        case 4:
          o_.OBJECT_DIRTY_THRESHOLD = strtoul(v, NULL, 0);
          break;
        case 5:
          o_.CONTAINER_SIZE = strtoul(v, NULL, 0);
          break;
        case 6:
          o_.output_dir = v;
          break;
        case 7:
          o_.read_phase = atoi(v);
          break;
      }
    }
    config_overrides++;
  }

  // output manifest file
  snprintf(output_manifest_, PATH_LEN, "%s/BB_MANIFEST.txt",
           o_.output_dir.c_str());

  object_map_ = new std::map<std::string, bbos_obj_t *>;
  object_container_map_ =
      new std::map<std::string, std::list<container_segment_t *> *>;
  dirty_bbos_size_ = 0;
  lru_objects_ = new std::list<bbos_obj_t *>;
  individual_objects_ = new std::list<bbos_obj_t *>;

  pthread_mutex_init(&bbos_mutex_, NULL);

  containers_built_ = 0;
  if (o_.read_phase == 0) {
    BINPACKING_SHUTDOWN_ = false;
    pthread_create(&binpacking_thread_, NULL, BuddyStore::binpacker_main, this);
  } else {
    /* build the object container map using the MANIFEST */
    std::ifstream containers(output_manifest_);
    if (!containers) {
      printf("Could not read manifest file!\n");
      assert(0);
    }
    std::string line;
    std::string token;
    std::string bbos_name;

    char container_name[PATH_LEN];
    char container_path[PATH_LEN];
    containers >> container_name;
    snprintf(container_path, PATH_LEN, "%s/%s", o_.output_dir.c_str(),
             container_name);
    printf("Reading container %s\n", container_path);
    build_object_container_map((const char *)container_path);
  }
}

BuddyStore::~BuddyStore() {
  BINPACKING_SHUTDOWN_ = true;
  pthread_join(binpacking_thread_, NULL); /* WAIT HERE */
  assert(!BINPACKING_SHUTDOWN_);
  assert(dirty_bbos_size_ == 0);
  assert(dirty_individual_size_ == 0);
  if (o_.read_phase == 0) {
    build_global_manifest(
        output_manifest_);  // important for booting next time and reading
  }
  printf(
      "============= BBOS MEASUREMENTS (o_.OBJ_CHUNK_SIZE = %lu, "
      "o_.PFS_CHUNK_SIZE = %lu) =============\n",
      o_.OBJ_CHUNK_SIZE, o_.PFS_CHUNK_SIZE);
  printf("AVERAGE DW CHUNK RESPONSE TIME = %f ns\n", avg_chunk_response_time_);
  printf("AVERAGE DW CONTAINER RESPONSE TIME = %f ns\n",
         avg_container_response_time_);
  printf("AVERAGE APPEND LATENCY = %f ns\n", avg_append_latency_);
  printf("AVERAGE TIME SPENT IDENTIFYING SEGMENTS TO BINPACK = %f ns\n",
         avg_binpack_time_);
  printf("NUMBER OF %lu BYTE CHUNKS WRITTEN = %" PRIu64 "\n", o_.PFS_CHUNK_SIZE,
         num_chunks_written_);
  printf("NUMBER OF %lu BYTE CONTAINERS WRITTEN = %" PRIu64 "\n",
         o_.CONTAINER_SIZE, num_containers_written_);
  printf("NUMBER OF %lu BYTE APPENDS = %" PRIu64 "\n", o_.OBJ_CHUNK_SIZE,
         num_appends_);
  printf("NUMBER OF BINPACKINGS DONE = %" PRIu64 "\n", num_binpacks_);
  printf("============================================\n");
  std::map<std::string, std::list<container_segment_t *> *>::iterator
      it_obj_cont_map = object_container_map_->begin();
  while (it_obj_cont_map != object_container_map_->end()) {
    std::list<container_segment_t *>::iterator it_c_segs =
        it_obj_cont_map->second->begin();
    while (it_c_segs != it_obj_cont_map->second->end()) {
      delete (*it_c_segs);
      it_c_segs++;
    }
    delete it_obj_cont_map->second;
    it_obj_cont_map++;
  }
  delete object_container_map_;
  std::map<std::string, bbos_obj_t *>::iterator it_obj_map =
      object_map_->begin();
  while (it_obj_map != object_map_->end()) {
    assert(it_obj_map->second->dirty_size == 0);
    std::list<chunk_info_t *>::iterator it_chunks =
        it_obj_map->second->lst_chunks->begin();
    while (it_chunks != it_obj_map->second->lst_chunks->end()) {
      delete (*it_chunks);
      it_chunks++;
    }
    pthread_mutex_destroy(&(it_obj_map->second->objmutex));
    delete it_obj_map->second;
    it_obj_map++;
  }
  pthread_mutex_destroy(&bbos_mutex_);
  delete object_map_;
  delete lru_objects_;
  delete individual_objects_;
}

/*
 * print_pot: print using power of two (MiB,GiB) if possible
 */
static void print_pot(FILE *to, const char *tag, uint64_t val) {
  uint64_t mb;
  if (val % (1024 * 1024)) {
    fprintf(to, "\t%-20.20s= %" PRIu64 "\n", tag, val);
    return;
  }
  mb = val / (1024 * 1024);
  if (mb < 1024 || (mb % 1024) != 0) {
    fprintf(to, "\t%-20.20s= %" PRIu64 " MiB\n", tag, mb);
    return;
  }
  fprintf(to, "\t%-20.20s= %" PRIu64 " GiB\n", tag, mb / 1024);
  return;
}

/*
 * BuddyStore::print_config: print config for logs and so user can
 * easily verify the settings we are using
 */
void BuddyStore::print_config(FILE *fp) {
  fprintf(fp, "BuddyStore::print_config:\n");
  fprintf(fp, "\tdirectory           = %s\n", o_.output_dir.c_str());
  print_pot(fp, "pfs-chunk-size", o_.PFS_CHUNK_SIZE);
  print_pot(fp, "obj-chunk-size", o_.OBJ_CHUNK_SIZE);
  print_pot(fp, "container-size", o_.CONTAINER_SIZE);
  print_pot(fp, "binpack-threshold", o_.binpacking_threshold);
  print_pot(fp, "obj-dirty-threshold", o_.OBJECT_DIRTY_THRESHOLD);
  fprintf(fp, "\tbinpack-policy      = %d\n", o_.binpacking_policy);
  fprintf(fp, "\tread-phase          = %d\n", o_.read_phase);
}

std::list<binpack_segment_t> BuddyStore::get_objects(container_flag_t type) {
  switch (type) {
    case COMBINED:
      if ((BINPACKING_SHUTDOWN_ == true) && (dirty_bbos_size_ > 0)) {
        return all_binpacking_policy();
      }
      switch (o_.binpacking_policy) {
        case RR_WITH_CURSOR:
          return rr_with_cursor_binpacking_policy();
        case ALL:
          return all_binpacking_policy();
      }
      break;

    case INDIVIDUAL:
      return get_all_segments();
  }
  std::list<binpack_segment_t> segments;
  printf("Invalid binpacking policy selected!\n");
  return segments;
}

int BuddyStore::build_container(
    const char *c_name, std::list<binpack_segment_t> lst_binpack_segments) {
  struct timespec cont_ts_before, cont_ts_after, cnk_ts_before, cnk_ts_after;
  char c_path[PATH_LEN];
  snprintf(c_path, PATH_LEN, "%s/%s", o_.output_dir.c_str(), c_name);
  binpack_segment_t b_obj;
  size_t data_written = 0;
  off_t c_offset = 0;
  off_t start_offset = 0;
  timespec chunk_diff_ts;
  timespec container_diff_ts;
  clock_gettime(CLOCK_REALTIME, &cont_ts_before);
  FILE *fp = fopen(c_path, "w+");
  assert(fp != NULL);
  std::list<binpack_segment_t>::iterator it_bpack =
      lst_binpack_segments.begin();
  start_offset += fprintf(fp, "%lu\n", lst_binpack_segments.size());
  while (it_bpack != lst_binpack_segments.end()) {
    b_obj = *it_bpack;
    it_bpack++;
    start_offset += fprintf(fp, "%s:%u:%u:00000000\n", b_obj.obj->name,
                            b_obj.start_chunk, b_obj.end_chunk);
  }
  c_offset = start_offset;
  rewind(fp);  // we need to rewind the fp to the start because now we know
               // correct offsets
  fprintf(fp, "%lu\n", lst_binpack_segments.size());
  it_bpack = lst_binpack_segments.begin();
  // rewrite of metadata is required with the correct start offsets of data.
  while (it_bpack != lst_binpack_segments.end()) {
    b_obj = *it_bpack;
    it_bpack++;
    std::stringstream c_offset_stream;
    c_offset_stream << std::setfill('0') << std::setw(16) << start_offset;
    fprintf(fp, "%s:%u:%u:%s\n", b_obj.obj->name, b_obj.start_chunk,
            b_obj.end_chunk, c_offset_stream.str().c_str());
    start_offset += (o_.PFS_CHUNK_SIZE * (b_obj.end_chunk - b_obj.start_chunk));
  }

  it_bpack = lst_binpack_segments.begin();
  while (it_bpack != lst_binpack_segments.end()) {
    b_obj = *it_bpack;
    std::list<chunk_info_t *>::iterator it_chunks =
        b_obj.obj->lst_chunks->begin();
    while (it_chunks != b_obj.obj->lst_chunks->end() &&
           (*it_chunks)->id < b_obj.start_chunk) {
      it_chunks++;
    }
    while (it_chunks != b_obj.obj->lst_chunks->end() &&
           (*it_chunks)->id < b_obj.end_chunk) {
      // FIXME: write to DW in o_.PFS_CHUNK_SIZE - https://github.com/hpc/libhio
      clock_gettime(CLOCK_REALTIME, &cnk_ts_before);
      data_written =
          fwrite((*it_chunks)->buf, sizeof(char), (*it_chunks)->size, fp);
      clock_gettime(CLOCK_REALTIME, &cnk_ts_after);
      num_chunks_written_ += 1;
      timespec_diff(&cnk_ts_before, &cnk_ts_after, &chunk_diff_ts);
      avg_chunk_response_time_ *= (num_chunks_written_ - 1);
      avg_chunk_response_time_ +=
          ((chunk_diff_ts.tv_sec * 1000000000) + chunk_diff_ts.tv_nsec);
      avg_chunk_response_time_ /= num_chunks_written_;
      if (data_written != (*it_chunks)->size) abort();
      free((*it_chunks)->buf);

      // FIXME: Ideally we would reduce dirty size after writing to DW,
      //       but here we reduce it when we choose to binpack itself.
      it_chunks++;
    }

    // populate the object_container_map_
    container_segment_t *c_seg = new container_segment_t;
    strcpy(c_seg->container_name, c_name);
    c_seg->start_chunk =
        b_obj.start_chunk * (o_.PFS_CHUNK_SIZE / o_.OBJ_CHUNK_SIZE);
    c_seg->end_chunk = b_obj.end_chunk * (o_.PFS_CHUNK_SIZE / o_.OBJ_CHUNK_SIZE);
    c_seg->offset = c_offset;
    c_offset += (o_.OBJ_CHUNK_SIZE * (b_obj.end_chunk - b_obj.start_chunk));
    std::map<std::string, std::list<container_segment_t *> *>::iterator it_map =
        object_container_map_->find(std::string(b_obj.obj->name));
    if (it_map != object_container_map_->end()) {
      // entry already present in global manifest. Just add new segments.
      it_map->second->push_back(c_seg);
    } else {
      std::string bbos_name(b_obj.obj->name);
      std::list<container_segment_t *> *lst_segments =
          new std::list<container_segment_t *>;
      lst_segments->push_back(c_seg);
      object_container_map_->insert(
          it_map, std::pair<std::string, std::list<container_segment_t *> *>(
                      bbos_name, lst_segments));
    }
    it_bpack++;
  }
  fclose(fp);
  clock_gettime(CLOCK_REALTIME, &cont_ts_after);
  num_containers_written_ += 1;
  timespec_diff(&cont_ts_before, &cont_ts_after,
                &container_diff_ts);
  avg_container_response_time_ *= (num_containers_written_ - 1);
  avg_container_response_time_ +=
      ((container_diff_ts.tv_sec * 1000000000) + container_diff_ts.tv_nsec);
  avg_container_response_time_ /= num_containers_written_;
  return 0;
}

int BuddyStore::mkobj(const char *name, bbos_mkobj_flag_t type) {
  // Initialize an in-memory object
  if (create_bbos_cache_entry(name, type) == NULL) {
    return BB_ERROBJ;
  }
  std::map<std::string, bbos_obj_t *>::iterator it_obj_map =
      object_map_->find(std::string(name));
  pthread_mutex_unlock(&(it_obj_map->second->objmutex));
  return 0;
}

int BuddyStore::lock_server() { return pthread_mutex_lock(&bbos_mutex_); }

int BuddyStore::unlock_server() { return pthread_mutex_unlock(&bbos_mutex_); }

/* Get total dirty data size */
size_t BuddyStore::get_dirty_size() { return dirty_bbos_size_; }

/* Get number of individual objects. */
uint32_t BuddyStore::get_individual_obj_count() {
  return individual_objects_->size();
}

/* Get binpacking threshold */
size_t BuddyStore::get_binpacking_threshold() { return o_.binpacking_threshold; }

/* Get binpacking policy */
size_t BuddyStore::get_binpacking_policy() { return o_.binpacking_policy; }

/* Get name of next container */
const char *BuddyStore::get_next_container_name(char *path,
                                                container_flag_t type) {
  // TODO: get container name from a microservice
  switch (type) {
    case COMBINED:
      snprintf(path, PATH_LEN, "bbos_%d.con.write", containers_built_++);
      break;
    case INDIVIDUAL:
      snprintf(path, PATH_LEN, "bbos_%d.con.read", containers_built_++);
      break;
  }
  return (const char *)path;
}

/* Get size of BB object */
size_t BuddyStore::get_size(const char *name) {
  std::map<std::string, bbos_obj_t *>::iterator it_obj_map =
      object_map_->find(std::string(name));
  if (it_obj_map == object_map_->end()) {
    std::map<std::string, std::list<container_segment_t *> *>::iterator it_map =
        object_container_map_->find(std::string(name));
    assert(it_map != object_container_map_->end());
    bbos_obj_t *obj = create_bbos_cache_entry(
        name, WRITE_OPTIMIZED);  // FIXME: place correct object type
    std::list<container_segment_t *> *lst_segments = it_map->second;
    std::list<container_segment_t *>::iterator it_segs = lst_segments->begin();
    while (it_segs != it_map->second->end()) {
      container_segment_t *c_seg = (*it_segs);
      for (int i = c_seg->start_chunk; i < c_seg->end_chunk; i++) {
        chunk_info_t *chunk = new chunk_info_t;
        chunk->buf = NULL;
        chunk->size = 0;
        chunk->id = i;
        obj->lst_chunks->push_back(chunk);
        obj->size += o_.PFS_CHUNK_SIZE;
      }
      it_segs++;
    }
    pthread_mutex_unlock(&(obj->objmutex));
    return obj->size;
  } else {
    pthread_mutex_lock(&(it_obj_map->second->objmutex));
  }
  pthread_mutex_unlock(&(it_obj_map->second->objmutex));
  return it_obj_map->second->size;
}

/* Append to a BB object */
size_t BuddyStore::append(const char *name, void *buf, size_t len) {
  struct timespec ts_before, ts_after, append_diff_ts;
  clock_gettime(CLOCK_REALTIME, &ts_before);

  bbos_obj_t *obj = object_map_->find(std::string(name))->second;
  assert(obj != NULL);
  pthread_mutex_lock(&obj->objmutex);
  chunk_info_t *last_chunk = obj->lst_chunks->back();
  size_t data_added = 0;
  chunkid_t next_chunk_id = 0;
  if (obj->lst_chunks->empty() || (last_chunk->size == o_.PFS_CHUNK_SIZE)) {
    // we need to create a new chunk and append into it.
    if (!obj->lst_chunks->empty()) {
      next_chunk_id = last_chunk->id + 1;
      obj->last_full_chunk = last_chunk->id;
    }
    chunk_info_t *chunk = make_chunk(next_chunk_id);
    obj->lst_chunks->push_back(chunk);
    last_chunk = obj->lst_chunks->back();
  }
  data_added += add_data(last_chunk, buf, o_.OBJ_CHUNK_SIZE);
  // NOTE: we always assume data will be received in o_.OBJ_CHUNK_SIZE
  obj->size += data_added;
  obj->dirty_size += data_added;
  if (obj->dirty_size > o_.OBJECT_DIRTY_THRESHOLD &&
      obj->type == WRITE_OPTIMIZED && obj->marked_for_packing == false) {
    // add to head of LRU list for binpacking consideration
    obj->marked_for_packing = true;
    lru_objects_->push_front(obj);
  }
  switch (obj->type) {
    case WRITE_OPTIMIZED:
      dirty_bbos_size_ += data_added;
      break;
    case READ_OPTIMIZED:
      dirty_individual_size_ += data_added;
      break;
  }
  pthread_mutex_unlock(&obj->objmutex);

  clock_gettime(CLOCK_REALTIME, &ts_after);
  num_appends_ += 1;
  timespec_diff(&ts_before, &ts_after, &append_diff_ts);
  avg_append_latency_ *= (num_appends_ - 1);
  avg_append_latency_ +=
      ((append_diff_ts.tv_sec * 1000000000) + append_diff_ts.tv_nsec);
  avg_append_latency_ /= num_appends_;

  return data_added;
}

/* Read from a BB object */
size_t BuddyStore::read(const char *name, void *buf, off_t offset, size_t len) {
  bbos_obj_t *obj = object_map_->find(std::string(name))->second;
  if (obj == NULL && o_.read_phase == 1) {
    obj = populate_object_metadata(name, WRITE_OPTIMIZED);
  } else {
    pthread_mutex_lock(&obj->objmutex);
  }

  assert(obj != NULL);
  if (offset >= obj->size) {
    return BB_INVALID_READ;
  }
  size_t data_read = 0;
  size_t size_to_be_read = 0;
  size_t offset_to_be_read = 0;
  std::list<chunk_info_t *>::iterator it_chunks = obj->lst_chunks->begin();
  chunkid_t chunk_num = offset / o_.PFS_CHUNK_SIZE;
  int chunk_obj_offset =
      (offset - (chunk_num * o_.PFS_CHUNK_SIZE)) / o_.OBJ_CHUNK_SIZE;
  for (int i = 0; i < chunk_num; i++) {
    it_chunks++;
  }
  chunk_info_t *chunk = *it_chunks;
  if (chunk->buf == NULL) {
    // first fetch data from container into memory
    off_t c_offset = chunk->c_seg->offset;
    c_offset += (o_.PFS_CHUNK_SIZE * (chunk_num - chunk->c_seg->start_chunk));
    chunk->buf = (void *)malloc(sizeof(char) * o_.PFS_CHUNK_SIZE);
    assert(chunk->buf != NULL);
    FILE *fp_seg = fopen(chunk->c_seg->container_name, "r");
    int seek_ret = fseek(fp_seg, c_offset, SEEK_SET);
    if (seek_ret != 0) abort();
    size_t read_size = fread(chunk->buf, o_.PFS_CHUNK_SIZE, 1, fp_seg);
    if (read_size != 1) abort();
    fclose(fp_seg);
  }
  offset_to_be_read = offset - (o_.PFS_CHUNK_SIZE * chunk_num) +
                      (o_.OBJ_CHUNK_SIZE * chunk_obj_offset);
  size_to_be_read = o_.PFS_CHUNK_SIZE;
  if (len < size_to_be_read) {
    size_to_be_read = len;
  }
  data_read += get_data(chunk, buf, offset_to_be_read, size_to_be_read);
  if (o_.read_phase == 1) {
    free(chunk->buf);
    chunk->size = 0;
  }
  pthread_mutex_unlock(&obj->objmutex);
  return data_read;
}

}  // namespace bb
}  // namespace pdlfs
