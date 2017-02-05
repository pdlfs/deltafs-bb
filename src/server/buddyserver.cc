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
#include <aio.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/time.h>
#include "buddyserver.h"

namespace pdlfs {
namespace bb {

static void* binpacking_decorator(void *args);
static void destructor_decorator(int);
static HG_THREAD_RETURN_TYPE bbos_mkobj_handler(void *args);
static HG_THREAD_RETURN_TYPE bbos_append_handler(void *args);
static HG_THREAD_RETURN_TYPE bbos_read_handler(void *args);
static HG_THREAD_RETURN_TYPE bbos_get_size_handler(void *args);
static hg_return_t bbos_mkobj_handler_decorator(hg_handle_t handle);
static hg_return_t bbos_append_handler_decorator(hg_handle_t handle);
static hg_return_t bbos_read_handler_decorator(hg_handle_t handle);
static hg_return_t bbos_get_size_handler_decorator(hg_handle_t handle);

static size_t OBJ_CHUNK_SIZE;
static size_t PFS_CHUNK_SIZE;
static bool BINPACKING_SHUTDOWN;
static bool GLOBAL_RPC_SHUTDOWN;
static na_class_t *server_network_class;
static hg_context_t *server_hg_context;
static hg_class_t *server_hg_class;
static hg_bool_t server_hg_progress_shutdown_flag;
static hg_id_t server_mkobj_rpc_id;
static hg_id_t server_append_rpc_id;
static hg_id_t server_read_rpc_id;
static hg_id_t server_get_size_rpc_id;

/* metric calculation code */
static uint64_t chunk_latency_hist[10];
static timespec chunk_ts_before;
static timespec chunk_ts_after;
static timespec container_ts_before;
static timespec container_ts_after;
static timespec append_ts_before;
static timespec append_ts_after;
static timespec binpack_ts_before;
static timespec binpack_ts_after;
static uint64_t num_chunks_written = 0;
static uint64_t num_containers_written = 0;
static uint64_t num_appends = 0;
static uint64_t num_binpacks = 0;
static double avg_chunk_response_time = 0.0;
static double avg_container_response_time = 0.0;
static double avg_append_latency = 0.0;
static double avg_binpack_time = 0.0;

static hg_thread_pool_t *thread_pool;

static void *bs_obj;

struct bbos_append_cb {
  void *buffer;
  hg_bulk_t bulk_handle;
  hg_handle_t handle;
  hg_size_t size;
  char name[PATH_LEN];
};

struct bbos_read_cb {
  void *buffer;
  hg_bulk_t bulk_handle;
  hg_bulk_t remote_bulk_handle;
  hg_handle_t handle;
  char name[PATH_LEN];
  off_t offset;
  size_t size;
};

/* dedicated thread function to drive Mercury progress */
static void* rpc_progress_fn(void* rpc_index)
{
    hg_return_t ret;
    unsigned int actual_count;

    while(!server_hg_progress_shutdown_flag && !GLOBAL_RPC_SHUTDOWN) {
        do {
            ret = HG_Trigger(server_hg_context, 0, 1, &actual_count);
        } while((ret == HG_SUCCESS) && actual_count && !server_hg_progress_shutdown_flag);

        if(!server_hg_progress_shutdown_flag)
            HG_Progress(server_hg_context, 100);
    }

    return(NULL);
}

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

class BuddyServer
{
  private:
    std::map<std::string, bbos_obj_t *> *object_map;
    std::map<std::string, std::list<container_segment_t *> *> *object_container_map;
    size_t dirty_bbos_size;
    size_t dirty_individual_size;
    size_t binpacking_threshold;
    binpacking_policy_t binpacking_policy;
    std::list<bbos_obj_t *> *lru_objects;
    std::list<bbos_obj_t *> *individual_objects;
    pthread_t binpacking_thread;
    pthread_t progress_thread;
    struct sigaction sa;
    size_t OBJECT_DIRTY_THRESHOLD;
    size_t CONTAINER_SIZE;
    char dw_mount_point[PATH_LEN];
    pthread_mutex_t bbos_mutex;
    int containers_built;
    char output_manifest[PATH_LEN];
    char server_url[PATH_LEN];
    int port;

    chunk_info_t *make_chunk(chunkid_t id) {
      chunk_info_t *new_chunk = new chunk_info_t;
      new_chunk->id = id;
      new_chunk->buf = (void *) malloc(sizeof(char) * PFS_CHUNK_SIZE);
      if(new_chunk == NULL) {
        return NULL;
      }
      new_chunk->size = 0;
      return new_chunk;
    }

    size_t add_data(chunk_info_t *chunk, void *buf, size_t len) {
      // Checking of whether data can fit into this chunk has to be done outside
      void *ptr = memcpy((void *)((char *)chunk->buf + chunk->size), buf, len);
      assert(ptr != NULL);
      chunk->size += len;
      return len;
    }

    size_t get_data(chunk_info_t *chunk, void *buf, off_t offset, size_t len) {
      void *ptr = memcpy(buf, (void *)((char *)chunk->buf + offset), len);
      assert(ptr != NULL);
      return len;
    }

    std::list<binpack_segment_t> all_binpacking_policy() {
      std::list<binpack_segment_t> segments;
      //FIXME: hardcoded to all objects
      std::map<std::string, bbos_obj_t *>::iterator it_map = object_map->begin();
      while(it_map != object_map->end()) {
        binpack_segment_t seg;
        seg.obj = (*it_map).second;
        pthread_mutex_lock(&seg.obj->mutex);
        if(seg.obj->type == READ_OPTIMIZED) {
          it_map++;
          continue;
        }
        seg.start_chunk = seg.obj->cursor;
        seg.end_chunk = seg.obj->lst_chunks->size();
        size_t last_chunk_size = seg.obj->lst_chunks->back()->size;
        seg.obj->dirty_size -= ((seg.end_chunk - 1) - seg.start_chunk) * PFS_CHUNK_SIZE;
        seg.obj->dirty_size -= last_chunk_size;
        pthread_mutex_unlock(&seg.obj->mutex);
        dirty_bbos_size -= ((seg.end_chunk - 1) - seg.start_chunk) * PFS_CHUNK_SIZE;
        dirty_bbos_size -= last_chunk_size;
        segments.push_back(seg);
        it_map++;
      }
      return segments;
    }

    std::list<binpack_segment_t> rr_with_cursor_binpacking_policy() {
      std::list<binpack_segment_t> segments;
      chunkid_t num_chunks = CONTAINER_SIZE / PFS_CHUNK_SIZE;
      std::list<bbos_obj_t *> packed_objects_list;
      while(num_chunks > 0 && lru_objects->size() > 0) {
        bbos_obj_t *obj = lru_objects->front();
        lru_objects->pop_front();
        binpack_segment_t seg;
        pthread_mutex_lock(&obj->mutex);
        seg.obj = obj;
        seg.start_chunk = obj->cursor;
        seg.end_chunk = seg.start_chunk;
        if((obj->last_full_chunk - seg.start_chunk) > num_chunks) {
          seg.end_chunk += num_chunks;
          obj->cursor += num_chunks;
        } else {
          seg.end_chunk = obj->last_full_chunk;
          obj->cursor = obj->last_full_chunk;
        }
        obj->dirty_size -= (seg.end_chunk - seg.start_chunk) * PFS_CHUNK_SIZE;
        dirty_bbos_size -= (seg.end_chunk - seg.start_chunk) * PFS_CHUNK_SIZE;
        if(obj->dirty_size > OBJECT_DIRTY_THRESHOLD) {
          packed_objects_list.push_back(obj);
        } else {
          obj->marked_for_packing = false;
        }
        pthread_mutex_unlock(&obj->mutex);
        segments.push_back(seg);
        num_chunks -= (seg.end_chunk - seg.start_chunk);
      }
      while (packed_objects_list.size() > 0) {
        bbos_obj_t *obj = packed_objects_list.front();
        packed_objects_list.pop_front();
        lru_objects->push_back(obj);
      }
      return segments;
    }

    std::list<binpack_segment_t> get_all_segments() {
      std::list<binpack_segment_t> segments;
      bbos_obj_t *obj = individual_objects->front();
      individual_objects->pop_front();
      chunkid_t num_chunks = obj->dirty_size / PFS_CHUNK_SIZE;
      while(num_chunks > 0) {
        binpack_segment_t seg;
        seg.obj = obj;
        seg.start_chunk = obj->cursor;
        seg.end_chunk = obj->lst_chunks->size();
        obj->dirty_size -= (seg.end_chunk - seg.start_chunk) * PFS_CHUNK_SIZE;
        switch (obj->type) {
          case WRITE_OPTIMIZED: dirty_bbos_size -= (seg.end_chunk - seg.start_chunk) * PFS_CHUNK_SIZE;
                                break;
          case READ_OPTIMIZED: dirty_individual_size -= (seg.end_chunk - seg.start_chunk) * PFS_CHUNK_SIZE;
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
    void build_global_manifest(const char *manifest_name) {
      // we have to iterate through the object container map and write it to
      // a separate file.
      FILE *fp = fopen(manifest_name, "w+");
      std::map<std::string, uint32_t> container_map;
      std::map<std::string, std::list<container_segment_t *> *>::iterator it_map = object_container_map->begin();
      while(it_map != object_container_map->end()) {
        std::list<container_segment_t *>::iterator it_list = it_map->second->begin();
        while(it_list != it_map->second->end()) {
          std::map<std::string, uint32_t>::iterator it_c_map = container_map.find(std::string((*it_list)->container_name));
          if(it_c_map == container_map.end()) {
            container_map.insert(it_c_map, std::pair<std::string, uint32_t>(std::string((*it_list)->container_name), container_map.size()));
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
    int build_object_container_map(const char *container_name) {
      std::ifstream container(container_name);
      if(!container) {
        return -BB_ENOCONTAINER;
      }
      std::string line;
      std::string token;
      std::string bbos_name;

      container >> containers_built; // first line contains number of objects.
      int num_objs = containers_built;
      std::getline(container, line); // this is the empty line

      while(num_objs > 0) {
        std::getline(container, line);
        int i = 0;
        char *end;
        container_segment_t *c_seg = new container_segment_t;
        strcpy(c_seg->container_name, container_name);
        size_t pos = 0;
        std::string delimiter(":");
        while ((pos = line.find(delimiter)) != std::string::npos) {
          token = line.substr(0, pos);
          switch(i) {
            case 0: bbos_name = token;
                    break;
            case 1: c_seg->start_chunk = strtoul(token.c_str(), &end, 10);
                    break;
            case 2: c_seg->end_chunk = strtoul(token.c_str(), &end, 10);
                    break;
          };
          line.erase(0, pos + delimiter.length());
          i++;
        }
        c_seg->offset = strtoul(line.c_str(), &end, 10);
        std::map<std::string, std::list<container_segment_t *> *>::iterator it_map = object_container_map->find(bbos_name);
        if(it_map != object_container_map->end()) {
          // entry exists. place segment in right position.
          std::list<container_segment_t *> *lst_segments = it_map->second;
          std::list<container_segment_t *>::iterator it_list = lst_segments->begin();
          while(it_list != lst_segments->end()) {
            if((*it_list)->start_chunk < c_seg->start_chunk) {
              it_list++;
            } else {
              break;
              lst_segments->insert(it_list, c_seg);
            }
          }
          lst_segments->insert(it_list, c_seg);
        } else {
          std::list<container_segment_t *> *lst_segments = new std::list<container_segment_t *>;
          lst_segments->push_back(c_seg);
          object_container_map->insert(it_map, std::pair<std::string, std::list<container_segment_t *> *>(bbos_name, lst_segments));
        }
        num_objs--;
      }
    }

    bbos_obj_t *create_bbos_cache_entry(const char *name, mkobj_flag_t type) {
      bbos_obj_t *obj = new bbos_obj_t;
      obj->lst_chunks = new std::list<chunk_info_t *>;
      obj->last_chunk_flushed = 0;
      obj->dirty_size = 0;
      obj->size = 0;
      obj->type = type;
      obj->cursor = 0;
      obj->marked_for_packing = false;
      obj->last_full_chunk = 0;
      pthread_mutex_init(&obj->mutex, NULL);
      pthread_mutex_lock(&obj->mutex);
      sprintf(obj->name, "%s", name);
      std::map<std::string, bbos_obj_t*>::iterator it_map = object_map->begin();
      object_map->insert(it_map, std::pair<std::string, bbos_obj_t*>(std::string(obj->name), obj));
      if(type == READ_OPTIMIZED) {
        individual_objects->push_back(obj);
      }
      return obj;
    }

  public:
    BuddyServer(const char *config_file) {
      assert(config_file != NULL);
      std::ifstream configuration(config_file);
      if(!configuration) {
        exit(-BB_CONFIG_ERROR);
      }
      int i = 0;
      const char *out_manifest;
      char knob[PATH_LEN];
      while(configuration >> knob) {
        switch (i) {
          case 0: strncpy(dw_mount_point, knob, PATH_LEN); // DW mount point
                  break;
          case 1: snprintf(output_manifest, PATH_LEN, "%s%s", dw_mount_point, knob); // output manifest file
                  break;
          case 2: port = atoi(knob);
                  break;
          case 3: snprintf(server_url, PATH_LEN, "tcp://%s:%d", knob, port);
                  server_network_class = NA_Initialize(server_url, NA_TRUE);
                  assert(server_network_class);
                  break;
          case 4: PFS_CHUNK_SIZE = strtoul(knob, NULL, 0);
                  break;
          case 5: OBJ_CHUNK_SIZE = strtoul(knob, NULL, 0);
                  break;
          case 6: hg_thread_pool_init(atoi(knob), &thread_pool);
                  break;
          case 7: binpacking_threshold = strtoul(knob, NULL, 0);
                  break;
          case 8: binpacking_policy = (binpacking_policy_t) atoi(knob);
                  break;
          case 9: OBJECT_DIRTY_THRESHOLD = strtoul(knob, NULL, 0);
                  break;
          case 10: CONTAINER_SIZE = strtoul(knob, NULL, 0);
                  break;
        }
        i++;
      }

      /* signal handling to capture ctrl + c */
      memset( &sa, 0, sizeof(sa) );
      sa.sa_handler = destructor_decorator;
      sigfillset(&sa.sa_mask);
      sigaction(SIGINT, &sa, NULL);

      object_map = new std::map<std::string, bbos_obj_t *>;
      object_container_map = new std::map<std::string, std::list<container_segment_t *> *>;
      dirty_bbos_size = 0;
      bs_obj = (void *) this;
      lru_objects = new std::list<bbos_obj_t *>;
      individual_objects = new std::list<bbos_obj_t *>;

      pthread_mutex_init(&bbos_mutex, NULL);

      server_hg_class = HG_Init_na(server_network_class);
      assert(server_hg_class);

      server_hg_context = HG_Context_create(server_hg_class);
      assert(server_hg_context);

      server_hg_progress_shutdown_flag = false;
      pthread_create(&progress_thread, NULL, rpc_progress_fn, NULL);
      server_mkobj_rpc_id = MERCURY_REGISTER(server_hg_class, "bbos_mkobj_rpc", bbos_mkobj_in_t, bbos_mkobj_out_t, bbos_mkobj_handler_decorator);
      server_append_rpc_id = MERCURY_REGISTER(server_hg_class, "bbos_append_rpc", bbos_append_in_t, bbos_append_out_t, bbos_append_handler_decorator);
      server_read_rpc_id = MERCURY_REGISTER(server_hg_class, "bbos_read_rpc", bbos_read_in_t, bbos_read_out_t, bbos_read_handler_decorator);
      server_get_size_rpc_id = MERCURY_REGISTER(server_hg_class, "bbos_get_size_rpc", bbos_get_size_in_t, bbos_get_size_out_t, bbos_get_size_handler_decorator);

      containers_built = 0;
      BINPACKING_SHUTDOWN = false;
      pthread_create(&binpacking_thread, NULL, binpacking_decorator, this);
    }

    ~BuddyServer() {
      while(!GLOBAL_RPC_SHUTDOWN) {
        sleep(1);
      }
      pthread_join(progress_thread, NULL);
      HG_Hl_finalize();
      hg_thread_pool_destroy(thread_pool);
      pthread_join(binpacking_thread, NULL);
      assert(!BINPACKING_SHUTDOWN);
      assert(dirty_bbos_size == 0);
      assert(dirty_individual_size == 0);
      build_global_manifest(output_manifest); // important for booting next time and reading
      printf("============= BBOS MEASUREMENTS of %s =============\n", server_url);
      printf("AVERAGE DW CHUNK RESPONSE TIME = %f ns\n", avg_chunk_response_time);
      printf("AVERAGE DW CONTAINER RESPONSE TIME = %f ns\n", avg_container_response_time);
      printf("AVERAGE APPEND LATENCY = %f ns\n", avg_append_latency);
      printf("AVERAGE TIME SPENT IDENTIFYING SEGMENTS TO BINPACK = %f ns\n", avg_binpack_time);
      printf("NUMBER OF %lu BYTE CHUNKS WRITTEN = %lu\n", PFS_CHUNK_SIZE, num_chunks_written);
      printf("NUMBER OF %lu BYTE CONTAINERS WRITTEN = %lu\n", CONTAINER_SIZE, num_containers_written);
      printf("NUMBER OF %lu BYTE APPENDS = %lu\n", OBJ_CHUNK_SIZE, num_appends);
      printf("NUMBER OF BINPACKINGS DONE = %lu\n", num_binpacks);
      printf("============================================\n");
      std::map<std::string, std::list<container_segment_t *> *>::iterator it_obj_cont_map = object_container_map->begin();
      while(it_obj_cont_map != object_container_map->end()) {
        std::list<container_segment_t *>::iterator it_c_segs = it_obj_cont_map->second->begin();
        while(it_c_segs != it_obj_cont_map->second->end()) {
          delete (*it_c_segs);
          it_c_segs++;
        }
        delete it_obj_cont_map->second;
        it_obj_cont_map++;
      }
      delete object_container_map;
      std::map<std::string, bbos_obj_t *>::iterator it_obj_map = object_map->begin();
      while(it_obj_map != object_map->end()) {
        assert(it_obj_map->second->dirty_size == 0);
        std::list<chunk_info_t *>::iterator it_chunks = it_obj_map->second->lst_chunks->begin();
        while(it_chunks != it_obj_map->second->lst_chunks->end()) {
          delete (*it_chunks);
          it_chunks++;
        }
        pthread_mutex_destroy(&(it_obj_map->second->mutex));
        delete it_obj_map->second;
        it_obj_map++;
      }
      pthread_mutex_destroy(&bbos_mutex);
      delete object_map;
      delete lru_objects;
      delete individual_objects;
    }

    std::list<binpack_segment_t> get_objects(container_flag_t type=COMBINED) {
      switch (type) {
        case COMBINED: if((BINPACKING_SHUTDOWN == true) && (dirty_bbos_size > 0)) {
                         return all_binpacking_policy();
                       }
                       switch (binpacking_policy) {
                         case RR_WITH_CURSOR: return rr_with_cursor_binpacking_policy();
                         case ALL: return all_binpacking_policy();
                       }
                       break;

        case INDIVIDUAL: return get_all_segments();
      }
      std::list<binpack_segment_t> segments;
      printf("Invalid binpacking policy selected!\n");
      return segments;
    }

    int build_container(const char *c_name,
      std::list<binpack_segment_t> lst_binpack_segments) {
      char c_path[PATH_LEN];
      snprintf(c_path, PATH_LEN, "%s%s", dw_mount_point, c_name);
      binpack_segment_t b_obj;
      size_t data_written = 0;
      off_t c_offset = 0;
      timespec chunk_diff_ts;
      timespec container_diff_ts;
      clock_gettime(CLOCK_REALTIME, &container_ts_before);
      FILE *fp = fopen(c_path, "w+");;
      assert(fp != NULL);
      std::list<binpack_segment_t>::iterator it_bpack = lst_binpack_segments.begin();
      fprintf(fp, "%lu\n", lst_binpack_segments.size());
      while(it_bpack != lst_binpack_segments.end()) {
        b_obj = *it_bpack;
        it_bpack++;
        fprintf(fp, "%s:%u:%u:%lu\n", b_obj.obj->name, b_obj.start_chunk, b_obj.end_chunk, c_offset);
        c_offset += (PFS_CHUNK_SIZE * (b_obj.end_chunk - b_obj.start_chunk));
      }

      it_bpack = lst_binpack_segments.begin();
      c_offset = 0;
      while(it_bpack != lst_binpack_segments.end()) {
        b_obj = *it_bpack;
        std::list<chunk_info_t *>::iterator it_chunks = b_obj.obj->lst_chunks->begin();
        while(it_chunks != b_obj.obj->lst_chunks->end() && (*it_chunks)->id < b_obj.start_chunk) {
          it_chunks++;
        }
        while(it_chunks != b_obj.obj->lst_chunks->end() && (*it_chunks)->id < b_obj.end_chunk) {
          //FIXME: write to DW in PFS_CHUNK_SIZE - https://github.com/hpc/libhio
          clock_gettime(CLOCK_REALTIME, &chunk_ts_before);
          data_written = fwrite((*it_chunks)->buf, sizeof(char), (*it_chunks)->size, fp);
          clock_gettime(CLOCK_REALTIME, &chunk_ts_after);
          num_chunks_written += 1;
          timespec_diff(&chunk_ts_before, &chunk_ts_after, &chunk_diff_ts);
          avg_chunk_response_time *= (num_chunks_written - 1);
          avg_chunk_response_time += ((chunk_diff_ts.tv_sec * 1000000000) + chunk_diff_ts.tv_nsec);
          avg_chunk_response_time /= num_chunks_written;
          assert(data_written == (*it_chunks)->size);
          free((*it_chunks)->buf);

          //FIXME: Ideally we would reduce dirty size after writing to DW,
          //       but here we reduce it when we choose to binpack itself.
          it_chunks++;
        }

        // populate the object_container_map
        container_segment_t *c_seg = new container_segment_t;
        strcpy(c_seg->container_name, c_name);
        c_seg->start_chunk = b_obj.start_chunk * (PFS_CHUNK_SIZE / OBJ_CHUNK_SIZE);
        c_seg->end_chunk = b_obj.end_chunk * (PFS_CHUNK_SIZE / OBJ_CHUNK_SIZE);
        c_seg->offset = c_offset;
        c_offset += (OBJ_CHUNK_SIZE * (b_obj.end_chunk - b_obj.start_chunk));
        std::map<std::string, std::list<container_segment_t *> *>::iterator it_map = object_container_map->find(std::string(b_obj.obj->name));
        if(it_map != object_container_map->end()) {
          // entry already present in global manifest. Just add new segments.
          it_map->second->push_back(c_seg);
        } else {
          std::string bbos_name(b_obj.obj->name);
          std::list<container_segment_t *> *lst_segments = new std::list<container_segment_t *>;
          lst_segments->push_back(c_seg);
          object_container_map->insert(it_map, std::pair<std::string, std::list<container_segment_t *> *>(bbos_name, lst_segments));
        }
        it_bpack++;
      }
      fclose(fp);
      clock_gettime(CLOCK_REALTIME, &container_ts_after);
      num_containers_written += 1;
      timespec_diff(&container_ts_before, &container_ts_after, &container_diff_ts);
      avg_container_response_time *= (num_containers_written - 1);
      avg_container_response_time += ((container_diff_ts.tv_sec * 1000000000) + container_diff_ts.tv_nsec);
      avg_container_response_time /= num_containers_written;
      return 0;
    }

    int mkobj(const char *name, mkobj_flag_t type=WRITE_OPTIMIZED) {
      // Initialize an in-memory object
      if(create_bbos_cache_entry(name, type) == NULL) {
        return -BB_ERROBJ;
      }
      std::map<std::string, bbos_obj_t *>::iterator it_obj_map = object_map->find(std::string(name));
      pthread_mutex_unlock(&(it_obj_map->second->mutex));
      return 0;
    }

    int lock_server() {
      return pthread_mutex_lock(&bbos_mutex);
    }

    int unlock_server() {
      return pthread_mutex_unlock(&bbos_mutex);
    }

    /* Get total dirty data size */
    size_t get_dirty_size() {
      return dirty_bbos_size;
    }

    /* Get number of individual objects. */
    uint32_t get_individual_obj_count() {
      return individual_objects->size();
    }

    /* Get binpacking threshold */
    size_t get_binpacking_threshold() {
      return binpacking_threshold;
    }

    /* Get binpacking policy */
    size_t get_binpacking_policy() {
      return binpacking_policy;
    }

    /* Get name of next container */
    const char *get_next_container_name(char *path, container_flag_t type) {
      //TODO: get container name from a microservice
      switch (type) {
        case COMBINED: snprintf(path, PATH_LEN, "bbos_%d.con.write", containers_built++);
                       break;
        case INDIVIDUAL: snprintf(path, PATH_LEN, "bbos_%d.con.read", containers_built++);
                         break;
      }
      return (const char *)path;
    }

    /* Get size of BB object */
    size_t get_size(const char *name) {
      std::map<std::string, bbos_obj_t *>::iterator it_obj_map = object_map->find(std::string(name));
      if(it_obj_map == object_map->end()) {
        std::map<std::string, std::list<container_segment_t *> *>::iterator it_map = object_container_map->find(std::string(name));
        assert(it_map != object_container_map->end());
        bbos_obj_t *obj = create_bbos_cache_entry(name, WRITE_OPTIMIZED); //FIXME: place correct object type
        std::list<container_segment_t *> *lst_segments = it_map->second;
        std::list<container_segment_t *>::iterator it_segs = lst_segments->begin();
        while(it_segs != it_map->second->end()) {
          container_segment_t *c_seg = (*it_segs);
          for(int i=c_seg->start_chunk; i<c_seg->end_chunk; i++) {
            chunk_info_t *chunk = new chunk_info_t;
            chunk->buf = NULL;
            chunk->size = 0;
            chunk->id = i;
            obj->lst_chunks->push_back(chunk);
            obj->size += PFS_CHUNK_SIZE;
          }
          it_segs++;
        }
        pthread_mutex_unlock(&(obj->mutex));
        return obj->size;
      } else {
        pthread_mutex_lock(&(it_obj_map->second->mutex));
      }
      pthread_mutex_unlock(&(it_obj_map->second->mutex));
      return it_obj_map->second->size;
    }

    /* Append to a BB object */
    size_t append(const char *name, void *buf, size_t len) {
      bbos_obj_t *obj = object_map->find(std::string(name))->second;
      assert(obj != NULL);
      pthread_mutex_lock(&obj->mutex);
      chunk_info_t *last_chunk = obj->lst_chunks->back();
      size_t data_added = 0;
      size_t data_size_for_chunk = 0;
      chunkid_t next_chunk_id = 0;
      if(obj->lst_chunks->empty() || (last_chunk->size == PFS_CHUNK_SIZE)) {
        // we need to create a new chunk and append into it.
        if(!obj->lst_chunks->empty()) {
          next_chunk_id = last_chunk->id + 1;
          obj->last_full_chunk = last_chunk->id;
        }
        chunk_info_t *chunk = make_chunk(next_chunk_id);
        obj->lst_chunks->push_back(chunk);
        last_chunk = obj->lst_chunks->back();
      }
      data_added += add_data(last_chunk, buf, OBJ_CHUNK_SIZE);
      //NOTE: we always assume data will be received in OBJ_CHUNK_SIZE
      obj->size += data_added;
      obj->dirty_size += data_added;
      if(obj->dirty_size > OBJECT_DIRTY_THRESHOLD && obj->type == WRITE_OPTIMIZED && obj->marked_for_packing == false) {
        // add to head of LRU list for binpacking consideration
        obj->marked_for_packing = true;
        lru_objects->push_front(obj);
      }
      switch (obj->type) {
        case WRITE_OPTIMIZED: dirty_bbos_size += data_added;
                              break;
        case READ_OPTIMIZED: dirty_individual_size += data_added;
                             break;
      }
      pthread_mutex_unlock(&obj->mutex);
      return data_added;
    }

    /* Read from a BB object */
    size_t read(const char *name, void *buf, off_t offset, size_t len) {
      bbos_obj_t *obj = object_map->find(std::string(name))->second;
      assert(obj != NULL);
      pthread_mutex_lock(&obj->mutex);
      if(offset >= obj->size) {
        return -BB_INVALID_READ;
      }
      size_t data_read = 0;
      size_t data_to_be_read = 0;
      std::list<chunk_info_t *>::iterator it_chunks = obj->lst_chunks->begin();
      chunkid_t chunk_num = offset / PFS_CHUNK_SIZE;
      int chunk_obj_offset = (offset - (chunk_num * PFS_CHUNK_SIZE)) / OBJ_CHUNK_SIZE;
      for(int i = 0; i < chunk_num; i++) {
        it_chunks++;
      }
      chunk_info_t *chunk = *it_chunks;
      if(chunk->buf == NULL) {
        // first fetch data from container into memory
        std::list<container_segment_t *> *lst_segments = object_container_map->find(std::string(obj->name))->second;
        std::list<container_segment_t *>::iterator it_segs = lst_segments->begin();
        container_segment_t *seg = NULL;
        while(it_segs != lst_segments->end()) {
          seg = *it_segs;
          if(chunk_num <= seg->start_chunk && chunk_num < seg->end_chunk) {
            break;
          }
          it_segs++;
        }
        off_t c_offset = seg->offset;
        c_offset += (PFS_CHUNK_SIZE * (chunk_num - seg->start_chunk));
        chunk->buf = (void *) malloc (sizeof(char) * PFS_CHUNK_SIZE);
        assert(chunk->buf != NULL);
        FILE *fp_seg = fopen(seg->container_name, "r");
        int seek_ret = fseek(fp_seg, c_offset, SEEK_SET);
        assert(seek_ret == 0);
        size_t read_size = fread(chunk->buf, PFS_CHUNK_SIZE, 1, fp_seg);
        assert(read_size == 1);
        fclose(fp_seg);
      }
      data_read += get_data(chunk, buf, offset - (PFS_CHUNK_SIZE * chunk_num) + (OBJ_CHUNK_SIZE * chunk_obj_offset), data_to_be_read);
      pthread_mutex_unlock(&obj->mutex);
      return data_read;
    }
};

static HG_THREAD_RETURN_TYPE bbos_mkobj_handler(void *args) {
  hg_handle_t *handle = (hg_handle_t *) args;
  bbos_mkobj_out_t out;
  bbos_mkobj_in_t in;
  int ret = HG_Get_input(*handle, &in);
  assert(ret == HG_SUCCESS);
  out.status = ((BuddyServer *)bs_obj)->mkobj(in.name, (mkobj_flag_t) in.type);
  ret = HG_Respond(*handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  free(args);
  return (hg_thread_ret_t) NULL;
}

static hg_return_t bbos_append_decorator(const struct hg_cb_info *info) {
  struct bbos_append_cb *append_info = (struct bbos_append_cb*)info->arg;
  bbos_append_out_t out;
  timespec append_diff_ts;
  BuddyServer *bs = (BuddyServer *) bs_obj;
  char name[PATH_LEN];
  int ret;
  size_t len = 0;
  clock_gettime(CLOCK_REALTIME, &append_ts_before);
  out.size = bs->append(append_info->name, append_info->buffer, append_info->size);
  clock_gettime(CLOCK_REALTIME, &append_ts_after);
  num_appends += 1;
  timespec_diff(&append_ts_before, &append_ts_after, &append_diff_ts);
  avg_append_latency *= (num_appends - 1);
  avg_append_latency += ((append_diff_ts.tv_sec * 1000000000) + append_diff_ts.tv_nsec);
  avg_append_latency /= num_appends;
  ret = HG_Respond(append_info->handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  HG_Destroy(append_info->handle);
  free(append_info->buffer);
  free(append_info);
  return HG_SUCCESS;
}

static HG_THREAD_RETURN_TYPE bbos_append_handler(void *args) {
  bbos_append_in_t in;
  hg_handle_t *handle = (hg_handle_t *) args;
  struct bbos_append_cb *append_info = (struct bbos_append_cb *) malloc (sizeof(struct bbos_append_cb));
  int ret = HG_Get_input(*handle, &in);
  assert(ret == HG_SUCCESS);
  size_t input_size = HG_Bulk_get_size(in.bulk_handle);
  append_info->buffer = (void *) calloc(1, input_size);
  assert(append_info->buffer);
  append_info->handle = *handle;
  struct hg_info *hgi = HG_Get_info(*handle);
  assert(hgi);
  snprintf(append_info->name, PATH_LEN, "%s", in.name);
  append_info->size = input_size;
  ret = HG_Bulk_create(hgi->hg_class, 1,
              &(append_info->buffer), &(input_size),
  	          HG_BULK_WRITE_ONLY, &(append_info->bulk_handle));
  assert(ret == HG_SUCCESS);
  ret = HG_Bulk_transfer(hgi->context, bbos_append_decorator,
          append_info, HG_BULK_PULL, hgi->addr, in.bulk_handle, 0,
          append_info->bulk_handle, 0, input_size, HG_OP_ID_IGNORE);
  assert(ret == HG_SUCCESS);
  free(args);
  return (hg_thread_ret_t) NULL;
}

static hg_return_t bbos_read_decorator(const struct hg_cb_info *info) {
  bbos_read_out_t out;
  bbos_read_cb *read_info = (struct bbos_read_cb *) info->arg;
  out.size = read_info->size;
  int ret = HG_Respond(read_info->handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  HG_Destroy(read_info->handle);
  free(read_info);
  (void)ret;
  return HG_SUCCESS;
}

static HG_THREAD_RETURN_TYPE bbos_read_handler(void *args) {
  bbos_read_in_t in;
  hg_handle_t *handle = (hg_handle_t *) args;
  struct bbos_read_cb *read_info = (struct bbos_read_cb *) malloc (sizeof(struct bbos_read_cb));
  int ret = HG_Get_input(*handle, &in);
  assert(ret == HG_SUCCESS);
  void *outbuf = (void *) calloc(1, in.size);
  assert(outbuf);
  read_info->size = ((BuddyServer *)bs_obj)->read(in.name, outbuf, in.offset, in.size);
  read_info->remote_bulk_handle = in.bulk_handle;
  read_info->handle = *handle;
  struct hg_info *hgi = HG_Get_info(*handle);
  assert(hgi);
  ret = HG_Bulk_create(hgi->hg_class, 1,
              &(outbuf), &(read_info->size),
  	          HG_BULK_WRITE_ONLY, &(read_info->bulk_handle));
  assert(ret == HG_SUCCESS);
  ret = HG_Bulk_transfer(hgi->context, bbos_read_decorator, read_info,
          HG_BULK_PUSH, hgi->addr, in.bulk_handle, 0, read_info->bulk_handle,
          0, read_info->size, HG_OP_ID_IGNORE);
  assert(ret == HG_SUCCESS);
  free(outbuf);
  free(args);
  return (hg_thread_ret_t) NULL;
}

static HG_THREAD_RETURN_TYPE bbos_get_size_handler(void *args) {
  hg_handle_t *handle = (hg_handle_t *) args;
  bbos_get_size_out_t out;
  bbos_get_size_in_t in;
  int ret = HG_Get_input(*handle, &in);
  assert(ret == HG_SUCCESS);
  out.size = ((BuddyServer *)bs_obj)->get_size(in.name);
  ret = HG_Respond(*handle, NULL, NULL, &out);
  assert(ret == HG_SUCCESS);
  (void)ret;
  free(args);
  return (hg_thread_ret_t) NULL;
}

static hg_return_t bbos_mkobj_handler_decorator(hg_handle_t handle) {
  struct hg_thread_work *work = (struct hg_thread_work *) malloc (sizeof(struct hg_thread_work));
  work->func = bbos_mkobj_handler;
  work->args = (void *) malloc (sizeof(hg_handle_t));
  memcpy(work->args, &handle, sizeof(handle));
  hg_thread_pool_post(thread_pool, work);
  return HG_SUCCESS;
}

static hg_return_t bbos_read_handler_decorator(hg_handle_t handle) {
  struct hg_thread_work *work = (struct hg_thread_work *) malloc (sizeof(struct hg_thread_work));
  work->func = bbos_read_handler;
  work->args = (void *) malloc (sizeof(hg_handle_t));
  memcpy(work->args, &handle, sizeof(handle));
  hg_thread_pool_post(thread_pool, work);
  return HG_SUCCESS;
}

static hg_return_t bbos_append_handler_decorator(hg_handle_t handle) {
  struct hg_thread_work *work = (struct hg_thread_work *) malloc (sizeof(struct hg_thread_work));
  work->func = bbos_append_handler;
  work->args = (void *) malloc (sizeof(hg_handle_t));
  memcpy(work->args, &handle, sizeof(handle));
  hg_thread_pool_post(thread_pool, work);
  return HG_SUCCESS;
}

static hg_return_t bbos_get_size_handler_decorator(hg_handle_t handle) {
  struct hg_thread_work *work = (struct hg_thread_work *) malloc (sizeof(struct hg_thread_work));
  work->func = bbos_get_size_handler;
  work->args = (void *) malloc (sizeof(hg_handle_t));
  memcpy(work->args, &handle, sizeof(handle));
  hg_thread_pool_post(thread_pool, work);
  return HG_SUCCESS;
}

static void destructor_decorator(int) {
  GLOBAL_RPC_SHUTDOWN = true;
  BINPACKING_SHUTDOWN = true;
}

static void invoke_binpacking(BuddyServer *bs, container_flag_t type) {
  std::list<binpack_segment_t> lst_binpack_segments;
  /* we need to binpack */
  bs->lock_server();
  /* identify segments of objects to binpack. */
  timespec binpack_diff_ts;
  clock_gettime(CLOCK_REALTIME, &binpack_ts_before);
  lst_binpack_segments = bs->get_objects(type);
  clock_gettime(CLOCK_REALTIME, &binpack_ts_after);
  num_binpacks += 1;
  timespec_diff(&binpack_ts_before, &binpack_ts_after, &binpack_diff_ts);
  avg_binpack_time *= (num_binpacks - 1);
  avg_binpack_time += ((binpack_diff_ts.tv_sec * 1000000000) + binpack_diff_ts.tv_nsec);
  avg_binpack_time /= num_binpacks;
  bs->unlock_server();
  char path[PATH_LEN];
  if(lst_binpack_segments.size() > 0) {
    bs->build_container(bs->get_next_container_name(path, type),
                        lst_binpack_segments);
    //TODO: stage out DW file to lustre - refer https://github.com/hpc/libhio.
  }
}

static void* binpacking_decorator(void *args) {
  BuddyServer *bs = (BuddyServer *)args;
  printf("\nStarting binpacking thread...\n");
  do {
    if(bs->get_dirty_size() >= bs->get_binpacking_threshold()) {
      invoke_binpacking(bs, COMBINED);
    }
    sleep(1);
  } while(!BINPACKING_SHUTDOWN);
  /* pack whatever is remaining */
  invoke_binpacking(bs, COMBINED);
  while (bs->get_individual_obj_count() > 0) {
    invoke_binpacking(bs, INDIVIDUAL);
  }
  printf("Shutting down binpacking thread\n");
  BINPACKING_SHUTDOWN = false;
  pthread_exit(NULL);
}

} // namespace bb
} // namespace pdlfs
