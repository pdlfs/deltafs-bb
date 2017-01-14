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
#include "src/server/buddyserver.h"
//#include "src/server/interface.h"

namespace pdlfs {
namespace bb {

class BuddyServer//: public Server
{
  private:
    std::map<std::string, bbos_obj_t *> *object_map;
    std::map<std::string, std::list<container_segment_t *> *> *object_container_map;
    //oid_t running_oid;
    size_t PFS_CHUNK_SIZE;
    size_t OBJ_CHUNK_SIZE;
    size_t dirty_bbos_size;

    chunk_info_t *make_chunk(chunkid_t id) {
      chunk_info_t *new_chunk = new chunk_info_t;
      new_chunk->id = id;
      new_chunk->buf = (void *) malloc(sizeof(char) * OBJ_CHUNK_SIZE);
      if(new_chunk == NULL) {
        return NULL;
      }
      new_chunk->size = 0;
      return new_chunk;
    }

    size_t add_data(chunk_info_t *chunk, void *buf, size_t len) {
      // Checking of whether data can fit into this chunk has to be done outside
      assert(memcpy((char *)chunk->buf + chunk->size, buf, len) != NULL);
      chunk->size += len;
      return len;
    }

    size_t get_data(chunk_info_t *chunk, void *buf, off_t offset, size_t len) {
      assert(memcpy(buf, (char *)chunk->buf + offset, len) != NULL);
      return len;
    }

    std::list<binpack_segment_t> get_objects(binpacking_policy policy) {
      std::list<binpack_segment_t> segments;
      //FIXME: hardcoded to all objects
      std::map<std::string, bbos_obj_t *>::iterator it_map = object_map->begin();
      while(it_map != object_map->end()) {
        binpack_segment_t seg;
        seg.obj = (*it_map).second;
        seg.start_chunk = 0;
        seg.end_chunk = seg.obj->lst_chunks->size();
        segments.push_back(seg);
        it_map++;
      }
      return segments;
    }

    int build_container(const char *c_name, std::list<binpack_segment_t> lst_binpack_segments) {
      //TODO: get container name from a microservice
      FILE *fp = fopen(c_name, "w+");
      assert(fp != NULL);

      binpack_segment_t b_obj;
      off_t c_offset = 0;
      std::list<binpack_segment_t>::iterator it_bpack = lst_binpack_segments.begin();
      fprintf(fp, "%lu\n", lst_binpack_segments.size());
      while(it_bpack != lst_binpack_segments.end()) {
        b_obj = *it_bpack;
        it_bpack++;
        fprintf(fp, "%s:%u:%u:%lu\n", b_obj.obj->name, b_obj.start_chunk, b_obj.end_chunk, c_offset);
        c_offset += (OBJ_CHUNK_SIZE * (b_obj.end_chunk - b_obj.start_chunk));
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
          //FIXME: write to DW in PFS_CHUNK_SIZE
          fwrite((*it_chunks)->buf, sizeof(char), OBJ_CHUNK_SIZE, fp);
          b_obj.obj->dirty_size -= OBJ_CHUNK_SIZE;
          dirty_bbos_size -= OBJ_CHUNK_SIZE;
          it_chunks++;
        }

        // populate the object_container_map
        container_segment_t *c_seg = new container_segment_t;
        strcpy(c_seg->container_name, c_name);
        c_seg->start_chunk = b_obj.start_chunk;
        c_seg->end_chunk = b_obj.end_chunk;
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
      return 0;
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
    int build_object_container_map(char *container_name) {
      std::ifstream container(container_name);
      if(!container) {
        return -BB_ENOCONTAINER;
      }
      std::string line;
      std::string token;
      std::string bbos_name;

      int num_objs = 0;
      container >> num_objs; // first line contains number of objects.
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

    bbos_obj_t *create_bbos_cache_entry(const char *name) {
      bbos_obj_t *obj = new bbos_obj_t;
      //obj->id = running_oid++;
      obj->lst_chunks = new std::list<chunk_info_t *>;
      obj->last_chunk_flushed = 0;
      obj->dirty_size = 0;
      obj->size = 0;
      sprintf(obj->name, "%s", name);
      std::map<std::string, bbos_obj_t*>::iterator it_map = object_map->begin();
      object_map->insert(it_map, std::pair<std::string, bbos_obj_t*>(std::string(obj->name), obj));
      return obj;
    }

    void destroy_data_structures() {
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
        std::list<chunk_info_t *>::iterator it_chunks = it_obj_map->second->lst_chunks->begin();
        while(it_chunks != it_obj_map->second->lst_chunks->end()) {
          delete (*it_chunks);
          it_chunks++;
        }
        delete it_obj_map->second;
        it_obj_map++;
      }
      delete object_map;
    }

  public:
    BuddyServer(size_t pfs_chunk_size=8388608, size_t obj_chunk_size=2097152) {
      //running_oid = 0;
      OBJ_CHUNK_SIZE = obj_chunk_size;
      PFS_CHUNK_SIZE = pfs_chunk_size;
      object_map = new std::map<std::string, bbos_obj_t *>;
      object_container_map = new std::map<std::string, std::list<container_segment_t *> *>;
      dirty_bbos_size = 0;
    }

    ~BuddyServer() {
      destroy_data_structures();
    }

    int mkobj(char *name) {
      // Initialize an in-memory object
      if(create_bbos_cache_entry(name) == NULL) {
        return -BB_ERROBJ;
      }
      return 0;
    }

    //int init(int fan_in);

    //virtual int listen(void *args);

    /* Write to a BB object */
    //virtual size_t write(oid_t id, void *buf, off_t offset, size_t len);


    /* Get size of BB object */
    size_t get_size(const char *name) {
      std::map<std::string, bbos_obj_t *>::iterator it_obj_map = object_map->find(std::string(name));
      if(it_obj_map == object_map->end()) {
        std::map<std::string, std::list<container_segment_t *> *>::iterator it_map = object_container_map->find(std::string(name));
        assert(it_map != object_container_map->end());
        bbos_obj_t *obj = create_bbos_cache_entry(name);
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
            obj->size += OBJ_CHUNK_SIZE;
          }
          it_segs++;
        }
        return obj->size;
      }
      return it_obj_map->second->size;
    }

    /* Append to a BB object */
    size_t append(const char *name, void *buf, size_t len) {
      bbos_obj_t *obj = object_map->find(std::string(name))->second;
      chunk_info_t *last_chunk = obj->lst_chunks->back();
      size_t data_added = 0;
      size_t data_size_for_chunk = 0;
      chunkid_t next_chunk_id = 0;
      if(obj->lst_chunks->empty() || (last_chunk->size == OBJ_CHUNK_SIZE)) {
        // we need to create a new chunk and append into it.
        if(!obj->lst_chunks->empty()) {
          next_chunk_id = last_chunk->id + 1;
        }
        chunk_info_t *chunk = make_chunk(next_chunk_id);
        obj->lst_chunks->push_back(chunk);
        last_chunk = obj->lst_chunks->back();
      }
      if(len <= (OBJ_CHUNK_SIZE - last_chunk->size)) {
        data_size_for_chunk = len;
      } else {
        data_size_for_chunk = OBJ_CHUNK_SIZE - last_chunk->size;
      }
      data_added += add_data(last_chunk, buf, data_size_for_chunk);
      obj->size += data_added;
      obj->dirty_size += data_added;
      dirty_bbos_size += data_added;
      return data_added;
    }

    /* Read from a BB object */
    size_t read(const char *name, void *buf, off_t offset, size_t len) {
      bbos_obj_t *obj = object_map->find(std::string(name))->second;
      if(offset >= obj->size) {
        return -BB_INVALID_READ;
      }
      size_t data_read = 0;
      size_t data_to_be_read = 0;
      std::list<chunk_info_t *>::iterator it_chunks = obj->lst_chunks->begin();
      chunkid_t chunk_num = offset / OBJ_CHUNK_SIZE;
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
        c_offset += (OBJ_CHUNK_SIZE * (chunk_num - seg->start_chunk));
        chunk->buf = (void *) malloc (sizeof(char) * OBJ_CHUNK_SIZE);
        assert(chunk->buf != NULL);
        FILE *fp_seg = fopen(seg->container_name, "r");
        assert(fseek(fp_seg, c_offset, SEEK_SET) == c_offset);
        assert(fread(chunk->buf, OBJ_CHUNK_SIZE, 1, fp_seg) == 1);
        fclose(fp_seg);
      }
      if((offset + len) < ((chunk_num + 1) * OBJ_CHUNK_SIZE)) {
        data_to_be_read = len;
      } else {
        data_to_be_read = ((chunk_num + 1) * OBJ_CHUNK_SIZE) - offset;
      }
      data_read += get_data(chunk, buf, offset - (OBJ_CHUNK_SIZE * chunk_num), data_to_be_read);
      return data_read;
    }

    /* Sync a BB object to underlying PFS */
    //virtual int sync(oid_t id);

    /* Construct underlying PFS object by stitching BB object fragments */
    pfsid_t binpack(const char *container_name, binpacking_policy policy) {
      //TODO: to be called via a separate thread based on dirty_bbos_size threshold
      build_container(container_name, get_objects(policy));
    }

    /* Stage in file from PFS to BB */
    //virtual oid_t *stage_in(pfsid_t pid, stage_in_policy policy);

    /* Stage out file from BB to PFS */
    //virtual int stage_out(pfsid_t pid, stage_out_policy policy);

    /* Destroy BB server instance. */
    //virtual int destroy();

    /* Shutdown BB server instance */
    int shutdown(const char *manifest_name) {
      build_global_manifest(manifest_name);
      return 0;
    }

    /* Bootstrap from given global manifest */
    int bootstrap(const char *manifest_name) {
      std::ifstream manifest(manifest_name);
      if(!manifest) {
        return -BB_ENOMANIFEST;
      }

      char container_name[PATH_LEN];
      while(manifest >> container_name) {
        build_object_container_map(container_name);
      }
    }
};

} // namespace bb
} // namespace pdlfs
