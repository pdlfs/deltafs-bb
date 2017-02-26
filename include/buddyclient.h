/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <mercury_hl_macros.h>
/* Avoid compilation warning */
#ifndef _WIN32
    #undef _GNU_SOURCE
#endif
#include <mercury_thread.h>
#include <mercury_proc_string.h>
#include <mercury_config.h>
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

namespace pdlfs {
namespace bb {

#define PATH_LEN 256

enum mkobj_flag_t {READ_OPTIMIZED, WRITE_OPTIMIZED};

enum ACTION {MKOBJ, APPEND, READ, GET_SIZE};

#define BB_CONFIG_ERROR 3

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

class BuddyClient
{
  private:
    hg_thread_t progress_thread;
    int port;

  public:
    BuddyClient();
    ~BuddyClient();
    int mkobj(const char *name, mkobj_flag_t type=WRITE_OPTIMIZED);
    size_t append(const char *name, void *buf, size_t len);
    size_t read(const char *name, void *buf, off_t offset, size_t len);
    int get_size(const char *name);
};

} // namespace bb
} // namespace pdlfs
