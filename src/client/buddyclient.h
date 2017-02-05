/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_macros.h>
#include <mercury_request.h>
#include <mercury_hl.h>
#include <mercury_hl_macros.h>
#include <mercury_thread.h>
#include <mercury_proc_string.h>
#include <mercury_config.h>

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

} // namespace bb
} // namespace pdlfs
