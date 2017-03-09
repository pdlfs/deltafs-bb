/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mercury.h>
#include <mercury_macros.h>

#include "bbos/bbos_api.h"

namespace pdlfs {
namespace bb {

/* mkobj */
MERCURY_GEN_PROC( bbos_mkobj_in_t,     ((hg_const_string_t)(name))
                                       ((hg_bool_t)(readopt)) )
MERCURY_GEN_PROC( bbos_mkobj_out_t,    ((hg_int32_t)(status)) )

/* append */
MERCURY_GEN_PROC( bbos_append_in_t,    ((hg_const_string_t)(name))
                                       ((hg_bulk_t)(bulk_handle)) )
MERCURY_GEN_PROC( bbos_append_out_t,   ((hg_int64_t)(size)) )

/* read */
MERCURY_GEN_PROC( bbos_read_in_t,      ((hg_const_string_t)(name))
                                       ((hg_int64_t)(offset))
                                       ((hg_uint64_t)(size))
                                       ((hg_bulk_t)(bulk_handle)) )
MERCURY_GEN_PROC( bbos_read_out_t,     ((hg_int64_t)(size)) )

/* get_size */
MERCURY_GEN_PROC( bbos_get_size_in_t,  ((hg_const_string_t)(name)) )
MERCURY_GEN_PROC( bbos_get_size_out_t, ((hg_int64_t)(size)) )

}  // namespace bb
}  // namespace pdlfs
