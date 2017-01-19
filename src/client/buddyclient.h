/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "../rpc/rpc_engine.h"
// #include "../rpc/rpc.h"

namespace pdlfs {
namespace bb {

#define PATH_LEN 256

enum ACTION {MKOBJ, APPEND, READ};

/* struct used to carry state of overall operation across callbacks */
typedef struct {
    int value;
    hg_size_t size;
    void* buffer;
    hg_bulk_t bulk_handle;
    hg_handle_t handle;
} bbos_rpc_state_t;

} // namespace bb
} // namespace pdlfs
