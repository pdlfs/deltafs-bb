/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <iostream>

namespace pdlfs {
namespace bb {

class Server
{
  public:
    virtual ~ServerInterface() {}

    /* Initialize BB server with the compute node fan in. */
    virtual int bb_init();

    /* Handler for the RPC on the BB server */
    virtual int bb_listen(char *path, int flag);

    /* Write to a particular stream ID. */
    virtual int bb_write(int s_id, const void *buf, size_t len);

    /* Commit a particular steam ID to persistent storage. */
    virtual int bb_sync(int s_id);

    /* Close a particluar stream ID and remove all its state. */
    virtual int bb_destroy(int s_id);
};

} // namespace bb
} // namespace pdlfs
