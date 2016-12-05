/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <iostream.h>

namespace pdlfs {
namespace bb {

class Client
{
  public:
    virtual ~DeltaFSBBClientInterface() {}

    /* Initialize BB client with required number of parallel logs. */
    virtual int bb_init(int num_parallel_logs);

    /*
     * Open a BB client file for write. Flags can be O_TRICKLE, O_BULK.
     * O_TRICKLE performs RPC to BB server on every write.
     * O_BULK performs RPC to server on close.
     */
    virtual int bb_open(char *path, int flag);

    /* Write to an open BB client file. */
    virtual int bb_write(int fd, const void *buf, size_t len);

    /* Close a BB client file. */
    virtual int bb_close(int fd);
};

}
}
