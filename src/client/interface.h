/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

namespace pdlfs {
namespace bb {

typedef uint64_t oid_t;

class Client
{
  public:
    virtual ~Client() {}

    /* Initialize BB client. */
    virtual int init();

    /*
     * Open a BB client file for write. Flags can be O_TRICKLE, O_BULK.
     * O_TRICKLE performs RPC to BB server on every write.
     * O_BULK performs RPC to server on close.
     */
    virtual oid_t open(char *path, int flag);

    /* Write to an open BB client file. */
    virtual size_t write(oid_t id, const void *buf, off_t offset, size_t len);

    /* Read from an open BB client file. */
    virtual size_t read(oid_t id, const void *buf, off_t offset, size_t len);

    /* Append to an open BB client file. */
    virtual size_t append(oid_t id, const void *buf, size_t len);

    /* Destroy a BB object. */
    virtual int destory(char *path);

    /* Close a BB client file. */
    virtual int close(oid_t);

    /* Sync a BB file to underlying PFS */
    virtual int sync(oid_t);
};

}
}
