/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <cstdint>
#include <cstdio>

namespace pdlfs {
namespace bb {

typedef uint64_t oid_t;
typedef uint64_t pfsid_t;
enum binpacking_policy { GREEDY };
enum stage_out_policy { SEQ_OUT, PAR_OUT };
enum stage_in_policy { SEQ_IN, PAR_IN };

class Server
{
  public:
    virtual ~Server() {}

    /* Initialize BB server with the compute node fan in. */
    virtual int init(int fan_in);

    /* Handler for the RPC on the BB server */
    virtual int listen(void *args);

    /* Make a BB object */
    virtual oid_t mkobj(void *args);

    /* Write to a BB object */
    virtual size_t write(oid_t id, void *buf, off_t offset, size_t len);

    /* Read from a BB object */
    virtual size_t read(oid_t id, void *buf, off_t offset, size_t len);

    /* Sync a BB object to underlying PFS */
    virtual int sync(oid_t id);

    /* Evict an object from BB to underlying PFS and free space */
    virtual int evict(oid_t id);

    /* Construct underlying PFS object by stitching BB object fragments */
    virtual pfsid_t binpack(oid_t *lst_id, binpacking_policy policy);

    /* Stage in file from PFS to BB */
    virtual oid_t *stage_in(pfsid_t pid, stage_in_policy policy);

    /* Stage out file from BB to PFS */
    virtual int stage_out(pfsid_t pid, stage_out_policy policy);

    /* Destroy BB server instance. */
    virtual int destroy();
};

} // namespace bb
} // namespace pdlfs
