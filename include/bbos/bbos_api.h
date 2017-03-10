/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

/*
 * bbos_api.h  client API for RPC access to burst buffer i/o server
 * 07-Mar-2017
 */

#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

/* bbos_handle_t: opaque pointer to bbos client state */
typedef void *bbos_handle_t;

/* error codes (all negative) */
#define BB_SUCCESS        0          /* successful return */
#define BB_FAILED        -1          /* generic failure */
#define BB_INVALID_READ  -2          /* read past EOF */
#define BB_ENOCONTAINER  -3          /* container not present */
#define BB_ERROBJ        -4          /* mkobj failed */
#define BB_ENOOBJ        -5          /* no such object */

/*
 * bbos objects can be optimized for reading or writing when they are
 * created.  read optimized is typically for index-like structures
 * (that get read all at once), while write optimized is for data.
 */
enum bbos_mkobj_flag_t {
    READ_OPTIMIZED,
    WRITE_OPTIMIZED
};

/**
 * bbos_init: init bbos client and return a handle
 * @param local local mercury URL (to use with HG_Init())
 * @param server mercury-style URL of server to connect to
 * @param bbosp returned handle is put in *bbosp
 * @return BB_SUCCESS or an error code
 */
int bbos_init(const char *local, const char *server, bbos_handle_t *bbosp);

/**
 * bbos_finalize: finialize bbos client
 * @param bbos bbos client to finalize
 */
void bbos_finalize(bbos_handle_t bbos);

/**
 * bbos_mkobj: make a new bbos log object
 * @param bbos client handle
 * @param name name of new object
 * @param flag optimization to use for new object
 * @return BB_SUCCESS or an error code
 */
int bbos_mkobj(bbos_handle_t bbos, const char *name, bbos_mkobj_flag_t flag);

/**
 * bbos_append: append data to bbos object.
 * XXX: is short append possible or is it all or nothing?
 *
 * @param bbos client handle
 * @param name name of object to append to
 * @param buf buffer to append
 * @param len length of buffer
 * @return #bytes appended or error code if <0
 */
ssize_t bbos_append(bbos_handle_t bbos, const char *name, void *buf,
                    size_t len);

/**
 * bbos_read: read data from bbos object
 * XXX: is short read possible or is it all or nothing?
 *
 * @param bbos client handle
 * @param name name of object to read from
 * @param buf where to place read data
 * @param offset offset in bbos object to read from
 * @param len number of bytes to read
 * @return number of bytes read or error code if <0
 */
ssize_t bbos_read(bbos_handle_t bbos, const char *name, void *buf,
                  off_t offset, size_t len);

/**
 * bbos_get_size: get current size of bbos object
 * @param bbos client handle
 * @param name name of object to size
 * @return size of object or error code if <0
 */
off_t bbos_get_size(bbos_handle_t bbos, const char *name);

#ifdef __cplusplus
}   /* extern "C" */
#endif
