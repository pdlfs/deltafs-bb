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

/* XXX: correct place for these? */
#define BB_ENOSPC 1
#define BB_SYNC_ERROR 2
#define BB_CONFIG_ERROR 3
#define BB_ENOMEM 4
#define BB_INVALID_READ 5
#define BB_ENOMANIFEST 6
#define BB_ENOCONTAINER 7
#define BB_ERROBJ 8
#define BB_ENOOBJ 9

/*
 * bbos objects can be optimized for reading or writing.  read
 * optimized is typically for index-like structures (that get read all
 * at once), while write optimized is for data.
 */
enum bbos_mkobj_flag_t {
    READ_OPTIMIZED,
    WRITE_OPTIMIZED
};

/**
 * bbos_init: init bbos client and return a handle
 * @param server mercury-style URL of server to connect to
 * @return handle to client
 */
void *bbos_init(char *server);
    
/**
 * bbos_finalize: finialize bbos client
 * @param bbos bbos client to finalize
 */
void bbos_finalize(void *bbos);

/**
 * bbos_mkobj: make a new bbos log object
 * @param bbos client handle
 * @param name name of new object
 * @param flag optimization to use for new object
 * @return XXX
 */
int bbos_mkobj(void *bbos, const char *name, bbos_mkobj_flag_t flag);

/**
 * bbos_append: append data to bbos object
 * @param bbos client handle
 * @param name name of object to append to
 * @param buf buffer to append
 * @param len length of buffer
 * @return number of bytes appended (XXX?  error?  short append possible?)
 */
size_t bbos_append(void *bbos, const char *name, void *buf, size_t len);

/**
 * bbos_read: read data from bbos object
 * @param bbos client handle
 * @param name name of object to read from
 * @param buf where to place read data
 * @param offset offset in bbos object to read from
 * @param len number of bytes to read
 * @return number of bytes read (XXX? error?)
 */ 
size_t bbos_read(void *bbos, const char *name, void *buf, 
                 off_t offset, size_t len);

/**
 * bbos_get_size: get current size of bbos object
 * @param bbos client handle
 * @param name name of object to size
 * @return size of object (XXX: error?)
 */
off_t bbos_get_size(void *bbos, const char *name);


#ifdef __cplusplus
}   /* extern "C" */
#endif
