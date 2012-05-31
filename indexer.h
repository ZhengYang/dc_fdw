/*-------------------------------------------------------------------------
 *
 * indexer.h
 *		  indexer for document collections foreign-data wrapper.
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *		  contrib/dc_fdw/indexer.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef INDEXER_H
#define INDEXER_H

/* Debug mode flag */
#define DEBUG
#define NOT_USED
#define DC_F_BUFFER_SIZE 4*1024*1024 /* default buffer size for a file to be 4 MB ~= 4 million characters*/


#include "postgres.h"
#include "funcapi.h"
#include "storage/fd.h"
#include "tsearch/ts_utils.h"




int dc_index(char* pathname);

#endif   /* INDEXER_H */