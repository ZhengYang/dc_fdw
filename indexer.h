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


#include "postgres.h"
#include "funcapi.h"
#include "storage/fd.h"


int dc_index(char* pathname);

#endif   /* INDEXER_H */