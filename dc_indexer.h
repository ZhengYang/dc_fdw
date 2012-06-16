/*-------------------------------------------------------------------------
 *
 * dc_indexer.h
 *		  indexer for document collections foreign-data wrapper.
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *		  contrib/dc_fdw/dc_indexer.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DC_INDEXER_H
#define DC_INDEXER_H

/* Debug mode flag */
#define DEBUG
#define NOT_USED
#define DC_F_BUFFER_SIZE 4*1024*1024 /* default buffer size for a file to be 4 MB ~= 4 million characters*/

#include <stdlib.h>
#include "postgres.h"
#include "funcapi.h"
#include "storage/fd.h"
#include "tsearch/ts_utils.h"

#include "tsearch/ts_locale.h"
#include "tsearch/ts_public.h"
#include "tsearch/ts_cache.h"
#include "tsearch/ts_type.h"
#include "utils/builtins.h" /* cstring_to_text */
#include "utils/hsearch.h" /* hashtable */
#include "tsearch/ts_locale.h" /* lower str */
#include "nodes/pg_list.h" /* linked list api */


typedef struct PostingEntry
{
    int doc_id;
} PostingEntry;

typedef struct DictionaryEntry
{
	char		key[100];
	List        *plist; /* postings list of document ids */
} DictionaryEntry;


int dc_index(char *datapath, char *indexpath);
int cmpPostingEntries(const void *p1, const void *p2);

#endif   /* DC_INDEXER_H */