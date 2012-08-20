/*-------------------------------------------------------------------------
 *
 * qual_pushdown.h
 *		  Indexer & searcher for document collections foreign-data wrapper.
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *		  contrib/dc_fdw/qual_pushdown.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef QUAL_PUSHDOWN_H
#define QUAL_PUSHDOWN_H

#include "postgres.h"

#include <stdlib.h>
#include <math.h>

#include "funcapi.h"
#include "storage/fd.h"
#include "tsearch/ts_utils.h"
#include "tsearch/ts_locale.h"
#include "tsearch/ts_public.h"
#include "tsearch/ts_cache.h"
#include "tsearch/ts_type.h"
#include "utils/builtins.h"     /* cstring_to_text */
#include "utils/hsearch.h"      /* hashtable */
#include "tsearch/ts_locale.h"  /* lower str */
#include "nodes/pg_list.h"      /* linked list api */
#include "qual_extract.h"       /* qual extraction utility */


/* Debug mode flag */
/* #define DEBUG*/

#define KEYSIZE 100000  /* hash key length in bytes */
#define MAXELEM 100     /* maximum number of elements expected */
#define DEFAULT_INDEX_BUFF_SIZE 1 /* 1MB for default buffer size */
#define ALL "ALL"       /* term representing a global posting list */

/*
 * In-memory structure when indexing collection
 */
typedef struct DictionaryEntry
{
	char		key[100];
	List        *plist; /* postings list of document ids */
} DictionaryEntry;

/*
 * In-memory structure when searching
 */
typedef struct PostingInfo {
    char key[100]; /* dictionary key */
    int ptr; /* point to the posting file position */
    int len; /* length of the bytes to read */
} PostingInfo;

/*
 * Collection-wise stats
 */
typedef struct CollectionStats {
    int numOfDocs;  /* number of documents in the collection */
    int numOfBytes; /* total number of bytes of the collection */
    double bytesPerDoc;/* average size of doc */
} CollectionStats;

/* index utility */
int imIndex(char *datapath, char *indexpath);
int spimIndex(char *datapath, char *indexpath, int buffer_size);

/* search utility */
File openStat (char *indexpath);
File openDict (char *indexpath);
File openPost (char *indexpath);
File openDoc (char *fname);

void closeStat (File sfile);
void closeDict (File dfile);
void closePost (File pfile);
void closeDoc (File file);

int loadDict(HTAB **dict, File dfile);
int loadStat(CollectionStats **stats, File sfile);
int loadDoc(char **buf, File file);

List * evalQualTree(PushableQualNode *node, HTAB *dict, File pfile, List *allList);
List * searchTerm(char *term, HTAB *dict, File pfile, bool isALL, bool indexing);
List * pIntersect(List *list1, List *list2);
List * pIntersectNot(List *list1, List *list2);
List * pUnion(List *list1, List *list2);
List * pNegate(List *list, List *allList);

#endif   /* QUAL_PUSHDOWN_H */