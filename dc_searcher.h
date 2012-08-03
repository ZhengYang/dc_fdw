/*-------------------------------------------------------------------------
 *
 * dc_search.h
 *		  searcher for document collections foreign-data wrapper.
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *		  contrib/dc_fdw/dc_searcher.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DC_SEARCHER_H
#define DC_SEARCHER_H

#include <math.h>
#include "dc_indexer.h"

#include "deparse.h"

typedef struct PostingInfo {
    char key[100]; /* dictionary key */
    int ptr; /* point to the posting file position */
    int len; /* length of the bytes to read */
} PostingInfo;

int dc_load_dict(HTAB **dict, char *indexpath);
int dc_load_stat(char *indexpath, int *num_of_docs, int *num_of_bytes);
List * searchTerm(char *term, HTAB *dict, File pfile, bool isALL);
List * pIntersect(List *list1, List *list2);
List * pIntersectNot(List *list1, List *list2);
List * pUnion(List *list1, List *list2);
List * pNegate(List *list, List *allList);
List * evalQualTree(PushableQualNode *qualNode, HTAB *dict, char *indexpath, List *allList);

#endif   /* DC_SEARCHER_H */