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
 
int dc_load_dict(char *indexpath);
int dc_load_stat(char *indexpath, int *num_of_docs, int *num_of_bytes);