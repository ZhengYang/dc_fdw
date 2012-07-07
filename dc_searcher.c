/*-------------------------------------------------------------------------
 *
 * dc_search.c
 *		  searcher for document collections foreign-data wrapper.
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *		  contrib/dc_fdw/dc_searcher.c
 *
 *-------------------------------------------------------------------------
 */
 
#include "dc_indexer.h"
#include "dc_searcher.h"


int dc_load_dict(char *indexpath)
{
    return 0;
}

int dc_load_stat(char *indexpath, int *num_of_docs, int *num_of_bytes)
{
    File stat_file;
    StringInfoData sid_stat_dir;
    int status;
    int sz;
    char *buffer;
    
#ifdef DEBUG
     elog(NOTICE, "dc_load_stat");
#endif

    initStringInfo(&sid_stat_dir);
    appendStringInfo(&sid_stat_dir, "%s/stats", indexpath);

#ifdef DEBUG
    elog(NOTICE, "STATS FILE NAME: %s", sid_stat_dir.data);
#endif
    stat_file = PathNameOpenFile(sid_stat_dir.data, O_RDONLY,  0666);
    
    sz = FileSeek(stat_file, 0, SEEK_END);
    FileSeek(stat_file, 0, SEEK_SET);
    buffer = (char *) palloc(sizeof(char) * (sz + 1) );
    FileRead(stat_file, buffer, sz);
    buffer[sz] = 0;
    
    /* number of documents in the doc collection */
    status = sscanf(buffer,
            "NUM_OF_DOCS:%d\nNUM_OF_BYTES:%d",
			 num_of_docs, num_of_bytes);
	elog(NOTICE, "---%d", *num_of_docs);
	if (status != 2)
		elog(ERROR, "Cannot read stats file!");
    return 0;
}