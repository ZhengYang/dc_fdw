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

int dc_load_dict(char *indexpath)
{
    File dict_file;
    int sz;
    int o = 0;
    char *token;
    char *buffer;
    StringInfoData sid_dict_dir;
    StringInfoData sid_post_dir;
    
    /* dictinaary setting */
    HASHCTL info;
    HTAB * dict;
    HASH_SEQ_STATUS status;
    DictionaryEntry *d_entry;
    PostingInfo *re;
    StringInfoData sid_term;
    int ptr = 0;

#ifdef DEBUG
    elog(NOTICE, "dc_load_dict");
#endif
    
    initStringInfo(&sid_dict_dir);
    appendStringInfo(&sid_dict_dir, "%s/dictionary", indexpath);

#ifdef DEBUG
    elog(NOTICE, "%s", sid_dict_dir.data);
#endif

    /*
     * initialize hash dictionary
     */
    info.keysize = 100000;
    info.entrysize = sizeof(DictionaryEntry);
    dict = hash_create ("dict", 100, &info, HASH_ELEM);
    
    
    dict_file = PathNameOpenFile(sid_dict_dir.data, O_RDONLY,  0666);
    sz = FileSeek(dict_file, 0, SEEK_END);
    FileSeek(dict_file, 0, SEEK_SET);
    buffer = (char *) palloc(sizeof(char) * (sz + 1) );
    FileRead(dict_file, buffer, sz);
    buffer[sz] = 0;

    token = strtok(buffer, " \n");
    initStringInfo(&sid_term);
    while ( token != NULL )
    {
        bool found;
        /* term token */
        if (o % 3 == 0)
        {
            appendStringInfo(&sid_term, "%s", token);
        }
        /* pointer to start position */
        else if (o % 3 == 1) {
            ptr = atoi(token);
        }
        /* length of the plist string*/
        else if (o % 3 == 2) {
            re = (PostingInfo *) hash_search(dict, (void *) sid_term.data, HASH_ENTER, &found);
            if (found == TRUE)
                elog(NOTICE, "Dictionary file '%s' corrupted!", sid_dict_dir.data);
            else {
                re->ptr = ptr;
                re->len = atoi(token);
                elog(NOTICE, "%s:%s\n", sid_term.data, token);
            }
            resetStringInfo(&sid_term);
        }
        
        token = strtok(NULL, " \n");
        o ++;
    }
    
    return 0;
}