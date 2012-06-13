/*-------------------------------------------------------------------------
 *
 * indexer.c
 *		  indexer for document collections foreign-data wrapper.
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *		  contrib/dc_fdw/indexer.c
 *
 *-------------------------------------------------------------------------
 */
 
#include "indexer.h"

int dc_index(char *datapath, char *indexpath)
{
    DIR *dir;
    struct dirent *dirent;
    int num_of_files = 0;
    char *buffer;
    StringInfoData sid_data_dir;
    StringInfoData sid_dict_dir;
    StringInfoData sid_post_dir;
    File curr_file;
    File dict_file;
    File post_file;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    /* dictionary settings */
    HASHCTL info;
    HTAB * dict;
    HASH_SEQ_STATUS status;
    DictionaryEntry *d_entry;
    /* index file cursors */
    int cursor;
    
#ifdef DEBUG
    elog(NOTICE, "%s", "dc_index");
    elog(NOTICE, "PATH NAME: %s", datapath);
    elog(NOTICE, "PATH LEN: %d", (int) strlen(datapath));
#endif
    
    /*
     * initialize hash dictionary
     */
    info.keysize = 100000;
    info.entrysize = sizeof(DictionaryEntry);
    dict = hash_create ("dict", 100, &info, HASH_ELEM);
    
    
    dir = AllocateDir(datapath);
    if (dir == NULL) {
        // TODO: use pgsql error reporting
        elog(NOTICE, "ERROR: Path not found!");
        return -1;
    }
    
    /*
     * Loop through data dir to read each of the files in the dir
     * and tokenize the content of the files.
     */
    while( (dirent = ReadDir(dir, datapath)) != NULL)
    {
        int   sz;
        bool  found;
        DictionaryEntry *re;
        Oid  cfgId;
        TSVector tsvector;
        int o;
        char *lexemesptr;
        WordEntry *curentryptr;
        
#ifdef DEBUG
        elog(NOTICE, "-FILE NAME: %s", dirent->d_name);
        elog(NOTICE, "-FILE NAME LEN: %d", dirent->d_namlen);
#endif
        
        /* 
         * concat path and fname to full file name
         * (ignore . and ..)
         */
        if (strcmp(".", dirent->d_name) == 0) continue;
        if (strcmp("..", dirent->d_name) == 0) continue;
        initStringInfo(&sid_data_dir);
        appendStringInfo(&sid_data_dir, "%s/%s", datapath, dirent->d_name);

#ifdef DEBUG
        elog(NOTICE, "-FILE PATH NAME: %s", sid_data_dir.data);
#endif

        
        /*
         * 1. open file for processing
         * 2. seek to the end and get the length of the file
         * 3. rewind to the begining for reading
         * 4. read file content into buffer
         */
        curr_file = PathNameOpenFile(sid_data_dir.data, O_RDONLY,  mode);
        sz = FileSeek(curr_file, 0, SEEK_END);
        FileSeek(curr_file, 0, SEEK_SET);
        buffer = (char *) palloc(sizeof(char) * (sz + 1) );
        FileRead(curr_file, buffer, sz);
        buffer[sz] = 0;
        
#ifdef DEBUG
        //elog(NOTICE, "-FILE SIZE: %d", sz);
        //elog(NOTICE, "-FILE CONTENT: \n%s", buffer);
#endif
        
        /*
         * tokenization:
         * 1. Parse current document into a TSVector
         * 2. Iterate the WordEntries in TSVector and add them into global Dictionary
         */
        cfgId = getTSCurrentConfig(true);
        tsvector = (TSVector) DirectFunctionCall1( to_tsvector, PointerGetDatum(cstring_to_text(buffer)) );
        lexemesptr = STRPTR(tsvector);
        curentryptr = ARRPTR(tsvector);
        for (o = 0; o < tsvector->size; o++) {
            StringInfoData str;
            PostingEntry *p_entry;
            
            initStringInfo (&str);
            appendBinaryStringInfo (&str, lexemesptr + curentryptr->pos, curentryptr->len);
#ifdef DEBUG
            //elog(NOTICE, "--TOKEN: %s", str.data);
#endif
                  
            /* search in the dictionary hash table to see if the entry already exists */
            re = (DictionaryEntry *) hash_search(dict, (void *) str.data, HASH_ENTER, &found);
            if (found == TRUE) // term appears in the dictionary
            {
                PostingEntry *last_entry;

                last_entry = (PostingEntry *) llast(re->plist);
#ifdef DEBUG
                //elog(NOTICE, "last_entry: %d, current_doc: %d", last_entry->doc_id, atoi(dirent->d_name) );
#endif

                // already in the postings list
                if ( last_entry->doc_id == atoi(dirent->d_name) )
                {
                    // do nothing
                    elog(NOTICE, "do nothing");
                }
                // not in the postings list
                else {
                    p_entry = (PostingEntry *) palloc(sizeof(PostingEntry));
                    p_entry->doc_id = atoi(dirent->d_name);
                    re->plist = lappend(re->plist, p_entry);
                }
#ifdef DEBUG
                //elog(NOTICE, "HASH RV(FOUND): %s", (char *) re->key);
                //elog(NOTICE, "SIZE: %d", list_length(re->plist));
#endif
                
            }
            else // term first appearing in the dictionary
            {
                
                p_entry = (PostingEntry *) palloc(sizeof(PostingEntry));
                p_entry->doc_id = atoi(dirent->d_name);
                re->plist = list_make1(p_entry);
#ifdef DEBUG
                //elog(NOTICE, "HASH RV(NOT): %s", (char *) re->key);
                //elog(NOTICE, "SIZE: %d", list_length(re->plist));
#endif
                
            }
            curentryptr ++;
        }

        /*
         *  Clean-up:
         *  1. close paser handle
         *  2. free buffer memory
         *  3. close file
         */
        pfree(buffer);
        pfree(tsvector);
        FileClose(curr_file);
        
        /*
         * document collection size counter
         */
        num_of_files ++;
    }
    
#ifdef DEBUG
    elog(NOTICE, "NUM OF FILES: %d", num_of_files);
#endif

    /* Dumping hashtable into index file */
    initStringInfo(&sid_dict_dir);
    initStringInfo(&sid_post_dir);
    
    appendStringInfo(&sid_dict_dir, "%s/dictionary", indexpath);
    appendStringInfo(&sid_post_dir, "%s/postings", indexpath);
#ifdef DEBUG
        elog(NOTICE, "INDEX FILE NAME: %s", sid_dict_dir.data);
        elog(NOTICE, "INDEX FILE NAME: %s", sid_post_dir.data);
#endif
    dict_file = PathNameOpenFile(sid_dict_dir.data, O_RDWR | O_CREAT,  0666);
    post_file = PathNameOpenFile(sid_post_dir.data, O_RDWR | O_CREAT,  0666);
    
    /* iterate keys */
    hash_seq_init(&status, dict);
    cursor = 0;
    while ((d_entry = (DictionaryEntry *) hash_seq_search(&status)) != NULL)
	{
	    ListCell   *cell;
        StringInfoData sid_plist_line;
        StringInfoData sid_dict_line;

#ifdef DEBUG	    
        elog(NOTICE, "%s", d_entry->key);
#endif
        /* write dict entry */
        initStringInfo(&sid_dict_line);
        appendStringInfo(&sid_dict_line, "%s %d\n", d_entry->key, cursor);
        FileWrite (dict_file, sid_dict_line.data, strlen(sid_dict_line.data));		
        
        /* write postings list */
        initStringInfo(&sid_plist_line);
        /* serialize the list into a string of integers */
	    foreach(cell, d_entry->plist)
	    {
	        PostingEntry *p_entry;
            p_entry = (PostingEntry *) lfirst(cell);
            appendStringInfo(&sid_plist_line, "%d ", p_entry->doc_id);
	    }
        FileWrite (post_file, sid_plist_line.data, strlen(sid_plist_line.data));
        cursor += strlen(sid_plist_line.data);
	    
#ifdef DEBUG
        elog(NOTICE, "%s", sid_plist_line.data);
#endif

	}
    
    FileClose(dict_file);
    FileClose(post_file);
    FreeDir(dir);
    return 0;
}