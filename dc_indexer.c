/*-------------------------------------------------------------------------
 *
 * dc_indexer.c
 *		  indexer for document collections foreign-data wrapper.
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *		  contrib/dc_fdw/dc_indexer.c
 *
 *-------------------------------------------------------------------------
 */
 
#include "dc_indexer.h"

int cmpPostingEntries(const void *p1, const void *p2) {
    int p1_docid;
    int p2_docid;
    
    p1_docid = ((PostingEntry *) p1)->doc_id;
    p2_docid = ((PostingEntry *) p2)->doc_id;
    return p1_docid - p2_docid;
}

int dc_index(char *datapath, char *indexpath)
{
    DIR *dir;
    struct dirent *dirent;
    char *buffer;
    StringInfoData sid_data_dir;
    StringInfoData sid_dict_dir;
    StringInfoData sid_post_dir;
    StringInfoData sid_stat_dir;
    File curr_file;
    File dict_file;
    File post_file;
    File stat_file;
    StringInfoData sid_stat_line;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    /* dictionary settings */
    HASHCTL info;
    HTAB * dict;
    HASH_SEQ_STATUS status;
    DictionaryEntry *d_entry;
    /* index file cursors */
    int cursor;
    /* stats and of dc */
    int num_of_files = 0;
    int num_of_bytes = 0;
    
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
        char *ALL = "ALL";
        
#ifdef DEBUG
        elog(NOTICE, "-FILE NAME: %s", dirent->d_name);
#ifdef _DIRENT_HAVE_D_NAMLEN
        elog(NOTICE, "-FILE NAME LEN: %d", dirent->d_namlen);
#else
        elog(NOTICE, "-FILE NAME LEN: %d", (int) strlen(dirent->d_name));
#endif /* _DIRENT_HAVE_D_NAMLEN */
#endif /* DEBUG */
        
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
         * 5. increase the size of total bytes in dc
         */
        curr_file = PathNameOpenFile(sid_data_dir.data, O_RDONLY,  mode);
        sz = FileSeek(curr_file, 0, SEEK_END);
        FileSeek(curr_file, 0, SEEK_SET);
        buffer = (char *) palloc(sizeof(char) * (sz + 1) );
        FileRead(curr_file, buffer, sz);
        buffer[sz] = 0;
        num_of_bytes += sz;
        
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
            appendBinaryStringInfo(&str, lexemesptr + curentryptr->pos, curentryptr->len);
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
        /* global entry for performing NOT */
        re = (DictionaryEntry *) hash_search(dict, (char *) ALL, HASH_ENTER, &found);
        if (found == TRUE)
        {
            PostingEntry *p_entry;
            p_entry = (PostingEntry *) palloc(sizeof(PostingEntry));
            p_entry->doc_id = atoi(dirent->d_name);
            re->plist = lappend(re->plist, p_entry);
        } 
        else {
            PostingEntry *p_entry;
            p_entry = (PostingEntry *) palloc(sizeof(PostingEntry));
            p_entry->doc_id = atoi(dirent->d_name);
            re->plist = list_make1(p_entry);
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
        PostingEntry *slist;
        PostingEntry *slist_curr;

#ifdef DEBUG	    
        elog(NOTICE, "%s", d_entry->key);
#endif		
        
        /* sort postings list by doc_id */
        slist = (PostingEntry *) palloc(list_length(d_entry->plist) * sizeof(PostingEntry));
        slist_curr = slist;
        foreach(cell, d_entry->plist)
        {
            PostingEntry *p_entry = (PostingEntry *) lfirst(cell);
            slist_curr->doc_id = p_entry->doc_id;
            slist_curr ++;
        }
        qsort((void *) slist, list_length(d_entry->plist), sizeof(PostingEntry), cmpPostingEntries);
        
        /* write postings list */
        initStringInfo(&sid_plist_line);
        /* serialize the list into a string of integers */
        for (slist_curr = slist; slist_curr < slist + list_length(d_entry->plist); slist_curr ++) {
            appendStringInfo(&sid_plist_line, "%d ", slist_curr->doc_id);
	    }
        FileWrite (post_file, sid_plist_line.data, strlen(sid_plist_line.data));
	    
	     /* write dict entry */
        initStringInfo(&sid_dict_line);
        appendStringInfo(&sid_dict_line, "%s %d %d\n", d_entry->key, cursor, sid_plist_line.len);
        FileWrite (dict_file, sid_dict_line.data, strlen(sid_dict_line.data));
	    
	    cursor += strlen(sid_plist_line.data);
#ifdef DEBUG
        elog(NOTICE, "%s", sid_plist_line.data);
#endif
        pfree(slist);
	}
    
    FileClose(dict_file);
    FileClose(post_file);
    FreeDir(dir);
    
    /*
     * collection stats information
     */
    initStringInfo(&sid_stat_dir);
    appendStringInfo(&sid_stat_dir, "%s/stats", indexpath);
#ifdef DEBUG
        elog(NOTICE, "STATS FILE NAME: %s", sid_stat_dir.data);
#endif
    stat_file = PathNameOpenFile(sid_stat_dir.data, O_RDWR | O_CREAT,  0666);
    
    /* number of documents in the doc collection */
    initStringInfo(&sid_stat_line);
    appendStringInfo(&sid_stat_line, "NUM_OF_DOCS:%d\n", num_of_files);
    FileWrite (stat_file, sid_stat_line.data, strlen(sid_stat_line.data));
    
    /* number of bytes in the doc collection */
    resetStringInfo(&sid_stat_line);
    appendStringInfo(&sid_stat_line, "NUM_OF_BYTES:%d", num_of_bytes);
    FileWrite (stat_file, sid_stat_line.data, strlen(sid_stat_line.data));
    
    FileClose(stat_file);	
    return 0;
}