/*-------------------------------------------------------------------------
 *
 * indexer.c
 *		  Indexer for document collections foreign-data wrapper.
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
 
#include "qual_pushdown.h"

int cmpDocIds(const void *p1, const void *p2);

/*
 * function compare 2 posting entries, essentially integers
 */
int
cmpDocIds(const void *p1, const void *p2)
{
    return ( *(int *)p1 - *(int *)p2 );
}

/*
 * Basic (in memory) index function
 */
int
imIndex(char *datapath, char *indexpath)
{
    /* Data directory */
    DIR             *datadir;
    struct dirent   *dirent;
    char            *fileContentBuf;
    StringInfoData  sidCurrFilePath;
    StringInfoData  sidDictFilePath;
    StringInfoData  sidPostFilePath;
    StringInfoData  sidStatFilePath;
    File            currFile;
    File            dictFile;
    File            postFile;
    File            statFile;
    StringInfoData  sidStatLine;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    
    /* dictionary settings */
    HASHCTL         info;
    HTAB            *dict;
    HASH_SEQ_STATUS status;
    DictionaryEntry *dEntry;
    
    /* index file cursors */
    int cursor = 0;
    
    /* stats and of dc */
    int dcNumOfFiles = 0;
    int dcNumOfBytes = 0;
    
#ifdef DEBUG
    elog(NOTICE, "%s", "imIndex");
    elog(NOTICE, "DATA PATH: %s", datapath);
#endif
    
    /* initialize hash dictionary */
    info.keysize = KEYSIZE;
    info.entrysize = sizeof(DictionaryEntry);
    dict = hash_create ("dict", MAXELEM, &info, HASH_ELEM);
    
    /* Initialize data path */
    datadir = AllocateDir(datapath);
    if (datadir == NULL)
    {
        elog(ERROR, "ERROR: Data path not found!");
        return -1;
    }
    
    /* Initialize path strings */
    initStringInfo(&sidCurrFilePath);
    initStringInfo(&sidDictFilePath);
    initStringInfo(&sidPostFilePath);
    initStringInfo(&sidStatFilePath);
    appendStringInfo(&sidDictFilePath, "%s/dict", indexpath);
    appendStringInfo(&sidPostFilePath, "%s/post", indexpath);
    appendStringInfo(&sidStatFilePath, "%s/stat", indexpath);
    
    /*
     * Loop through data dir to read each of the files in the dir
     * and tokenize the content of the files.
     */
    while( (dirent = ReadDir(datadir, datapath)) != NULL)
    {
        int             fileSize;
        bool            found;
        DictionaryEntry *re;
        Oid             cfgId;
        TSVector        tsvector;
        int             o;
        char            *lexemesptr;
        WordEntry       *curentryptr;
        
#ifdef DEBUG
        elog(NOTICE, "-FILE NAME: %s", dirent->d_name);
#endif /* DEBUG */
        
        /* 
         * concat path and fname to full file name
         * (ignore . and ..)
         */
        if (strcmp(".", dirent->d_name) == 0) continue;
        if (strcmp("..", dirent->d_name) == 0) continue;
        
        resetStringInfo(&sidCurrFilePath);
        appendStringInfo(&sidCurrFilePath, "%s/%s", datapath, dirent->d_name);

#ifdef DEBUG
        elog(NOTICE, "-CURR FILE NAME: %s", sidCurrFilePath.data);
#endif

        
        /*
         * 1. open file for processing
         * 2. seek to the end and get the length of the file
         * 3. rewind to the begining for reading
         * 4. read file content into buffer
         * 5. increase the size of total bytes in dc
         */
        currFile = PathNameOpenFile(sidCurrFilePath.data, O_RDONLY,  mode);
        fileSize = FileSeek(currFile, 0, SEEK_END);
        FileSeek(currFile, 0, SEEK_SET);
        fileContentBuf = (char *) palloc(sizeof(char) * (fileSize + 1) );
        FileRead(currFile, fileContentBuf, fileSize);
        fileContentBuf[fileSize] = 0;
        
        /*
         * tokenization:
         * 1. Parse current document into a TSVector
         * 2. Iterate the WordEntries in TSVector and add them into global Dictionary
         */
        cfgId = getTSCurrentConfig(true);
        tsvector = (TSVector) DirectFunctionCall1( to_tsvector, PointerGetDatum(cstring_to_text(fileContentBuf)) );
        lexemesptr = STRPTR(tsvector);
        curentryptr = ARRPTR(tsvector);
        for (o = 0; o < tsvector->size; o++) {
            StringInfoData sidToken;
            
            initStringInfo (&sidToken);
            appendBinaryStringInfo(&sidToken, lexemesptr + curentryptr->pos, curentryptr->len);
#ifdef DEBUG
            elog(NOTICE, "--TOKEN: %s", sidToken.data);
#endif
            /* search in the dictionary hash table to see if the entry already exists */
            re = (DictionaryEntry *) hash_search(dict, (void *) sidToken.data, HASH_ENTER, &found);
            if (found == TRUE) /* term appears in the dictionary */
            {
                re->plist = lappend_int(re->plist, atoi(dirent->d_name));
            }
            else /* term first appearing in the dictionary */
                re->plist = list_make1_int(atoi(dirent->d_name));
            curentryptr ++;
        }
        /* global entry for performing NOT */
        re = (DictionaryEntry *) hash_search(dict, ALL, HASH_ENTER, &found);
        if (found == TRUE)
            re->plist = lappend_int(re->plist, atoi(dirent->d_name)); 
        else
            re->plist = list_make1_int( atoi(dirent->d_name) );

        /*
         *  Clean-up:
         *  1. close paser handle
         *  2. free buffer memory
         *  3. close file
         */
        pfree(fileContentBuf);
        pfree(tsvector);
        FileClose(currFile);
        
        /*
         * document collection size counter
         */
        dcNumOfBytes += fileSize;
        dcNumOfFiles ++;
    }
    
#ifdef DEBUG
    elog(NOTICE, "NUM OF FILES: %d", dcNumOfFiles);
#endif

    
    /*
     * Dumping hashtable into index file
     */
#ifdef DEBUG
        elog(NOTICE, "-DICT FILE NAME: %s", sidDictFilePath.data);
        elog(NOTICE, "-POST FILE NAME: %s", sidPostFilePath.data);
#endif
    dictFile = PathNameOpenFile(sidDictFilePath.data, O_RDWR | O_CREAT,  0666);
    postFile = PathNameOpenFile(sidPostFilePath.data, O_RDWR | O_CREAT,  0666);
    
    /* iterate keys */
    hash_seq_init(&status, dict);
    while ((dEntry = (DictionaryEntry *) hash_seq_search(&status)) != NULL)
	{
	    ListCell   *cell;
        StringInfoData sidPostList;
        StringInfoData sidDictEntry;
        int *slist;
        int *slistCurr;

#ifdef DEBUG
        elog(NOTICE, "--DICT ENTRY:%s", dEntry->key);
#endif		
        
        /* sort postings list by doc_id */
        slist = (int *) palloc(list_length(dEntry->plist) * sizeof(int));
        slistCurr = slist;
        foreach(cell, dEntry->plist)
        {
            *slistCurr = lfirst_int(cell);;
            slistCurr ++;
        }
        qsort((void *) slist, list_length(dEntry->plist), sizeof(int), cmpDocIds);
        
        /* write postings list */
        initStringInfo(&sidPostList);
        /* serialize the list into a string of integers */
        for (slistCurr = slist; slistCurr < slist + list_length(dEntry->plist); slistCurr ++)
            appendStringInfo(&sidPostList, "%d ", *slistCurr);
        
        FileWrite (postFile, sidPostList.data, sidPostList.len);
	    
	     /* write dict entry */
        initStringInfo(&sidDictEntry);
        appendStringInfo(&sidDictEntry, "%s %d %d\n", dEntry->key, cursor, sidPostList.len);
        FileWrite (dictFile, sidDictEntry.data, sidDictEntry.len);
	    /* increase cursor */
	    cursor += sidPostList.len;
#ifdef DEBUG
        elog(NOTICE, "%s", sidPostList.data);
#endif
        pfree(slist);
	}
    
    FileClose(dictFile);
    FileClose(postFile);
    FreeDir(datadir);
    
    /*
     * Collection stats information
     */
#ifdef DEBUG
        elog(NOTICE, "-STATS FILE NAME: %s", sidStatFilePath.data);
#endif
    statFile = PathNameOpenFile(sidStatFilePath.data, O_RDWR | O_CREAT,  0666);
    
    /* number of documents in the doc collection */
    initStringInfo(&sidStatLine);
    appendStringInfo(&sidStatLine, "NUM_OF_DOCS:%d\n", dcNumOfFiles);
    FileWrite (statFile, sidStatLine.data, sidStatLine.len);
    
    /* number of bytes in the doc collection */
    resetStringInfo(&sidStatLine);
    appendStringInfo(&sidStatLine, "NUM_OF_BYTES:%d", dcNumOfBytes);
    FileWrite (statFile, sidStatLine.data, sidStatLine.len);
    
    FileClose(statFile);	
    return 0;
}


/*
 * Single-pass in-memory index function
 */
int
spimIndex(char *datapath, char *indexpath)
{
    /* Data directory */
    DIR             *datadir;
    struct dirent   *dirent;
    char            *fileContentBuf;
    StringInfoData  sidCurrFilePath;
    StringInfoData  sidDictFilePath;
    StringInfoData  sidPostFilePath;
    StringInfoData  sidStatFilePath;
    File            currFile;
    File            dictFile;
    File            postFile;
    File            statFile;
    StringInfoData  sidStatLine;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    
    /* dictionary settings */
    HASHCTL         info;
    HTAB            *dict;
    HASH_SEQ_STATUS status;
    DictionaryEntry *dEntry;
    
    /* index file cursors */
    int cursor = 0;
    
    /* stats and of dc */
    int dcNumOfFiles = 0;
    int dcNumOfBytes = 0;
    
#ifdef DEBUG
    elog(NOTICE, "%s", "spimIndex");
    elog(NOTICE, "DATA PATH: %s", datapath);
#endif
    
    /* initialize hash dictionary */
    info.keysize = KEYSIZE;
    info.entrysize = sizeof(DictionaryEntry);
    dict = hash_create ("dict", MAXELEM, &info, HASH_ELEM);
    
    /* Initialize data path */
    datadir = AllocateDir(datapath);
    if (datadir == NULL)
    {
        elog(ERROR, "ERROR: Data path not found!");
        return -1;
    }
    
    /* Initialize path strings */
    initStringInfo(&sidCurrFilePath);
    initStringInfo(&sidDictFilePath);
    initStringInfo(&sidPostFilePath);
    initStringInfo(&sidStatFilePath);
    appendStringInfo(&sidDictFilePath, "%s/dict", indexpath);
    appendStringInfo(&sidPostFilePath, "%s/post", indexpath);
    appendStringInfo(&sidStatFilePath, "%s/stat", indexpath);
    
    /*
     * Loop through data dir to read each of the files in the dir
     * and tokenize the content of the files.
     */
    while( (dirent = ReadDir(datadir, datapath)) != NULL)
    {
        int             fileSize;
        bool            found;
        DictionaryEntry *re;
        Oid             cfgId;
        TSVector        tsvector;
        int             o;
        char            *lexemesptr;
        WordEntry       *curentryptr;
        
#ifdef DEBUG
        elog(NOTICE, "-FILE NAME: %s", dirent->d_name);
#endif /* DEBUG */
        
        /* 
         * concat path and fname to full file name
         * (ignore . and ..)
         */
        if (strcmp(".", dirent->d_name) == 0) continue;
        if (strcmp("..", dirent->d_name) == 0) continue;
        
        resetStringInfo(&sidCurrFilePath);
        appendStringInfo(&sidCurrFilePath, "%s/%s", datapath, dirent->d_name);

#ifdef DEBUG
        elog(NOTICE, "-CURR FILE NAME: %s", sidCurrFilePath.data);
#endif

        
        /*
         * 1. open file for processing
         * 2. seek to the end and get the length of the file
         * 3. rewind to the begining for reading
         * 4. read file content into buffer
         * 5. increase the size of total bytes in dc
         */
        currFile = PathNameOpenFile(sidCurrFilePath.data, O_RDONLY,  mode);
        fileSize = FileSeek(currFile, 0, SEEK_END);
        FileSeek(currFile, 0, SEEK_SET);
        fileContentBuf = (char *) palloc(sizeof(char) * (fileSize + 1) );
        FileRead(currFile, fileContentBuf, fileSize);
        fileContentBuf[fileSize] = 0;
        
        /*
         * tokenization:
         * 1. Parse current document into a TSVector
         * 2. Iterate the WordEntries in TSVector and add them into global Dictionary
         */
        cfgId = getTSCurrentConfig(true);
        tsvector = (TSVector) DirectFunctionCall1( to_tsvector, PointerGetDatum(cstring_to_text(fileContentBuf)) );
        lexemesptr = STRPTR(tsvector);
        curentryptr = ARRPTR(tsvector);
        for (o = 0; o < tsvector->size; o++) {
            StringInfoData sidToken;
            
            initStringInfo (&sidToken);
            appendBinaryStringInfo(&sidToken, lexemesptr + curentryptr->pos, curentryptr->len);
#ifdef DEBUG
            elog(NOTICE, "--TOKEN: %s", sidToken.data);
#endif
            /* search in the dictionary hash table to see if the entry already exists */
            re = (DictionaryEntry *) hash_search(dict, (void *) sidToken.data, HASH_ENTER, &found);
            if (found == TRUE) /* term appears in the dictionary */
            {
                re->plist = lappend_int(re->plist, atoi(dirent->d_name));
            }
            else /* term first appearing in the dictionary */
                re->plist = list_make1_int(atoi(dirent->d_name));
            curentryptr ++;
        }
        /* global entry for performing NOT */
        re = (DictionaryEntry *) hash_search(dict, ALL, HASH_ENTER, &found);
        if (found == TRUE)
            re->plist = lappend_int(re->plist, atoi(dirent->d_name)); 
        else
            re->plist = list_make1_int( atoi(dirent->d_name) );

        /*
         *  Clean-up:
         *  1. close paser handle
         *  2. free buffer memory
         *  3. close file
         */
        pfree(fileContentBuf);
        pfree(tsvector);
        FileClose(currFile);
        
        /*
         * document collection size counter
         */
        dcNumOfBytes += fileSize;
        dcNumOfFiles ++;
    }
    
#ifdef DEBUG
    elog(NOTICE, "NUM OF FILES: %d", dcNumOfFiles);
#endif

    
    /*
     * Dumping hashtable into index file
     */
#ifdef DEBUG
        elog(NOTICE, "-DICT FILE NAME: %s", sidDictFilePath.data);
        elog(NOTICE, "-POST FILE NAME: %s", sidPostFilePath.data);
#endif
    dictFile = PathNameOpenFile(sidDictFilePath.data, O_RDWR | O_CREAT,  0666);
    postFile = PathNameOpenFile(sidPostFilePath.data, O_RDWR | O_CREAT,  0666);
    
    /* iterate keys */
    hash_seq_init(&status, dict);
    while ((dEntry = (DictionaryEntry *) hash_seq_search(&status)) != NULL)
	{
	    ListCell   *cell;
        StringInfoData sidPostList;
        StringInfoData sidDictEntry;
        int *slist;
        int *slistCurr;

#ifdef DEBUG
        elog(NOTICE, "--DICT ENTRY:%s", dEntry->key);
#endif		
        
        /* sort postings list by doc_id */
        slist = (int *) palloc(list_length(dEntry->plist) * sizeof(int));
        slistCurr = slist;
        foreach(cell, dEntry->plist)
        {
            *slistCurr = lfirst_int(cell);;
            slistCurr ++;
        }
        qsort((void *) slist, list_length(dEntry->plist), sizeof(int), cmpDocIds);
        
        /* write postings list */
        initStringInfo(&sidPostList);
        /* serialize the list into a string of integers */
        for (slistCurr = slist; slistCurr < slist + list_length(dEntry->plist); slistCurr ++)
            appendStringInfo(&sidPostList, "%d ", *slistCurr);
        
        FileWrite (postFile, sidPostList.data, sidPostList.len);
	    
	     /* write dict entry */
        initStringInfo(&sidDictEntry);
        appendStringInfo(&sidDictEntry, "%s %d %d\n", dEntry->key, cursor, sidPostList.len);
        FileWrite (dictFile, sidDictEntry.data, sidDictEntry.len);
	    /* increase cursor */
	    cursor += sidPostList.len;
#ifdef DEBUG
        elog(NOTICE, "%s", sidPostList.data);
#endif
        pfree(slist);
	}
    
    FileClose(dictFile);
    FileClose(postFile);
    FreeDir(datadir);
    
    /*
     * Collection stats information
     */
#ifdef DEBUG
        elog(NOTICE, "-STATS FILE NAME: %s", sidStatFilePath.data);
#endif
    statFile = PathNameOpenFile(sidStatFilePath.data, O_RDWR | O_CREAT,  0666);
    
    /* number of documents in the doc collection */
    initStringInfo(&sidStatLine);
    appendStringInfo(&sidStatLine, "NUM_OF_DOCS:%d\n", dcNumOfFiles);
    FileWrite (statFile, sidStatLine.data, sidStatLine.len);
    
    /* number of bytes in the doc collection */
    resetStringInfo(&sidStatLine);
    appendStringInfo(&sidStatLine, "NUM_OF_BYTES:%d", dcNumOfBytes);
    FileWrite (statFile, sidStatLine.data, sidStatLine.len);
    
    FileClose(statFile);	
    return 0;
}