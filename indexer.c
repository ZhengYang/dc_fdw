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
void dumpIndex(HTAB *dict, File dictFile, File postFile);

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
spimIndex(char *datapath, char *indexpath, int buffer_size)
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
    HTAB            *DICT;
    HTAB            *dict;
    HASH_SEQ_STATUS status;
    PostingInfo     *dEntry;
    
    /* List of dict and postings file */
    List *postfnames = NIL;
    List *dictfnames = NIL;
    File currDict;
    File currPost;
    
    /* List of dictionaries in memory */
    List *dicts = NIL;
    
    /* threshold for starting a new round (in bytes) */
    int bufThreshold = (buffer_size == 0 ? DEFAULT_INDEX_BUFF_SIZE : buffer_size) * 1024 * 1024;
    int bufCounter = 0;
    /* index counter */
    int iCounter = 0;
    StringInfoData sidTmpDictPath;
    StringInfoData sidTmpPostPath;
    
    /* index file cursors */
    int cursor = 0;
    
    /* stats and of dc */
    int dcNumOfFiles = 0;
    int dcNumOfBytes = 0;
    int i;
    
#ifdef DEBUG
    elog(NOTICE, "%s", "spimIndex");
    elog(NOTICE, "DATA PATH: %s", datapath);
#endif
    
    /* initialize hash dictionary */
    info.keysize = KEYSIZE;
    info.entrysize = sizeof(DictionaryEntry);
    DICT = hash_create ("DICT", MAXELEM, &info, HASH_ELEM);
    dict = hash_create ("dict", MAXELEM, &info, HASH_ELEM);
    
    /* Initialize data path */
    datadir = AllocateDir(datapath);
    if (datadir == NULL)
    {
        elog(ERROR, "ERROR: Data path not found!");
        return -1;
    }
    
    /* Initialize path strings */
    initStringInfo(&sidTmpDictPath);
    initStringInfo(&sidTmpPostPath);
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
        bool            foundGlobal;
        DictionaryEntry *re;
        Oid             cfgId;
        TSVector        tsvector;
        int             o;
        char            *lexemesptr;
        WordEntry       *curentryptr;
        
#ifdef DEBUG
        elog(NOTICE, "-FILE NAME: %s", dirent->d_name);
#endif /* DEBUG */
        
        if (bufCounter > bufThreshold)
        {   
            StringInfoData sidTmpDictPath;
            StringInfoData sidTmpPostPath;
            initStringInfo(&sidTmpDictPath);
            initStringInfo(&sidTmpPostPath);
            appendStringInfo(&sidTmpDictPath, "%s/%d.dict", indexpath, iCounter);
            appendStringInfo(&sidTmpPostPath, "%s/%d.post", indexpath, iCounter);
            elog(NOTICE, "I_DFILES:%s", sidTmpDictPath.data);
            /* serialize current buffer */
            currDict = PathNameOpenFile(sidTmpDictPath.data, O_RDWR | O_CREAT,  mode);
            currPost = PathNameOpenFile(sidTmpPostPath.data, O_RDWR | O_CREAT,  mode);
            dumpIndex(dict, currDict, currPost);
            hash_destroy(dict);
            dictfnames = lappend(dictfnames, (void *) sidTmpDictPath.data);
            postfnames = lappend(postfnames, (void *) sidTmpPostPath.data);
            /* start a new round */
            dict = hash_create ("dict", MAXELEM, &info, HASH_ELEM);
            /* reset counter */
            bufCounter = 0;
            iCounter ++;
        }
        /* 
         * concat path and fname to full file name
         * (ignore . and ..)
         */
        if (strcmp(".", dirent->d_name) == 0) continue;
        if (strcmp("..", dirent->d_name) == 0) continue;

        resetStringInfo(&sidCurrFilePath);
        appendStringInfo(&sidCurrFilePath, "%s/%s", datapath, dirent->d_name);


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
            //elog(NOTICE, "--TOKEN: %s", sidToken.data);
#endif
            /* search in the dictionary hash table to see if the entry already exists */
            re = (DictionaryEntry *) hash_search(dict, (void *) sidToken.data, HASH_ENTER, &found);
            hash_search(DICT, (void *) sidToken.data, HASH_ENTER, &foundGlobal);
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
        else {
            re->plist = list_make1_int( atoi(dirent->d_name) );
            hash_search(DICT, ALL, HASH_ENTER, &foundGlobal);
        }
        

        /*
         *  Clean-up:
         *  1. close paser handle
         *  2. free buffer memory
         *  3. close file
         */
        pfree(fileContentBuf);
        pfree(tsvector);
        FileClose(currFile);
        
        bufCounter += fileSize;
        
        /*
         * document collection size counter
         */
        dcNumOfBytes += fileSize;
        dcNumOfFiles ++;
    }
    
    /* serialize the remaining */
    initStringInfo(&sidTmpDictPath);
    initStringInfo(&sidTmpPostPath);
    appendStringInfo(&sidTmpDictPath, "%s/%d.dict", indexpath, iCounter);
    appendStringInfo(&sidTmpPostPath, "%s/%d.post", indexpath, iCounter);
    
    currDict = PathNameOpenFile(sidTmpDictPath.data, O_RDWR | O_CREAT,  mode);
    currPost = PathNameOpenFile(sidTmpPostPath.data, O_RDWR | O_CREAT,  mode);
    dumpIndex(dict, currDict, currPost);
    hash_destroy(dict);
    dictfnames = lappend(dictfnames, (void *) sidTmpDictPath.data);
    postfnames = lappend(postfnames, (void *) sidTmpPostPath.data);
    
#ifdef DEBUG
    elog(NOTICE, "NUM OF FILES: %d", dcNumOfFiles);
#endif

    
    /*
     * iterate keys in global dict and retrieve the postings
     */
#ifdef DEBUG
        elog(NOTICE, "-DICT FILE NAME: %s", sidDictFilePath.data);
        elog(NOTICE, "-POST FILE NAME: %s", sidPostFilePath.data);
#endif
    dictFile = PathNameOpenFile(sidDictFilePath.data, O_RDWR | O_CREAT,  0666);
    postFile = PathNameOpenFile(sidPostFilePath.data, O_RDWR | O_CREAT,  0666);
    /* open dicts one by one */
    for(i = 0; i < list_length(dictfnames); i++)
    {
        HTAB *currdict;
        
        char *dfname = (char *) list_nth(dictfnames, i);
        File currdfile = PathNameOpenFile(dfname, O_RDONLY,  0666);
        loadDict(&currdict, currdfile);
        FileClose(currdfile);
        dicts = lappend(dicts, currdict);
    }
    
    /* iterate keys */
    hash_seq_init(&status, DICT);
    while ((dEntry = (PostingInfo *) hash_seq_search(&status)) != NULL)
	{
	    ListCell    *cell;
        StringInfoData sidPostList;
        StringInfoData sidDictEntry;
        List *plist = NIL;
        int *slist;
        int *slistCurr;
        int i;

#ifdef DEBUG
        //elog(NOTICE, "--DICT ENTRY:%s", dEntry->key);
#endif		
        for(i = 0; i < list_length(dicts); i++)
        {
            char *pfname = (char *) list_nth(postfnames, i);
            File currpfile = PathNameOpenFile(pfname, O_RDONLY,  0666);
            plist = list_concat(plist, searchTerm(dEntry->key, (HTAB *) list_nth(dicts, i), currpfile, FALSE, TRUE));
            FileClose(currpfile);
        }
            
        /* sort postings list by doc_id */
        slist = (int *) palloc(list_length(plist) * sizeof(int));
        slistCurr = slist;
        foreach(cell, plist)
        {
            *slistCurr = lfirst_int(cell);
            slistCurr ++;
        }
        qsort((void *) slist, list_length(plist), sizeof(int), cmpDocIds);
        
        /* write postings list */
        initStringInfo(&sidPostList);
        /* serialize the list into a string of integers */
        for (slistCurr = slist; slistCurr < slist + list_length(plist); slistCurr ++)
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
        list_free(plist);
	}
    
    /* clean up handles, buffer and remove tmpfiles */
    for(i = 0; i < list_length(postfnames); i++)
    {
        char *pfname = (char *) list_nth(postfnames, i);
        char *dfname = (char *) list_nth(dictfnames, i);
        remove(pfname);
        remove(dfname);
    }
    FileClose(dictFile);
    FileClose(postFile);
    FreeDir(datadir);
    list_free(postfnames);
    list_free(dictfnames);
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
 * dump an in-memory hashtable to the disk
 */
void
dumpIndex(HTAB *dict, File dictFile, File postFile)
{
    HASH_SEQ_STATUS status;
    DictionaryEntry *dEntry;
    int cursor = 0;
#ifdef DEBUG
    elog(NOTICE, "dumpIndex");
#endif    
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
        elog(NOTICE, "plist:%s", sidPostList.data);
#endif
        pfree(slist);
	}
    FileClose(dictFile);
    FileClose(postFile);
}