/*-------------------------------------------------------------------------
 *
 * searcher.c
 *		  Searcher for document collections foreign-data wrapper.
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *		  contrib/dc_fdw/searcher.c
 *
 *-------------------------------------------------------------------------
 */

#include "qual_pushdown.h"

bool hasSkip(int curr, int interval, int total);
int skip(int curr, int interval);

/*
 * open stats file
 */
File
openStat (char *indexpath)
{
    StringInfoData sid_stat_dir;
    
    initStringInfo(&sid_stat_dir);
    appendStringInfo(&sid_stat_dir, "%s/stat", indexpath);
    
    return PathNameOpenFile(sid_stat_dir.data, O_RDONLY,  0666);
}

/*
 * open dict file
 */
File
openDict (char *indexpath)
{
    StringInfoData sid_dict_dir;
    
    initStringInfo(&sid_dict_dir);
    appendStringInfo(&sid_dict_dir, "%s/dict", indexpath);
    
    return PathNameOpenFile(sid_dict_dir.data, O_RDONLY,  0666);
}

/*
 * open postings file
 */
File
openPost (char *indexpath)
{
    StringInfoData sid_post_dir;
    
    initStringInfo(&sid_post_dir);
    appendStringInfo(&sid_post_dir, "%s/post", indexpath);
    
    return PathNameOpenFile(sid_post_dir.data, O_RDONLY,  0666);
}

/*
 * open a doc from collection
 */
File
openDoc (char *fname)
{
    return PathNameOpenFile(fname, O_RDONLY,  0666);
}

/*
 * close stats file
 */
void
closeStat (File sfile)
{
    FileClose(sfile);
}

/*
 * close dict file
 */
void
closeDict (File dfile)
{
    FileClose(dfile);
}

/*
 * close post file
 */
void
closePost (File pfile)
{
    FileClose(pfile);
}

/*
 * close doc
 */
void
closeDoc (File file)
{
    FileClose(file);
}

/*
 * load precalculated collection-wise stats
 */
int
loadStat(CollectionStats **stats, File sfile)
{
    int     sz;         /* size of stats file */
    char    *buf;       /* buffer for stats file content */
    int     status;     /* sscanf status */
    int     dcNumOfFiles;
    int     dcNumOfBytes;
    
#ifdef DEBUG
     elog(NOTICE, "loadStat");
#endif

    /* load file content into buffer */
    sz = FileSeek(sfile, 0, SEEK_END);
    FileSeek(sfile, 0, SEEK_SET);
    buf = (char *) palloc(sizeof(char) * (sz + 1) );
    FileRead(sfile, buf, sz);
    buf[sz] = 0;
    
    /* number of documents in the doc collection */
    status = sscanf(buf,
            "NUM_OF_DOCS:%d\nNUM_OF_BYTES:%d",
			 &dcNumOfFiles, &dcNumOfBytes);
	if (status != 2)
		elog(ERROR, "Cannot read stats file!");
	
    (*stats)->numOfDocs = dcNumOfFiles;
    (*stats)->numOfBytes = dcNumOfBytes;
    (*stats)->bytesPerDoc = ((double) dcNumOfBytes) / dcNumOfFiles;
	
    return 0;
}

/*
 * load dict into memory from file
 */
int
loadDict(HTAB **dict, File dfile)
{
    int             sz;     /* size of the dict file */
    char            *buf;
    char            *token;
    PostingInfo     *re;
    StringInfoData  sidTerm;
    int ptr = 0;            /* pointer to start position */
    int o = 0;
    
#ifdef DEBUG
    elog(NOTICE, "loadDict");
#endif
    
    /* load file content into buffer */
    sz = FileSeek(dfile, 0, SEEK_END);
    FileSeek(dfile, 0, SEEK_SET);
    buf = (char *) palloc(sizeof(char) * (sz + 1) );
    FileRead(dfile, buf, sz);
    buf[sz] = 0;

    initStringInfo(&sidTerm);
    token = strtok(buf, " \n");
    while ( token != NULL )
    {
        /* term token */
        if (o % 3 == 0)
        {
            appendStringInfo(&sidTerm, "%s", token);
        }
        /* pointer to start position */
        else if (o % 3 == 1) {
            ptr = atoi(token);
        }
        /* length of the plist string*/
        else if (o % 3 == 2) {
            bool found;
            re = (PostingInfo *) hash_search(*dict, (void *) sidTerm.data, HASH_ENTER, &found);
            if (found == TRUE)
            {
                elog(ERROR, "Dictionary file corrupted!");
                return -1;
            }
            else
            {
                re->ptr = ptr;
                re->len = atoi(token);
            }
            resetStringInfo(&sidTerm);
        }
        token = strtok(NULL, " \n");
        o ++;
    }
    return 0;
}


int
loadDoc(char **buf, File file)
{
    int sz;
    /* read the content of the file */
    sz = FileSeek(file, 0, SEEK_END);
    FileSeek(file, 0, SEEK_SET);
    *buf = (char *) palloc(sizeof(char) * (sz + 1) );
    FileRead(file, *buf, sz);
    (*buf)[sz] = 0;
    return 0;
}


/*
 * skip list functions
 */
bool
hasSkip(int curr, int interval, int total)
{
    if (total == curr)
        return FALSE;
    else {
        if (curr % interval == 0) {
            if (curr + interval < total)
                return TRUE;
            else
                return FALSE;
        }
        else
            return FALSE;
    }
}

int
skip(int curr, int interval)
{
    return curr + interval;
}


/*
 * return (list1 AND list2)
 */
List *
pIntersect(List *list1, List *list2)
{
    List *rList = NIL;
    int list1len = list_length(list1);
    int list2len = list_length(list2);
    int skipInterval1 = (int) sqrt((double) list1len);
    int skipInterval2 = (int) sqrt((double) list2len);
    int list1curr = 0;
    int list2curr = 0;
    /*
    elog(NOTICE, "%s", "pIntersect");
    elog(NOTICE, "List1:%d", list1len);
    elog(NOTICE, "List2:%d", list2len);
    */
    while (list1curr < list1len && list2curr < list2len)
    {
        int entry1 = list_nth_int(list1, list1curr);
        int entry2 = list_nth_int(list2, list2curr);
        /*
        elog(NOTICE, "ListCurr1:%d:%d", list1curr, entry1);
        elog(NOTICE, "ListCurr2:%d:%d", list2curr, entry2);
        */
        if (entry1 == entry2)
        {
            rList = lappend_int(rList, entry1);
            list1curr ++;
            list2curr ++;
        }
        else if (entry1 < entry2) {
            if (hasSkip(list1curr, skipInterval1, list1len) && 
                list_nth_int(list1, skip(list1curr, skipInterval1)) <= entry2 ) 
            {
                while (hasSkip(list1curr, skipInterval1, list1len) && 
                    list_nth_int(list1, skip(list1curr, skipInterval1)) <= entry2)
                    list1curr = skip(list1curr, skipInterval1);
            }
            else
                list1curr ++;
        }
        else {
            if (hasSkip(list2curr, skipInterval2, list2len) && 
                list_nth_int(list2, skip(list2curr, skipInterval2)) <= entry1)
            {
                while (hasSkip(list2curr, skipInterval2, list2len)  && 
                    list_nth_int(list2, skip(list2curr, skipInterval2)) <= entry1 )
                    list2curr = skip(list2curr, skipInterval2);
            }
            else
                list2curr ++;
        }
    }
    
    return rList;
}


/*
 * return (list1 AND NOT list2)
 */
List *
pIntersectNot(List *list1, List *list2)
{
    List *rList = NIL;
    int list1len = list_length(list1);
    int list2len = list_length(list2);
    int list1curr = 0;
    int list2curr = 0;
    
    while (list1curr < list1len)
    {
        int entry1 = list_nth_int(list1, list1curr);
        
        if (list2curr < list2len)
        {
            int entry2 = list_nth_int(list2, list2curr);
            if (entry1 == entry2)
            {
                list1curr ++;
                list2curr ++;
            }
            else if (entry1 < entry2)
            {
                rList = lappend_int(rList, entry1);
                list1curr ++;
            }
            else if (entry1 > entry2)
            {
                list2curr ++;
            }
        }
        else {
            rList = lappend_int(rList, entry1);
            list1curr ++;
        }    
    }
    
    return rList;
}


/*
 * return (list1 OR list2)
 */
List *
pUnion(List *list1, List *list2)
{
    List *rList = NIL;
    int list1len = list_length(list1);
    int list2len = list_length(list2);
    int list1curr = 0;
    int list2curr = 0;

    while (list1curr < list1len || list2curr < list2len)
    {
        if (list1curr < list1len && list2curr < list2len)
        {
            int entry1 = list_nth_int(list1, list1curr);
            int entry2 = list_nth_int(list2, list2curr);
            if ( entry1 == entry2 )
            {
                rList = lappend_int(rList, entry1);
            }
            else {
                rList = lappend_int(rList, entry1);
                rList = lappend_int(rList, entry2);
            }
            list1curr ++;
            list2curr ++;
        }
        else if (list1curr < list1len) {
            int entry1 = list_nth_int(list1, list1curr);
            rList = lappend_int(rList, entry1);
            list1curr ++;
        }
        else if (list2curr < list2len) {
            int entry2 = list_nth_int(list2, list2curr);
            rList = lappend_int(rList, entry2);
            list2curr ++;
        }
    }
    return rList;
}


/*
 * return (NOT list)
 */
List *
pNegate(List *list, List *allList)
{
    return pIntersectNot(allList, list);
}


/*
 * retrive postings list by searching a term
 */
List *
searchTerm(char *text, HTAB * dict, File pfile, bool isALL)
{
    List *rList = NIL;
    bool found;
    char *pstr;
    char *token;
    Oid  cfgId;
    TSVector tsvector;
    char *lexemesptr;
    WordEntry *curentryptr;
    StringInfoData str;
    PostingInfo *re;
    char *term = text;

#ifdef DEBUG
    elog(NOTICE, "searchTerm");
    elog(NOTICE, "Term:%s", term);
#endif
    
    if (!isALL)
    {
        /* normalize term to root form */
        cfgId = getTSCurrentConfig(true);
        tsvector = (TSVector) DirectFunctionCall1( to_tsvector, PointerGetDatum(cstring_to_text(term)) );
        lexemesptr = STRPTR(tsvector);
        curentryptr = ARRPTR(tsvector);
        initStringInfo (&str);
        appendBinaryStringInfo (&str, lexemesptr + curentryptr->pos, curentryptr->len);
        term = str.data;
    }
    
    /* search term in the dictionary */
    re = (PostingInfo *) hash_search(dict, (void *) term, HASH_FIND, &found);   
    
    if (found)
    {
        /* load postings file */
        FileSeek(pfile, re->ptr, SEEK_SET);
        pstr = (char *) palloc(sizeof(char) * (re->len + 1) );
        FileRead(pfile, pstr, re->len);
        pstr[re->len] = 0;
        
        /* unserialize postings string */
        token = strtok(pstr, " ");
        while ( token != NULL )
        {
            rList = lappend_int(rList, atoi(token));
            token = strtok(NULL, " ");
        }
    }
    else
        rList = NIL;
    return rList;
}

/*
 * evaluate the qual tree
 */
List *
evalQualTree(PushableQualNode *node, HTAB *dict, File pfile, List *allList)
{
    List *rList = NIL;
    
#ifdef DEBUG
    elog(NOTICE, "evalQualTree");
#endif
    
    /*
     * if op_node (leaf node)
     * then retrieve postings
     */
    if (strcmp(node->optype.data, "op_node") == 0)
    {
        if ( strcmp( node->opname.data, "@@" ) == 0)
            rList = searchTerm(node->rightOperand.data, dict, pfile, FALSE);
        else if ( strcmp( node->opname.data, "=" ) == 0)
            rList = list_make1_int( atoi(node->rightOperand.data) );
    }
    /*
     * else bool_node (internal node)
     * perform boolean operations
     */
    else if (strcmp((node->optype).data, "bool_node") == 0) 
    {
        ListCell *cell;
        if (strcmp((node->opname).data, "AND") == 0)
        {
            bool firstNode = TRUE;
            foreach(cell, node->childNodes)
            {
                PushableQualNode *childNode = (PushableQualNode *) lfirst(cell);
                
                if (firstNode)
                {
                    rList = evalQualTree(childNode, dict, pfile, allList);
                    firstNode = FALSE;
                }
                else
                    rList = pIntersect(rList, evalQualTree(childNode, dict, pfile, allList));
            }
        }
        else if (strcmp((node->opname).data, "OR") == 0)
        {
            foreach(cell, node->childNodes)
            {
                PushableQualNode *childNode = (PushableQualNode *) lfirst(cell);
                rList = pUnion(rList, evalQualTree(childNode, dict, pfile, allList));
            }
        }
        else if (strcmp((node->opname).data, "NOT") == 0)
        {
            PushableQualNode *childNode = (PushableQualNode *) list_nth (node->childNodes, 0);
            rList = pNegate(evalQualTree(childNode, dict, pfile, allList), allList);
        }
    }
    return rList;
}