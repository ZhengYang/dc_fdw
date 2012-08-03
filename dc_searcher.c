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

#include "dc_searcher.h"

bool hasSkip(int curr, int interval, int total);
int skip(int curr, int interval);

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

int
dc_load_dict(HTAB **dict, char *indexpath)
{
    File dict_file;
    int sz;
    int o = 0;
    char *token;
    char *buffer;
    StringInfoData sid_dict_dir;
    
    PostingInfo *re;
    StringInfoData sid_term;
    int ptr = 0;
    
    HASH_SEQ_STATUS status;
    int cursor;
    PostingInfo *d_entry;

#ifdef DEBUG
    elog(NOTICE, "enter dc_load_dict");
#endif
    
    initStringInfo(&sid_dict_dir);
    appendStringInfo(&sid_dict_dir, "%s/dictionary", indexpath);

#ifdef DEBUG
    elog(NOTICE, "%s", sid_dict_dir.data);
#endif
    
    
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
            re = (PostingInfo *) hash_search(*dict, (void *) sid_term.data, HASH_ENTER, &found);
            if (found == TRUE)
            {
                elog(NOTICE, "Dictionary file '%s' corrupted!", sid_dict_dir.data);
                return -1;
            }
            else
            {
                re->ptr = ptr;
                re->len = atoi(token);
            }
            resetStringInfo(&sid_term);
        }
        
        token = strtok(NULL, " \n");
        o ++;
    }
/*
    hash_seq_init(&status, *dict);
    cursor = 0;
    while ((d_entry = (PostingInfo *) hash_seq_search(&status)) != NULL)
	{
        elog(NOTICE, "D_ENTRY: %s", d_entry->key);
    }
*/  
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
            if (curr + interval > total)
                return FALSE;
            else
                return TRUE;
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
    List *rList;
    int list1len = list_length(list1);
    int list2len = list_length(list2);
    int skipInterval1 = (int) sqrt((double) list1len);
    int skipInterval2 = (int) sqrt((double) list2len);
    int list1curr = 0;
    int list2curr = 0;
    
    while (list1curr < list1len && list2curr < list2len)
    {
        PostingEntry *entry1 = (PostingEntry *) list_nth(list1, list1curr);
        PostingEntry *entry2 = (PostingEntry *) list_nth(list2, list2curr);
        if (entry1->doc_id == entry2->doc_id)
        {
            rList = lappend_int(rList, entry1->doc_id);
            list1curr ++;
            list2curr ++;
        }
        else if (entry1->doc_id < entry2->doc_id) {
            if (hasSkip(list1curr, skipInterval1, list1len) && 
                ((PostingEntry *) list_nth(list1, skip(list1curr, skipInterval1)))->doc_id <= entry2->doc_id ) 
            {
                while (hasSkip(list1curr, skipInterval1, list1len) && 
                    ((PostingEntry *) list_nth(list1, skip(list1curr, skipInterval1)))->doc_id <= entry2->doc_id)
                    list1curr = skip(list1curr, skipInterval1);
            }
            else
                list1curr ++;
        }
        else {
            if (hasSkip(list2curr, skipInterval2, list2len) && 
                ((PostingEntry *) list_nth(list2, skip(list2curr, skipInterval2)))->doc_id <= entry1->doc_id)
            {
                while (hasSkip(list2curr, skipInterval2, list2len)  && 
                    ((PostingEntry *) list_nth(list2, skip(list2curr, skipInterval2)))->doc_id <= entry1->doc_id )
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
    List *rList;
    int list1len = list_length(list1);
    int list2len = list_length(list2);
    int list1curr = 0;
    int list2curr = 0;
    
    while (list1curr < list1len)
    {
        PostingEntry *entry1 = (PostingEntry *) list_nth(list1, list1curr);
        
        if (list2curr < list2len)
        {
            PostingEntry *entry2 = (PostingEntry *) list_nth(list2, list2curr);
            if (entry1->doc_id == entry2->doc_id)
            {
                list1curr ++;
                list2curr ++;
            }
            else if (entry1->doc_id < entry2->doc_id)
            {
                rList = lappend_int(rList, entry1->doc_id);
                list1curr ++;
            }
            else if (entry1->doc_id > entry2->doc_id)
            {
                list2curr ++;
            }
        }
        else {
            rList = lappend_int(rList, entry1->doc_id);
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

    elog(NOTICE, "%s", "pUnion");
    elog(NOTICE, "List1:%d", list1len);
    elog(NOTICE, "List2:%d", list2len);

    while (list1curr < list1len || list2curr < list2len)
    {
        if (list1curr < list1len && list2curr < list2len)
        {
            PostingEntry *entry1 = (PostingEntry *) list_nth(list1, list1curr);
            PostingEntry *entry2 = (PostingEntry *) list_nth(list2, list2curr);
            if ( entry1->doc_id == entry2->doc_id )
            {
                rList = lappend_int(rList, entry1->doc_id);
            }
            else {
                rList = lappend_int(rList, entry1->doc_id);
                rList = lappend_int(rList, entry2->doc_id);
            }
            list1curr ++;
            list2curr ++;
        }
        else if (list1curr < list1len) {
            PostingEntry *entry1 = (PostingEntry *) list_nth(list1, list1curr);
            rList = lappend_int(rList, entry1->doc_id);
            list1curr ++;
        }
        else if (list2curr < list2len) {
            PostingEntry *entry2 = (PostingEntry *) list_nth(list2, list2curr);
            rList = lappend_int(rList, entry2->doc_id);
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
    char *term = text;

#ifdef DEBUG
    elog(NOTICE, "searchTerm: %s", text);
#endif
    elog(NOTICE, "term:%s", term);
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
    PostingInfo *re = (PostingInfo *) hash_search(dict, (void *) term, HASH_FIND, &found);   
    
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
 * evaluate the pushdown qual tree
 */
List *
evalQualTree(PushableQualNode *node, HTAB *dict, char *indexpath, List *allList)
{
    List *rList = NIL;
    StringInfoData sid_post_dir;
    File pfile;
    
    initStringInfo(&sid_post_dir);
    appendStringInfo(&sid_post_dir, "%s/postings", indexpath);
    
    pfile = PathNameOpenFile(sid_post_dir.data, O_RDONLY,  0666);
    
    /*
     * if op_node (leaf node)
     * then retrieve postings
     */
    if (strcmp((node->optype).data, "op_node") == 0)
    {
        rList = searchTerm((node->rightOperand).data, dict, pfile, FALSE);
    }
    /*
     * else if bool_node
     * perform boolean operations
     */
    else if (strcmp((node->optype).data, "bool_node") == 0) 
    {
        ListCell *cell;
        if (strcmp((node->opname).data, "AND") == 0)
        {
            foreach(cell, node->childNodes)
            {
                PushableQualNode *childNode = (PushableQualNode *) lfirst(cell);
                rList = pIntersect(rList, evalQualTree(childNode, dict, indexpath, allList));
            }
        }
        else if (strcmp((node->opname).data, "OR") == 0)
        {
            foreach(cell, node->childNodes)
            {
                PushableQualNode *childNode = (PushableQualNode *) lfirst(cell);
                rList = pUnion(rList, evalQualTree(childNode, dict, indexpath, allList));
            }
        }
        else if (strcmp((node->opname).data, "NOT") == 0)
        {
            PushableQualNode *childNode = (PushableQualNode *) list_nth (node->childNodes, 0);
            rList = pNegate(evalQualTree(childNode, dict, indexpath, allList), allList);
        }
    }
    return rList;
}