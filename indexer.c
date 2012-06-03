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

int dc_index(char *pathname)
{
    DIR *dir;
    struct dirent *dirent;
    int num_of_files = 0;
    char *buffer;
    StringInfoData sid_data_dir;
    File curr_file;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    TSVectorParseState parser_state;
    struct SN_env *sn_env;
    
#ifdef DEBUG
    elog(NOTICE, "%s", "dc_index");
    elog(NOTICE, "PATH NAME: %s", pathname);
    elog(NOTICE, "PATH LEN: %d", (int) strlen(pathname));
#endif

    dir = AllocateDir(pathname);
    if (dir == NULL) {
        // TODO: use pgsql error reporting
        elog(NOTICE, "ERROR: Path not found!");
        return -1;
    }
    
    /*
     * Loop through data dir to read each of the files in the dir
     * and tokenize the content of the files.
     */
    while( (dirent = ReadDir(dir, pathname)) != NULL)
    {
        char *strval;
        char *endptr;
        int   lenval;
        int   sz;
        
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
        appendStringInfo(&sid_data_dir, "%s/%s", pathname, dirent->d_name);

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
        buffer = (char *) palloc(sizeof(char) * sz);
        FileRead(curr_file, buffer, sz);
        
#ifdef DEBUG
        elog(NOTICE, "-FILE SIZE: %d", sz);
        elog(NOTICE, "-FILE CONTENT: \n%s", buffer);
#endif
        
        /*
         * tokenization:
         * 1. init tokenizer
         * 2. process one token at a time
         */
        parser_state = init_tsvector_parser(buffer, true, false);
        while (gettoken_tsvector(parser_state, &strval, &lenval, NULL, NULL, &endptr) == true)
        {
#ifdef DEBUG
            elog(NOTICE, "--TOKEN: %s", strval);
            elog(NOTICE, "--LENVAL: %d", lenval);
#endif
            /*
             * stemming
             */
            sn_env = english_ISO_8859_1_create_env();
            SN_set_current(sn_env, lenval, strval);
            english_ISO_8859_1_stem (sn_env);
            english_ISO_8859_1_close_env (sn_env);
            sn_env->p[sn_env->l] = 0;
            elog(NOTICE, "english_ISO_8859_1_stem stems '%s' to '%s'", strval, sn_env->p);
        }
        
        /*
         *  Clean-up:
         *  1. close paser handle
         *  2. free buffer memory
         *  3. close file
         */
        close_tsvector_parser(parser_state);
        pfree(buffer);
        FileClose(curr_file);
        
        /*
         * document collection size counter
         */
        num_of_files ++;
        break;
    }
    
#ifdef DEBUG
    elog(NOTICE, "NUM OF FILES: %d", num_of_files);
#endif
    FreeDir(dir);
    return 0;
}