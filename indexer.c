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
    //FILE *curr_file;
    char *buffer;
    StringInfoData sid_data_dir;
    File curr_file;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    
    elog(NOTICE, "PATH NAME: %s", pathname);
    elog(NOTICE, "PATH LEN: %d", (int) strlen(pathname));
    
    dir = AllocateDir(pathname);
    //elog(NOTICE, "%s", dir->pathname);
    if (dir == NULL) {
        // TODO: use pgsql error reporting
        elog(NOTICE, "ERROR: Path not found!");
        return -1;
    }
    
    
    while( (dirent = ReadDir(dir, pathname)) != NULL) {
        //elog(NOTICE, "FileName: %s", dirent->d_name);
        //elog(NOTICE, "d_namlen: %d", dirent->d_namlen);
        //elog(NOTICE, "d_reclen: %d", dirent->d_reclen);
        
        /* ignore . and .. */
        if (strcmp(".", dirent->d_name) == 0) continue;
        if (strcmp("..", dirent->d_name) == 0) continue;
        
        /* concat to full path name */
        initStringInfo(&sid_data_dir);
        appendStringInfo(&sid_data_dir, "%s/%s", pathname, dirent->d_name);
        //elog(NOTICE, "Fullname: %s", sid_data_dir.data);
        buffer = (char *) palloc(sizeof(char) * 100);
        
        /* open file for processing */
        curr_file = PathNameOpenFile(sid_data_dir.data, O_RDONLY,  mode);
        //curr_file = AllocateFile(sid_data_dir.data, "r");
        FileRead(curr_file, buffer, 100);
        elog(NOTICE, "CONTENT: %s", buffer);
        //FreeFile(curr_file);
        FileClose(curr_file);
        num_of_files ++;
        break;
    }
    elog(NOTICE, "NUM OF FILES: %d", num_of_files);
    FreeDir(dir);
    return 0;
}