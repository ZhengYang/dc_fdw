/*-------------------------------------------------------------------------
 *
 * qual_extract.h
 *		  Quals extraction utility
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *		  contrib/dc_fdw/qual_extract.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef QUAL_EXTRACT_H
#define QUAL_EXTRACT_H

#include "postgres.h"
#include "foreign/foreign.h"
#include "nodes/relation.h"
#include "utils/relcache.h"

/*
 * Tree Nodes for qual pushdown evaluation
 */
typedef struct PushableQualNode
{
    StringInfoData  opname;         /* bool_node: [AND, OR, NOT] op_node: [@@, =] */
    StringInfoData  optype;         /* [bool_node, op_node] */
    StringInfoData  leftOperand;    /* for op_node only */
    StringInfoData  rightOperand;   /* for op_node only */
    List            *childNodes;    /* for bool_node only */
    List            *plist;         /* postings list assoc with this qual */
} PushableQualNode;

/*
 * Extraction function
 */
int extractQuals(PushableQualNode **qualRoot, PlannerInfo *root, RelOptInfo *baserel, List *mapping);
void freeQualTree(PushableQualNode *qualRoot);
void printQualTree(PushableQualNode *qualRoot, int indentLevel);

#endif   /* QUAL_EXTRACT_H */