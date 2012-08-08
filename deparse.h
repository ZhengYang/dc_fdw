/*-------------------------------------------------------------------------
 *
 * deparse.h
 *		  Quals extraction utility
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *		  contrib/dc_fdw/deparse.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DC_DEPARSE_H
#define DC_DEPARSE_H

#include "postgres.h"
#include "foreign/foreign.h"
#include "nodes/relation.h"
#include "utils/relcache.h"

typedef struct PushableQualNode
{
    StringInfoData  opname; /* AND, OR, NOT if bool_node, '@@' else */
    StringInfoData  optype; /* [bool_node, op_node, func_node] */
    StringInfoData  leftOperand; /* for op_node only */
    StringInfoData  rightOperand; /* for op_node only */
    List            *childNodes; /* list of child PushableQualNodes */
    List            *plist; /* postings list assoc with this qual */
} PushableQualNode;

/*
 * Get string representation which can be used in SQL statement from a node.
 */
int extractQuals(PushableQualNode **qualRoot, 
                PlannerInfo *root, 
                RelOptInfo *baserel);
int deparseExpr(PushableQualNode *qual,
                Expr *node,
                PlannerInfo *root);
int deparseVar(PushableQualNode *qual, Var *node, PlannerInfo *root);
int deparseConst(PushableQualNode *qual, Const *node, PlannerInfo *root);
int deparseBoolExpr(PushableQualNode *qual, BoolExpr *node, PlannerInfo *root);
int deparseFuncExpr(PushableQualNode *qual, FuncExpr *node, PlannerInfo *root);
int deparseOpExpr(PushableQualNode *qual, OpExpr *node, PlannerInfo *root);
void evalQual(PushableQualNode *qualRoot, int indentLevel);
void copyTree(QTNode *qtTree, PushableQualNode *pqTree);

#endif   /* DC_DEPARSE_H */