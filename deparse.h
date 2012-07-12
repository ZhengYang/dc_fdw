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

typedef struct PushableQual
{
    StringInfoData  left;
    StringInfoData  opname;
    StringInfoData  right;
} PushableQual;

/*
 * Get string representation which can be used in SQL statement from a node.
 */
void sort_quals(PlannerInfo *root, RelOptInfo *baserel);
 					
int deparseExpr(PushableQual *qual, Expr *expr, PlannerInfo *root);
int deparseVar(PushableQual *qual, Var *node, PlannerInfo *root);
int deparseConst(PushableQual *qual, Const *node, PlannerInfo *root);
//void deparseBoolExpr(StringInfo buf, BoolExpr *node, PlannerInfo *root);
//void deparseFuncExpr(StringInfo buf, FuncExpr *node, PlannerInfo *root);
int deparseOpExpr(PushableQual *qual, OpExpr *node, PlannerInfo *root);

#endif   /* DC_DEPARSE_H */