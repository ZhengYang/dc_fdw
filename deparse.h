/*-------------------------------------------------------------------------
 *
 * deparse.h
 *		  indexer for document collections foreign-data wrapper.
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
/*
 * Context for walk-through the expression tree.
 */
typedef struct foreign_executable_cxt
{
	PlannerInfo	   *root;
	RelOptInfo	   *foreignrel;
	bool			has_param;
} foreign_executable_cxt;
/*
 * Get string representation which can be used in SQL statement from a node.
 */
 /* in deparse.c */
 void deparseSimpleSql(StringInfo buf,
 					  Oid relid,
 					  PlannerInfo *root,
 					  RelOptInfo *baserel);
 void appendWhereClause(StringInfo buf,
 					   bool has_where,
 					   List *exprs,
 					   PlannerInfo *root);
 void sortConditions(PlannerInfo *root,
 					RelOptInfo *baserel,
 					List **remote_conds,
 					List **param_conds,
 					List **local_conds);
 void deparseAnalyzeSql(StringInfo buf, Relation rel);
void deparseExpr(StringInfo buf, Expr *expr, PlannerInfo *root);
void deparseRelation(StringInfo buf, Oid relid, PlannerInfo *root,
							bool need_prefix);
void deparseVar(StringInfo buf, Var *node, PlannerInfo *root,
					   bool need_prefix);
void deparseConst(StringInfo buf, Const *node, PlannerInfo *root);
void deparseBoolExpr(StringInfo buf, BoolExpr *node, PlannerInfo *root);
void deparseNullTest(StringInfo buf, NullTest *node, PlannerInfo *root);
void deparseDistinctExpr(StringInfo buf, DistinctExpr *node,
								PlannerInfo *root);
void deparseRelabelType(StringInfo buf, RelabelType *node,
							   PlannerInfo *root);
void deparseFuncExpr(StringInfo buf, FuncExpr *node, PlannerInfo *root);
void deparseParam(StringInfo buf, Param *node, PlannerInfo *root);
void deparseScalarArrayOpExpr(StringInfo buf, ScalarArrayOpExpr *node,
									 PlannerInfo *root);
void deparseOpExpr(StringInfo buf, OpExpr *node, PlannerInfo *root);
void deparseArrayRef(StringInfo buf, ArrayRef *node, PlannerInfo *root);
void deparseArrayExpr(StringInfo buf, ArrayExpr *node, PlannerInfo *root);

/*
 * Determine whether an expression can be evaluated on remote side safely.
 */
bool is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr,
							bool *has_param);
bool foreign_expr_walker(Node *node, foreign_executable_cxt *context);
bool is_builtin(Oid procid);

#endif   /* DC_DEPARSE_H */