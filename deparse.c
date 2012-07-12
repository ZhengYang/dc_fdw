/*-------------------------------------------------------------------------
 *
 * deparse.c
 *		  Quals extraction utility
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *		  contrib/dc_fdw/deparse.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "catalog/pg_class.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "deparse.h"


/*
 * Examine each element in the list baserestrictinfo of baserel, and sort them
 * into three groups: remote_conds contains conditions which can be evaluated
 *   - remote_conds is push-down safe, and don't contain any Param node
 *   - param_conds is push-down safe, but contain some Param node
 *   - local_conds is not push-down safe
 *
 * Only remote_conds can be used in remote EXPLAIN, and remote_conds and
 * param_conds can be used in final remote query.
 */
void
sort_quals(PlannerInfo *root, RelOptInfo *baserel)
{
	ListCell	   *lc;

	foreach(lc, baserel->baserestrictinfo)
	{
        PushableQual *qual = (PushableQual *) palloc(sizeof(PushableQual));
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
		
		elog(NOTICE, "NODE_TAG: %d", nodeTag(ri->clause));
		elog(NOTICE, "NODE_STR: %s", nodeToString(ri->clause));
        
        if (deparseExpr(qual, ri->clause, root) == 0 )
            elog(NOTICE, "QUAL: %s %s %s", qual->left.data, qual->opname.data, qual->right.data);
        else
            elog(NOTICE, "Unsupported quals encountered, ignore it!");
	}
}


/*
 * Deparse given expression into buf.  Actual string operation is delegated to
 * node-type-specific functions.
 *
 * Note that switch statement of this function MUST match the one in
 * foreign_expr_walker to avoid unsupported error..
 */
int
deparseExpr(PushableQual *qual, Expr *node, PlannerInfo *root)
{
	/*
	 * This part must be match foreign_expr_walker.
	 */
	switch (nodeTag(node))
	{
		case T_Const:
		    elog(NOTICE, "T_Const");
			return deparseConst(qual, (Const *) node, root);
			break;
		case T_BoolExpr:
		    elog(NOTICE, "T_BoolExpr");
            return -1;//deparseBoolExpr(qual, (BoolExpr *) node, root);
			break;
		case T_FuncExpr:
    		elog(NOTICE, "T_FuncExpr");
            return -1;//deparseFuncExpr(qual, (FuncExpr *) node, root);
    		break;
    	case T_OpExpr:
    	    elog(NOTICE, "T_OpExpr");
    		return deparseOpExpr(qual, (OpExpr *) node, root);
    		break;
        case T_Var:
		    elog(NOTICE, "T_Var");
			return deparseVar(qual, (Var *) node, root);
    		break;
    	/* Unsupported quals */	
		case T_NullTest:
		    elog(NOTICE, "T_NullTest: not pushable");
            return -1;
			break;
		case T_DistinctExpr:
		    elog(NOTICE, "T_DistinctExpr: not pushable");
            return -1;
			break;
		case T_RelabelType:
		    elog(NOTICE, "T_RelabelType: not pushable");
            return -1;
			break;
		case T_Param:
		    elog(NOTICE, "T_Param: not pushable");
            return -1;
			break;
		case T_ScalarArrayOpExpr:
		    elog(NOTICE, "T_ScalarArrayOpExpr: not pushable");
            return -1;
			break;
		case T_ArrayRef:
		    elog(NOTICE, "T_ArrayRef: not pushable");
            return -1;
			break;
		case T_ArrayExpr:
		    elog(NOTICE, "T_ArrayExpr: not pushable");
            return -1;
			break;
		default:
			{
				ereport(ERROR,
						(errmsg("unsupported expression for deparse"),
						 errdetail("%s", nodeToString(node))));
			}
            return -1;
			break;
	}
}

/*
 * Deparse node into buf, with relation qualifier if need_prefix was true.  If
 * node is a column of a foreign table, use value of colname FDW option (if any)
 * instead of attribute name.
 */
int
deparseVar(PushableQual *qual,
		   Var *node,
		   PlannerInfo *root)
{
	RangeTblEntry  *rte;
	char		   *colname = NULL;
	List		   *options;
	ListCell	   *lc;

	/* node must not be any of OUTER_VAR,INNER_VAR and INDEX_VAR. */
	Assert(node->varno >= 1 && node->varno <= root->simple_rel_array_size);

	/* Get RangeTblEntry from array in PlannerInfo. */
	rte = root->simple_rte_array[node->varno];

	/*
	 * If the node is a column of a foreign table, and it has colname FDW
	 * option, use its value.
	 */
	if (rte->relkind == RELKIND_FOREIGN_TABLE)
	{
		options = GetForeignColumnOptions(rte->relid, node->varattno);
		foreach(lc, options)
		{
			DefElem	   *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "colname") == 0)
			{
				colname = defGetString(def);
				break;
			}
		}
	}

	/*
	 * If the node refers a column of a regular table or it doesn't have colname
	 * FDW option, use attribute name.
	 */
	if (colname == NULL)
		colname = get_attname(rte->relid, node->varattno);

	if (qual->left.len == 0 && 
	    qual->right.len == 0 && 
	    strcmp(qual->opname.data, "@@") == 0)
	{
        appendStringInfo(&qual->left, "%s", colname);
        return 0;
	}
	else {
	    elog(NOTICE, "Var not supported!");
        return -1;
	}
}


/*
 * Deparse given constant value into buf.  This function have to be kept in
 * sync with get_const_expr.
 */
int
deparseConst(PushableQual *qual,
			 Const *node,
			 PlannerInfo *root)
{
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;
	bool		isfloat = false;

	if (node->constisnull)
	{
        elog(NOTICE, "Const Null is unsupported!");
		return -1;
	}
	
	/* check if the qual is in good shape to be pushed down */
    if (strcmp(qual->opname.data, "@@") == 0 &&
        qual->left.len != 0 &&
        qual->right.len == 0)
    {
        getTypeOutputInfo(node->consttype,
    					  &typoutput, &typIsVarlena);
    	extval = OidOutputFunctionCall(typoutput, node->constvalue);
        
    	switch (node->consttype)
    	{
    		case ANYARRAYOID:
    		case ANYNONARRAYOID:
    		case BOOLOID:
    			elog(ERROR, "anyarray and anyenum are not supported");
    			break;
    		case INT2OID:
    		case INT4OID:
    		case INT8OID:
    		case OIDOID:
    		case FLOAT4OID:
    		case FLOAT8OID:
    		case NUMERICOID:
    			{
    				/*
    				 * No need to quote unless they contain special values such as
    				 * 'Nan'.
    				 */
    				if (strspn(extval, "0123456789+-eE.") == strlen(extval))
    				{
    					if (extval[0] == '+' || extval[0] == '-')
    						appendStringInfo(&qual->right, "%s", extval);
    					else
    						appendStringInfoString(&qual->right, extval);
    					if (strcspn(extval, "eE.") != strlen(extval))
    						isfloat = true;	/* it looks like a float */
    				}
    				else
    					appendStringInfo(&qual->right, "'%s'", extval);
    			}
    			break;
    		case BITOID:
    		case VARBITOID:
    			appendStringInfo(&qual->right, "B'%s'", extval);
    			break;
    		default:
    			{
    				const char *valptr;
    				
    				for (valptr = extval; *valptr; valptr++)
    				{
    					char		ch = *valptr;

    					/*
    					 * standard_conforming_strings of remote session should be
    					 * set to similar value as local session.
    					 */
    					if (SQL_STR_DOUBLE(ch, !standard_conforming_strings))
    						appendStringInfoChar(&qual->right, ch);
    					appendStringInfoChar(&qual->right, ch);
    				}
    			}
    			break;
    	}
        return 0;
    }
    else {
        elog(NOTICE, "Const not supported!");
        return -1;
    }
}

/*
int
deparseBoolExpr(StringInfo buf,
				BoolExpr *node,
				PlannerInfo *root)
{
	ListCell   *lc;
	char	   *op;
	bool		first;

	switch (node->boolop)
	{
		case AND_EXPR:
			op = "AND";
			break;
		case OR_EXPR:
			op = "OR";
			break;
		case NOT_EXPR:
			appendStringInfo(buf, "(NOT ");
			deparseExpr(buf, list_nth(node->args, 0), root);
			appendStringInfo(buf, ")");
			return;
	}

	first = true;
	appendStringInfo(buf, "(");
	foreach(lc, node->args)
	{
		if (!first)
			appendStringInfo(buf, " %s ", op);
		deparseExpr(buf, (Expr *) lfirst(lc), root);
		first = false;
	}
	appendStringInfo(buf, ")");
}
*/
/*
 * Deparse given node which represents a function call into buf.  We treat only
 * explicit function call and explicit cast (coerce), because others are
 * processed on remote side if necessary.
 *
 * Function name (and type name) is always qualified by schema name to avoid
 * problems caused by different setting of search_path on remote side.
 */
 /*
int
deparseFuncExpr(StringInfo buf,
				FuncExpr *node,
				PlannerInfo *root)
{
	Oid				pronamespace;
	const char	   *schemaname;
	const char	   *funcname;
	ListCell	   *arg;
	bool			first;

	pronamespace = get_func_namespace(node->funcid);
	schemaname = quote_identifier(get_namespace_name(pronamespace));
	funcname = quote_identifier(get_func_name(node->funcid));

	if (node->funcformat == COERCE_EXPLICIT_CALL)
	{
		
		appendStringInfo(buf, "%s.%s(", schemaname, funcname);
		first = true;
		foreach(arg, node->args)
		{
			if (!first)
				appendStringInfo(buf, ", ");
			deparseExpr(buf, lfirst(arg), root);
			first = false;
		}
		appendStringInfoChar(buf, ')');
	}
	else if (node->funcformat == COERCE_EXPLICIT_CAST)
	{
		
		appendStringInfoChar(buf, '(');
		deparseExpr(buf, linitial(node->args), root);
		appendStringInfo(buf, ")::%s", funcname);
	}
	else
	{
		deparseExpr(buf, linitial(node->args), root);
	}
}
*/
/*
 * Deparse given operator expression into buf.  To avoid problems around
 * priority of operations, we always parenthesize the arguments.  Also we use
 * OPERATOR(schema.operator) notation to determine remote operator exactly.
 */
int
deparseOpExpr(PushableQual *qual,
			  OpExpr *node,
			  PlannerInfo *root)
{
	HeapTuple	tuple;
	Form_pg_operator form;
	const char *opnspname;
	char	   *opname;
	char		oprkind;
	ListCell   *arg;

	/* Retrieve necessary information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);
	opnspname = quote_identifier(get_namespace_name(form->oprnamespace));
	/* opname is not a SQL identifier, so we don't need to quote it. */
	opname = NameStr(form->oprname);
	oprkind = form->oprkind;
	ReleaseSysCache(tuple);

    /* the only type of qual we can push down is <column> @@ <key> */
    elog(NOTICE, "opnspname:%s", opnspname);
    elog(NOTICE, "opname:%s", opname);
    elog(NOTICE, "oprkind:%c", oprkind);
    
    if (strcmp(opnspname, "pg_catalog") == 0 && 
        strcmp(opname, "@@") == 0 &&
        oprkind == 'b' )
    {
        char    *opname;
        initStringInfo(&qual->opname);
        appendStringInfo(&qual->opname, "%s", "@@");
        initStringInfo(&qual->left);
        initStringInfo(&qual->right);
        
        arg = list_head(node->args);
        if (deparseExpr(qual, lfirst(arg), root) != 0)
            return -1;
        arg = list_tail(node->args);
        return deparseExpr(qual, lfirst(arg), root);
    }
    else {
        elog(NOTICE, "OpExpr not supported!");
        return -1;
    }
}