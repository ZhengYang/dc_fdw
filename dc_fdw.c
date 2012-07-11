/*-------------------------------------------------------------------------
 *
 * dc_fdw.c
 *		  foreign-data wrapper for server-side document collections.
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *		  contrib/dc_fdw/dc_fdw.c
 *
 *-------------------------------------------------------------------------
 */

/* Debug mode flag */
#define DEBUG
 
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/rel.h"
#include "optimizer/var.h"

#include "dc_indexer.h"
#include "dc_searcher.h"
#include "deparse.h"

PG_MODULE_MAGIC;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct DcFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

/*
 * Valid options for dc_fdw.
 *
 * Note: If you are adding new option for user mapping, you need to modify
 * dcGetOptions(), which currently doesn't bother to look at user mappings.
 */
static struct DcFdwOption valid_options[] = {
    /* Foreign table options */
	
	/* where the data files located */
	{"data_dir", ForeignTableRelationId},
	/* where the index file located */
	{"index_dir", ForeignTableRelationId},
	
	{"language", ForeignTableRelationId},
	{"encoding", ForeignTableRelationId},

	/* Sentinel */
	{NULL, InvalidOid}
};

/*
 * FDW-specific information for RelOptInfo.fdw_private.
 */
typedef struct DcFdwPlanState
{
	char        *data_dir;      /* documents to read */
    char        *index_dir;     /* index to output */
    char        *language;      /* language of the documents */
    char        *encoding;      /* encoding of the documents */
    List        *options;
	BlockNumber pages;			/* estimate of file's physical size */
    int         dc_size;        /* collection size in bytes */
	int		    ndocs;		    /* estimate of number of rows in file */
} DcFdwPlanState;


/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct DcFdwExecutionState
{
	char	   *data_dir;		    /* dc to read */
    DIR        *dir_state;
    AttInMetadata *attinmeta;
    int         dc_size;        /* collection size in bytes */
	int		    ndocs;		    /* estimate of number of rows in file */
} DcFdwExecutionState;

/*
 * SQL functions
 */
extern Datum dc_fdw_handler(PG_FUNCTION_ARGS);
extern Datum dc_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(dc_fdw_handler);
PG_FUNCTION_INFO_V1(dc_fdw_validator);

/*
 * FDW callback routines
 */
static void dcGetForeignRelSize(PlannerInfo *root, 
                                RelOptInfo *baserel,
                                Oid foreigntableid);
static void dcGetForeignPaths(PlannerInfo *root, 
                                RelOptInfo *baserel,
                                Oid foreigntableid);
static ForeignScan *dcGetForeignPlan(PlannerInfo *root, 
                                    RelOptInfo *baserel, 
                                    Oid foreigntableid, 
                                    ForeignPath *best_path, 
                                    List *tlist,
                                    List *scan_clauses);
static void dcExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void dcBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *dcIterateForeignScan(ForeignScanState *node);
static void dcReScanForeignScan(ForeignScanState *node);
static void dcEndForeignScan(ForeignScanState *node);
static bool dcAnalyzeForeignTable(Relation relation, 
                                    AcquireSampleRowsFunc *func, 
                                    BlockNumber *totalpages);

/*
 * Helper functions
 */
static bool is_valid_option(const char *option, Oid context);
static void dcGetOptions(Oid foreigntableid,
			   char **data_dir, char **index_dir, char **language, char **encoding, List **other_options);
static void estimate_size(PlannerInfo *root, RelOptInfo *baserel, 
                DcFdwPlanState *fdw_private);
static void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
            	DcFdwPlanState *fdw_private,
            	Cost *startup_cost, Cost *total_cost);
static int dc_acquire_sample_rows(Relation onerel, int elevel,
                HeapTuple *rows, int targrows,
            	double *totalrows, double *totaldeadrows);



/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
dc_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

#ifdef DEBUG
    elog(NOTICE, "dc_fdw_handler");
#endif

	//fdwroutine->PlanForeignScan = dcPlanForeignScan;
	fdwroutine->GetForeignRelSize = dcGetForeignRelSize;
    fdwroutine->GetForeignPaths = dcGetForeignPaths;
    fdwroutine->GetForeignPlan = dcGetForeignPlan;
	fdwroutine->ExplainForeignScan = dcExplainForeignScan;
	fdwroutine->BeginForeignScan = dcBeginForeignScan;
	fdwroutine->IterateForeignScan = dcIterateForeignScan;
	fdwroutine->ReScanForeignScan = dcReScanForeignScan;
	fdwroutine->EndForeignScan = dcEndForeignScan;
	fdwroutine->AnalyzeForeignTable = dcAnalyzeForeignTable;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses dc_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
dc_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	char	   *data_dir = NULL;
    char       *index_dir = NULL;
    char       *language = NULL;
    char       *encoding = NULL;
	List	   *other_options = NIL;
	ListCell   *cell;

#ifdef DEBUG
    elog(NOTICE, "dc_fdw_validator");
#endif

	/*
	 * Only superusers are allowed to set options of a dc_fdw foreign table.
	 * This is because the data_dir is one of those options, and we don't want
	 * non-superusers to be able to determine which file gets read.
	 *
	 * Putting this sort of permissions check in a validator is a bit of a
	 * crock, but there doesn't seem to be any other place that can enforce
	 * the check more cleanly.
	 *
	 * Note that the valid_options[] array disallows setting data_dir at any
	 * options level other than foreign table --- otherwise there'd still be a
	 * security hole.
	 */
	 
	if (catalog == ForeignTableRelationId && !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("only superuser can change options of a dc_fdw foreign table")));

	/*
	 * Check that only options supported by dc_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);
		
		if (!is_valid_option(def->defname, catalog))
		{
			struct DcFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 errhint("Valid options in this context are: %s",
							 buf.data)));
		}

		if (strcmp(def->defname, "data_dir") == 0)
		{
			if (data_dir)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("redundant options")));
			data_dir = defGetString(def);
		}
		
		if (strcmp(def->defname, "index_dir") == 0)
		{
			if (index_dir)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("redundant options")));
			index_dir = defGetString(def);
		}
		
		if (strcmp(def->defname, "language") == 0)
		{
			if (language)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("redundant options")));
			language = defGetString(def);
		}
		
		if (strcmp(def->defname, "encoding") == 0)
		{
			if (encoding)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("redundant options")));
			encoding = defGetString(def);
		}
		else
			other_options = lappend(other_options, def);
	}

	/*
	 * Dcname option is required for dc_fdw foreign tables.
	 */
	if (catalog == ForeignTableRelationId && data_dir == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("data_dir is required for dc_fdw foreign tables")));
	
	/*
	 * Start indexing procedures
	 * Note that validate function is called when 
	 *
	 * 1. Creating ForeignServer 
	 * 2. Creating ForeignTable
	 * 3. Creating UserMapping
	 *
	 * Call the index function only when ForeignTable is being created.
	 */
	if (catalog == ForeignTableRelationId) {
	    elog(NOTICE, "%s", "Creating Foreign Table...");
	    elog(NOTICE, "%s", "Start indexing document collection...");
	    dc_index(data_dir, index_dir);
	}
    
    
	PG_RETURN_VOID();
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
is_valid_option(const char *option, Oid context)
{
	struct DcFdwOption *opt;

#ifdef DEBUG
    elog(NOTICE, "is_valid_option");
#endif

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch the options for a dc_fdw foreign table.
 *
 * We have to separate out "data_dir" from the other options because
 * it must not appear in the options list passed to the core COPY code.
 */
static void
dcGetOptions(Oid foreigntableid,
			   char **data_dir, char **index_dir, char **language, char **encoding, List **other_options)
{
	ForeignTable        *table;
	ForeignServer       *server;
	ForeignDataWrapper  *wrapper;
	List	            *options;
	ListCell            *lc,
			            *prev;

#ifdef DEBUG
    elog(NOTICE, "dcGetOptions");
#endif


	/*
	 * Extract options from FDW objects.  We ignore user mappings & server because
	 * dc_fdw doesn't have any options that can be specified there.
	 *
	 * (XXX Actually, given the current contents of valid_options[], there's
	 * no point in examining anything except the foreign table's own options.
	 * Simplify?)
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);

	/*
	 * Separate out individual options.
	 */
	*data_dir = NULL;
    *index_dir = NULL;
    *language = NULL;
    *encoding = NULL;
    
	prev = NULL;
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		
#ifdef DEBUG
        elog(NOTICE, "<options> %s:%s", def->defname, defGetString(def));
#endif

		if (strcmp(def->defname, "data_dir") == 0)
		{
			*data_dir = defGetString(def);
            continue;
		}
		
		if (strcmp(def->defname, "index_dir") == 0)
		{
            *index_dir = defGetString(def);
            continue;
		}
		
		if (strcmp(def->defname, "language") == 0)
		{
            *language = defGetString(def);
            continue;
		}
		
		if (strcmp(def->defname, "encoding") == 0)
		{
            *encoding = defGetString(def);
		}
	}
	
	/*
	 * The validator should have checked that a data_dir directory was included in the
	 * options, but check again, just in case.
	 */
	if (*data_dir == NULL)
		elog(ERROR, "data_dir is required for dc_fdw foreign tables");
	*other_options = options;
}

/*
 * dcPlanForeignScan
 *		Create a FdwPlan for a scan on the foreign table
 */

//static FdwPlan *
//dcPlanForeignScan(Oid foreigntableid,
//					PlannerInfo *root,
//					RelOptInfo *baserel)
//{
//	FdwPlan    *fdwplan;
//	/*char	   *data_dir;*/
//	/*List	   *options;*/
//
//#ifdef DEBUG
//    elog(NOTICE, "dcPlanForeignScan");
//#endif
//
//	/* Fetch options --- we only need data_dir at this point */
//	/*dcGetOptions(foreigntableid, &data_dir, &options);*/
//	
//	/* Construct FdwPlan with cost estimates */
//	fdwplan = makeNode(FdwPlan);
//	/*estimate_costs(root, baserel, data_dir,
//				   &fdwplan->startup_cost, &fdwplan->total_cost);*/
//	fdwplan->startup_cost = 100;
//    fdwplan->total_cost = 100;
//	return fdwplan;
//}


/*
 * dcGetForeignRelSize
 *		Obtain relation size estimates for a foreign table
 */
static void
dcGetForeignRelSize(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid)
{
	DcFdwPlanState *fdw_private;
    int num_of_docs;
    int num_of_bytes;
    
#ifdef DEBUG
    elog(NOTICE, "dcGetForeignRelSize");
#endif

	/*
	 * Fetch options.  We only need filename at this point, but we might as
	 * well get everything and not need to re-fetch it later in planning.
	 */
	fdw_private = (DcFdwPlanState *) palloc(sizeof(DcFdwPlanState));
	dcGetOptions(foreigntableid,
    			   &fdw_private->data_dir, &fdw_private->index_dir,
    			   &fdw_private->language, &fdw_private->encoding,
                   &fdw_private->options);
	baserel->fdw_private = (void *) fdw_private;

	/* Estimate relation size */
    dc_load_stat(fdw_private->index_dir, &num_of_docs, &num_of_bytes);
    baserel->rows = (double) num_of_docs;
    elog(NOTICE, "NUM OF DOCS: %f", baserel->rows);
    fdw_private->dc_size = num_of_bytes;
    fdw_private->ndocs = num_of_docs;
	//estimate_size(root, baserel, fdw_private);
}

/*
 * dcGetForeignPaths
 *		Create possible access paths for a scan on the foreign table
 *
 *		Currently we don't support any push-down feature, so there is only one
 *		possible access path, which simply returns all records in the order in
 *		the data file.
 */
static void
dcGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{
	DcFdwPlanState *fdw_private = (DcFdwPlanState *) baserel->fdw_private;
	Cost		startup_cost;
	Cost		total_cost;
	
	List		   *remote_conds = NIL;
	List		   *param_conds = NIL;
	List		   *local_conds = NIL;
	
    StringInfo buf;

#ifdef DEBUG
    elog(NOTICE, "dcGetForeignPaths");
#endif

	/* Estimate costs */
	estimate_costs(root, baserel, fdw_private,
				   &startup_cost, &total_cost);

	/* Create a ForeignPath node and add it as only possible path */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
									 NIL));		/* no fdw_private data */
    //elog(NOTICE, "xixihaha: %s", nodeToString(baserel));
    
	/*
	 * If data file was sorted, and we knew it somehow, we could insert
	 * appropriate pathkeys into the ForeignPath node to tell the planner
	 * that.
	 */
     buf = makeStringInfo();
	 sortConditions(root, baserel, &remote_conds, &param_conds, &local_conds);
	 if (list_length(remote_conds) > 0)
	     appendWhereClause(buf, true, remote_conds, root);
	 elog(NOTICE, "SHIN: %s", buf->data);
}

/*
 * dcGetForeignPlan
 *		Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
dcGetForeignPlan(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses)
{
	Index		scan_relid = baserel->relid;

#ifdef DEBUG
    elog(NOTICE, "dcGetForeignPlan");
#endif


	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check.  So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							NIL);		/* no private state either */
}


/*
 * dcExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
dcExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	char	   *data_dir;
    char       *index_dir;
    char       *language;
    char       *encoding;
	List	   *options;
	DcFdwExecutionState	   *fdw_private;
	char	   *sql;
	int         num_of_docs;
    int         num_of_bytes;

#ifdef DEBUG
    elog(NOTICE, "dcExplainForeignScan");
#endif


	/* Fetch options --- we only need data_dir at this point */
	dcGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
				   &data_dir, &index_dir, &language, &encoding, &options);
	/* load stats */
	dc_load_stat(index_dir, &num_of_docs, &num_of_bytes);
	ExplainPropertyText("Foreign Document Collection", data_dir, es);
	
	//sql = strVal(list_nth(fdw_private, 1));
	//ExplainPropertyText("Remote SQL", sql, es);
	
	/* Suppress dc size if we're not showing cost details */
	ExplainPropertyLong("Foreign Document Collection Size", (long) num_of_bytes, es);
	ExplainPropertyLong("Number of Documents", (long) num_of_docs, es);
	//if (es->costs)
	//{
		
	//}
}

/*
 * dcBeginForeignScan
 *		Initiate access to the dc by creating CopyState
 */
static void
dcBeginForeignScan(ForeignScanState *node, int eflags)
{
	char	   *data_dir;
    char       *index_dir;
    char       *language;
    char       *encoding;
	List	   *options;
	DcFdwExecutionState *festate;
    int         num_of_docs;
    int         num_of_bytes;
    //List       *qual_list;
    //ListCell		*lc;

#ifdef DEBUG
    elog(NOTICE, "dcBeginForeignScan");
#endif
	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Fetch options of foreign table */
	dcGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
				   &data_dir, &index_dir, &language, &encoding, &options);
	
	/* Load dictionary into memory */
	//dc_load_dict(index_dir);
    elog(NOTICE, "%d", node->ss.ps.type);
	
	/*
    foreach (lc, node->ss.ps.qual)
    {
        OpExpr	*op;
        ExprState  *state = lfirst(lc);
        Node	*left;
        
        if (IsA(state->expr, OpExpr))
        {
            OpExpr	*op = (OpExpr *) node;
            if (list_length(op->args) == 2)
                elog(NOTICE, "%s", "OK");
            else
                elog(NOTICE, "%s", "NOT OK");
            left = list_nth(op->args, 0);
        }
        elog(NOTICE, "%d", state->type);
        elog(NOTICE, "%d", state->expr->type);
    }
    */
    dc_load_stat(index_dir, &num_of_docs, &num_of_bytes);
	/*
	 * Save state in node->fdw_state.  We must save enough information to call
	 * BeginCopyFrom() again.
	 */
	festate = (DcFdwExecutionState *) palloc(sizeof(DcFdwExecutionState));
	festate->data_dir = data_dir;
	festate->dir_state = AllocateDir(data_dir);
	festate->dc_size = num_of_bytes;        /* collection size in bytes */
	festate->ndocs = num_of_docs;		    /* estimate of number of rows in file */
	/* Store the additional state info */
    festate->attinmeta = TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);
    
	node->fdw_state = (void *) festate;
}

/*
 * dcIterateForeignScan
 *		Read next record from the data dc and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
dcIterateForeignScan(ForeignScanState *node)
{
	DcFdwExecutionState *festate = (DcFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	struct dirent *dirent;
    List *tupleItemList;
    

    while ( (dirent = ReadDir(festate->dir_state, festate->data_dir)) != NULL)
    {
        StringInfoData sid_data_dir;
        mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
        File curr_file;
        char *buffer;
        int sz;
        
        /* skip . and .. */
        if (strcmp(".", dirent->d_name) == 0) continue;
        if (strcmp("..", dirent->d_name) == 0) continue;
        
        /* get full path/name of the file */
        initStringInfo(&sid_data_dir);
        appendStringInfo(&sid_data_dir, "%s/%s", festate->data_dir, dirent->d_name);
        
        /* read the content of the file */
        curr_file = PathNameOpenFile(sid_data_dir.data, O_RDONLY,  mode);
        sz = FileSeek(curr_file, 0, SEEK_END);
        FileSeek(curr_file, 0, SEEK_SET);
        buffer = (char *) palloc(sizeof(char) * (sz + 1) );
        FileRead(curr_file, buffer, sz);
        buffer[sz] = 0;
        
        tupleItemList = list_make3(dirent->d_name, dirent->d_name, buffer);  
        
        /* only get 1 entry */
        break;
    }

	/*
	 * The protocol for loading a virtual tuple into a slot is first
	 * ExecClearTuple, then fill the values/isnull arrays, then
	 * ExecStoreVirtualTuple.  If we don't find another row in the dc, we
	 * just skip the last step, leaving the slot empty as required.
	 *
	 * We can pass ExprContext = NULL because we read all columns from the
	 * dc, so no need to evaluate default expressions.
	 *
	 * We can also pass tupleOid = NULL because we don't allow oids for
	 * foreign tables.
	 */
    ExecClearTuple(slot);
	if (dirent != NULL)
	{
	    char **values;
        HeapTuple tuple;
        int i;

        values = (char **) palloc(sizeof(char *) * 3);
        for (i = 0; i < 3; i++)
        {
            values[i] = list_nth(tupleItemList, i);
        }

        tuple = BuildTupleFromCStrings(festate->attinmeta, values);
        ExecStoreTuple(tuple, slot, InvalidBuffer, FALSE);
	}

	return slot;
}

/*
 * dcEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
dcEndForeignScan(ForeignScanState *node)
{
	DcFdwExecutionState *festate = (DcFdwExecutionState *) node->fdw_state;

#ifdef DEBUG
    elog(NOTICE, "dcEndForeignScan");
#endif

	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	//if (festate)
	//	EndCopyFrom(festate->cstate);
}

/*
 * dcReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
dcReScanForeignScan(ForeignScanState *node)
{
	/*DcFdwExecutionState *festate = (DcFdwExecutionState *) node->fdw_state;*/

#ifdef DEBUG
    elog(NOTICE, "dcReScanForeignScan");
#endif

}

/*
 * dcAnalyzeForeignTable
 *		Test whether analyzing this foreign table is supported
 */
static bool
dcAnalyzeForeignTable(Relation relation,
						AcquireSampleRowsFunc *func,
						BlockNumber *totalpages)
{
	char	   *data_dir;
	char	   *index_dir;
	char	   *language;
	char	   *encoding;
	List	   *options;
	struct stat stat_buf;

	/* Fetch options of foreign table */
	dcGetOptions(RelationGetRelid(relation),
	        &data_dir, &index_dir,
	        &language, &encoding,
	        &options);

	/*
	 * Get size of the file.  (XXX if we fail here, would it be better to just
	 * return false to skip analyzing the table?)
	 */

	/*
	 * Convert size to pages.  Must return at least 1 so that we can tell
	 * later on that pg_class.relpages is not default.
	 */

	*func = dc_acquire_sample_rows;

	return true;
}



/*
 * Estimate size of a foreign table.
 *
 * The main result is returned in baserel->rows.  We also set
 * fdw_private->pages and fdw_private->ntuples for later use in the cost
 * calculation.
 */
static void
estimate_size(PlannerInfo *root, RelOptInfo *baserel,
			  DcFdwPlanState *fdw_private)
{
	/* Open stats file and Save the output-rows estimate for the planner */
	baserel->rows = 7000;
    elog(NOTICE, "TP: %d", baserel->tuples);
}



/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   DcFdwPlanState *fdw_private,
			   Cost *startup_cost, Cost *total_cost)
{
	BlockNumber pages = fdw_private->pages;
	double		ntuples = fdw_private->ndocs;
	Cost		run_cost = 0;
	Cost		cpu_per_tuple;

	/*
	 * We estimate costs almost the same way as cost_seqscan(), thus assuming
	 * that I/O costs are equivalent to a regular table file of the same size.
	 * However, we take per-tuple CPU costs as 10x of a seqscan, to account
	 * for the cost of parsing records.
	 */
	run_cost += seq_page_cost * pages;

	*startup_cost = baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost * 10 + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * ntuples;
	*total_cost = *startup_cost + run_cost;
}


/*
 * dc_acquire_sample_rows -- acquire a random sample of rows from the table
 *
 * Selected rows are returned in the caller-allocated array rows[],
 * which must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also count the total number of rows in the file and return it into
 * *totalrows.	Note that *totaldeadrows is always set to 0.
 *
 * Note that the returned list of rows is not always in order by physical
 * position in the file.  Therefore, correlation estimates derived later
 * may be meaningless, but it's OK because we don't use the estimates
 * currently (the planner only pays attention to correlation for indexscans).
 */
static int
dc_acquire_sample_rows(Relation onerel, int elevel,
                        HeapTuple *rows, int targrows,
                        double *totalrows, double *totaldeadrows)
{
    return 0;
}