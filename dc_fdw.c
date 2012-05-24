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

#include "indexer.h"

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

	/* oids option is not supported */
	{"language", ForeignTableRelationId},
	{"encoding", ForeignTableRelationId},

	/* Sentinel */
	{NULL, InvalidOid}
};

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct DcFdwExecutionState
{
	char	   *data_dir;		    /* dc to read */
	List	   *options;		/* merged COPY options, excluding data_dir */
	CopyState	cstate;			/* state of reading dc */
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
static FdwPlan *dcPlanForeignScan(Oid foreigntableid,
					PlannerInfo *root,
					RelOptInfo *baserel);
static void dcExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void dcBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *dcIterateForeignScan(ForeignScanState *node);
static void dcReScanForeignScan(ForeignScanState *node);
static void dcEndForeignScan(ForeignScanState *node);

/*
 * Helper functions
 */
static bool is_valid_option(const char *option, Oid context);
static void dcGetOptions(Oid foreigntableid,
			   char **data_dir, char **index_dir, char **language, char **encoding, List **other_options);
static void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   const char *data_dir,
			   Cost *startup_cost, Cost *total_cost);


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

	fdwroutine->PlanForeignScan = dcPlanForeignScan;
	fdwroutine->ExplainForeignScan = dcExplainForeignScan;
	fdwroutine->BeginForeignScan = dcBeginForeignScan;
	fdwroutine->IterateForeignScan = dcIterateForeignScan;
	fdwroutine->ReScanForeignScan = dcReScanForeignScan;
	fdwroutine->EndForeignScan = dcEndForeignScan;

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
	    dc_index(data_dir);
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
	 * Extract options from FDW objects.  We ignore user mappings because
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
static FdwPlan *
dcPlanForeignScan(Oid foreigntableid,
					PlannerInfo *root,
					RelOptInfo *baserel)
{
	FdwPlan    *fdwplan;
	/*char	   *data_dir;*/
	/*List	   *options;*/

#ifdef DEBUG
    elog(NOTICE, "dcPlanForeignScan");
#endif

	/* Fetch options --- we only need data_dir at this point */
	/*dcGetOptions(foreigntableid, &data_dir, &options);*/
	
	/* Construct FdwPlan with cost estimates */
	fdwplan = makeNode(FdwPlan);
	/*estimate_costs(root, baserel, data_dir,
				   &fdwplan->startup_cost, &fdwplan->total_cost);*/
	/*fdwplan->fdw_private = NIL;*/ /* not used */
	fdwplan->startup_cost = 100;
    fdwplan->total_cost = 100;
	return fdwplan;
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

#ifdef DEBUG
    elog(NOTICE, "dcExplainForeignScan");
#endif


	/* Fetch options --- we only need data_dir at this point */
	dcGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
				   &data_dir, &index_dir, &language, &encoding, &options);

	ExplainPropertyText("Foreign Document Collection", data_dir, es);

	/* Suppress dc size if we're not showing cost details */
	if (es->costs)
	{
		struct stat stat_buf;

		if (stat(data_dir, &stat_buf) == 0)
			ExplainPropertyLong("Foreign Document Collection Size", (long) stat_buf.st_size,
								es);
	}
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
	CopyState	cstate;
	DcFdwExecutionState *festate;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Fetch options of foreign table */
	dcGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
				   &data_dir, &index_dir, &language, &encoding, &options);

	/*
	 * Create CopyState from FDW options.  We always acquire all columns, so
	 * as to match the expected ScanTupleSlot signature.
	 */
	cstate = BeginCopyFrom(node->ss.ss_currentRelation,
						   data_dir,
						   NIL,
						   options);

	/*
	 * Save state in node->fdw_state.  We must save enough information to call
	 * BeginCopyFrom() again.
	 */
	festate = (DcFdwExecutionState *) palloc(sizeof(DcFdwExecutionState));
	festate->data_dir = data_dir;
	festate->options = options;
	festate->cstate = cstate;

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
	bool		found;
	ErrorContextCallback errcontext;

	/* Set up callback to identify error line number. */
	errcontext.callback = CopyFromErrorCallback;
	errcontext.arg = (void *) festate->cstate;
	errcontext.previous = error_context_stack;
	error_context_stack = &errcontext;

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
	found = NextCopyFrom(festate->cstate, NULL,
						 slot->tts_values, slot->tts_isnull,
						 NULL);
	if (found)
		ExecStoreVirtualTuple(slot);

	/* Remove error callback. */
	error_context_stack = errcontext.previous;

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
	if (festate)
		EndCopyFrom(festate->cstate);
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
 * Estimate costs of scanning a foreign table.
 */
static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   const char *data_dir,
			   Cost *startup_cost, Cost *total_cost)
{
	struct stat stat_buf;
	BlockNumber pages;
	int			tuple_width;
	double		ntuples;
	double		nrows;
	Cost		run_cost = 0;
	Cost		cpu_per_tuple;

	/*
	 * Get size of the dc.  It might not be there at plan time, though, in
	 * which case we have to use a default estimate.
	 */
	if (stat(data_dir, &stat_buf) < 0)
		stat_buf.st_size = 10 * BLCKSZ;

	/*
	 * Convert size to pages for use in I/O cost estimate below.
	 */
	pages = (stat_buf.st_size + (BLCKSZ - 1)) / BLCKSZ;
	if (pages < 1)
		pages = 1;

	/*
	 * Estimate the number of tuples in the dc.  We back into this estimate
	 * using the planner's idea of the relation width; which is bogus if not
	 * all columns are being read, not to mention that the text representation
	 * of a row probably isn't the same size as its internal representation.
	 * FIXME later.
	 */
	tuple_width = MAXALIGN(baserel->width) + MAXALIGN(sizeof(HeapTupleHeaderData));

	ntuples = clamp_row_est((double) stat_buf.st_size / (double) tuple_width);

	/*
	 * Now estimate the number of rows returned by the scan after applying the
	 * baserestrictinfo quals.	This is pretty bogus too, since the planner
	 * will have no stats about the relation, but it's better than nothing.
	 */
	nrows = ntuples *
		clauselist_selectivity(root,
							   baserel->baserestrictinfo,
							   0,
							   JOIN_INNER,
							   NULL);

	nrows = clamp_row_est(nrows);

	/* Save the output-rows estimate for the planner */
	baserel->rows = nrows;

	/*
	 * Now estimate costs.	We estimate costs almost the same way as
	 * cost_seqscan(), thus assuming that I/O costs are equivalent to a
	 * regular table dc of the same size.  However, we take per-tuple CPU
	 * costs as 10x of a seqscan, to account for the cost of parsing records.
	 */
	run_cost += seq_page_cost * pages;

	*startup_cost = baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost * 10 + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * ntuples;
	*total_cost = *startup_cost + run_cost;
}