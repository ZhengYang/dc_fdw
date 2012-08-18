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
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "utils/memutils.h"
#include "utils/rel.h"


#include "qual_pushdown.h"
#include "qual_extract.h"

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
	
	/* column mapping options */
	{"id_col", ForeignTableRelationId},
	{"text_col", ForeignTableRelationId},
	
	/* Sentinel */
	{NULL, InvalidOid}
};

/*
 * FDW-specific information for RelOptInfo.fdw_private.
 */
typedef struct DcFdwPlanState
{
	char            *data_dir;      /* documents to read */
    char            *index_dir;     /* index to output */
    List            *mapping;       /* column mapping function */
    CollectionStats *stats;         /* collection-wise stats */
	BlockNumber     pages;			/* estimate of collection's physical size */
	double		    ntuples;		/* estimate of number of rows in collection */
    List *          rlist;          /* reduced list of doc ids by quals pushdown */
} DcFdwPlanState;


/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct DcFdwExecutionState
{
	char            *data_dir;	/* dc to read */
    DIR             *dir_state; /* for sequential scan only */
    AttInMetadata   *attinmeta;
    CollectionStats *stats;     /* collection-wise stats */
    int             dc_size;    /* collection size in bytes */
	double          ntuples;	/* estimate of number of rows in file */
    List            *rlist;     /* reduced list of doc ids by quals pushdown */
    int             rlistptr;   /* for looping through the rList */
    int             *mask;      /* mask for column mapping */
    int             ncols;      /* number of columns in the table */   
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
                        char **data_dir,
                        char **index_dir,
                        List **col_mapping);
static void estimate_size(PlannerInfo *root,
                        RelOptInfo *baserel,
                        DcFdwPlanState *fdw_private,
                        CollectionStats *stats);
static void estimate_costs(PlannerInfo *root,
                        RelOptInfo *baserel,
                        DcFdwPlanState *fdw_private,
                        Cost *startup_cost,
                        Cost *total_cost);
static int dc_acquire_sample_rows(Relation onerel,
                                int elevel,
                                HeapTuple *rows,
                                int targrows,
                                double *totalrows,
                                double *totaldeadrows);
int dc_col_mapping_mask(Relation rel, List *mapping_list, int **mask);
void cstring_tuple(Datum **tuple_as_array, bool **nulls, int *mask, int mask_len, List *values);

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
    char	   *id_col = NULL;
    char       *text_col = NULL;
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
				 errmsg("Only superuser can change options of a dc_fdw foreign table")));

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
		
		if (strcmp(def->defname, "id_col") == 0)
		{
			if (id_col)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("redundant options")));
			id_col = defGetString(def);
		}
		
		if (strcmp(def->defname, "text_col") == 0)
		{
			if (text_col)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("redundant options")));
			text_col = defGetString(def);
		}
		else
		    /* essentially all column mapping options which are optional */
			other_options = lappend(other_options, def);
	}

	/*
	 * data_dir & index_dir options are required for dc_fdw foreign tables.
	 */
	if (catalog == ForeignTableRelationId && data_dir == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("data_dir is required for dc_fdw foreign tables")));
	if (catalog == ForeignTableRelationId && index_dir == NULL)
         ereport(ERROR,
         		(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
         		errmsg("index_dir is required for dc_fdw foreign tables")));
    if (catalog == ForeignTableRelationId && id_col == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                errmsg("id_col is required for dc_fdw foreign tables")));
    if (catalog == ForeignTableRelationId && text_col == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
                errmsg("text_col is required for dc_fdw foreign tables")));
	
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
	    elog(NOTICE, "%s", "-Creating Foreign Table...");
	    elog(NOTICE, "%s", "-Start indexing document collection...");
	    imIndex(data_dir, index_dir);
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
			   char **data_dir, char **index_dir, List **col_mapping)
{
	ForeignTable        *table;
	ForeignServer       *server;
	ForeignDataWrapper  *wrapper;
    char                *text_col;
    char                *id_col;
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
		
		if (strcmp(def->defname, "id_col") == 0)
		{
			id_col = defGetString(def);
            continue;
		}
		
		if (strcmp(def->defname, "text_col") == 0)
		{
			text_col = defGetString(def);
            continue;
		}
		
	}
	
	/*
	 * The validator should have checked that a data_dir directory was included in the
	 * options, but check again, just in case.
	 */
	if (*data_dir == NULL)
		elog(ERROR, "data_dir is required for dc_fdw foreign tables");
	if (*data_dir == NULL)
		elog(ERROR, "index_dir is required for dc_fdw foreign tables");
	if (text_col == NULL)
		elog(ERROR, "text_col is required for dc_fdw foreign tables");
	if (id_col == NULL)
		elog(ERROR, "id_col is required for dc_fdw foreign tables");
	
	/* column mapping list, index0: id, index1: content */
	*col_mapping = list_make2(id_col, text_col);
}


/*
 * dcGetForeignRelSize
 *		Obtain relation size estimates for a foreign table
 */
static void
dcGetForeignRelSize(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid)
{
	DcFdwPlanState      *fpstate;
    /* File handles */
    File                statFile;
    File                dictFile;
    File                postFile;
    /* stat info */
    CollectionStats     *stats;
    /* dict settings */
    HTAB                *dict;
    HASHCTL             info;
    /* qual eval */
    PushableQualNode    *qualRoot;
    List                *allList;
    
#ifdef DEBUG
    elog(NOTICE, "dcGetForeignRelSize");
#endif

    /*
     * init stats data
     */
    stats = (CollectionStats *) palloc(sizeof(CollectionStats));
    /*
     * initialize hash dictionary
     */
    info.keysize = KEYSIZE;
    info.entrysize = sizeof(DictionaryEntry);
    dict = hash_create("dict", MAXELEM, &info, HASH_ELEM);
    
    /*
	 * Fetch options.  We only need filename at this point, but we might as
	 * well get everything and not need to re-fetch it later in planning.
	 */
	fpstate = (DcFdwPlanState *) palloc(sizeof(DcFdwPlanState));
	dcGetOptions(foreigntableid,
	                &fpstate->data_dir,
	                &fpstate->index_dir,
	                &fpstate->mapping);
	baserel->fdw_private = (void *) fpstate;
    
    /*
     * Fetch collection-wise stats
     */
    statFile = openStat(fpstate->index_dir);
    loadStat(&stats, statFile);
    closeStat(statFile);
    fpstate->stats = stats;
    
    /*
     * fill in dc size information
     */
	estimate_size(root, baserel, fpstate, stats);

	/*
	 * Load Dictionary. Dict is stored in memory for fast access and
	 * postings lists are in hard disk as it may be too large to fit
	 * into main memory.
	 */
    dictFile = openDict(fpstate->index_dir);
	loadDict(&dict, dictFile);
    closeDict(dictFile);
    
    /*
     * Extract Quals. We only extract quals that we can push down and 
 	 * convert them into a tree structure for evaluation.
 	 *
     * Evaluate QualTree. Filtered doc_id list. 
	 */
	postFile = openPost(fpstate->index_dir);
    allList = searchTerm(ALL, dict, postFile, TRUE);
	
	/* no quals to push down */
	if (extractQuals(&qualRoot, root, baserel, fpstate->mapping) == 0)
	{
#ifdef DEBUG
        elog(NOTICE, "No quals to pushdown, sequential scan");
#endif
        fpstate->rlist = allList;
    }
    /* there are quals available to pushdown */
    else
    {
        fpstate->rlist = evalQualTree(qualRoot, dict, postFile, allList);
#ifdef DEBUG
        printQualTree(qualRoot, 1);
#endif
    }

#ifdef DEBUG
    elog(NOTICE, "rlist length:%d", list_length(fpstate->rlist));
#endif
    closePost(postFile);
    
    freeQualTree(qualRoot);
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
	DcFdwPlanState  *fpstate = (DcFdwPlanState *) baserel->fdw_private;
    ForeignPath     *path;
	Cost            startup_cost;
	Cost            total_cost;
	List            *fdw_private;

#ifdef DEBUG
    elog(NOTICE, "dcGetForeignPaths");
#endif

	/* Estimate costs */
	estimate_costs(root, baserel, fpstate,
				   &startup_cost, &total_cost);
	
	/* result list after pushing down. */
	fdw_private = lappend(NIL, fpstate->rlist);
	fdw_private = lappend(fdw_private, fpstate->stats);
	
	/* Create a ForeignPath node and add it as only possible path */
	path = create_foreignscan_path(root, baserel,
								    baserel->rows,
									startup_cost,
									total_cost,
									NIL,		/* no pathkeys */
									NULL,		/* no outer rel either */
									fdw_private);
	add_path(baserel, (Path *) path);
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
    DcFdwPlanState  *fpstate = (DcFdwPlanState *) baserel->fdw_private;
	Index scan_relid = baserel->relid;
	List *fdw_private;
	
#ifdef DEBUG
    elog(NOTICE, "dcGetForeignPlan");
#endif

    /* result list after pushing down. */
	fdw_private = lappend(NIL, fpstate->rlist);
	fdw_private = lappend(fdw_private, fpstate->stats);
	
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
							fdw_private);
}


/*
 * dcExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
dcExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	char            *data_dir;
    char            *index_dir;
	List            *col_mapping;
    CollectionStats *stats;

#ifdef DEBUG
    elog(NOTICE, "dcExplainForeignScan");
#endif

    /* retrieve stats list */
    stats = (CollectionStats *) list_nth( (List *) ((ForeignScan *) node->ss.ps.plan)->fdw_private, 1);
	/* Fetch options --- we only need data_dir at this point */
	dcGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
				   &data_dir, &index_dir, &col_mapping);
				   
	ExplainPropertyText("Foreign Document Collection", data_dir, es);
	ExplainPropertyLong("Foreign Document Collection Size", (long) stats->numOfBytes, es);
	ExplainPropertyLong("Number of Documents", (long) stats->numOfDocs, es);
	ExplainPropertyText("Index Location", index_dir, es);
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
	DcFdwExecutionState *festate;
    int         *mask;
    List        *mappingList;
    int         numOfColumns;
    Relation    rel;

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
				   &data_dir, &index_dir, &mappingList);
	rel = heap_open(RelationGetRelid(node->ss.ss_currentRelation), AccessShareLock);
    numOfColumns = dc_col_mapping_mask(rel, mappingList, &mask);
    heap_close(rel, NoLock);
    
	/*
	 * Save state in node->fdw_state.  We must save enough information to call
	 * BeginCopyFrom() again.
	 */
	festate = (DcFdwExecutionState *) palloc(sizeof(DcFdwExecutionState));
	festate->rlist = (List *) list_nth( (List *) ((ForeignScan *) node->ss.ps.plan)->fdw_private, 0);
	festate->stats = (CollectionStats *) list_nth( (List *) ((ForeignScan *) node->ss.ps.plan)->fdw_private, 1);
    festate->rlistptr = 0;
	festate->data_dir = data_dir;
	festate->dir_state = AllocateDir(data_dir);
	festate->mask = mask;
    festate->ncols = numOfColumns;
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
    List *tupleItemList;
    bool end_of_list = FALSE;
    Datum *values;
    bool *nulls;

#ifdef DEBUG
    elog(NOTICE, "dcIterateForeignScan");
#endif
    
    if (list_length(festate->rlist) > festate->rlistptr)
    {
        StringInfoData sidDocPath;
        StringInfoData sidFName;
        File currFile;
        char *buf;
        int doc_id = list_nth_int(festate->rlist, festate->rlistptr);
        
        initStringInfo(&sidFName);
        appendStringInfo(&sidFName, "%d", doc_id);
        
        /* get full path/name of the file */
        initStringInfo(&sidDocPath);
        appendStringInfo(&sidDocPath, "%s/%s", festate->data_dir, sidFName.data);
        
        /*
         * load file content into buffer
         */
        currFile = openDoc(sidDocPath.data);
        loadDoc(&buf, currFile);
        closeDoc(currFile);
        
        tupleItemList = list_make2(sidFName.data, buf);  
        
        festate->rlistptr += 1;
    }
    else
        end_of_list = TRUE;
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
	if (!end_of_list)
	{
        HeapTuple tuple;
        
        values = (Datum *) palloc(festate->ncols * sizeof(Datum));
    	nulls = (bool *) palloc(festate->ncols * sizeof(bool));
        cstring_tuple(&values, &nulls, festate->mask, festate->ncols, tupleItemList);
        tuple = BuildTupleFromCStrings(festate->attinmeta, (char **) values);
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
	if (festate == NULL)
		return;
}

/*
 * dcReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
dcReScanForeignScan(ForeignScanState *node)
{
	DcFdwExecutionState *festate = (DcFdwExecutionState *) node->fdw_state;

#ifdef DEBUG
    elog(NOTICE, "dcReScanForeignScan");
#endif
    /* If we haven't have valid result yet, nothing to do. */
    if (festate->rlist == NIL)
		return;

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
	char            *data_dir;
	char            *index_dir;
	List	        *mappings;
    File            statFile;
    CollectionStats *stats;
	
#ifdef DEBUG
    elog(NOTICE, "dcAnalyzeForeignTable");
#endif

	/* Fetch options of foreign table */
	dcGetOptions(RelationGetRelid(relation),
	        &data_dir, &index_dir, &mappings);
	/*
	 * Get size of the collection.  (XXX if we fail here, would it be better to just
	 * return false to skip analyzing the table?)
	 */
	statFile = openStat(index_dir);
    loadStat(&stats, statFile);
    closeStat(statFile);
    
	/*
	 * Convert size to pages.  Must return at least 1 so that we can tell
	 * later on that pg_class.relpages is not default.
	 */
	*totalpages = (stats->numOfBytes + (BLCKSZ - 1)) / BLCKSZ;
 	if (*totalpages < 1)
 		*totalpages = 1;
 		
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
			  DcFdwPlanState *fpstate, CollectionStats *stats)
{   
	BlockNumber pages;
	double		nrows;
    int         nbytes = stats->numOfBytes;

	/*
	 * Convert size to pages for use in I/O cost estimate later.
	 */
	pages = (nbytes + (BLCKSZ - 1)) / BLCKSZ;
	if (pages < 1)
		pages = 1;
	fpstate->pages = pages;

	/*
	 * Estimate the number of tuples in the collection.
	 */
	fpstate->ntuples = (double) stats->numOfDocs;

	/*
	 * Now estimate the number of rows returned by the scan after applying the
	 * baserestrictinfo quals.
	 */
	nrows = fpstate->ntuples *
		clauselist_selectivity(root,
							   baserel->baserestrictinfo,
							   0,
							   JOIN_INNER,
							   NULL);

	nrows = clamp_row_est(nrows);

	/* Save the output-rows estimate for the planner */
	baserel->rows = nrows;
}



/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   DcFdwPlanState *fpstate,
			   Cost *startup_cost, Cost *total_cost)
{
	BlockNumber pages = fpstate->pages;
	double		ntuples = fpstate->ntuples;
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
dc_acquire_sample_rows(Relation rel, int elevel,
                        HeapTuple *rows, int targrows,
                        double *totalrows, double *totaldeadrows)
{
    int             numrows = 0;
	double          rowstoskip = -1;	/* -1 means not set yet */
	double          rstate;
	TupleDesc       tupDesc;
	char            *data_dir;
    char            *index_dir;
	List            *mappings;
    int             *mask;
    int             mask_len;
    Datum           *values;
    bool            *nulls;
    DIR             *dir;
    struct dirent   *dirent;
	MemoryContext   oldcontext = CurrentMemoryContext;
	MemoryContext   tupcontext;
    AttInMetadata   *attinmeta;

#ifdef DEBUG
    elog(NOTICE, "dc_acquire_sample_rows");
#endif

	Assert(rel);
	Assert(targrows > 0);
	
	
	/* Fetch options of foreign table */
	dcGetOptions(RelationGetRelid(rel),
	            &data_dir,
	            &index_dir,
                &mappings);
                
    tupDesc = RelationGetDescr(rel);
    attinmeta = TupleDescGetAttInMetadata(rel->rd_att);
    mask_len = dc_col_mapping_mask(rel, mappings, &mask);
    values = (Datum *) palloc(mask_len * sizeof(Datum));
	nulls = (bool *) palloc(mask_len * sizeof(bool));
	
    /* prepare to read collection */
    dir = AllocateDir(data_dir);
    
	/*
	 * Use per-tuple memory context to prevent leak of memory used to read
	 * rows from the file with Copy routines.
	 */
	tupcontext = AllocSetContextCreate(CurrentMemoryContext,
									   "dc_fdw temporary context",
									   ALLOCSET_DEFAULT_MINSIZE,
									   ALLOCSET_DEFAULT_INITSIZE,
									   ALLOCSET_DEFAULT_MAXSIZE);
									   
	/* Prepare for sampling rows */
	rstate = anl_init_selection_state(targrows);

	*totalrows = 0;
	*totaldeadrows = 0;
	
	while ( (dirent = ReadDir(dir, data_dir)) != NULL)
    {   
        File            currFile;
        List            *colData;
        StringInfoData  sidDocPath;
        StringInfoData  sidFName;
        char            *buf;
        
		/* Check for user-requested abort or sleep */
		vacuum_delay_point();
		
        if (strcmp(".", dirent->d_name) == 0) continue;
        if (strcmp("..", dirent->d_name) == 0) continue;
        
		/* Fetch next row */
		MemoryContextReset(tupcontext);
		MemoryContextSwitchTo(tupcontext);
		
		/* get full path/name of the file */
		initStringInfo(&sidFName);
        appendStringInfo(&sidFName, "%d", atoi(dirent->d_name));
        initStringInfo(&sidDocPath);
        appendStringInfo(&sidDocPath, "%s/%s", data_dir, sidFName.data);
        
        /*
         * load file content into buffer
         */
        currFile = openDoc(sidDocPath.data);
        loadDoc(&buf, currFile);
        closeDoc(currFile);
           
        colData = list_make2(sidFName.data, buf);
        cstring_tuple(&values, &nulls, mask, mask_len, colData);
        
		MemoryContextSwitchTo(oldcontext);

		/*
		 * The first targrows sample rows are simply copied into the
		 * reservoir.  Then we start replacing tuples in the sample until we
		 * reach the end of the relation. This algorithm is from Jeff Vitter's
		 * paper (see more info in commands/analyze.c).
		 */
		if (numrows < targrows)
		{
			rows[numrows++] = BuildTupleFromCStrings(attinmeta, (char **) values);
		}
		else
		{
			/*
			 * t in Vitter's paper is the number of records already processed.
			 * If we need to compute a new S value, we must use the
			 * not-yet-incremented value of totalrows as t.
			 */
			if (rowstoskip < 0)
				rowstoskip = anl_get_next_S(*totalrows, targrows, &rstate);

			if (rowstoskip <= 0)
			{
				/*
				 * Found a suitable tuple, so save it, replacing one old tuple
				 * at random
				 */
				int			k = (int) (targrows * anl_random_fract());

				Assert(k >= 0 && k < targrows);
				heap_freetuple(rows[k]);
				rows[k] = BuildTupleFromCStrings(attinmeta, (char **) values);
			}

			rowstoskip -= 1;
		}

		*totalrows += 1;
	}

	/* Clean up. */
	MemoryContextDelete(tupcontext);

    FreeDir(dir);

	pfree(values);
	pfree(nulls);
    pfree(mask);

	/*
	 * Emit some interesting relation info
	 */
	ereport(elevel,
			(errmsg("\"%s\": table contains %.0f rows; "
					"%d rows in sample",
					RelationGetRelationName(rel),
					*totalrows, numrows)));

	return numrows;
}

/*
 * produce a mask for column mapping
 */
int
dc_col_mapping_mask(Relation rel, List *mappingList, int **mask)
{
    int i;
    /* Fetch the table column info */
    int numOfColumns = rel->rd_att->natts;
    *mask = (int *) palloc(sizeof(int) * numOfColumns);
    
#ifdef DEBUG
    elog(NOTICE, "dc_col_mapping_mask");
#endif
    
    for (i = 0; i < numOfColumns; i++)
    {
        StringInfoData  colName;
        int o = 0;
        ListCell   *colMapping;
        
        /* init mask value as null */
        (*mask)[i] = -1;

        /* retrieve the column name */
        initStringInfo(&colName);
        appendStringInfo(&colName, "%s", NameStr(rel->rd_att->attrs[i]->attname));
        
        /* check if the column name is mapping to a different name in remote table */
        foreach(colMapping, mappingList)
        {
            char *actualName = (char *) lfirst(colMapping);
            
            if (strcmp(actualName, colName.data) == 0)
            {
                (*mask)[i] = o;
                break;
            }
            o++;
        }
    }
    
    return numOfColumns;
}

/*
 * CString representation of the tuple
 */
void
cstring_tuple(Datum **tuple_as_array, bool **nulls, int *mask, int mask_len, List *values)
{
    int i;
    
    for (i = 0; i < mask_len; i++)
    {
        if (mask[i] == -1)
        {
            (*tuple_as_array)[i] = (Datum) NULL;
            (*nulls)[i] = TRUE;
        }
        else {
            (*tuple_as_array)[i] = (Datum) list_nth(values, mask[i]);;
            (*nulls)[i] = FALSE;
        }
    }
}