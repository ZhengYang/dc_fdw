PostgreSQL - Document Collection Foreign Data Wrapper (dc_fdw)
--------------------------------------------------------------

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for
the document collection on local disk.

More information can be found in the [wiki page](https://github.com/ZhengYang/dc_fdw/wiki)


###Building

No external library is needed.

###Limitations

Only these 4 types of quals and their boolean combinations can be 
pushed down:

	1. id = <integer>
	2. content @@ <term>
	3. to_tsquery ( <tsquery text> )
	4. plainto_tsquery ( <free text> )

Otherwise, a sequential scan on all the documents in the collection is expected.

###Usage

The following parameters can be set on a CouchDB foreign server:

	data_dir      [where the files in a document collection are located]
	index_dir     [where the index files are located: postings file, dictionary file]
	index_method  [either In-memory(IM) Indexing or Single-pass in-memory(SPIM) indexing]
	buffer_size   [when using SPIM indexing, this is the limit of memory available]
	id_col        [the column name for mapping doc id]
	text_col      [the column name for mapping doc content]

###Example

	CREATE EXTENSION dc_fdw;

	CREATE SERVER dc_server 
		FOREIGN DATA WRAPPER dc_fdw;

	CREATE FOREIGN TABLE dc_table (id int, content text) 
		SERVER dc_server
		OPTIONS (
		    data_dir '/pgsql/postgres/contrib/dc_fdw/data/reuters/training', 
	    	index_dir '/pgsql/postgres/contrib/dc_fdw/data/reuters/index',
	    	index_method 'SPIM',
	    	buffer_size '10',
	    	id_col 'id',
	    	text_col 'content'
	    );

-- 
Zheng Yang  
zhengyang4k@gmail.com
