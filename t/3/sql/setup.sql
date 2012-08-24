--
-- Test foreign-data wrapper dc_fdw.
--

-- Clear server and table
-- DROP FOREIGN TABLE IF EXISTS dc_table CASCADE;
-- DROP SERVER IF EXISTS dc_server;

-- Clear FDW
-- DROP EXTENSION IF EXISTS dc_fdw;

-- Install dc_fdw
-- CREATE EXTENSION dc_fdw;

-- Create server and table
-- CREATE SERVER dc_server FOREIGN DATA WRAPPER dc_fdw;

-- Validator tests
CREATE FOREIGN TABLE dc_table (id int, content text) 
	SERVER dc_server
	OPTIONS (
    	index_method 'SPIM',
    	buffer_size '10',
    	id_col 'id',
    	text_col 'content'
    ); --ERROR
CREATE FOREIGN TABLE dc_table (id int, content text) 
	SERVER dc_server
	OPTIONS (
	    data_dir '/tmp/dc_fdw/t/3/training', 
    	index_method 'SPIM',
    	buffer_size '10',
    	id_col 'id',
    	text_col 'content'
    ); --ERROR
CREATE FOREIGN TABLE dc_table (id int, content text) 
	SERVER dc_server
	OPTIONS (
		index_dir '/tmp/dc_fdw/t/3/index', 
    	index_method 'SPIM',
    	buffer_size '10',
    	id_col 'id',
    	text_col 'content'
    ); --ERROR
CREATE FOREIGN TABLE dc_table (id int, content text) 
	SERVER dc_server
	OPTIONS (
		data_dir '/tmp/dc_fdw/t/3/training',
		index_dir '/tmp/dc_fdw/t/3/index', 
    	buffer_size '10',
    	id_col 'id',
    	text_col 'content'
    ); --ERROR
CREATE FOREIGN TABLE dc_table (id int, content text) 
	SERVER dc_server
	OPTIONS (
	    data_dir '/tmp/dc_fdw/t/3/training', 
    	index_dir '/tmp/dc_fdw/t/3/index',
    	index_method 'SPIM',
    	buffer_size '10',
    	text_col 'content'
    ); --ERROR
CREATE FOREIGN TABLE dc_table (id int, content text) 
	SERVER dc_server
	OPTIONS (
	    data_dir '/tmp/dc_fdw/t/3/training', 
    	index_dir '/tmp/dc_fdw/t/3/index',
    	index_method 'SPIM',
    	buffer_size '10',
    	id_col 'id'
    ); --ERROR
CREATE FOREIGN TABLE dc_table (id int, content text) 
	SERVER dc_server
	OPTIONS (
	    data_dir '/tmp/dc_fdw/t/3/training', 
    	index_dir '/tmp/dc_fdw/t/3/index',
    	index_method 'SPIM',
    	buffer_size '10',
    	id_col 'id',
    	text_col 'content'
    );
