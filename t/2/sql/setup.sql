CREATE FOREIGN TABLE dc_table_2 (id text, title text, content text) 
SERVER dc_server
OPTIONS (
    data_dir '/tmp/dc_fdw/t/2/training',
    index_dir '/tmp/dc_fdw/t/2/index',
    language 'en',
    encoding 'ascii'
);


