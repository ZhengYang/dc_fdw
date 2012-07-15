CREATE FOREIGN TABLE dc_table_1 (id text, title text, content text) 
SERVER dc_server
OPTIONS (
    data_dir '/tmp/dc_fdw/t/1/training',
    index_dir '/tmp/dc_fdw/t/1/index',
    language 'en',
    encoding 'ascii'
);


