CREATE FOREIGN TABLE dc_table_1 (id text, title text, content text) 
SERVER dc_server
OPTIONS (
    data_dir '/tmp/dc_fdw/t/1/training',
    index_dir '/tmp/dc_fdw/t/1/index',
    index_method 'SPIM',
    buffer_size '10',
    id_col 'id',
    text_col 'content'
);


