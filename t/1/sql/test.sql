-- `Brazilian' must be found in the document.
SELECT id,title,substr(content,0,16) FROM dc_table_1 WHERE content @@ 'Brazilian';
-- Case insensitive.
SELECT id,title,substr(content,0,16) FROM dc_table_1 WHERE content @@ 'brazilian';
-- `brazil' is a different word from `Brazilian'.
SELECT id,title,substr(content,0,16) FROM dc_table_1 WHERE content @@ 'brazil';

-- `Cocoa'.
SELECT id,title,substr(content,0,16) FROM dc_table_1 WHERE content @@ 'Cocoa';

-- `Chocolate' - Must fail.
SELECT id,title,substr(content,0,16) FROM dc_table_1 WHERE content @@ 'Chocolate';

--
-- BOOLEAN conditional test
--

-- `Brazillian' AND 'Cocoa'.
SELECT id,title,substr(content,0,16) FROM dc_table_1 WHERE content @@ 'Brazilian' AND content @@ 'Cocoa';

-- `Brazillian' AND 'Chocolate' - Must fail.
SELECT id,title,substr(content,0,16) FROM dc_table_1 WHERE content @@ 'Brazilian' AND content @@ 'Chocolate';

-- `Brazillian' OR 'Cocoa'.
SELECT id,title,substr(content,0,16) FROM dc_table_1 WHERE content @@ 'Brazilian' OR content @@ 'Cocoa';

-- `Brazillian' OR 'Chocolate'.
SELECT id,title,substr(content,0,16) FROM dc_table_1 WHERE content @@ 'Brazilian' OR content @@ 'Chocolate';
