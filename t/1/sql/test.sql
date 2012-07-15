-- `Brazilian' must be found in the document.
SELECT id,title FROM dc_table_1 WHERE content @@ 'Brazilian';
-- Case insensitive.
SELECT id,title FROM dc_table_1 WHERE content @@ 'brazilian';
-- `brazil' is a different word from `Brazilian'.
SELECT id,title FROM dc_table_1 WHERE content @@ 'brazil';
