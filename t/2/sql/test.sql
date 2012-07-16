SELECT id,title FROM dc_table_2 WHERE content @@ 'January';

SELECT id,title FROM dc_table_2 WHERE content @@ 'Cocoa';

SELECT id,title FROM dc_table_2 WHERE content @@ 'feedgrain';

SELECT id,title FROM dc_table_2 WHERE content @@ 'Cocoa' AND content @@ 'feedgrain';

SELECT id,title FROM dc_table_2 WHERE content @@ 'Cocoa' OR content @@ 'feedgrain';
