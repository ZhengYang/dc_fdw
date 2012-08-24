-- Basic query tests
SELECT * FROM dc_table WHERE id = 1;
SELECT * FROM dc_table WHERE content @@ 'Singapore';
SELECT * FROM dc_table WHERE content @@ to_tsquery('Singapore & Japan');
SELECT * FROM dc_table WHERE content @@ plainto_tsquery('Singapore Japan');
EXPLAIN SELECT * FROM dc_table WHERE content @@ 'Singapore';
ANALYZE dc_table;

-- Misc query tests
SELECT * FROM dc_table WHERE id = 1 AND content @@ 'Singapore';
SELECT * FROM dc_table WHERE content @@ 'Singapore' AND content @@ 'Japan' AND content @@ 'China';
SELECT * FROM dc_table WHERE content @@ 'Singapore' OR content @@ 'Japan' AND content @@ 'China';
SELECT * FROM dc_table WHERE content @@ to_tsquery('Singapore & Japan | China');
SELECT * FROM dc_table WHERE content @@ to_tsquery('(Singapore | National) & Pork & Board');
SELECT * FROM dc_table WHERE content @@ to_tsquery('National & Pork & Board') AND content @@ 'Singapore';
SELECT * FROM dc_table WHERE content @@ plainto_tsquery('Singapore Japan China');
SELECT * FROM dc_table WHERE content @@ plainto_tsquery('National Pork Board') AND content @@ 'Singapore';

