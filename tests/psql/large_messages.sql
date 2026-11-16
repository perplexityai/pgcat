-- pool: psql_tx
-- Messages much larger than pgcat's read buffers, in both directions:
-- large DataRow, many rows, a large Parse, and a large Bind parameter.
SELECT length(repeat('x', 5000000)) AS len, md5(repeat('x', 5000000)) AS hash;
SELECT count(*), md5(string_agg(g::text, ',')) FROM generate_series(1, 200000) g;
SELECT g, repeat('y', 100000) AS wide FROM generate_series(1, 30) g \g /dev/null
SELECT 'wide rows ok' AS status;

-- Build a ~1 MB statement text, then run it simple and extended.
SELECT format('SELECT length(%L) AS big_literal', repeat('z', 1000000)) AS big_query \gset
:big_query;
:big_query \parse big_stmt
\bind_named big_stmt \g
:big_query \bind \g

-- A ~1 MB Bind parameter.
SELECT repeat('p', 1000000) AS big_param \gset
SELECT length($1) AS param_len, md5($1) AS param_md5 \bind :big_param \g

-- Many columns.
SELECT format('SELECT %s', string_agg(format('%s AS c%s', g, g), ', ')) AS wide_query
  FROM generate_series(1, 1500) g \gset
:wide_query \g /dev/null
SELECT 'many columns ok' AS status;
