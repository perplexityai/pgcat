-- pool: psql_tx
-- timeout: 10
-- PgBouncer test_close_prepared_statement / test_close_batched_*. Drivers
-- that pipeline (psycopg 3, pgx, JDBC) send Close in the same batch as other
-- work when they deallocate.
SELECT 1 AS x \parse p_exec
SELECT 2 AS y \parse p_other
\echo sending the batched Close
\startpipeline
\bind_named p_exec \sendpipeline
\close_prepared p_other
\endpipeline
\startpipeline
\bind_named p_exec \sendpipeline
\close_prepared p_exec
\endpipeline
SELECT 'not stuck' AS status;
