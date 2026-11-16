-- pool: psql_tx
-- pool: psql_tx_small_cache
-- xfail: DISCARD ALL / DEALLOCATE ALL drop pgcat's server-side PGCAT_n statements but pgcat keeps them in its server cache, so every later client running that SQL on the connection gets "prepared statement PGCAT_n does not exist"
-- xfail-shows: ERROR:  prepared statement "PGCAT_\d+" does not exist
-- A client's DISCARD ALL or DEALLOCATE ALL removes only its own prepared
-- statements (as in Postgres), and must not break other clients that share
-- the server connection.
SELECT 'stmt' AS kind, $1::int AS v \parse mine
\bind_named mine 1 \g
DEALLOCATE ALL;
\bind_named mine 2 \g
SELECT 'stmt' AS kind, $1::int AS v \parse mine
\bind_named mine 3 \g

\connect
-- Same SQL text the previous client prepared, so pgcat reuses its server
-- statement.
SELECT 'stmt' AS kind, $1::int AS v \parse theirs
\bind_named theirs 4 \g
DISCARD ALL;
\bind_named theirs 5 \g

\connect
SELECT 'stmt' AS kind, $1::int AS v \parse third
\bind_named third 6 \g
SELECT 'stmt' AS kind, $1::int AS v \bind 7 \g
