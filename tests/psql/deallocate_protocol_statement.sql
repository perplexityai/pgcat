-- pool: psql_tx
-- xfail: SQL DEALLOCATE of a protocol-level statement goes to the server under the client's name, which pgcat renamed to PGCAT_n, so it fails where Postgres succeeds
-- xfail-shows: deallocate_protocol_statement\.sql:8: ERROR:  prepared statement "to_drop" does not exist
-- Drivers such as psycopg 3 and Npgsql run DEALLOCATE <name> on statements
-- they prepared with Parse.
SELECT 1 AS one \parse to_drop
\bind_named to_drop \g
DEALLOCATE to_drop;
\bind_named to_drop \g
SELECT 2 AS two \parse to_drop
\bind_named to_drop \g
