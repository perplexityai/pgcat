-- pool: psql_session
-- xfail: session pools clean a returned server connection with RESET ALL / DEALLOCATE ALL only when they saw SET or PREPARE, never DISCARD ALL, so temp tables, advisory locks, WITH HOLD cursors and LISTEN reach the next client
-- xfail-shows: ^ (temp_tables|advisory_locks|held_cursors|listening) +\| +[1-9]
-- Session-level state other than settings and prepared statements.
-- PgBouncer runs server_reset_query (DISCARD ALL by default) for this.
CREATE TEMP TABLE psql_leaked_temp (x int);
SELECT pg_advisory_lock(777) AS took_lock;
DECLARE psql_held CURSOR WITH HOLD FOR SELECT 1;
LISTEN psql_leaked_channel;
\connect
SELECT 'temp_tables' AS leaked, count(*) FROM pg_class WHERE relname = 'psql_leaked_temp' AND relpersistence = 't'
UNION ALL SELECT 'advisory_locks', count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()
UNION ALL SELECT 'held_cursors', count(*) FROM pg_cursors
UNION ALL SELECT 'listening', count(*) FROM pg_listening_channels();
