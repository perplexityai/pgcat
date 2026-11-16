-- pool: psql_tx
-- pool: psql_session
-- xfail: pgcat only resets a server connection after SET outside a transaction; a session SET (including SET ROLE) committed inside BEGIN ... COMMIT stays on the server connection and reaches the next client
-- xfail-shows: ^ (role|statement_timeout|search_path) +\| t$
-- A SET without LOCAL inside a committed transaction changes the session.
-- The next client on the server connection must not inherit it; for
-- SET ROLE that is a privilege leak.
BEGIN;
SET statement_timeout = '4321ms';
SET search_path = pg_catalog;
SET ROLE psql_scenario_role;
COMMIT;
\connect
SELECT 'role' AS leaked, current_user = 'psql_scenario_role' AS present
UNION ALL SELECT 'statement_timeout', current_setting('statement_timeout') = '4321ms'
UNION ALL SELECT 'search_path', current_setting('search_path') = 'pg_catalog';
