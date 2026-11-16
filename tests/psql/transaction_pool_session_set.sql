-- pool: psql_tx
-- Transaction pools give up a client's session SETs of untracked parameters
-- and its SQL PREPAREs once the transaction ends: pgcat sends RESET ALL /
-- DEALLOCATE ALL when it returns the server connection, which is what keeps
-- them from reaching other clients. Postgres is not the reference for this
-- pgcat-specific behavior; the .out file is.
SET statement_timeout = '1234ms';
SHOW statement_timeout;
SET statement_timeout = '1234ms' \; SHOW statement_timeout;
BEGIN;
PREPARE sql_prepared AS SELECT 'from PREPARE' AS src;
EXECUTE sql_prepared;
COMMIT;
EXECUTE sql_prepared;
PREPARE sql_prepared AS SELECT 'prepared again' AS src;
