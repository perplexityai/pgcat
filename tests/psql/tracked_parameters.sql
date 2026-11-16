-- pool: psql_tx
-- pool: psql_session
-- Parameters pgcat tracks per client (TimeZone, DateStyle, application_name,
-- client_encoding, standard_conforming_strings) must keep their client's
-- value across transactions, even though in transaction mode each one may
-- run after pgcat RESET the server connection, and must not reach the next
-- client.
SET TimeZone = 'America/New_York';
SET DateStyle = 'SQL, DMY';
SET application_name = 'scenario_app';
SELECT '2024-06-01 12:00:00+00'::timestamptz AS ny_time;
BEGIN;
SELECT current_setting('TimeZone') AS tz, current_setting('DateStyle') AS ds,
       current_setting('application_name') AS app;
COMMIT;
-- A SET that needs cleanup (statement_timeout is not tracked) makes pgcat
-- RESET ALL the server between transactions; tracked values must survive it.
SET statement_timeout = '5s';
SELECT current_setting('TimeZone') AS tz, '2024-06-01'::date AS d,
       current_setting('application_name') AS app;
RESET TimeZone;
SELECT current_setting('TimeZone') AS tz_after_reset;

SET client_encoding = 'LATIN1';
SELECT current_setting('client_encoding') AS enc;
RESET client_encoding;

\connect
SELECT current_setting('TimeZone') AS tz, current_setting('DateStyle') AS ds,
       current_setting('application_name') AS app,
       current_setting('client_encoding') AS enc;
