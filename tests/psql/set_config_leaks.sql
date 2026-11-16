-- pool: psql_tx
-- pool: psql_session
-- xfail: set_config(..., false) returns a SELECT command tag, so pgcat doesn't mark the server connection for cleanup and the setting reaches the next client
-- xfail-shows: ^ (work_mem|role) +\| t$
SELECT set_config('work_mem', '7777kB', false) AS set_work_mem;
SELECT set_config('role', 'psql_scenario_role', false) AS set_role;
\connect
SELECT 'work_mem' AS leaked, current_setting('work_mem') = '7777kB' AS present
UNION ALL SELECT 'role', current_user = 'psql_scenario_role';
