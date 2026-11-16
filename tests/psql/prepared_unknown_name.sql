-- pool: psql_tx
-- xfail: with the statement cache on, Bind of an unknown statement name gets a FATAL and pgcat closes the client connection; Postgres returns an ERROR and the session continues
-- xfail-shows: FATAL:  prepared statement "never_prepared" does not exist
-- PgBouncer test_describe_non_existent_prepared_statement.
\bind_named never_prepared \g
SELECT 'still connected' AS status;
