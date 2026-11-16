-- pool: psql_tx
-- xfail: with the statement cache on, a second Parse of a client's statement name silently replaces it; Postgres refuses with "prepared statement already exists" and keeps the original
-- xfail-shows: but prepared statement "PGCAT_\d+" requires 0
-- PgBouncer test_prepared_disallow_name_reuse. A driver relying on the
-- error (or a bug in one reusing names) would execute the wrong SQL.
SELECT $1::int + 1 AS plus_one \parse s1
\bind_named s1 1 \g
SELECT 'other' AS other \parse s1
\bind_named s1 2 \g
