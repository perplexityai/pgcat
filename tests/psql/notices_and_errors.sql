-- pool: psql_tx
-- Asynchronous NoticeResponse messages and ErrorResponse fields (SQLSTATE,
-- DETAIL, HINT, CONTEXT, position) must pass through pgcat unchanged.
\set VERBOSITY verbose
DO $$ BEGIN RAISE NOTICE 'notice from DO'; RAISE WARNING 'warning from DO'; END $$;

CREATE OR REPLACE FUNCTION psql_fail(code text) RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'custom failure %', code
    USING ERRCODE = code, DETAIL = 'some detail', HINT = 'some hint';
END $$;
SELECT psql_fail('P0042');
SELECT psql_fail('22012');
\set VERBOSITY default

-- Syntax error with a cursor position.
SELECT 1 +* 2;
-- Notices inside a transaction that then fails.
BEGIN;
DO $$ BEGIN RAISE NOTICE 'inside transaction'; END $$;
SELECT * FROM table_that_does_not_exist;
ROLLBACK;
-- Notices interleaved with the rows of one statement.
CREATE OR REPLACE FUNCTION psql_noisy(i int) RETURNS int LANGUAGE plpgsql AS $$
BEGIN RAISE NOTICE 'row %', i; RETURN i; END $$;
SELECT psql_noisy(g) FROM generate_series(1, 3) g;
DO $$ BEGIN FOR i IN 1..3 LOOP RAISE NOTICE 'loop %', i; END LOOP; END $$;
SELECT 'connection still usable' AS status;
DROP FUNCTION psql_fail(text);
DROP FUNCTION psql_noisy(int);
