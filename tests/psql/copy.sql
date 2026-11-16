-- pool: psql_tx
-- pool: psql_session
-- COPY sub-protocol through the pooler: success, errors before and after
-- CopyDone, inside and outside explicit transactions, and COPY TO. A COPY
-- that fails must leave the server connection usable for the next statement
-- and for the next client.
DROP TABLE IF EXISTS psql_copy;
CREATE TABLE psql_copy (id int PRIMARY KEY, v text);

COPY psql_copy FROM STDIN WITH (FORMAT csv);
1,one
2,two
\.

-- Data error mid-stream.
COPY psql_copy FROM STDIN WITH (FORMAT csv);
3,three
not-a-number,bad
\.

-- Constraint error detected at the end.
COPY psql_copy FROM STDIN WITH (FORMAT csv);
4,four
1,duplicate
\.
SELECT id, v FROM psql_copy ORDER BY id;

-- Failed COPY inside a transaction aborts it.
BEGIN;
INSERT INTO psql_copy VALUES (5, 'five');
COPY psql_copy FROM STDIN WITH (FORMAT csv);
x,y
\.
SELECT 'ignored';
ROLLBACK;

-- Successful COPY inside a transaction, then COPY TO in the same one.
BEGIN;
COPY psql_copy FROM STDIN WITH (FORMAT text);
6	six
7	seven
\.
COPY (SELECT id, v FROM psql_copy ORDER BY id) TO STDOUT WITH (FORMAT csv, HEADER);
COMMIT;

-- COPY TO with many rows: only the shape of the stream matters.
COPY (SELECT g, repeat('x', g % 50) FROM generate_series(1, 2000) g) TO STDOUT \g /dev/null
SELECT count(*) AS rows, sum(id) AS id_sum FROM psql_copy;

-- psql's \copy uses COPY ... FROM STDIN as well.
\copy psql_copy FROM stdin
8	eight
\.

\connect
SELECT id, v FROM psql_copy ORDER BY id;
DROP TABLE psql_copy;
