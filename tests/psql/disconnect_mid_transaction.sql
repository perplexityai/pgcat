-- pool: psql_tx
-- pool: psql_session
-- A client that disconnects inside a transaction: its writes must be rolled
-- back and its locks released before the server connection serves another
-- client (pgcat sends ROLLBACK on checkin instead of closing the server).
DROP TABLE IF EXISTS psql_disconnect;
CREATE TABLE psql_disconnect (id int PRIMARY KEY, v text);
INSERT INTO psql_disconnect VALUES (1, 'committed');

BEGIN;
INSERT INTO psql_disconnect VALUES (2, 'never committed');
UPDATE psql_disconnect SET v = 'never committed' WHERE id = 1;
SELECT id FROM psql_disconnect WHERE id = 1 FOR UPDATE;
SELECT pg_advisory_xact_lock(4242);
-- \connect closes this client mid-transaction.
\connect
-- Blocking forms: they wait for the previous session's locks to go, so the
-- direct run doesn't race the old backend's exit.
SELECT pg_advisory_xact_lock(4242);
UPDATE psql_disconnect SET v = 'updated by next client' WHERE id = 1;
SELECT id, v FROM psql_disconnect ORDER BY id;
SELECT current_setting('transaction_isolation') AS isolation,
       now() = statement_timestamp() AS not_in_old_transaction;

-- Disconnect while the transaction is already aborted.
BEGIN;
SELECT 1/0;
\connect
SELECT 'fresh client' AS status;

-- Disconnect inside a pipeline's open transaction.
\startpipeline
BEGIN \bind \sendpipeline
INSERT INTO psql_disconnect VALUES ($1, 'pipelined') \bind 3 \sendpipeline
\syncpipeline
\endpipeline
\connect
SELECT id, v FROM psql_disconnect ORDER BY id;
DROP TABLE psql_disconnect;
