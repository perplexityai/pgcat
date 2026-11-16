-- pool: psql_tx
-- pool: psql_session
-- Multi-statement simple Query messages (psql sends statements joined with
-- \; as one Query). pgcat answers some statements itself and must keep the
-- implicit-transaction, error and ReadyForQuery semantics of Postgres:
-- an error ends the batch, rolls back the implicit transaction, and the next
-- message on the same connection works.
DROP TABLE IF EXISTS psql_batches;
CREATE TABLE psql_batches (id int PRIMARY KEY, note text);

-- One Query, three statements, the second fails: nothing commits.
INSERT INTO psql_batches VALUES (1, 'first') \; SELECT 1/0 \; INSERT INTO psql_batches VALUES (3, 'third');
SELECT count(*) AS after_failed_batch FROM psql_batches;

-- An explicit COMMIT inside the batch makes the statements before it stick.
INSERT INTO psql_batches VALUES (1, 'kept') \; COMMIT \; SELECT 1/0;
SELECT id, note FROM psql_batches ORDER BY id;

-- BEGIN without COMMIT in one Query leaves the transaction open for the
-- next Query, which must reach the same server connection.
BEGIN \; INSERT INTO psql_batches VALUES (2, 'in txn');
SELECT id, note FROM psql_batches ORDER BY id;
ROLLBACK;
SELECT count(*) AS after_rollback FROM psql_batches;

-- An error inside an explicit transaction aborts it until ROLLBACK.
BEGIN;
INSERT INTO psql_batches VALUES (5, 'doomed');
INSERT INTO psql_batches VALUES (1, 'duplicate');
SELECT 'ignored';
COMMIT;
SELECT id, note FROM psql_batches ORDER BY id;

-- Savepoints recover part of a transaction.
BEGIN;
INSERT INTO psql_batches VALUES (6, 'before savepoint');
SAVEPOINT sp;
INSERT INTO psql_batches VALUES (1, 'duplicate');
ROLLBACK TO SAVEPOINT sp;
INSERT INTO psql_batches VALUES (7, 'after savepoint');
RELEASE SAVEPOINT sp;
COMMIT;
SELECT id, note FROM psql_batches ORDER BY id;

-- Empty queries and comment-only queries get EmptyQueryResponse.
;
/* only a comment */ ;
SELECT 'still connected' AS status;

-- Results of several statements in one Query come back in order.
SELECT 1 AS a \; SELECT 2 AS b \; VALUES (3, 'c'), (4, 'd');

DROP TABLE psql_batches;
