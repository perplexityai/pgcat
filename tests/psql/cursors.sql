-- pool: psql_tx
-- pool: psql_session
-- SQL cursors live inside a transaction, so in a transaction pool every
-- FETCH must reach the server connection that declared the cursor.
-- psql's FETCH_COUNT reads big results in batches the same way.
BEGIN;
DECLARE cur CURSOR FOR SELECT g FROM generate_series(1, 10) g;
FETCH 3 FROM cur;
MOVE 2 FROM cur;
FETCH NEXT FROM cur;
FETCH ALL FROM cur;
CLOSE cur;
FETCH 1 FROM cur;
ROLLBACK;

\set FETCH_COUNT 7
SELECT g, g * g AS sq FROM generate_series(1, 20) g;
\unset FETCH_COUNT

BEGIN;
DECLARE scroll_cur SCROLL CURSOR FOR SELECT g FROM generate_series(1, 5) g;
FETCH LAST FROM scroll_cur;
FETCH ABSOLUTE 2 FROM scroll_cur;
FETCH BACKWARD 1 FROM scroll_cur;
COMMIT;
-- The cursor ended with the transaction.
FETCH 1 FROM scroll_cur;
