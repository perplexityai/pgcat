-- pool: psql_tx
-- pool: psql_tx_small_cache
-- Protocol-level named prepared statements (Parse with a name). In
-- transaction mode pgcat renames them to shared server-side statements, so a
-- client must still see exactly Postgres's per-connection semantics, also
-- when the previous client on the same server connection prepared the same
-- name or the same SQL, and when the statement cache evicts.
SELECT $1::int + 1 AS plus_one \parse s1
\bind_named s1 1 \g
\bind_named s1 41 \g

-- Close then reuse the name for different SQL.
\close_prepared s1
SELECT 'replaced' AS replaced \parse s1
\bind_named s1 \g

-- Wrong parameter type.
SELECT $1::int AS typed \parse s_typed
\bind_named s_typed 'not a number' \g

-- Parse error, then the same name parses fine.
SELEC 1 \parse s_bad
SELECT 'fixed' AS fixed \parse s_bad
\bind_named s_bad \g

-- More statements than psql_tx_small_cache holds, re-executed in reverse.
SELECT 1 AS c1 \parse c1
SELECT 2 AS c2 \parse c2
SELECT 3 AS c3 \parse c3
SELECT 4 AS c4 \parse c4
\bind_named c4 \g
\bind_named c3 \g
\bind_named c2 \g
\bind_named c1 \g

-- Prepared statements inside an explicit transaction, and after an error
-- in it.
BEGIN;
SELECT txid_current() IS NOT NULL AS in_txn \parse in_txn
\bind_named in_txn \g
\bind_named c1 \g
SELECT 1/0;
\bind_named c1 \g
ROLLBACK;
\bind_named c1 \g

-- A new client gets the server connection this one used, and the names it
-- prepared there must be its own.
\connect
SELECT 'second client' AS who, $1::int * 2 AS doubled \parse s1
\bind_named s1 21 \g
-- Same SQL text as the first client's c3, under a different name.
SELECT 3 AS c3 \parse mine
\bind_named mine \g
\bind_named mine \g

-- Unnamed statements.
SELECT $1::text AS unnamed \bind 'x' \g
SELECT $1::int, $2::text, $3::bool \bind 1 'two' true \g
