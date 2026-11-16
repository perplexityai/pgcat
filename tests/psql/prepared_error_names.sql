-- pool: psql_tx
-- xfail: with the statement cache on, server errors about a prepared statement name pgcat's internal PGCAT_n name instead of the client's
-- xfail-shows: but prepared statement "PGCAT_\d+" requires 1
-- Errors must name what the client prepared; PGCAT_n means nothing to the
-- client and differs between pgcat processes.
SELECT $1::int AS needs_one \parse s_param
\bind_named s_param \g
\bind_named s_param 7 \g
