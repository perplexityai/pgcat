# psql scenarios

Each `.sql` file here is a raw-SQL script that
[`tests/python/test_psql_scenarios.py`](../python/test_psql_scenarios.py) runs
with psql, once directly against Postgres and once through pgcat. The two
transcripts (stdout, then stderr, exactly as psql prints them) must match: a
correct pooler is invisible to the client.

A scenario with a `<name>.out` file next to it is compared to that file
instead. Use this only for behavior that belongs to pgcat, where Postgres
can't be the reference (for example, a transaction pool dropping session
`SET`s).

## Writing a scenario

- Use plain SQL plus psql meta-commands. psql 18 adds the extended-protocol
  commands: `\parse`, `\bind`, `\bind_named`, `\close_prepared`, and
  `\startpipeline` / `\syncpipeline` / `\endpipeline`.
- Every pool has `pool_size = 1`, so each `\connect` gives a new pgcat client
  the previous client's server connection. That is how a scenario checks
  that one client's state doesn't reach the next.
- Leave the database as you found it, because both runs start from the same
  state. Drop any tables you create, including at the start in case an
  earlier run was interrupted.
- Don't print anything that varies between runs, such as backend PIDs,
  timestamps, or OIDs.
- Errors are expected (psql runs with `ON_ERROR_STOP=0`). Their text is part
  of the transcript.

Header lines (SQL comments at the very top):

| Header | Meaning |
|---|---|
| `-- pool: <name>` | pgcat pool to run through; repeat for more. Default `psql_tx`. Pools: `psql_tx` (statement cache 100), `psql_tx_small_cache` (cache 2), `psql_tx_no_cache`, `psql_session`. |
| `-- xfail: <reason>` | Known pgcat bug. The case counts as an expected failure only when the pgcat run shows that bug (see `xfail-shows`). It fails once pgcat matches the reference, so remove the header in the fix. |
| `-- xfail-shows: <regex>` | Required with `xfail`. A regex that must match the output only the pgcat run printed (the `+` lines of the diff), or the hang report for a scenario that hangs. Any other failure, including any failure of the direct Postgres run, fails the test. Match only output the bug itself produces: a labeled result row (`leaked | t`), or for a hang, the full report showing how far psql got. |
| `-- timeout: <secs>` | Limit for each psql run (default 60). Use it for scenarios that hang. |

## Running

```sh
pytest -q -rxX tests/python/test_psql_scenarios.py
pytest -q tests/python/test_psql_scenarios.py -k prepared_statements
```

This needs Postgres on 127.0.0.1:5432 loaded with
`tests/sharding/query_routing_setup.sql`, a built `target/debug/pgcat`, and
psql 18 or newer. The runner checks `PGCAT_TEST_PSQL`, then
`/usr/lib/postgresql/*/bin/psql`, then `psql` on `PATH`. Without psql 18 the
suite is skipped, unless `PGCAT_TEST_PSQL` is set, in which case it fails.

To regenerate the `.out` files, run with `PGCAT_PSQL_UPDATE=1` and review the
diff before committing.
