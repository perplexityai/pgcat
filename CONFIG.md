# PgCat Configurations
## `general` Section

### host
```
path: general.host
default: "0.0.0.0"
```

What IP to run on, 0.0.0.0 means accessible from everywhere.

### port
```
path: general.port
default: 6432
```

Port to run on, same as PgBouncer used in this example.

### enable_prometheus_exporter
```
path: general.enable_prometheus_exporter
default: true
```

Whether to enable prometheus exporter or not. The exporter also serves the `/ready` readiness probe.

### prometheus_exporter_port
```
path: general.prometheus_exporter_port
default: 9930
```

Port at which prometheus exporter listens on.

### connect_timeout
```
path: general.connect_timeout
default: 1000 # milliseconds
```

How long the client waits to obtain a server connection before aborting (ms).
This is similar to PgBouncer's `query_wait_timeout`.
The client then gets an ERROR `could not get connection from the pool` with SQLSTATE `53300` (too_many_connections), and its connection stays usable.
An extended-protocol batch waiting at a Sync or Flush is dropped, and never runs.
After a Flush no ReadyForQuery is sent, and extended-protocol messages are discarded until the next Sync, which answers with one ReadyForQuery.

### idle_timeout
```
path: general.idle_timeout
default: 30000 # milliseconds
```

How long an idle connection with a server is left open (ms).

### server_lifetime
```
path: general.server_lifetime
default: 86400000 # 24 hours
```

Max connection lifetime before it's closed, even if actively used.

### server_round_robin
```
path: general.server_round_robin
default: false
```

Whether to use round robin for server selection or not.

### server_tls
```
path: general.server_tls
default: false
```

Whether to use TLS for server connections or not.

### verify_server_certificate
```
path: general.verify_server_certificate
default: false
```

Whether to verify server certificate or not.

### verify_config
```
path: general.verify_config
default: true
```

Whether to verify config or not.

### idle_client_in_transaction_timeout
```
path: general.idle_client_in_transaction_timeout
default: 0 # milliseconds
```

How long a client is allowed to be idle while in a transaction (ms).

### healthcheck_timeout
```
path: general.healthcheck_timeout
default: 1000 # milliseconds
```

How much time to give the health check query to return with a result (ms).

### healthcheck_delay
```
path: general.healthcheck_delay
default: 30000 # milliseconds
```

How long to keep connection available for immediate re-use, without running a healthcheck query on it

### shutdown_timeout
```
path: general.shutdown_timeout
default: 60000 # milliseconds
```

How much time to give clients during shutdown before forcibly killing client connections (ms). pgcat uses the value in effect when it receives SIGINT, including one set by a reload. A reload after SIGINT doesn't change it.

### shutdown_drain_spread
```
path: general.shutdown_drain_spread
default: 0 # milliseconds
```

On SIGINT, close each idle client after a random delay between 0 and this value, instead of closing them all at once. This spreads their reconnects to other instances over the window. Must be less than `shutdown_timeout`. 0 closes idle clients right away.

Each client gets a FATAL `terminating connection due to administrator command` before pgcat closes it. Its SQLSTATE is `PS001` when pgcat forwarded nothing the client sent after its last ReadyForQuery, so none of it ran and the client can send it again on a new connection. A non-admin client that connects after SIGINT gets `PS001` too. If pgcat did forward something since, as when it let a server connection go mid-exchange, or a plugin hook's query was cut off or lost the server, the SQLSTATE is `08006` instead: what ran may have had effects. Before `PS001`, pgcat sent this error with SQLSTATE `58000`.

### ban_time
```
path: general.ban_time
default: 60 # seconds
```

How long to ban a server if it fails a health check (seconds).

### log_client_connections
```
path: general.log_client_connections
default: false
```

If we should log client connections

### log_client_disconnections
```
path: general.log_client_disconnections
default: false
```

If we should log client disconnections

### autoreload
```
path: general.autoreload
default: 15000 # milliseconds
```

When set, PgCat automatically reloads its configurations at the specified interval (in milliseconds) if it detects changes in the configuration file. The default interval is 15000 milliseconds or 15 seconds.

### worker_threads
```
path: general.worker_threads
default: 5
```

Number of worker threads the Runtime will use (4 by default).

### plugin_hook_concurrency
```
path: general.plugin_hook_concurrency
default: 256
```

Most plugin hooks that may run at once in the whole process. A hook that finds none free waits
within its pool's `plugin_hook_timeout`. Read when pgcat starts.

### trace_sample_ratio
```
path: general.trace_sample_ratio
default: 0.0
```

The share of traced requests whose statements pgcat records as spans, from 0 to 1. Reloadable. 0 turns
statement tracing off and records nothing; if pgcat starts at 0, no exporter is created. Once tracing
has been on, setting it back to 0 stops new spans but keeps the exporter until restart.

A client joins a statement to its trace with a leading [sqlcommenter](https://google.github.io/sqlcommenter/)
comment carrying a [W3C traceparent](https://www.w3.org/TR/trace-context/#traceparent-header):

```sql
/*application='api',traceparent='00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'*/ SELECT ...
```

pgcat reads only `traceparent` (version `00`, lowercase hex, as the comment's only traceparent), and
only from a comment at the very start of a simple Query or of a Parse; a trailing comment is not read.
Most sqlcommenter implementations append the comment by default; some can prepend it, e.g. Rails
`prepend_comment`. A client that appends it gets no spans. `tracestate` is not read. A
statement gets a span only when its caller sampled the trace (the traceparent's sampled flag) and the
trace id falls within the ratio, decided as OpenTelemetry's `TraceIdRatioBased` sampler decides, so
every pgcat at the same ratio keeps the same traces. pgcat starts no traces of its own: every span is
a child of a traceparent the client sent, and a statement without one isn't traced, except in a case
listed under Known gaps below.

Each simple Query and each Execute gets one `pgcat.statement` span of kind server, a child of the
caller's span. It starts when pgcat reads the statement and ends at the server's ReadyForQuery for
it: a simple Query's own, and for an Execute, the one for its batch's Sync or Flush. The Executes of
a pipelined batch all end there and share its outcome: if any of them fails, all carry the first
error. A simple Query the client sends while extended-protocol messages wait for their Sync or
Flush ends only its own span, whether a server runs it or pgcat answers or refuses it: those
messages run at their Sync or Flush, and the server's reply to it ends theirs. A span's attributes
are `db.system.name` (`postgresql`), `pgcat.pool.name`, `pgcat.pool.user`, `pgcat.server.role`,
`pgcat.shard`, `pgcat.protocol` (`simple` or `extended`), and three about the caller, as the client
was when pgcat read the statement:

- `pgcat.identity`: the client's `PGCAT_IDENTITY` state, `present`, `absent`, `expired` or
  `refused`, as the SQL observer labels it (see `sql_observer_enabled`).
- `pgcat.identity.verified`: `true` if a plugin verified the identity's token. It is `false` whenever
  the identity isn't `present`, and for any identity on a pool where nothing verifies identities,
  since pgcat then keeps the token as the client sent it.
- `pgcat.client`: the client's `application_name` if it is one of the pool's `sql_observer_clients`,
  matched exactly, else `other`. The list is empty by default, so every span says `other` and none
  carries a name the config doesn't list; it takes at most 33 values per pool and user, as the
  counts' `client` label does.

None of them comes from the identity's token, tenant or claims. A failed statement has an error
status, `error.type` and, when the server sent one, `db.response.status_code` (the SQLSTATE).

A statement pgcat answers with its own error, without a server running it, gets a span too. For
example, one that a plugin, a hook timeout, the query parser, the identity verifier or the tenant
guard refuses; one planned before a reload changed the pool's plugins; a COMMIT or ROLLBACK in a
query whose results a plugin rewrites; one in a batch that mixes statements pgcat answers itself
with ones for the server; an identity command pgcat can't run; one no server could be checked out
for (`connect_timeout`), and what waits for a Sync when that ends the session
(`checkout_failure_limit`); and the statement at which a reload ends the session, having changed the
identity's verifier or, for a client holding a server, the pool's tenant guard or server-text
withholding. Such a reload ends the session at the next message at which pgcat checks the reloaded
settings, a Query, Parse, Bind, Execute, Sync or Flush, so in the extended protocol an Execute pgcat
hasn't read by then gets no span. Nor does a statement whose pool a reload
removed before a server was checked out for it: pgcat answers it with `58000` ("No pool configured")
and ends the session. The span ends when pgcat sends the error. It has an error status,
`pgcat.refused` (`true`) and, as `error.type`, the SQLSTATE of that error (`58000` if it has none,
as the client sees it; `_OTHER` if it isn't five digits or capital letters). Nothing else from the
error is read, and the span has no `pgcat.server.role`, `pgcat.shard` or `db.response.status_code`,
since no server may have been checked out. When pgcat refuses the rest of a batch after the server
failed one of its statements, the spans keep the server's error. A pipelined batch's statements
share the refusal, as they share its outcome. A batch pgcat refuses at a Sync or Flush during a
simple Query's COPY IN doesn't refuse the COPY, which goes on and ends at its own ReadyForQuery.

Statement tracing never puts the statement, its comment or parameters in spans, and its own logs
don't include them; other pgcat logging is unchanged. Other statements pgcat answers itself, such
as the identity commands it runs, have no span. Neither do those pgcat hasn't sent to a server when
it sees the client disconnect, such as Executes still waiting for their Sync. A statement a server
is already running still ends at the server's reply, even if the client has closed its socket by
then.

Known gaps:

- A batch the server rejects forgets the prepared statements it traced, including ones the server
  created, so their later executions have no span until they're parsed again.
- pgcat sees SQL `DEALLOCATE` and `DISCARD` only in a simple Query it successfully parses, and then
  forgets every traced statement. Run otherwise, or as a prepared statement, they leave a traced name
  in place: if SQL `PREPARE` then reuses the name, the new statement is traced under the old one's
  traceparent.
- After a reload that removes a name from `sql_observer_clients`, the client's next statement can
  still carry that name in `pgcat.client`. A client holding a server checked out before the reload
  keeps carrying it until that checkout ends, which in session mode is when the client disconnects,
  or until a reload changes the pool's plugins; the statement after either can carry it too.

Spans are exported over OTLP/HTTP (protobuf) by OpenTelemetry's batch processor: its queue is bounded
and drops spans when full, so a slow or unreachable collector costs spans, never statement latency.
Export failures are logged when they start and when they stop. The standard `OTEL_BSP_*` variables
tune the queue, `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` the resource (`service.name`
defaults to `pgcat`). Queued spans are flushed at shutdown, for up to 5 seconds.

### trace_otlp_endpoint
```
path: general.trace_otlp_endpoint
default: <none>
```

The OTLP/HTTP traces URL statement spans are sent to, e.g. `http://otel-collector:4318/v1/traces`.
Must be `http://` with a host, a valid port if one is written, and no credentials, query or fragment.
When not set, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` applies, then
`http://localhost:4318/v1/traces`; one with credentials, a query or an invalid port gets no spans.
Read when `trace_sample_ratio` first goes above 0; changing it later takes effect at restart.

pgcat sends only `Host` (from the endpoint) and `Content-Type: application/x-protobuf`; it doesn't
forward headers from `OTEL_EXPORTER_OTLP_HEADERS` or `OTEL_EXPORTER_OTLP_TRACES_HEADERS`.

### tcp_keepalives_idle
```
path: general.tcp_keepalives_idle
default: 5
```

Number of seconds of connection idleness to wait before sending a keepalive packet to the server.

### tcp_keepalives_count
```
path: general.tcp_keepalives_count
default: 5
```

Number of unacknowledged keepalive packets allowed before giving up and closing the connection.

### tcp_keepalives_interval
```
path: general.tcp_keepalives_interval
default: 5
```

### tcp_user_timeout
```
path: general.tcp_user_timeout
default: 10000
```
A linux-only parameters that defines the amount of time in milliseconds that transmitted data may remain unacknowledged or buffered data may remain untransmitted (due to zero window size) before TCP will forcibly disconnect


### tls_certificate
```
path: general.tls_certificate
default: <UNSET>
example: "server.cert"
```

Path to TLS Certificate file to use for TLS connections

### tls_private_key
```
path: general.tls_private_key
default: <UNSET>
example: "server.key"
```

Path to TLS private key file to use for TLS connections

### admin_username
```
path: general.admin_username
default: "admin_user"
```

User name to access the virtual administrative database (pgbouncer or pgcat)
Connecting to that database allows running commands like `SHOW POOLS`, `SHOW DATABASES`, etc..

### admin_password
```
path: general.admin_password
default: "admin_pass"
```

Password to access the virtual administrative database

### auth_query
```
path: general.auth_query
default: <UNSET>
example: "SELECT $1"
```

Query to be sent to servers to obtain the hash used for md5 authentication. The connection will be
established using the database configured in the pool. This parameter is inherited by every pool
and can be redefined in pool configuration.

### auth_query_user
```
path: general.auth_query_user
default: <UNSET>
example: "sharding_user"
```

User to be used for connecting to servers to obtain the hash used for md5 authentication by sending the query
specified in `auth_query_user`. The connection will be established using the database configured in the pool.
This parameter is inherited by every pool and can be redefined in pool configuration.

### auth_query_password
```
path: general.auth_query_password
default: <UNSET>
example: "sharding_user"
```

Password to be used for connecting to servers to obtain the hash used for md5 authentication by sending the query
specified in `auth_query_user`. The connection will be established using the database configured in the pool.
This parameter is inherited by every pool and can be redefined in pool configuration.

### dns_cache_enabled
```
path: general.dns_cache_enabled
default: false
```
When enabled, ip resolutions for server connections specified using hostnames will be cached
and checked for changes every `dns_max_ttl` seconds. If a change in the host resolution is found
old ip connections are closed (gracefully) and new connections will start using new ip.

### dns_max_ttl
```
path: general.dns_max_ttl
default: 30
```
Specifies how often (in seconds) cached ip addresses for servers are rechecked (see `dns_cache_enabled`).

## `pools.<pool_name>` Section

### pool_mode
```
path: pools.<pool_name>.pool_mode
default: "transaction"
```

Pool mode (see PgBouncer docs for more).
`session` one server connection per connected client
`transaction` one server connection per client transaction

### load_balancing_mode
```
path: pools.<pool_name>.load_balancing_mode
default: "random"
```

Load balancing mode
`random` selects the server at random
`loc` selects the server with the least outstanding busy connections

### checkout_failure_limit
```
path: pools.<pool_name>.checkout_failure_limit
default: 0 (disabled)
```

`Maximum number of checkout failures a client is allowed before it
gets disconnected. This is needed to prevent persistent client/server
imbalance in high availability setups where multiple PgCat instances are placed
behind a single load balancer. If for any reason a client lands on a PgCat instance that has 
a large number of connected clients, it might get stuck in perpetual checkout failure loop especially
in session mode
`

The client gets a FATAL `checkout failure limit reached` with SQLSTATE `53300` before pgcat closes its connection.

### default_role
```
path: pools.<pool_name>.default_role
default: "any"
```

If the client doesn't specify, PgCat routes traffic to this role by default.
`any` round-robin between primary and replicas,
`replica` round-robin between replicas only without touching the primary,
`primary` all queries go to the primary unless otherwise specified.

### db_activity_based_routing
```
path: pools.<pool_name>.db_activity_based_routing
default: false
```

If enabled, PgCat will route queries to the primary if the queried table was recently written to.
Only relevant when `query_parser_enabled` *and* `query_parser_read_write_splitting` is enabled.

##### Considerations:
- *This feature is experimental and may not work as expected.*
- This feature only works when the same PgCat instance is used for both reads and writes to the database.
- This feature is not relevant when the primary is not part of the pool of databases used for load balancing of read queries.
- If more than one PgCat instance is used for HA purposes, this feature will not work as expected. A way to still make it work is by using sticky sessions.

### db_activity_based_ms_init_delay
```
path: pools.<pool_name>.db_activity_based_ms_init_delay
default: 100
```

The delay in milliseconds before the first activity-based routing check is performed.

### db_activity_ttl
```
path: pools.<pool_name>.db_activity_ttl
default: 900
```

The time in seconds after which a DB is considered inactive when no queries/updates are performed to it.

### table_mutation_cache_ms_ttl
```
path: pools.<pool_name>.table_mutation_cache_ms_ttl
default: 50
```

The time in milliseconds after a write to a table that all queries to that table will be routed to the primary.

### prepared_statements_cache_size
```
path: general.prepared_statements_cache_size
default: 0
```

Size of the prepared statements cache. 0 means disabled.
TODO: update documentation

### sync_application_name
```
path: pools.<pool_name>.sync_application_name
default: true
```

Whether pgcat sends the client's `application_name` to the server. By default, when a client
takes a server connection whose `application_name` differs from the client's, pgcat runs
`SET application_name` on it first, so the server sees each client's name.

When false, server connections keep the name pgcat connects with, `pgcat`, which is what the
server reports in `pg_stat_activity` and to `SHOW application_name`. Clients with different
names then share server connections without that extra round trip at checkout. If a client
changes the name on the server (`SET application_name`, `set_config()`), pgcat sets `pgcat` back
the next time the connection is checked out, so in transaction mode the change lasts until the
end of the client's transaction. The name the client connected with is still what pgcat uses
for `SHOW CLIENTS` and logs; `SHOW SERVERS` shows the client's current name, which follows the
client's own `SET application_name`. The other tracked parameters (`TimeZone`,
`client_encoding`, ...) are synced either way.

A reload that changes the setting recreates the pool, as for other pool settings. Each client
switches to the new pool when it next takes a server connection; a session-mode client keeps
the server connection it holds until it disconnects.

### query_parser_enabled
```
path: pools.<pool_name>.query_parser_enabled
default: true
```

If Query Parser is enabled, we'll attempt to parse
every incoming query to determine if it's a read or a write.
If it's a read query, we'll direct it to a replica. Otherwise, if it's a write,
we'll direct it to the primary.

### plugins_require_parse
```
path: pools.<pool_name>.plugins_require_parse
default: false
```

If plugins are configured and this setting is enabled, queries the query parser can't handle
(syntax it doesn't understand, or longer than `query_parser_max_length`) that might touch a table
a plugin checks are rejected with an error instead of being forwarded to the server without the
plugins seeing them. A query might touch such a table if its text names one (in any case), or has
Unicode escapes (`U&"..."`), which can spell any name. While any table is checked, so might a
`VACUUM`, which can process every table, a `DO` block, `CREATE FUNCTION` or `CREATE PROCEDURE`,
whose body is a string that can name any table, or, if its text has one of those words, a query
that pgcat can't split into tokens or whose inline `COPY ... FROM STDIN` data ends with a `\.` line
or nests more than four COPYs. Other queries the parser can't handle are forwarded, for the server
to answer.
`ONLY` before a table name counts as something the parser can't handle: it reads `FROM ONLY t` as a
table named `ONLY`.

With this setting, or while any process-wide plugin is registered (which makes pgcat require
parsing on every pool), while table access lists any table, it also refuses a `CREATE FUNCTION` or
`CREATE PROCEDURE` the parser reads whose body is a string, so no string body reaches the server.
Without either, table access checks names only: those queries are forwarded, and so is a
string-bodied function the parser reads.

A plugin configured in a pool's `[plugins.<name>]` section refuses, while it's on, every query the
parser can't handle on that pool, with or without this setting, unless it declares that it checks
only some tables (see docs/plugin-registration.md). `RESET` is still forwarded.

Statements pgcat refuses, here or in a plugin, fail like statements the server refuses: with an
ERROR (the connection stays usable) and a SQLSTATE, `0A000` for queries pgcat can't parse, `42501`
for statements a plugin denies and `XX000` for a plugin that failed, unless the plugin gives its own.
A refused statement inside a transaction fails the transaction, so the rest of it can't commit: the
client can only roll it back.

### plugin_hook_timeout
```
path: pools.<pool_name>.plugin_hook_timeout
default: 5000 # milliseconds
```

Longest a plugin may take on one statement, waiting for a free hook slot (see
`plugin_hook_concurrency`) included. A plugin that takes longer fails the statement with `PH001`
(`PH002` if it never got a slot), instead of holding the client's connection. The same bound
applies to a configured plugin verifying a `SET PGCAT_IDENTITY` token: the `SET` fails with
`PH001` or `PH002`, and the previous identity is cleared.

### identity_required_for_plugins
```
path: pools.<pool_name>.identity_required_for_plugins
default: false
```

If enabled, plugins that support it reject the statements they cover when the client hasn't
attached a caller identity to its connection. Each plugin decides which statements it covers; the
built-in `table_access`, `intercept`, `query_logger` and `prewarmer` plugins ignore this setting.
A configured plugin (see `pools.<pool_name>.plugins.<name>` Sections) can require it on the pools
it's enabled on. Clients attach an identity with commands answered by pgcat itself, over the simple
or extended protocol:

```sql
SET PGCAT_IDENTITY TO '<token>';  -- or SET PGCAT_IDENTITY = '<token>'
SHOW PGCAT_IDENTITY;              -- whether it's set, and a fingerprint (never the token)
RESET PGCAT_IDENTITY;
```

What the identity is attached to, and for how long:

- **The client connection.** pgcat keeps the token on the client connection, not on a server
  connection, so it survives transaction pooling: an autocommit statement carries it without a
  `BEGIN`, and so does every statement after it.
- **Until the client replaces or clears it.** It's held until the next `SET PGCAT_IDENTITY`,
  `RESET PGCAT_IDENTITY`, `DISCARD ALL` or `RESET ALL`, or until the client disconnects.
  `COMMIT` and `ROLLBACK` don't touch it, including the `ROLLBACK` of a transaction it was set
  in. A plugin that verifies tokens (see below) can refuse one: the `SET` then fails and the
  previous identity is cleared too. An identity whose verified token expires is forgotten.
- **Never sent to Postgres.** The commands are answered by pgcat, and take effect when pgcat
  answers them: one in a batch pgcat refuses (one that also has statements for the server)
  changes nothing. Queries that mix an identity command with other SQL, or use another form of
  it (`SET LOCAL PGCAT_IDENTITY`), are rejected so the token can't reach the server, its logs or
  `pg_stat_statements`. So are `SET`, `RESET` and `SHOW` of a setting named with Unicode escapes
  (`SET U&"..."`), which pgcat doesn't decode, and `set_config()` calls whose name is
  `pgcat_identity` (in any case, also as `pg_catalog.set_config` or in a `DO` block), or isn't a
  string literal in a query that mentions `pgcat_identity` or has Unicode escapes. These are all
  refused with `0A000`. Other SQL goes to Postgres as it is, with any token a client puts in a
  literal, or in a `set_config()` whose name pgcat can't read (an expression that doesn't spell
  it, a bound parameter or, on a pool without `identity_tenant_parameter`, a name with backslash
  escapes after the client turns `standard_conforming_strings` off). pgcat logs a fingerprint
  instead of the token. Fingerprints are keyed per pgcat process: the same token has a different
  one on another pgcat instance or after a restart, so they only correlate log lines of one
  process.

### identity_tenant_parameter
```
path: pools.<pool_name>.identity_tenant_parameter
default: <none>
example: "app.tenant"
```

A custom session parameter that carries the tenant of the client's identity to Postgres, so that
row-level security can use it in autocommit statements and transactions alike:

```sql
CREATE POLICY tenant_rows ON accounts
  USING (tenant_id::text = NULLIF(current_setting('app.tenant', true), ''));
```

The tenant comes from a plugin that verifies the token of `SET PGCAT_IDENTITY`, one in a
`plugins.<name>` section (below). A pool has one such plugin at most; with none there is no
tenant. A pool with one must set this parameter: pgcat refuses to start otherwise. pgcat treats
the parameter like the startup parameters it tracks: when a client checks out a server
connection, and before each of its statements is sent, pgcat sets the parameter to the client's
tenant, or resets it if the client has none. A server connection
therefore carries a client's tenant only while that client is using it: the next client sets its
own tenant or resets it before its first statement. Read the parameter with `missing_ok`
(`current_setting(name, true)`): it's NULL on a connection pgcat never set it on and `''` after
a reset.

Postgres doesn't report custom parameters back, so pgcat tracks what it set on each server
connection. It sets the parameter at every checkout, since the last client's statements may have
changed it without naming it (a function, view, rule or default that sets it). Within a checkout
it sets it again when it can't be sure: after a transaction in which it set it ends (a `ROLLBACK` undoes a `SET`) or rolls back to a savepoint,
and after the client or pgcat's cleanup runs `DISCARD ALL` or `RESET ALL`. It also sets it again
after any statement that could name it (one that mentions both parts of its name, `set_config`,
`EXECUTE` or a Unicode escape) and wasn't rejected, in case it changed the parameter anyway.

One case is left: when pgcat sets the parameter inside a transaction (the client changed its
identity in it, or pgcat had to set it again), and the client sends `ROLLBACK` and more statements
in one simple query, those statements run with the value from before the transaction. That value
is the same client's earlier tenant, never another client's: the server connection stays with the
client for the whole transaction.

Clients can't set the parameter themselves: queries that `SET` or `RESET` it (also in
`ALTER ... SET` or a function's `SET` clause), or call `set_config()` with its name or with a name
that isn't a string literal, are rejected (`42501`). So are queries pgcat can't read as Postgres
would: ones with Unicode escapes (`U&'...'`, `U&"..."`), which can spell the parameter or
`set_config` without writing either, and every `DO` block and `CREATE FUNCTION` or
`CREATE PROCEDURE`, whose bodies are SQL with their own quoting and escapes inside a string.
pgcat can't see what existing database objects do, so the database has to keep the client from
changing the parameter through them (see below).

pgcat finds these statements by reading queries the way Postgres splits them into strings and
comments, which depends on `standard_conforming_strings` and `client_encoding`. In a pool with
this setting, clients can't change either (`SET client_encoding` or `SET NAMES` to UTF8 is
allowed), and a client whose session would have `standard_conforming_strings` off or a client
encoding other than UTF8, from its startup parameters or the database's defaults, is refused at
connection with `0A000`. If the server reports either later (a function changed it), the client
is disconnected with the same error and the server connection is closed.

Row-level security only holds if the database also keeps the pool's role under it:

- The role doesn't own the tables (owners bypass their policies unless the table has
  `FORCE ROW LEVEL SECURITY`), and isn't a superuser or `BYPASSRLS`.
- The role can't `SET ROLE` or `SET SESSION AUTHORIZATION` to a role that is any of those: it
  isn't a member of one.
- The role can't execute any function or procedure that sets the tenant parameter (revoke
  `EXECUTE` on such functions from `PUBLIC`), and no view, rule, trigger or column default it can
  reach calls one. Not creating or owning one isn't enough: calling one is.
- The role can't create such objects: it has no `CREATE` on any schema, and no `TEMPORARY` on the
  database (`REVOKE TEMPORARY ON DATABASE ... FROM PUBLIC`), which also stops temporary views,
  rules and column defaults, and temporary tables. Revoking `USAGE ON LANGUAGE plpgsql` from
  `PUBLIC` also stops `DO` blocks at the database.
- The database and role defaults keep `standard_conforming_strings` on, and the database encoding
  lets clients use UTF8.

If a function that sets the parameter is reachable anyway, pgcat can't stop a statement from
calling it and reading under the changed value in the same statement (`SELECT set_tenant('x'),
(SELECT ... FROM docs)`), nor the same client's later statements in that checkout from running
under it. The next checkout sets the client's tenant again.

### plugin_response_batch_rows
```
path: pools.<pool_name>.plugin_response_batch_rows
default: 500
```

Plugins that rewrite result rows receive them in batches of up to this many rows.

### plugin_response_batch_bytes
```
path: pools.<pool_name>.plugin_response_batch_bytes
default: 1048576 # 1 MB
```

Plugins that rewrite result rows receive them in batches of up to this many bytes,
or `plugin_response_batch_rows` rows, whichever comes first.

### sql_observer_enabled
```
path: pools.<pool_name>.sql_observer_enabled
default: false
```

If enabled, pgcat counts the SQL statements clients send for execution through the pool and
exports the counts with the Prometheus metrics. A statement is counted once, when pgcat admits
it, as the client wrote it: before plugins rewrite or refuse it, and whether or not the server
then runs it. Commands pgcat answers itself (`PGCAT_IDENTITY`, its custom commands) and the SQL
pgcat sends on its own are not counted.

Simple queries the query parser parsed are counted, and so is the first Execute of each portal
whose statement the parser parsed (once per Bind, as the client Parsed it; fetching more rows
with further Executes counts nothing). The count is a lower bound, except after pgcat refuses a
rebind of a portal, which can count that portal's next Execute again (it then fails at the
server, as the refusal fails the transaction). What pgcat could not count is
reported as gap events: each one is a message whose statements went uncounted, not a number of
statements. To count Executes pgcat keeps a small, fixed-size record per client (at most 128
statements and 128 portals, names up to 128 bytes, kept as keyed hashes rather than copies, at
most 16,512 bytes in all), allocated only for clients of pools that observe. The relation
classifier's 16 KiB scratch is one per worker thread, not per client. When the record overflows
or is invalidated, it is dropped, never grown, and the Executes it can't account for are gap
events.

With the prepared statement cache on (a transaction-mode pool with a nonzero
[`prepared_statements_cache_size`](#prepared_statements_cache_size)), a named statement isn't kept
in this record. Its classification is kept once on the pool's cached statement, set by the first
client whose Parse of that SQL text the parser parsed and no plugin rewrote, so it depends only on
the SQL text and the pool's parser and observer config, and pgcat finds it through the client's own
prepared statements, so the 128-statement bound doesn't apply to it: it is counted for as long as
the client's prepared statement makes it run. Each cached statement pgcat holds is 48 bytes larger
for this, with the observer off too. Where the cached statement's classification isn't this client's
(the unnamed statement, statements a plugin rewrote, and statements this client classified
differently), the client's own is kept with the client's prepared statement, so it goes, and is put
back, with it; each hash bucket of a client's prepared statements is 16 bytes larger for this, also
with the observer off, and with the observer on each statement with its own classification takes one
44-byte allocation, made with its Parse. The record still keeps the statements pgcat answers itself,
as well as all portals; with the cache on, these tables start empty and grow as they are used, up to
the same 128 entries each, so a client binding more than 128 distinct portals still overflows. With
the cache off, the record and its bounds are as described above.

What a batch of extended protocol messages stages in the record stands once staged: pgcat doesn't
take it back when it refuses the batch or the server answers it with an error, and counts already
made stay. A name the batch defined counts when it is executed, though the server may not have
created it. A name the batch redefined counts as the new definition until it is defined again, so
with the prepared statement cache off, after pgcat refuses a batch that redefines a statement, or
the server fails it before the redefinition, the server keeps running the old definition while its
Executes are counted as the new one. With the cache on, a statement is counted as what the client's
prepared statement runs: pgcat puts back the client's prepared statements when it refuses a batch,
and the count follows, and a batch the server fails was sent, so what it prepared stands in pgcat
and in the counts. A name the batch closed is forgotten, so its later Executes are gap events; with
the cache on, a statement is forgotten only when pgcat drops it from the client's prepared
statements. This only changes the counts, never what pgcat or the server runs.

- `pgcat_sql_observer_observing{pool,user}`: 1 if the pool counts, 0 if not. A pool that isn't
  counting exports no counts, so it can't be mistaken for one without traffic. This reflects the
  pool's current config, but each client reads the setting from its own copy of the pool's
  settings, refreshed when it next takes a server from the pool, so a reload is not an instant
  switch for every client: one still holding the older settings (such as a session-mode client,
  or one whose statements pgcat answers itself) can keep counting after the pool stops observing,
  or not count yet after it starts. The counts are cumulative, so what such a client counted while
  the pool didn't export it shows once the pool observes again.
- `pgcat_sql_observer_attempts_total{pool,user,client,identity}`: statements counted, by the
  client's `application_name` (`client`, see below) and its `PGCAT_IDENTITY` state when the
  statement was admitted (see below).
- `pgcat_sql_observer_gap_events_total{pool,user,client,reason}`: messages not counted, by the
  client's `application_name` (`client`) and by reason:
  `unparsed_query` (a simple query the parser didn't parse, or with the parser disabled),
  `refused_unparsed` (a simple query refused before it was parsed), `unparsed_statement` (an Execute
  of a prepared statement the parser didn't parse, or of one dropped from the record before the
  server was last idle: pgcat forgets why a statement is missing when a transaction ends, so read
  this reason as statement metadata unavailable, not only as parser coverage), `unknown_portal` (an
  Execute of a portal pgcat has no record of, such as a SQL cursor's) and `metadata_invalidated` (an
  Execute whose statement was dropped from the record: a statement that ended the portal, a
  Close, the server being taken from the client, or the record's bounds).

- `pgcat_sql_observer_relation_attempts_total{pool,user,relation,identity}`: for each counted
  statement, the tables it names, from the statement as the client wrote it (before plugins
  rewrite it). `relation` is the bare table name, without its schema (the database is the `pool`
  label), or one of three fixed labels: `other` (a new name once the pool's names are full, see
  below), `unknown` (anything pgcat can't attribute: a function or other non-table source, a name
  with three parts, an unquoted non-ASCII name, unsupported syntax, more than 16 distinct
  relations, or a statement prepared before the pool was removed and added back) and
  `no_explicit_relation` (a statement naming no relation, such as `SELECT 1` or
  `BEGIN`). A statement naming several relations counts once in each, and once per relation
  however often it repeats one, so these are not statement counts: the series do not sum to
  `attempts_total`. Names fold as PostgreSQL folds them: unquoted names to lower case, `"quoted"`
  names exactly. A name PostgreSQL would accept unquoted (lower-case ASCII letters, digits, `_`
  and `$`, not starting with a digit) is its own label, so `Public.Orders` and `"public"."orders"`
  are both `orders`; any other name keeps its quotes, with embedded quotes doubled (`"Orders"`,
  `"order items"`), as does a table named like a fixed label (`"other"`). A name is cut to 63
  bytes on a character boundary, as PostgreSQL cuts identifiers, so names PostgreSQL reads as one
  share a label; the label is then cut to 64 characters, quotes included, and names sharing a
  label share its series. A quoted name of 63 one-byte characters loses only its closing quote,
  but embedded quotes are doubled, so a name loses real content once its characters plus its
  embedded quotes exceed 63: one embedded quote in a 63-character name is enough. Two names
  PostgreSQL keeps apart that differ only past that cut share a label (a deliberate false
  positive). Dropping the schema and
  never consulting the search path means a temp table, or the same name in two schemas, counts
  under one label (a deliberate false positive). CTE names, aliases and function names are never
  labels. A view is labeled by its own name, not the tables under it. Overflowing the relation
  bounds changes only these series, never `attempts_total` or the gap events.

Every table name the parser sees is recorded, in the order first seen, up to 1,000 names per pool
and user. These counters are shared by that pool's clients and are not part of the per-client
record above. A name keeps its series while the pool exists: names are never evicted, and a config
reload that keeps the pool keeps them all (and their counts). Removing the pool from the config
and adding it back starts an empty set. The counters are allocated 64 names at a time as names
are first seen, so a pool that never counts a name allocates none. When a pool first reaches 800
names, and again when it reaches 1,000, pgcat logs one warning with the pool, user and count. Past
1,000, new names count as `other`, and pgcat logs a warning naming up to 16 of the names seen
since the previous such warning, at the next overflow at least a minute later (per pool and user);
a name seen under contention, or after the last overflow, may go unlisted. The counts are exact
either way. A name takes its place when pgcat first classifies a statement naming it, which is at
Parse for a prepared statement, so a statement that is only prepared, or is refused after it was
parsed, still takes one, though it counts nothing.
The relation series take up to 1,003 × 4 series per pool and user. Datadog folds metric tag
values to lower-case names of at most 200 characters, other characters becoming `_`, so a quoted
or mixed-case label may be merged with another there.

The attempt and gap counters label the client's `application_name` (as pgcat tracks it for the
client: the startup parameter, or a later `SET application_name` once the server reports it) as
`client`: its own name if it is one of the pool's `sql_observer_clients`, else `other`. A name
matches only exactly (case and all); every other name, the empty one included, counts as `other`,
so by default every client counts as `other`. `client` takes at most 33 values per pool and user
whatever clients send, and the counters are a fixed 2,376 bytes per pool and user (33 × 9
counters, plus the names, allocated whether or not the pool observes), shared by its clients,
with nothing added per client. A pool exports `other` from the
start and a configured client once it has counted a statement or a gap event (in both metrics),
so an unused client exports no series; the attempt and gap series take at most 33 × 9 per pool
and user. The label is never read from `PGCAT_IDENTITY` or its token, and it only labels the
counts.

Both counted metrics label the client's `PGCAT_IDENTITY` state as `identity`:

- `present`: a verified, unexpired identity is set.
- `absent`: none was ever set, or it was reset (`RESET PGCAT_IDENTITY`, `DISCARD ALL` or
  `RESET ALL`).
- `expired`: the identity set expired. It stays `expired` until the next successful `SET` or a reset.
- `refused`: the last `SET PGCAT_IDENTITY` was refused. It stays `refused` until the next successful
  `SET` or a reset.

The state only labels the counts: authorization and plugins see the client as having no identity
whenever it isn't `present`. The first time a relation is counted for a pool and user with an
identity other than `present`, pgcat logs one info line on the `pgcat::sql_observer::callers` log
target with the pool, user, relation, identity state, `application_name` and client address
(at most once per relation and state, so at most 3,000 lines per pool and user); the relation and
`application_name` are in double quotes. In this line and the overflow warning, client-chosen text
(table names, `application_name`) is escaped: `\`, `"`, control characters (such as a newline,
`\n`, or an escape, `\u{1b}`) and Unicode line and bidi controls.

Statements counted with an identity other than `present` are also sampled on the
`pgcat::sql_observer::unidentified` log target: one info line per such statement, with the pool,
user, identity state, the `application_name` (cut to 64 bytes on a character boundary, then
escaped as above, so a long name can't grow the line), the client's address (to
find a client whose name is missing or generic) and the first relation the statement
names that has a label (else its fixed label: `other`, `unknown` or `no_explicit_relation`), at
most 10 lines per minute per pool and user. Statements past the budget aren't logged; the next
line says how many went unlogged since the line before it (a statement counted while another
thread holds the budget may go uncounted there too). The line never carries the SQL text or the
`PGCAT_IDENTITY` token.

The global attempt and gap counters survive a config reload for as long as the pool exists, and
so do the relation series. A query admitted just before a reload enables the parser is recorded
once, as the gap it was admitted as; its statements are not counted as well.

### sql_observer_clients
```
path: pools.<pool_name>.sql_observer_clients
default: []
```

The `application_name`s the SQL observer counts under their own `client` label (see
`sql_observer_enabled`); every other client counts as `other`. Statement spans take their
`pgcat.client` from the same list, whether or not the pool observes (see `trace_sample_ratio`).
At most 32 names, each 1 to 64 bytes, distinct, without control characters or commas, and not
`other`. A name keeps its counters across reloads for as long as the pool exists, even after it is
removed from the list, so the counts stay monotonic; a pool takes at most 32 names over its
lifetime, and a name added past that counts as `other`, with a warning.

### primary_reads_enabled
```
path: pools.<pool_name>.primary_reads_enabled
default: true
```

If the query parser is enabled and this setting is enabled, the primary will be part of the pool of databases used for
load balancing of read queries. Otherwise, the primary will only be used for write
queries. The primary can always be explicitly selected with our custom protocol.

### sharding_key_regex
```
path: pools.<pool_name>.sharding_key_regex
default: <UNSET>
example: '/\* sharding_key: (\d+) \*/'
```

Allow sharding commands to be passed as statement comments instead of
separate commands. If these are unset this functionality is disabled.

### sharding_function
```
path: pools.<pool_name>.sharding_function
default: "pg_bigint_hash"
```

So what if you wanted to implement a different hashing function,
or you've already built one and you want this pooler to use it?
Current options:
`pg_bigint_hash`: PARTITION BY HASH (Postgres hashing function)
`sha1`: A hashing function based on SHA1

### auth_query
```
path: pools.<pool_name>.auth_query
default: <UNSET>
example: "SELECT $1"
```

Query to be sent to servers to obtain the hash used for md5 authentication. The connection will be
established using the database configured in the pool. This parameter is inherited by every pool
and can be redefined in pool configuration.

### auth_query_user
```
path: pools.<pool_name>.auth_query_user
default: <UNSET>
example: "sharding_user"
```

User to be used for connecting to servers to obtain the hash used for md5 authentication by sending the query
specified in `auth_query_user`. The connection will be established using the database configured in the pool.
This parameter is inherited by every pool and can be redefined in pool configuration.

### auth_query_password
```
path: pools.<pool_name>.auth_query_password
default: <UNSET>
example: "sharding_user"
```

Password to be used for connecting to servers to obtain the hash used for md5 authentication by sending the query
specified in `auth_query_user`. The connection will be established using the database configured in the pool.
This parameter is inherited by every pool and can be redefined in pool configuration.

### automatic_sharding_key
```
path: pools.<pool_name>.automatic_sharding_key
default: <UNSET>
example: "data.id"
```

Automatically parse this from queries and route queries to the right shard!

### idle_timeout
```
path: pools.<pool_name>.idle_timeout
default: 40000
```

Idle timeout can be overwritten in the pool

### connect_timeout
```
path: pools.<pool_name>.connect_timeout
default: 3000
```

Connect timeout can be overwritten in the pool

## `pools.<pool_name>.users.<user_index>` Section

### username
```
path: pools.<pool_name>.users.<user_index>.username
default: "sharding_user"
```

PostgreSQL username used to authenticate the user and connect to the server
if `server_username` is not set.

### password
```
path: pools.<pool_name>.users.<user_index>.password
default: "sharding_user"
```

PostgreSQL password used to authenticate the user and connect to the server
if `server_password` is not set.

### server_username
```
path: pools.<pool_name>.users.<user_index>.server_username
default: <UNSET>
example: "another_user"
```

PostgreSQL username used to connect to the server.

### server_password
```
path: pools.<pool_name>.users.<user_index>.server_password
default: <UNSET>
example: "another_password"
```

PostgreSQL password used to connect to the server.

### pool_size
```
path: pools.<pool_name>.users.<user_index>.pool_size
default: 9
```

Maximum number of server connections that can be established for this user.
The maximum number of connection from a single Pgcat process to any database in the cluster
is the sum of pool_size across all users.

### min_pool_size
```
path: pools.<pool_name>.users.<user_index>.min_pool_size
default: 0
```

Minimum number of idle server connections to retain for this pool.

### statement_timeout
```
path: pools.<pool_name>.users.<user_index>.statement_timeout
default: 0
```

Maximum query duration. Dangerous, but protects against DBs that died in a non-obvious way.
0 means it is disabled.

### connect_timeout
```
path: pools.<pool_name>.users.<user_index>.connect_timeout
default: <UNSET> # milliseconds
```

How long the client waits to obtain a server connection before aborting (ms).
This is similar to PgBouncer's `query_wait_timeout`.
If unset, uses the `connect_timeout` defined globally.

## `pools.<pool_name>.shards.<shard_index>` Section

### servers
```
path: pools.<pool_name>.shards.<shard_index>.servers
default: [["127.0.0.1", 5432, "primary"], ["localhost", 5432, "replica"]]
```

Array of servers in the shard, each server entry is an array of `[host, port, role]`

### mirrors
```
path: pools.<pool_name>.shards.<shard_index>.mirrors
default: <UNSET>
example: [["1.2.3.4", 5432, 0], ["1.2.3.4", 5432, 1]]
```

Array of mirrors for the shard, each mirror entry is an array of `[host, port, index of server in servers array]`
Traffic hitting the server identified by the index will be sent to the mirror.

### database
```
path: pools.<pool_name>.shards.<shard_index>.database
default: "shard0"
```

Database name (e.g. "postgres")

## `pools.<pool_name>.plugins.<name>` Sections

Besides the built-in plugins, an executable embedding pgcat can register plugins that a pool turns
on and configures in its own `[pools.<pool_name>.plugins.<name>]` section, or inherits from a global
`[plugins.<name>]` one (see `docs/plugin-registration.md`). The plugin registered under that name
defines the section's settings. pgcat refuses to start, and a reload keeps the previous config, if
no plugin of that name is registered (the standalone `pgcat` binary registers none, so the images
`Dockerfile` and `Dockerfile.dev` build and the deb package refuse every such section), if the plugin
refuses the section, or if an enabled section applies to a pool without `query_parser_enabled`.
Such a plugin can verify the pool's `SET PGCAT_IDENTITY` tokens; a pool where more than one plugin
would verify them is refused the same way. Each such plugin starts
once, when pgcat starts, and pgcat doesn't start if one fails to or takes longer than 60 seconds. A
reload never starts one: a plugin that needs to have started on a pool fails closed on a pool a
reload first turns it on for. Logs show whether each such plugin is enabled, not its settings,
with one exception: a value the plugin can't parse is quoted in the error that fails the config.
Keep secrets out of plugin sections.

pgcat counts the plugin errors clients got, for every plugin, built-in or registered, in the
`/metrics` counter `pgcat_hook_errors_total{plugin,hook,sqlstate}`. A plugin's decisions
(`Deny`, which a plugin may count in its own metrics) and pgcat's own refusals aren't counted.
Each label takes fixed values, never a message, a token or a tenant:

- `plugin`: the name a plugin was registered under, or a built-in's (`testing`,
  `query_logger`, `intercept`, `table_access`). It's the plugin whose hook failed, the one running
  when the plugin chain timed out, and, for a planned statement's later hooks, the one that planned
  it. `none` for `PH002` (no hook ran) and for a statement no plugin is known to have planned.
- `hook`: `verify_identity` (a `SET PGCAT_IDENTITY` token, or a client's identity verified again
  after a reload changed its verifier), `query` and `parse` (the plugin chain on a simple Query or a
  Parse), and a planned statement's `execute`, `checkout`, `bind`, `param_description`,
  `row_description` and `rows`.
- `sqlstate`: the client's: `PH001` (timed out), `PH002` (no free hook slot), `28000` (the verifier
  refused a `SET PGCAT_IDENTITY` token), `0A000` (it refused, after a reload, the identity a client
  held, which ends the session), `42501` (a plugin's error denied the statement), `XX000`, or a code
  the plugin chose. A code that isn't five of `[0-9A-Z]` is counted as `XX000`.
