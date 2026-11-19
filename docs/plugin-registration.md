# Embedding PgCat with registered plugins

The executable can supply static Rust plugins without adding their implementation
or configuration schema to PgCat. The standalone executable supplies an empty
registry and retains its existing behavior.

```rust,ignore
let mut plugins = pgcat::plugins::registry::PluginRegistry::default();
plugins
    .register("policy", MyPlugin::new())
    .map_err(|_| "invalid plugin registration")?;
pgcat::runtime::run_with_plugins(plugins)?;
```

Handle registration errors using the executable's error policy. Registration
rejects empty or duplicate names with `Error::BadConfig`; names are static code
identifiers, not tenant data or configuration values.

## Ownership and startup

`run_with_plugins` consumes the registry, installs it once, loads the normal
configuration and runs the existing server. Call it once on the main thread,
outside an existing Tokio runtime. A second installation is an error, not a
replacement. The executable chooses its allocator; the standalone binary retains
jemalloc on supported targets.

This is a process-owning entry point, not a restartable server hosted inside
another application. It retains upstream's CLI parsing, global logger, signal
handlers and runtimes. Upstream's query-router setup, config parsing,
metrics-address parsing, listener binding and pool creation failures exit the
process with `exitcode::CONFIG` rather than returning `Err`; other fallible setup
and duplicate installation can return `Err`.

The startup loop deliberately remains in `src/main.rs`, compiled as a private
library module, so future upstream fixes merge into the same file we execute.
`src/runtime.rs` only adapts registration to that entry point. The standalone
`main()` and allocator live in `src/bin/pgcat.rs`, outside the library's module
tree. This avoids moving the loop and keeping an unrelated wrapper
at its upstream path, which prevents Git from following the move and forces
manual transplants during sync. If upstream syncability is no longer needed,
rename the retained startup module to `src/server_startup.rs` and update its
module wiring.

## Build and allocator contract

Build and run the standalone executable with ordinary `cargo build` and
`cargo run`, optionally adding `--release` or `--features test-plugins`.
Cargo explicitly selects `src/bin/pgcat.rs` as the binary root and `src/lib.rs`
remains the library root. There is no binary/library mode flag or build helper.

The library's module tree excludes `src/bin/pgcat.rs`. It selects no allocator, so the
embedding application can declare its own global allocator or use Rust's
default. The standalone binary selects jemalloc only on non-MSVC x86_64, retaining
the upstream platform policy; other platforms retain their default allocator.

A stale binary target that still uses `src/main.rs` fails to compile. The
retained module references the library's private module wiring, so the failure
persists even if an upstream sync restores a `fn main()` in that file.
On jemalloc platforms, the real entry point also references its allocator
declaration, so deleting it or giving it inconsistent cfgs fails compilation.
`tests/check_allocator_build.sh` checks valid builds and the stale-root failure,
including a simulated restored `fn main()`. The external-plugin example defines
its own allocator to detect an allocator accidentally introduced into the library.
On supported native x86_64 CI hosts, the script also checks that the executable
actually initializes jemalloc, covering accidental removal of `#[global_allocator]`.

When moving an external build's pin, including a Bazel overlay:

- Use `src/bin/pgcat.rs` as the standalone binary root. Keep `src/main.rs` among
  the library's explicit sources, not as the executable's root.
- The library starts at `src/lib.rs` and excludes `src/bin/pgcat.rs`. The private
  plugin executable has its own entry point and allocator; no mode flags are needed.
- Build both standalone and embedding targets. Run the plugin integration tests
  against the embedding executable, and verify a stale startup-root target
  fails rather than producing a fallback binary.

Run the Rust tests with `cargo test --features test-plugins`.
Normal all-target builds and checks are supported, including
`cargo build --all-targets --all-features` and
`cargo clippy --all-targets --all-features`.

## Shared plugin state

Registered implementations are `Send + Sync` and shared across connections.
Callbacks take `&self`: do not store client identity or statement state in the
shared instance. Use `PluginContext` and the returned `StatementPlan`, which owns
the existing Bind, checkout and response hooks. Shared caches may be retained by
the plugin. A registered plugin has no startup or shutdown; a configured plugin
starts and stops the background tasks it owns (below).

`QueryRouter::with_plugins(Arc<PluginRegistry>)` supports explicit injection for
library callers and isolated tests. It does not install a process-wide registry.

## Dispatch contract

- `on_query` handles simple queries. `on_parse` handles extended protocol and
  defaults to `on_query`; its context includes declared parameter types.
- Registration order is execution order. Registered plugins run before built-in
  query logging, interception and table access. Every callback sees the original
  statements, not another plugin's rewrite.
- After all plugins ran, a final rewrite is parsed again from the SQL that is
  sent, under `query_parser_max_length`, and checked as parsed: a plugin's
  statements can print as SQL that names other tables than they hold. The
  parse happens once per rewritten query, only when one of the checks below
  applies, and never for a query no plugin rewrote. A rewrite that can't be
  parsed again, is longer than `query_parser_max_length`, or uses `FROM ONLY`
  is refused with XX000 `a plugin's rewrite can't be parsed`, even with
  `plugins_require_parse = false`. A configured plugin's own rewrite is
  checked as it built it if its `rewrite_checked_as_built` says so (below).
- Table access checks the original statements and the final rewrite. A rewrite
  that reaches a blocked table is refused with 42501
  `permission denied for rewritten statement`, which does not name the table.
- A configured plugin that checks rewrites refuses another plugin's final
  rewrite that reaches what it covers (below). SQL that a plan's
  `on_execute_query` hook sends isn't parsed again or checked: plugins are
  trusted code.
- The first Deny or Intercept terminates the chain. Only one plugin may transform
  a statement: multiple rewrites, multiple plans, or a rewrite and a plan from
  different plugins are refused. One plugin may return both its rewrite and its
  matching plan; plans are not silently combined.
- Callback errors propagate through the existing client error path. Use
  `messages::with_sqlstate` and the named plugin SQLSTATE constants. Error
  messages must be safe to return to clients; never include protected data.
- Client dispatch remains inside `run_hook`, using existing timeout and
  concurrency limits. Direct library callers of router execution methods must
  use `run_hook` themselves, as before.
- Registered plugins are process-wide and independent of pool TOML. Their
  presence activates plugin dispatch even if pool query-parser routing is off.
  Unparseable ordinary queries are refused, not passed through unchecked.
- Parser diagnostics omit raw SQL and raw parser-error text. The standard
  runtime suppresses `sqlparser` dependency logs in every output format, even
  when `RUST_LOG` explicitly enables that dependency.
- Existing admin and proxy control paths retain their handling. This is a query
  plugin API, not a claim that all protocol operations are mediated.
- Configuration reload does not replace the registry. Registered plugins have
  no TOML of their own; a plugin that pools turn on and configure is a
  configured plugin (below), which can also verify identities, start
  background tasks and add metrics. Neither kind supports dynamic loading.

## Configured plugins

A plugin that only some pools run, with settings in pool TOML, implements
`ConfiguredPlugin` and is registered with `register_configured`:

```rust,ignore
plugins
    .register_configured("policy", MyPolicy::new())
    .map_err(|_| "invalid plugin registration")?;
```

```toml
[pools.app.plugins.policy]
enabled = true
# Settings only MyPolicy knows.
```

- Core keeps each `[plugins.<name>]` section as written and has the plugin
  registered under that name parse it into its `Config` type when the config is
  loaded or reloaded. Core carries no plugin's schema; use
  `#[serde(deny_unknown_fields)]` so a misspelled setting fails the config.
- A pool runs the sections it has, or inherits the global `[plugins]` ones if it
  has none, as for the built-in plugins. On other pools the plugin never runs,
  and those pools behave as if it weren't registered: unlike a registered
  plugin, it doesn't make every pool parse every statement.
- Fail closed: a section no plugin of that name is registered for, a section
  its plugin can't parse, or one its `validate` refuses for the pool it applies
  to fails the config. pgcat doesn't start (`exitcode::CONFIG`) and a reload
  keeps the previous config. So does an enabled section on a pool without
  `query_parser_enabled`, which couldn't run it. The standalone binary has no
  configured plugins, so it refuses any such section. Global sections are
  checked even if no pool inherits them. A section that reaches the router
  without being loaded by its plugin refuses every statement (`XX000`).
- Names share one namespace with registered plugins and can't be a built-in
  section (`intercept`, `table_access`, `query_logger`, `prewarmer`,
  `testing`).
- Dispatch: configured plugins run after the registered and built-in ones, in
  registration order, and the chain rules above apply (first Deny or Intercept
  ends it, one transformation per statement, the final rewrite is checked).
  As for the newer built-ins, a callback error denies the statement.
- Rewrites: a plugin plans the client's statements, not another plugin's
  rewrite of them. One whose `checks_rewrites` is true for a pool's section
  gets the final rewrite (`Overwrite`) of every other plugin on the pool,
  never its own, in `check_rewrite`, once all plugins ran: as parsed again
  from the SQL that is sent (above), so it sees the tables the server will
  read. An error refuses the statement with the error as the message: give it
  a SQLSTATE, and keep it safe to return to the client. A plugin whose
  `rewrite_checked_as_built` is true builds its rewrite from the client's
  parsed statements and names it quotes itself, so its statements hold the
  tables their SQL names: its own rewrite is checked, by table access and the
  plugins that check rewrites, as it built it, not parsed again, and may hold
  SQL that sqlparser prints but can't parse (a `SELECT` of no columns).
- Prepared statements: a pool's plan generation (`PluginContext::plan_generation`)
  changes when the section of a plugin that is on changes, the pool's own or
  the global one it inherits (a reload that changes a global section recreates
  the pools that inherit it, as for their own settings). With the prepared
  statement cache, statements prepared before are planned again from the
  client's Parse; without it they fail and must be prepared again. A statement
  a plugin answered itself (`Intercept`) fails too, with or without the cache,
  as does an Execute of a portal bound to it before the change: its answer, and
  the columns the client was told it returns, came from the old section. Its
  Bind gets an `ERROR` (`XX000`, prepare it again), so the connection stays
  usable. A batch in flight across the change is refused, as is an Execute of
  any other portal bound before it (an identity `SET`'s is verified again
  instead).
- `sensitive_queries` keeps SQL text, literals (the sharding keys routing finds
  in them included) and parse errors out of the logs for the pool.
- While a plugin is on, pgcat refuses (`0A000`) every query on the pool that
  the parser can't read (longer than `query_parser_max_length`, or syntax it
  doesn't know), whether or not the pool sets `plugins_require_parse`, so
  every statement that reaches the server went through the plugin.
  `RESET <name>` and `RESET ALL` are still forwarded. A plugin that checks only
  some tables can return true from `allows_unparsed_outside_gated_tables` to
  keep the table-scoped rule instead: with `plugins_require_parse`, an
  unparsed query is refused only if its text names one of its `gated_tables`
  (in any case), has a Unicode escape (`U&`), or, while `gated_tables` is
  non-empty, has a `DO` block or a `CREATE FUNCTION` or `CREATE PROCEDURE`
  (their bodies are strings) or has one of those words and can't be
  tokenized, or has inline `COPY ... FROM STDIN` data that ends with a `\.`
  line or nests more than four COPYs, and others are forwarded without its
  hooks; without it, every unparsed query is forwarded. That check reads the
  text, so SQL outside those statements that builds a name
  (`'secr' || 'ets'`) passes it.
- With `plugins_require_parse`, while any table is listed in table access, no
  string body reaches the server. Table access refuses a `CREATE FUNCTION` or
  `CREATE PROCEDURE` it parses whose body is a string (`AS $$...$$` or
  `AS '...'`, in any language), which it can't read. A body given as SQL
  (`BEGIN ATOMIC`, `RETURN`) is parsed and checked by the tables it names.
  `DO`, a string-bodied `CREATE PROCEDURE`, and a `CREATE FUNCTION` with an
  option the parser can't read (such as `COST` or `ROWS`) don't parse, and
  are refused as unparsed queries that might touch a listed table
  (CONFIG.md). Without `plugins_require_parse`, table access checks names
  only: a string body is forwarded, parsed or not, and can read a listed
  table. Either way, functions that run SQL given as text, such as
  `query_to_xml`, aren't refused. The refusal covers the tables listed in
  table access: a configured plugin sees a parsed string-bodied function like
  any statement, and refuses it itself if its tables need that.
- `withholds_server_text` is for pools whose rows hold protected values, which
  Postgres quotes in its diagnostics (a constraint violation's DETAIL is the
  whole row). The client then gets only the severity, SQLSTATE and object
  names of the server's errors and notices, with a fixed message. A session
  holding its server ends at its next statement if a reload changes it, as it
  does if its tenant parameter changes. A section the binary hasn't loaded
  withholds too.
- Identities: a plugin whose `verifies_identity` is true for a pool's section
  verifies the tokens clients set there with `SET PGCAT_IDENTITY`, through
  `verify_identity`. The `VerifiedIdentity` it returns sets the tenant parameter
  and its `claims` reach the pool's plugin callbacks through
  `PluginContext::identity`. A pool has one verifier at most, the testing
  plugin included: two fail the config, as does a verifier on a pool
  without `identity_tenant_parameter`. On a pool with a section its
  plugin hasn't loaded every token is refused (unless the test-only `testing`
  verifier is on, which takes priority), and on a pool with no verifier any
  token is kept as it is, without a tenant or claims, even with
  `identity_required_for_plugins`. A plugin that needs a verified identity
  checks for its own claims (`Identity::claims::<T>()`), not
  `identity().is_some()`. It doesn't decide on the identity in `on_parse`,
  though: a Parse's result is kept for the prepared statement and isn't asked
  for again when the client's identity changes.
- A refusal's message reaches the client after `PGCAT_IDENTITY refused: `
  (`28000`) and clears the identity the client had. It must not contain the
  token, a secret or a tenant identifier; pgcat replaces one that contains the
  token, or 20 or more characters in a row from `A-Z a-z 0-9 - _` (which a
  slice of a token, or a long identifier, has), with `the plugin refused the
  identity`, and logs only the token's fingerprint. `verify_identity` is async
  and runs like a hook, under the pool's `plugin_hook_timeout` and
  `general.plugin_hook_concurrency`: one that doesn't
  finish in time, or gets no slot, fails the `SET` with the hook's `PH001` or
  `PH002` instead of `28000`, and also clears the identity. pgcat doesn't retry
  the `SET`; what happens next is up to the client.
- A reload that changes what verifies identities on a pool (its verifier, or
  the verifier's section; the testing plugin's included) has pgcat verify the
  token of each client that holds an identity again, once, before that
  client's next statement runs, in a transaction too, and before any plugin
  sees the old claims. The same tenant and equal claims keep the identity,
  with the new result's expiry, as a new `SET` of the token would get: an
  expiry that changed doesn't end the session, one already past forgets the
  identity before the statement runs.
  Claims are compared with `==`, so the plugin's claims type implements
  `PartialEq` (`IdentityClaims`). Anything
  else (a refusal, another tenant or other claims, no verifier any more, or
  verification turned off) ends the session with `0A000` (`connect again`), as
  a reload changing `withholds_server_text` does, and a server the client held
  isn't used again. A verification that times out or gets no slot fails that
  statement with `PH001` or `PH002`, never running it with the old identity,
  and the next statement verifies again. A reload that leaves the verifier and
  its section as they were verifies nothing.
- Lifecycle: pgcat calls each configured plugin's `start(enabled)` once, in
  registration order, after the pools are created and before any client is
  accepted, on the server runtime (`tokio::spawn` works). pgcat already
  listens, and serves `/metrics`, while plugins start: clients that connect
  meanwhile wait. `enabled` says whether a pool turns the plugin on, in its own
  section or the global one it inherits; the plugin is started either way. A
  reload never starts a plugin, so one that needs `start` to have run with
  `enabled` must fail closed on a pool a reload first turns it on for.
- A `start` error stops pgcat with `exitcode::CONFIG`, after the plugins started
  before it are shut down. pgcat logs `Plugin "<name>" failed to start`, never
  the error: the plugin logs its reason itself, without secrets.
- A `start` that takes longer than `lifecycle::START_TIMEOUT` (60 s) fails the
  same way, logged as `Plugin "<name>" didn't start within 60s`. It's dropped,
  not shut down. Plugins bound their own startup I/O well within it.
- `start` is async and must not block its thread (synchronous I/O,
  `std::thread::sleep`, a blocking lock): the timeout fires only at an await
  point, so pgcat would wait for a blocked `start` however long it takes.
  Blocking work goes in `tokio::task::spawn_blocking`.
- `start` returns an error rather than panic: a panic's message is printed,
  pgcat exits with 101 instead of `exitcode::CONFIG`, and the plugins started
  before aren't shut down.
- When pgcat stops (SIGTERM, or SIGINT once the clients are gone or
  `shutdown_timeout` passed), it calls `shutdown` on the started plugins, the
  last started first, before `Shutting down...`. `shutdown` stops and joins the
  tasks `start` spawned. pgcat waits `lifecycle::SHUTDOWN_TIMEOUT` (10 s) for
  each at most, then logs a warning and goes on to the next.
- Clients may still be connected then: a plugin's hooks and `verify_identity`
  can run during and after its `shutdown`, and must keep failing closed
  without what it stopped.
- Metrics: `prometheus_lines` adds the plugin's lines to the `/metrics` page, in
  the text exposition format, whether or not the plugin is on or started. It's
  called on each scrape, on a runtime worker: it reads cached state and doesn't
  block. Name them `pgcat_<plugin>_...`; their labels and values must never
  carry tenants, tokens, keys or protected values.
- pgcat drops each line that isn't one canonical text-format line (`# HELP`,
  `# TYPE` or a sample) of a metric named `pgcat_<plugin>_...`, and warns once,
  naming the plugin, so one bad line can't fail the whole scrape.
- So that each plugin's prefix is its own, `register_configured` refuses
  (`BadConfig`) a name whose `pgcat_<name>_` overlaps core's metric families
  (`databases`, `servers`, `stats`, `pools`, `sql`, `hook`) or another
  configured plugin's: `a` and `a_b` can't both register.
- pgcat counts a plugin's hook errors, timeouts (`PH001`) and identity
  refusals (`28000`, or `0A000` when a reload's verification ends the
  session) in `pgcat_hook_errors_total{plugin,hook,sqlstate}`, labelled
  with the plugin's registered name (see `CONFIG.md`): a plugin doesn't count
  them itself.
- Logs show whether each section is enabled (`"policy": true`), never its
  settings. `validate` messages are logged after the section's name
  (`plugins."policy": <message>`), and a section that doesn't parse is reported
  with serde's message, which can quote the offending value, like TOML errors
  elsewhere in the file: keep secrets out of sections.

## Synthetic consumer and tests

`tests/fixtures/registered_plugin.rs` compiles as the separate
`registered_plugin` example crate and uses only public PgCat APIs. It is gated by
`test-plugins`, so a normal production build excludes it. It explicitly selects
`System` as its allocator to prove the library does not impose one.

```sh
cargo build --features test-plugins --example registered_plugin
PGCAT_TEST_PLUGINS=1 pytest -q tests/python/test_registered_plugins.py
```

`tests/fixtures/configured_plugin.rs` is the same kind of consumer for a
configured plugin; `tests/python/test_configured_plugins.py` runs it against a
pool that turns it on and one that doesn't, through reloads, verifies identities
with it, starts and stops it with pgcat and reads its metrics, and checks the
startup failures above against both the example and the standalone binary.

```sh
cargo build --features test-plugins --all-targets
PGCAT_TEST_PLUGINS=1 pytest -q tests/python/test_configured_plugins.py
```

The integration tests use the same local PostgreSQL setup as the existing hook
tests. They exercise simple/extended rewrites, denials, timeouts, errors,
transaction recovery, Bind and response hooks, cached plans, pipelining and
asyncpg Flush behavior, including conflicting transformations and recovery after
a pipelined refusal. The synthetic plugin intentionally refuses multi-statement
simple queries rather than replacing only part of a batch.
Unit tests inject separate registries instead of mutating
global process state.

This interface is a foundation for moving implementations out of the core.
Configured plugins supply per-pool settings, policy-generation invalidation,
sensitive-query logging, identity verification, a startup and shutdown for the
background tasks they own, and metrics.
