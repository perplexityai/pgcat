//! Plugins that pool TOML turns on. Each owns a `[plugins.<name>]` section:
//! core keeps it as written and only its plugin parses and checks it, so core
//! carries no plugin's configuration schema.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use log::error;
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer};
use sqlparser::ast::Statement;

use super::PluginRegistry;
use crate::config::{Plugins, Pool};
use crate::errors::Error;
use crate::identity::{IdentityToken, VerifiedIdentity};
use crate::plugins::{PluginContext, PluginResult};

/// A plugin a pool turns on with its `[pools.<pool>.plugins.<name>]` section,
/// or the global `[plugins.<name>]` one a pool without its own `plugins`
/// inherits. On other pools it never runs, and the pool behaves as if the
/// plugin weren't registered. Like the built-in plugins, it runs only with
/// `query_parser_enabled`: an enabled section on a pool without it fails the
/// config.
///
/// The instance is shared by every pool and connection, as for
/// [`RegisteredPlugin`](super::RegisteredPlugin): keep caller and statement
/// state in the context and the returned plan. Each callback gets the parsed
/// section of the pool it runs for.
#[async_trait]
pub trait ConfiguredPlugin: Send + Sync + 'static {
    /// The plugin's section, parsed once each time the config is loaded or
    /// reloaded. Use `#[serde(deny_unknown_fields)]` so a misspelled setting
    /// fails the config instead of being ignored. A section that doesn't parse
    /// fails it with serde's message, which can quote the offending value, as
    /// TOML errors in the rest of the file do: keep secrets out of the section.
    type Config: DeserializeOwned + Send + Sync + 'static;

    /// Whether the section turns the plugin on for its pool.
    fn enabled(&self, config: &Self::Config) -> bool;

    /// Check the section against the pool it applies to, whether or not it's
    /// enabled, when the config is loaded or reloaded. An error fails the
    /// load: pgcat doesn't start, and a reload keeps the previous config.
    /// The message is logged after the section's name, so it must not hold
    /// secrets.
    fn validate(&self, _config: &Self::Config, _pool: &Pool) -> Result<(), String> {
        Ok(())
    }

    /// A simple Query on a pool the plugin is on.
    async fn on_query(
        &self,
        config: &Self::Config,
        context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error>;

    /// A Parse on a pool the plugin is on. Its context has the declared
    /// parameter types. The result is kept for the prepared statement and not
    /// asked for again when the client's identity changes: it mustn't depend
    /// on the identity.
    async fn on_parse(
        &self,
        config: &Self::Config,
        context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        self.on_query(config, context, statements).await
    }

    /// Queries on the pool can hold values that must not be logged: core keeps
    /// their SQL text, literals and parse errors out of its logs.
    fn sensitive_queries(&self, _config: &Self::Config) -> bool {
        false
    }

    /// The pool's rows can hold protected values, which Postgres quotes in its
    /// diagnostics (a constraint violation's DETAIL is the whole row): core
    /// withholds the text of the server's errors and notices, keeping only
    /// their severity, SQLSTATE and the names of the objects involved. A
    /// session holding its server can't follow a reload that changes it.
    fn withholds_server_text(&self, _config: &Self::Config) -> bool {
        false
    }

    /// Whether a query the parser can't read (longer than
    /// `query_parser_max_length`, or syntax it doesn't know) may reach the
    /// server without the plugin's hooks when it doesn't name what the plugin
    /// gates. By default it can't: while the plugin is on, pgcat refuses
    /// every query on the pool that it couldn't parse (`0A000`), whether or
    /// not the pool sets `plugins_require_parse`, so every statement that
    /// reaches the server went through the plugin. `RESET <name>` and
    /// `RESET ALL` still reach it.
    ///
    /// A plugin that checks only some tables can return true to keep the
    /// table-scoped rule: with `plugins_require_parse`, an unparsed query is
    /// refused only if it may touch one of [`Self::gated_tables`], and others
    /// are sent unchecked; without it, every unparsed query is sent.
    fn allows_unparsed_outside_gated_tables(&self, _config: &Self::Config) -> bool {
        false
    }

    /// Names the plugin gates: the tables it checks statements on, and any
    /// other name a statement it can't check mustn't mention (a function that
    /// runs SQL given as text, say). They matter only to a plugin that
    /// [`Self::allows_unparsed_outside_gated_tables`]: with
    /// `plugins_require_parse`, a query the parser can't read is then refused
    /// if its text contains one of them, or has a Unicode escape (`U&`),
    /// instead of being sent unchecked. The check reads the text, not the
    /// statement: SQL that builds a name (`'secr' || 'ets'`) passes it.
    fn gated_tables<'c>(&self, _config: &'c Self::Config) -> Vec<&'c str> {
        Vec::new()
    }

    /// Whether the plugin checks another plugin's rewrite on the pool, with
    /// [`Self::check_rewrite`].
    fn checks_rewrites(&self, _config: &Self::Config) -> bool {
        false
    }

    /// Once all plugins ran, check the final rewrite (`Overwrite`) of a
    /// statement on a pool the plugin is on and `checks_rewrites`: another
    /// plugin's, never its own. The plugin planned the client's statements,
    /// not another plugin's rewrite of them, so this is where it refuses one
    /// that reaches what it covers. `sent` is the rewrite as the server will
    /// read it: its SQL, parsed again, unless the configured plugin that
    /// rewrote is [`Self::rewrite_checked_as_built`].
    ///
    /// An error refuses the statement with the error as the message: give it
    /// a SQLSTATE (`messages::with_sqlstate`). It must be safe to return to
    /// the client.
    fn check_rewrite(&self, _config: &Self::Config, _sent: &[Statement]) -> Result<(), String> {
        Ok(())
    }

    /// Whether the plugin's own rewrite is checked as the plugin built it, not
    /// parsed again from its SQL. Only for a plugin that builds its rewrite
    /// from the client's parsed statements and names it quotes itself, so its
    /// statements hold the tables their SQL names, and whose rewrite pgcat
    /// can't parse again (a `SELECT` of no columns prints, but doesn't parse).
    fn rewrite_checked_as_built(&self, _config: &Self::Config) -> bool {
        false
    }

    /// Whether the plugin verifies the token of `SET PGCAT_IDENTITY` on the
    /// pool. A pool on which more than one plugin does fails the config, as
    /// does one without `identity_tenant_parameter`.
    fn verifies_identity(&self, _config: &Self::Config) -> bool {
        false
    }

    /// Verify the token of `SET PGCAT_IDENTITY`, on a pool the plugin is on
    /// and `verifies_identity`. The result names the tenant pgcat sets
    /// `identity_tenant_parameter` to, when the identity expires, and what the
    /// plugin keeps from the token for its callbacks (`Identity::claims`).
    ///
    /// An error refuses the token and clears the client's identity. The client
    /// gets the message after `PGCAT_IDENTITY refused: `, so it must not hold
    /// the token, a secret or a tenant identifier. pgcat replaces a message
    /// that holds the token, or 20 or more characters in a row from
    /// `A-Z a-z 0-9 - _` (a slice of one), with a fixed one. pgcat logs the
    /// token's fingerprint, never the token; so must the plugin.
    ///
    /// Like the other hooks, it runs under the pool's `plugin_hook_timeout`
    /// and `general.plugin_hook_concurrency`. A verification that doesn't
    /// finish in time, or gets no slot, fails the `SET` with the hook's
    /// `PH001` or `PH002` instead of 28000, and also clears the identity.
    ///
    /// A reload that changes the pool's verifier, or its section, has pgcat
    /// verify a client's token again before its next statement. The client
    /// keeps its identity only if the result names the same tenant and its
    /// claims are equal (`==`), and then expires when this result says;
    /// otherwise its session ends.
    async fn verify_identity(
        &self,
        _config: &Self::Config,
        _token: &IdentityToken,
    ) -> Result<VerifiedIdentity, String> {
        Err(NO_VERIFIER.into())
    }

    /// Once, when pgcat starts: after the pools are created and before any
    /// client is accepted, on the server runtime, so the plugin can spawn the
    /// background tasks it owns. pgcat already listens: clients that connect
    /// meanwhile wait. `enabled` says whether a pool turns the plugin on, in
    /// its own section or the global one it inherits. A reload never starts a
    /// plugin: one that needs `start` to have run with `enabled` fails closed
    /// on a pool a reload turns it on for.
    ///
    /// An error stops pgcat (`exitcode::CONFIG`) once the plugins started
    /// before it are shut down. pgcat logs that the plugin failed to start,
    /// not the error: log the reason, without secrets, before returning it.
    /// So does a `start` that takes longer than
    /// [`START_TIMEOUT`](crate::plugins::lifecycle::START_TIMEOUT) (60 s): it's
    /// dropped, not shut down. Bound your own I/O well within it. The bound
    /// fires only at an await point: `start` must not block its thread (with
    /// synchronous I/O, `std::thread::sleep` or a blocking lock), or pgcat
    /// waits for it however long it takes. Put blocking work in
    /// `tokio::task::spawn_blocking`. Return an error rather than panic: a
    /// panic's message is printed, pgcat exits with 101, and the plugins
    /// started before aren't shut down.
    async fn start(&self, _enabled: bool) -> Result<(), Error> {
        Ok(())
    }

    /// Once, when pgcat stops, if `start` succeeded: stop and join the tasks
    /// it spawned. Plugins are shut down the last started first, and pgcat
    /// waits [`SHUTDOWN_TIMEOUT`](crate::plugins::lifecycle::SHUTDOWN_TIMEOUT)
    /// for each at most.
    ///
    /// Clients may still be connected: the plugin's hooks and
    /// `verify_identity` can run during and after `shutdown`, and must keep
    /// failing closed without what it stopped.
    async fn shutdown(&self) {}

    /// Lines for pgcat's Prometheus `/metrics` page, in the text exposition
    /// format, whether or not a pool turns the plugin on, and before `start`
    /// or after `shutdown` too. Called on each scrape, on a runtime worker:
    /// read cached state, don't block. Name the metrics `pgcat_<plugin>_...`;
    /// pgcat drops, with a warning naming the plugin, each line that isn't
    /// one canonical text-format line of such a metric. Their labels and
    /// values must never carry tenants, tokens, keys or protected values.
    fn prometheus_lines(&self, _lines: &mut Vec<String>) {}
}

/// `verify_identity` of a plugin that doesn't verify identities.
const NO_VERIFIER: &str = "the plugin doesn't verify identities";

type ParsedConfig = Arc<dyn Any + Send + Sync>;

/// A section as one of its plugin's callbacks takes it.
pub(crate) type AnyConfig = dyn Any + Send + Sync;

/// A `ConfiguredPlugin` with its configuration type erased, so the registry
/// can keep plugins with different types. Every call checks the type.
#[async_trait]
pub(crate) trait DynConfigured: Send + Sync {
    fn parse(&self, section: &toml::Value) -> Result<ParsedConfig, String>;
    /// Whether `config` is this plugin's type: parsed by it, or by a plugin of
    /// the same type.
    fn accepts(&self, config: &AnyConfig) -> bool;
    fn enabled(&self, config: &AnyConfig) -> bool;
    fn validate(&self, config: &AnyConfig, pool: &Pool) -> Result<(), String>;
    async fn run(
        &self,
        config: &AnyConfig,
        context: &PluginContext<'_>,
        statements: &[Statement],
        parse: bool,
    ) -> Result<PluginResult, Error>;
    fn sensitive_queries(&self, config: &AnyConfig) -> bool;
    fn withholds_server_text(&self, config: &AnyConfig) -> bool;
    fn allows_unparsed_outside_gated_tables(&self, config: &AnyConfig) -> bool;
    fn gated_tables<'c>(&self, config: &'c AnyConfig) -> Vec<&'c str>;
    fn checks_rewrites(&self, config: &AnyConfig) -> bool;
    fn check_rewrite(&self, config: &AnyConfig, sent: &[Statement]) -> Result<(), String>;
    fn rewrite_checked_as_built(&self, config: &AnyConfig) -> bool;
    fn verifies_identity(&self, config: &AnyConfig) -> bool;
    async fn verify_identity(
        &self,
        config: &AnyConfig,
        token: &IdentityToken,
    ) -> Result<VerifiedIdentity, String>;
    async fn start(&self, enabled: bool) -> Result<(), Error>;
    async fn shutdown(&self);
    fn prometheus_lines(&self, lines: &mut Vec<String>);
}

pub(crate) struct Typed<P>(pub(crate) P);

#[async_trait]
impl<P: ConfiguredPlugin> DynConfigured for Typed<P> {
    fn parse(&self, section: &toml::Value) -> Result<ParsedConfig, String> {
        let config: P::Config = section.clone().try_into().map_err(|err| err.to_string())?;
        Ok(Arc::new(config))
    }

    fn accepts(&self, config: &AnyConfig) -> bool {
        config.is::<P::Config>()
    }

    fn enabled(&self, config: &AnyConfig) -> bool {
        // A config of another type is never treated as off.
        config
            .downcast_ref::<P::Config>()
            .map_or(true, |config| self.0.enabled(config))
    }

    fn validate(&self, config: &AnyConfig, pool: &Pool) -> Result<(), String> {
        let config = config
            .downcast_ref::<P::Config>()
            .ok_or("a plugin section was parsed by another plugin")?;
        self.0.validate(config, pool)
    }

    async fn run(
        &self,
        config: &AnyConfig,
        context: &PluginContext<'_>,
        statements: &[Statement],
        parse: bool,
    ) -> Result<PluginResult, Error> {
        let config = config
            .downcast_ref::<P::Config>()
            .ok_or_else(|| Error::PluginError(not_loaded()))?;
        match parse {
            true => self.0.on_parse(config, context, statements).await,
            false => self.0.on_query(config, context, statements).await,
        }
    }

    fn sensitive_queries(&self, config: &AnyConfig) -> bool {
        config
            .downcast_ref::<P::Config>()
            .map_or(true, |config| self.0.sensitive_queries(config))
    }

    fn withholds_server_text(&self, config: &AnyConfig) -> bool {
        config
            .downcast_ref::<P::Config>()
            .map_or(true, |config| self.0.withholds_server_text(config))
    }

    fn allows_unparsed_outside_gated_tables(&self, config: &AnyConfig) -> bool {
        // A config of another type has every unparsed query refused.
        config.downcast_ref::<P::Config>().map_or(false, |config| {
            self.0.allows_unparsed_outside_gated_tables(config)
        })
    }

    fn gated_tables<'c>(&self, config: &'c AnyConfig) -> Vec<&'c str> {
        config
            .downcast_ref::<P::Config>()
            .map_or_else(Vec::new, |config| self.0.gated_tables(config))
    }

    fn checks_rewrites(&self, config: &AnyConfig) -> bool {
        // Fail closed: a config of another type checks, and refuses.
        config
            .downcast_ref::<P::Config>()
            .map_or(true, |config| self.0.checks_rewrites(config))
    }

    fn check_rewrite(&self, config: &AnyConfig, sent: &[Statement]) -> Result<(), String> {
        let config = config.downcast_ref::<P::Config>().ok_or_else(not_loaded)?;
        self.0.check_rewrite(config, sent)
    }

    fn rewrite_checked_as_built(&self, config: &AnyConfig) -> bool {
        // Fail closed: a config of another type has its rewrite parsed again.
        config
            .downcast_ref::<P::Config>()
            .map_or(false, |config| self.0.rewrite_checked_as_built(config))
    }

    fn verifies_identity(&self, config: &AnyConfig) -> bool {
        // Unlike gated_tables, fail closed: a config of another type counts as
        // a verifier, so the pool fails validation or the token is refused.
        config
            .downcast_ref::<P::Config>()
            .map_or(true, |config| self.0.verifies_identity(config))
    }

    async fn verify_identity(
        &self,
        config: &AnyConfig,
        token: &IdentityToken,
    ) -> Result<VerifiedIdentity, String> {
        let config = config.downcast_ref::<P::Config>().ok_or(NOT_LOADED)?;
        self.0.verify_identity(config, token).await
    }

    async fn start(&self, enabled: bool) -> Result<(), Error> {
        self.0.start(enabled).await
    }

    async fn shutdown(&self) {
        self.0.shutdown().await
    }

    fn prometheus_lines(&self, lines: &mut Vec<String>) {
        self.0.prometheus_lines(lines)
    }
}

/// The refusal of a statement on a pool with a section its plugin hasn't parsed.
pub(crate) fn not_loaded() -> String {
    crate::messages::with_sqlstate(crate::plugins::PLUGIN_FAILED, NOT_LOADED)
}

/// Why a statement or identity is refused on a pool with a section its plugin
/// hasn't parsed.
pub(crate) const NOT_LOADED: &str = "a plugin's configuration isn't loaded";

/// A `[plugins.<name>]` section, as written, and once the config is validated,
/// as its plugin parsed it. Equality and hashing use the section as written.
/// `Debug` never shows its values.
#[derive(Clone)]
pub struct PluginSection {
    raw: toml::Value,
    parsed: Option<Parsed>,
}

#[derive(Clone)]
struct Parsed {
    config: ParsedConfig,
    enabled: bool,
}

impl PluginSection {
    /// A section that no plugin parsed yet: it's parsed when its config is
    /// validated. Until then, a pool with it refuses the statements it parses.
    pub fn new(raw: toml::Value) -> PluginSection {
        PluginSection { raw, parsed: None }
    }

    /// The section as written.
    pub fn raw(&self) -> &toml::Value {
        &self.raw
    }

    /// The section as its plugin parsed it, if it did.
    pub(crate) fn parsed(&self) -> Option<&AnyConfig> {
        self.parsed.as_ref().map(|parsed| parsed.config.as_ref())
    }
}

impl std::fmt::Debug for PluginSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginSection")
            .field("parsed", &self.parsed.is_some())
            .finish_non_exhaustive()
    }
}

/// Whether the plugin is on, as of the last validation: for logging.
impl std::fmt::Display for PluginSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.parsed {
            Some(ref parsed) => write!(f, "{}", parsed.enabled),
            None => write!(f, "not loaded"),
        }
    }
}

impl PartialEq for PluginSection {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl Eq for PluginSection {}

impl Hash for PluginSection {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_value(&self.raw, state);
    }
}

/// Hash a TOML value consistently with its equality: tables by sorted key,
/// whatever map the toml crate uses, and both zeros alike.
fn hash_value<H: Hasher>(value: &toml::Value, state: &mut H) {
    use toml::Value;

    std::mem::discriminant(value).hash(state);
    match value {
        Value::String(value) => value.hash(state),
        Value::Integer(value) => value.hash(state),
        Value::Float(value) => {
            let value = if *value == 0.0 { 0.0 } else { *value };
            value.to_bits().hash(state)
        }
        Value::Boolean(value) => value.hash(state),
        Value::Datetime(value) => value.to_string().hash(state),
        Value::Array(values) => {
            values.len().hash(state);
            for value in values {
                hash_value(value, state);
            }
        }
        Value::Table(table) => {
            table.len().hash(state);
            let mut entries: Vec<_> = table.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            for (key, value) in entries {
                key.hash(state);
                hash_value(value, state);
            }
        }
    }
}

impl Serialize for PluginSection {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.raw.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PluginSection {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        toml::Value::deserialize(deserializer).map(PluginSection::new)
    }
}

/// A section of a pool's plugins, with the plugin that runs it.
pub(crate) enum Section<'a> {
    /// Its plugin parsed it, and it turns the plugin on.
    On(&'a dyn DynConfigured, &'a AnyConfig),
    /// Its plugin parsed it, and it leaves the plugin off.
    Off,
    /// No plugin of this registry parsed it: the pool fails closed.
    NotLoaded,
}

impl<'a> Section<'a> {
    pub(crate) fn resolve(
        plugin: Option<&'a dyn DynConfigured>,
        section: &'a PluginSection,
    ) -> Self {
        match (plugin, section.parsed()) {
            (Some(plugin), Some(config)) if plugin.accepts(config) => {
                match plugin.enabled(config) {
                    true => Section::On(plugin, config),
                    false => Section::Off,
                }
            }
            _ => Section::NotLoaded,
        }
    }
}

impl PluginRegistry {
    /// Parse each `[plugins.<name>]` section with the plugin registered under
    /// its name. A section no plugin is registered for fails the config: a
    /// binary without the plugin must not run the pool as if it were off.
    pub(crate) fn load_sections(&self, plugins: &mut Plugins) -> Result<(), Error> {
        for (name, section) in plugins.configured.iter_mut() {
            let plugin = match self.configured_plugin(name) {
                Some(plugin) => plugin,
                None => {
                    error!("plugins.{:?}: no plugin of that name is registered", name);
                    return Err(Error::BadConfig);
                }
            };
            let config = match plugin.parse(&section.raw) {
                Ok(config) => config,
                Err(err) => {
                    error!("plugins.{:?}: {}", name, err);
                    return Err(Error::BadConfig);
                }
            };
            section.parsed = Some(Parsed {
                enabled: plugin.enabled(config.as_ref()),
                config,
            });
        }
        Ok(())
    }

    /// Check the sections `pool` runs, its own or the global ones it inherits,
    /// with their plugins.
    pub(crate) fn validate_sections(&self, plugins: &Plugins, pool: &Pool) -> Result<(), Error> {
        for (name, section) in &plugins.configured {
            let plugin = self.configured_plugin(name);
            let (plugin, config) = match (plugin, section.parsed()) {
                (Some(plugin), Some(config)) if plugin.accepts(config) => (plugin, config),
                _ => {
                    error!("plugins.{:?}: the section isn't loaded", name);
                    return Err(Error::BadConfig);
                }
            };
            // Pool plugins only run with the query parser: a pool inheriting
            // the section without it would run as if the plugin were off.
            if plugin.enabled(config) && !pool.query_parser_enabled {
                error!(
                    "plugins.{:?} is enabled, but plugins only run on pools with \
                     query_parser_enabled = true",
                    name
                );
                return Err(Error::BadConfig);
            }
            if let Err(err) = plugin.validate(config, pool) {
                error!("plugins.{:?}: {}", name, err);
                return Err(Error::BadConfig);
            }
        }

        // One plugin at most decides who a pool's clients are.
        let verifiers = identity_verifiers(self, plugins);
        if verifiers.len() > 1 {
            error!(
                "plugins {:?} all verify identities on the pool: at most one may",
                verifiers
            );
            return Err(Error::BadConfig);
        }

        // The tenant the verifier names goes in the parameter, and its guard
        // keeps clients from setting it, or from changing how the server reads
        // the statements pgcat checks for identity commands.
        if let (Some(verifier), None) = (verifiers.first(), &pool.identity_tenant_parameter) {
            error!(
                "plugins.{:?} verifies identities, so the pool needs identity_tenant_parameter",
                verifier
            );
            return Err(Error::BadConfig);
        }
        Ok(())
    }
}

/// The enabled plugins in `plugins` that verify identity tokens.
fn identity_verifiers<'p>(registry: &PluginRegistry, plugins: &'p Plugins) -> Vec<&'p str> {
    let mut verifiers = Vec::new();
    #[cfg(any(test, feature = "test-plugins"))]
    if plugins
        .testing
        .as_ref()
        .is_some_and(|testing| testing.enabled && testing.verify_identity)
    {
        verifiers.push("testing");
    }
    for (name, section) in &plugins.configured {
        if let Section::On(plugin, config) =
            Section::resolve(registry.configured_plugin(name), section)
        {
            if plugin.verifies_identity(config) {
                verifiers.push(name.as_str());
            }
        }
    }
    verifiers
}
