//! This example is a separate crate: only pgcat's public library API is visible.
//! It registers a plugin each pool turns on, and configures, in its own
//! `[plugins.synthetic]` section.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use log::{error, info};
use parking_lot::Mutex;
use pgcat::{
    config::Pool,
    errors::Error,
    identity::{IdentityToken, VerifiedIdentity},
    messages::{
        command_complete, data_row, ready_for_query, row_description, with_sqlstate, DataType,
    },
    plugins::{
        registry::{ConfiguredPlugin, PluginRegistry},
        PluginContext, PluginOutput, PluginResult, PLUGIN_DENIED,
    },
};
use serde::Deserialize;
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};
use tokio::{sync::oneshot, task::JoinHandle};

/// With this set, `start` fails with its value as the error, which pgcat
/// must not log.
const START_FAILURE: &str = "PGCAT_SYNTHETIC_START_FAILURE";

/// `[plugins.synthetic]`. PgCat doesn't know these fields: the plugin parses
/// its section as written.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default)]
    enabled: bool,
    /// `SELECT 8001` is rewritten to `SELECT <answer>`; `SELECT 8009` is
    /// answered with it.
    answer: i64,
    /// Queries naming these tables must parse (`plugins_require_parse`).
    #[serde(default)]
    tables: Vec<String>,
    /// Only queries naming `tables` must parse: other unparsed ones reach the
    /// server without the plugin. Otherwise every one is refused.
    #[serde(default)]
    table_scoped: bool,
    /// The plugin's own setting, never logged.
    #[serde(default)]
    #[allow(dead_code)]
    note: String,
    /// Refuse a pool that doesn't require statements to parse.
    #[serde(default)]
    needs_parse: bool,
    /// The pool's queries hold values that must not be logged.
    #[serde(default)]
    sensitive: bool,
    /// Verify `SET PGCAT_IDENTITY` tokens: `synthetic:<tenant>`.
    #[serde(default)]
    verify_identity: bool,
    /// Kept in the claims of the identities the plugin verifies.
    #[serde(default)]
    claim: String,
    /// Never answer a verification: pgcat fails it at plugin_hook_timeout.
    #[serde(default)]
    hang: bool,
    /// The pool's rows hold protected values: pgcat withholds the text of
    /// the server's errors and notices.
    #[serde(default)]
    withhold_server_text: bool,
}

/// What the plugin keeps from a token it verified.
#[derive(PartialEq)]
struct Claims {
    tenant: String,
    claim: String,
}

/// The background task `start` spawns, and how to stop it.
struct Task {
    stop: oneshot::Sender<()>,
    handle: JoinHandle<()>,
}

#[derive(Default)]
struct Synthetic {
    task: Mutex<Option<Task>>,
}

/// The tokens the plugin was asked to verify, for `SELECT 8004`.
static VERIFICATIONS: AtomicUsize = AtomicUsize::new(0);

#[async_trait]
impl ConfiguredPlugin for Synthetic {
    type Config = Config;

    fn enabled(&self, config: &Config) -> bool {
        config.enabled
    }

    fn validate(&self, config: &Config, pool: &Pool) -> Result<(), String> {
        if config.needs_parse && !pool.plugins_require_parse {
            return Err("needs plugins_require_parse".into());
        }
        Ok(())
    }

    async fn on_query(
        &self,
        config: &Config,
        context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        let [statement] = statements else {
            return Ok(PluginOutput::Allow.into());
        };
        match statement.to_string().as_str() {
            // Answered by the plugin itself, with `<answer>`.
            "SELECT 8009" => {
                let mut response = row_description(&vec![("answer", DataType::Int4)]);
                response.extend_from_slice(&data_row(&vec![config.answer.to_string()]));
                response.extend_from_slice(&command_complete("SELECT 1"));
                response.extend_from_slice(&ready_for_query(false));
                Ok(PluginOutput::Intercept(response).into())
            }
            "SELECT 8001" => {
                let rewritten =
                    Parser::parse_sql(&PostgreSqlDialect {}, &format!("SELECT {}", config.answer))
                        .map_err(|_| Error::PluginError("invalid synthetic rewrite".into()))?;
                Ok(PluginOutput::Overwrite(rewritten).into())
            }
            "SELECT 8002" => Ok(PluginOutput::Deny(with_sqlstate(
                PLUGIN_DENIED,
                "synthetic configured plugin refused statement",
            ))
            .into()),
            // The tenant of the identity the plugin verified, from its claims.
            "SELECT 8003" => {
                let tenant = context
                    .identity()
                    .and_then(|identity| identity.claims::<Claims>())
                    .map_or("none", |claims| claims.tenant.as_str());
                let rewritten =
                    Parser::parse_sql(&PostgreSqlDialect {}, &format!("SELECT '{}'", tenant))
                        .map_err(|_| Error::PluginError("invalid synthetic rewrite".into()))?;
                Ok(PluginOutput::Overwrite(rewritten).into())
            }
            "SELECT 8004" => {
                let count = VERIFICATIONS.load(Ordering::Relaxed);
                let rewritten =
                    Parser::parse_sql(&PostgreSqlDialect {}, &format!("SELECT {count}"))
                        .map_err(|_| Error::PluginError("invalid synthetic rewrite".into()))?;
                Ok(PluginOutput::Overwrite(rewritten).into())
            }
            _ => Ok(PluginOutput::Allow.into()),
        }
    }

    fn sensitive_queries(&self, config: &Config) -> bool {
        config.sensitive
    }

    fn allows_unparsed_outside_gated_tables(&self, config: &Config) -> bool {
        config.table_scoped
    }

    fn gated_tables<'c>(&self, config: &'c Config) -> Vec<&'c str> {
        config.tables.iter().map(String::as_str).collect()
    }

    fn withholds_server_text(&self, config: &Config) -> bool {
        config.withhold_server_text
    }

    fn verifies_identity(&self, config: &Config) -> bool {
        config.verify_identity
    }

    async fn verify_identity(
        &self,
        config: &Config,
        token: &IdentityToken,
    ) -> Result<VerifiedIdentity, String> {
        VERIFICATIONS.fetch_add(1, Ordering::Relaxed);
        // Quotes the token, which a plugin mustn't: pgcat replaces the message.
        if token.as_str().starts_with("synthetic-echo") {
            return Err(format!("can't verify {}", token.as_str()));
        }
        // Never answers: pgcat refuses the token at plugin_hook_timeout.
        if token.as_str() == "synthetic-hangs" || config.hang {
            std::future::pending::<()>().await;
        }
        // Letters only, so the tenant can be put in SQL above.
        let tenant = token
            .as_str()
            .strip_prefix("synthetic:")
            .filter(|tenant| !tenant.is_empty() && tenant.bytes().all(|b| b.is_ascii_lowercase()))
            .ok_or("synthetic plugin refused the identity")?;
        Ok(VerifiedIdentity {
            tenant: Some(tenant.to_string()),
            expires_at: None,
            claims: Some(Arc::new(Claims {
                tenant: tenant.to_string(),
                claim: config.claim.clone(),
            })),
        })
    }

    async fn start(&self, enabled: bool) -> Result<(), Error> {
        if let Ok(reason) = std::env::var(START_FAILURE) {
            error!("synthetic plugin can't start");
            return Err(Error::PluginError(reason));
        }
        let (stop, stopped) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let _ = stopped.await;
        });
        *self.task.lock() = Some(Task { stop, handle });
        info!("synthetic plugin started, enabled: {}", enabled);
        Ok(())
    }

    async fn shutdown(&self) {
        let task = self.task.lock().take();
        if let Some(Task { stop, handle }) = task {
            let _ = stop.send(());
            let joined = handle.await.is_ok();
            info!("synthetic plugin stopped its task, joined: {}", joined);
        }
    }

    fn prometheus_lines(&self, lines: &mut Vec<String>) {
        let running = self
            .task
            .lock()
            .as_ref()
            .map_or(false, |task| !task.handle.is_finished());
        lines.push(
            "# HELP pgcat_synthetic_task_running Whether the synthetic plugin's task runs".into(),
        );
        lines.push("# TYPE pgcat_synthetic_task_running gauge".into());
        lines.push(format!(
            "pgcat_synthetic_task_running {}",
            u8::from(running)
        ));
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut plugins = PluginRegistry::default();
    plugins
        .register_configured("synthetic", Synthetic::default())
        .map_err(|_| "invalid synthetic plugin registration")?;
    pgcat::runtime::run_with_plugins(plugins)
}
