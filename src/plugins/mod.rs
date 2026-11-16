//! The plugin ecosystem.
//!
//! Plugins see every parsed query before it is sent to the server and can
//! allow it, deny it, answer it themselves, or rewrite it.
//! Example use cases:
//!   - block known bad queries
//!   - block access to system catalogs
//!   - block dangerous modifications like `DROP TABLE`
//!   - rewrite queries before they reach the server
//!   - etc
//!

pub mod diagnostics;
mod execute_context;
pub(crate) mod hook_errors;
pub mod intercept;
pub mod lifecycle;
pub mod prewarmer;
pub mod query_logger;
pub mod registry;
pub mod response;
mod server_query;
pub mod statement_plans;
pub mod table_access;
#[cfg(any(test, feature = "test-plugins"))]
pub mod testing;

use crate::{
    errors::Error,
    identity::Identity,
    messages::{or_sqlstate, with_sqlstate},
    pool::PoolSettings,
    query_router::QueryRouter,
};
use async_trait::async_trait;
use bytes::BytesMut;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use sqlparser::ast::{visit_expressions, Expr, Statement, Value, ValueWithSpan};
use std::any::Any;
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

pub use crate::messages::{BindParams, DataRow, FieldDescription, RowDescription};
pub use execute_context::{BatchState, BindPosition, ExecuteContext};
pub use intercept::Intercept;
pub use query_logger::QueryLogger;
pub use table_access::TableAccess;

/// The SQLSTATEs statements pgcat refuses get when the plugin gives none
/// (`messages::with_sqlstate`). A plugin refused it: insufficient_privilege.
pub const PLUGIN_DENIED: &str = "42501";
/// A plugin failed: internal_error.
pub const PLUGIN_FAILED: &str = "XX000";
/// pgcat can't handle the statement: feature_not_supported.
pub const UNSUPPORTED: &str = "0A000";
/// The client's messages don't fit together: protocol_violation.
pub const PROTOCOL_VIOLATION: &str = "08P01";
/// Too many parameters, or another limit: program_limit_exceeded.
pub const LIMIT_EXCEEDED: &str = "54000";
/// `SET PGCAT_IDENTITY` with a token pgcat refused:
/// invalid_authorization_specification.
pub const IDENTITY_REFUSED: &str = "28000";

/// Most parameters a statement can have: Postgres counts them in 16 bits.
pub const MAX_PARAMS: usize = u16::MAX as usize;
/// A plugin hook didn't finish within its pool's `plugin_hook_timeout`.
pub const HOOK_TIMED_OUT: &str = "PH001";
/// Too many plugin hooks were running for one more to start in time.
pub const HOOK_BUSY: &str = "PH002";

/// Slots for the plugin hooks running at once (`general.plugin_hook_concurrency`).
pub(crate) static HOOK_SLOTS: Lazy<Semaphore> = Lazy::new(|| {
    let slots = crate::config::get_config().general.plugin_hook_concurrency;
    Semaphore::new(slots.clamp(1, Semaphore::MAX_PERMITS))
});

/// Run a plugin hook within `timeout`, holding one of the process's hook slots
/// while it runs. A hook that can't start or finish in time fails its statement
/// instead of holding the client's connection.
pub async fn run_hook<T>(
    timeout: Duration,
    hook: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    run_hook_with(&HOOK_SLOTS, timeout, hook).await
}

pub(crate) async fn run_hook_with<T>(
    slots: &Semaphore,
    timeout: Duration,
    hook: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    let deadline = tokio::time::Instant::now() + timeout;
    let _slot = match tokio::time::timeout_at(deadline, slots.acquire()).await {
        Ok(Ok(slot)) => slot,
        _ => {
            return Err(Error::PluginError(with_sqlstate(
                HOOK_BUSY,
                "too many plugin hooks are running",
            )))
        }
    };
    match tokio::time::timeout_at(deadline, hook).await {
        Ok(result) => result,
        Err(_) => Err(Error::PluginError(with_sqlstate(
            HOOK_TIMED_OUT,
            "a plugin hook took too long",
        ))),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PluginOutput {
    /// Send the query as-is.
    Allow,
    /// Reject the query with this error message.
    Deny(String),
    /// Send these statements instead of the original query.
    Overwrite(Vec<Statement>),
    /// Answer the query with these bytes (RowDescription, DataRows,
    /// CommandComplete, ReadyForQuery) without talking to the server.
    Intercept(BytesMut),
}

/// Serialize rewritten statements back into a query string.
pub fn statements_to_sql(statements: &[Statement]) -> String {
    statements
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<String>>()
        .join("; ")
}

/// What a plugin decided about a query.
#[derive(Debug)]
pub struct PluginResult {
    pub output: PluginOutput,

    /// State kept for the Binds and results of this statement.
    pub plan: Option<StatementPlan>,
}

impl From<PluginOutput> for PluginResult {
    fn from(output: PluginOutput) -> PluginResult {
        PluginResult { output, plan: None }
    }
}

/// Context passed to plugins when a query is parsed.
pub struct PluginContext<'a> {
    pub query_router: &'a QueryRouter,

    /// Parameter types declared by the client's Parse, if this is a Parse.
    pub declared_param_types: &'a [i32],

    /// Caller identity set with `SET PGCAT_IDENTITY`.
    pub identity: Option<&'a Identity>,
}

impl<'a> PluginContext<'a> {
    pub fn pool_settings(&self) -> &PoolSettings {
        self.query_router.pool_settings()
    }

    /// Caller identity set with `SET PGCAT_IDENTITY`. Never log its token.
    pub fn identity(&self) -> Option<&Identity> {
        self.identity
    }

    /// Statements a plugin covers must fail without an identity.
    pub fn identity_required(&self) -> bool {
        self.pool_settings().identity_required_for_plugins
    }

    /// The plugin configuration this statement is planned with: it changes
    /// when a reload changes a section of a plugin that's on.
    pub fn plan_generation(&self) -> u64 {
        self.query_router.plan_generation()
    }

    /// Number of parameters the client will send for this statement:
    /// the highest `$n` used, or more if the client declared more types.
    /// Never more than `MAX_PARAMS + 1`: pgcat refuses such statements.
    pub fn client_param_count(&self, ast: &[Statement]) -> usize {
        max_placeholder(ast)
            .max(self.declared_param_types.len())
            .min(MAX_PARAMS + 1)
    }
}

#[async_trait]
pub trait Plugin {
    // Run before the query is sent to the server.
    #[allow(clippy::ptr_arg)]
    async fn run(
        &mut self,
        query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error>;

    /// Run on a Parse (extended protocol) instead of `run`. A plugin that wants
    /// to see the Binds or results of this statement returns a plan with its result.
    #[allow(clippy::ptr_arg)]
    async fn on_parse(
        &mut self,
        ctx: &PluginContext<'_>,
        ast: &Vec<Statement>,
    ) -> Result<PluginResult, Error> {
        Ok(self.run(ctx.query_router, ast).await?.into())
    }

    /// Run on a simple Query instead of `run`. A plugin that wants to see the
    /// results returns a plan; it applies to every result set of the query.
    #[allow(clippy::ptr_arg)]
    async fn on_query(
        &mut self,
        ctx: &PluginContext<'_>,
        ast: &Vec<Statement>,
    ) -> Result<PluginResult, Error> {
        Ok(self.run(ctx.query_router, ast).await?.into())
    }
}

/// Per-statement state a plugin keeps from Parse to the Binds and results that
/// use it. Stored with the client's prepared statement, keyed by its name
/// (including the unnamed statement).
pub struct StatementPlan {
    /// Called back for each Bind and result of this statement.
    pub hooks: Arc<dyn StatementHooks>,

    /// Plugin state, opaque to pgcat.
    pub state: Box<dyn Any + Send + Sync>,

    /// Number of parameters the client sends.
    pub client_params: usize,

    /// Types of the parameters the rewritten query adds after the client's,
    /// i.e. `$client_params+1...`. The hook fills them in at Bind.
    pub appended_param_types: Vec<i32>,

    /// Call the response hooks for this statement's results. Responses are then
    /// reassembled into whole messages instead of being forwarded as they arrive.
    pub wants_response: bool,

    /// The statement only reads (for a simple query: it's one statement that only
    /// reads). If a response hook fails, the client gets an error, so a statement
    /// that might write, or a batch of several, runs in a transaction that pgcat
    /// rolls back then. A lone read-only one doesn't need it.
    pub read_only: bool,

    /// Last RowDescription the server sent for this statement, before the hooks
    /// changed it. Lets hooks see column types for Executes that weren't described.
    pub(crate) described: Mutex<Option<RowDescription>>,

    /// The plugin that planned the statement, which the plugin chain stamps:
    /// its hooks' errors are counted for it.
    pub(crate) plugin: &'static str,
}

impl StatementPlan {
    pub fn new(hooks: Arc<dyn StatementHooks>, state: Box<dyn Any + Send + Sync>) -> Self {
        StatementPlan {
            hooks,
            state,
            client_params: 0,
            appended_param_types: Vec::new(),
            wants_response: false,
            read_only: false,
            described: Mutex::new(None),
            plugin: hook_errors::NO_PLUGIN,
        }
    }

    /// The message a client gets when this plan's `hook` fails with `err`,
    /// counted for the plugin that planned it.
    pub(crate) fn hook_failed(&self, hook: hook_errors::Hook, err: &Error) -> String {
        let message = or_sqlstate(PLUGIN_FAILED, &err.to_string());
        hook_errors::count(self.plugin, hook, &message);
        message
    }

    pub fn with_client_params(mut self, client_params: usize) -> Self {
        self.client_params = client_params;
        self
    }

    pub fn with_appended_params(mut self, types: Vec<i32>) -> Self {
        self.appended_param_types = types;
        self
    }

    pub fn with_response_hooks(mut self) -> Self {
        self.wants_response = true;
        self
    }

    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    pub fn state<T: 'static>(&self) -> Option<&T> {
        self.state.downcast_ref()
    }

    /// The server's responses for this statement need rewriting.
    pub fn rewrites_response(&self) -> bool {
        self.wants_response || !self.appended_param_types.is_empty()
    }
}

impl std::fmt::Debug for StatementPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementPlan")
            .field("client_params", &self.client_params)
            .field("appended_param_types", &self.appended_param_types)
            .field("wants_response", &self.wants_response)
            .field("read_only", &self.read_only)
            .finish()
    }
}

/// What the response hooks know about the rows they get.
pub struct RowsContext<'a> {
    /// Format of each column of the rows as sent by the server (0 text, 1 binary).
    pub formats: &'a [i16],

    /// The server's description of the rows, before `on_row_description` changed it,
    /// if the statement or portal was described.
    pub description: Option<&'a RowDescription>,
}

/// Hooks called for statements a plugin planned.
#[async_trait]
pub trait StatementHooks: Send + Sync {
    /// Look at a Bind before pgcat checks a server connection out for its
    /// batch, e.g. to refuse it for a key that isn't at hand without holding a
    /// connection. There's no server in `ctx`. Runs in message order; an error
    /// rejects the whole batch. Inside a transaction the client holds a server
    /// already, and only `on_bind` runs.
    async fn before_checkout(
        &self,
        _ctx: &mut ExecuteContext<'_>,
        _plan: &StatementPlan,
        _params: &BindParams,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Read, replace or append the parameters of a Bind. Runs when the batch is
    /// sent to the server, in message order. An error rejects the whole batch.
    async fn on_bind(
        &self,
        _ctx: &mut ExecuteContext<'_>,
        _plan: &StatementPlan,
        _params: &mut BindParams,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Rewrite a simple Query right before it's sent, when a server connection is
    /// held. Returns the SQL to send instead, if any. An error fails the query.
    async fn on_execute_query(
        &self,
        _ctx: &mut ExecuteContext<'_>,
        _plan: &StatementPlan,
    ) -> Result<Option<String>, Error> {
        Ok(None)
    }

    /// Rewrite the parameter types of a statement Describe. By default the
    /// parameters the plugin appended are hidden from the client.
    fn on_param_description(
        &self,
        plan: &StatementPlan,
        param_types: &mut Vec<i32>,
    ) -> Result<(), Error> {
        param_types.truncate(plan.client_params);
        Ok(())
    }

    /// Rewrite a RowDescription, e.g. drop or reorder fields. Only called if the
    /// plan wants response hooks.
    fn on_row_description(
        &self,
        _plan: &StatementPlan,
        _description: &mut RowDescription,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Rewrite a batch of rows. Called with up to `plugin_response_batch_rows` rows
    /// (or `plugin_response_batch_bytes`), and with what's left at the end of the
    /// result. An error replaces the rest of the result with an ErrorResponse.
    async fn on_rows(
        &self,
        _ctx: &mut ExecuteContext<'_>,
        _plan: &StatementPlan,
        _rows_ctx: &RowsContext<'_>,
        rows: Vec<DataRow>,
    ) -> Result<Vec<DataRow>, Error> {
        Ok(rows)
    }
}

/// Highest `$n` placeholder used in the statements, 0 if there are none.
pub fn max_placeholder(ast: &[Statement]) -> usize {
    let mut max = 0;

    for statement in ast {
        let _ = visit_expressions(statement, |expr| {
            if let Expr::Value(ValueWithSpan {
                value: Value::Placeholder(placeholder),
                ..
            }) = expr
            {
                if let Some(n) = placeholder
                    .strip_prefix('$')
                    .map(|n| n.parse::<usize>().unwrap_or(usize::MAX))
                {
                    max = max.max(n);
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }

    max
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_hooks_run_in_time_and_in_slots() {
        let slots = Semaphore::new(1);
        let quick = Duration::from_millis(50);

        assert_eq!(run_hook_with(&slots, quick, async { Ok(1) }).await, Ok(1));

        let slow = run_hook_with(&slots, quick, async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(1)
        });
        assert_eq!(
            slow.await,
            Err(Error::PluginError(with_sqlstate(
                HOOK_TIMED_OUT,
                "a plugin hook took too long"
            )))
        );

        // Every slot taken: the next hook fails once its time is up.
        let _taken = slots.acquire().await.unwrap();
        assert_eq!(
            run_hook_with(&slots, quick, async { Ok(1) }).await,
            Err(Error::PluginError(with_sqlstate(
                HOOK_BUSY,
                "too many plugin hooks are running"
            )))
        );
    }
    use bytes::BytesMut;

    #[test]
    fn test_max_placeholder() {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        let parse = |sql| Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();

        assert_eq!(max_placeholder(&parse("SELECT 1")), 0);
        assert_eq!(
            max_placeholder(&parse("SELECT * FROM t WHERE a = $2 AND b IN ($10, $1)")),
            10
        );
        assert_eq!(
            max_placeholder(&parse("INSERT INTO t (a) VALUES ($1), ($3)")),
            3
        );
    }

    // ==================== PluginOutput Tests ====================

    #[test]
    fn test_plugin_output_allow() {
        let output = PluginOutput::Allow;
        assert_eq!(output, PluginOutput::Allow);
    }

    #[test]
    fn test_plugin_output_deny() {
        let output = PluginOutput::Deny("access denied".to_string());
        match output {
            PluginOutput::Deny(msg) => assert_eq!(msg, "access denied"),
            _ => panic!("Expected Deny variant"),
        }
    }

    #[test]
    fn test_plugin_output_overwrite() {
        // Empty statement vector
        let output = PluginOutput::Overwrite(vec![]);
        match output {
            PluginOutput::Overwrite(stmts) => assert!(stmts.is_empty()),
            _ => panic!("Expected Overwrite variant"),
        }
    }

    #[test]
    fn test_plugin_output_intercept() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"test data");
        let output = PluginOutput::Intercept(buf.clone());
        match output {
            PluginOutput::Intercept(data) => assert_eq!(data, buf),
            _ => panic!("Expected Intercept variant"),
        }
    }

    #[test]
    fn test_plugin_output_equality() {
        assert_eq!(PluginOutput::Allow, PluginOutput::Allow);
        assert_ne!(PluginOutput::Allow, PluginOutput::Deny("test".to_string()));

        let deny1 = PluginOutput::Deny("same".to_string());
        let deny2 = PluginOutput::Deny("same".to_string());
        let deny3 = PluginOutput::Deny("different".to_string());
        assert_eq!(deny1, deny2);
        assert_ne!(deny1, deny3);
    }

    #[test]
    fn test_plugin_output_clone() {
        let original = PluginOutput::Deny("test message".to_string());
        let cloned = original.clone();
        assert_eq!(original, cloned);
    }

    #[test]
    fn test_plugin_output_debug() {
        let output = PluginOutput::Allow;
        let debug_str = format!("{:?}", output);
        assert_eq!(debug_str, "Allow");

        let deny_output = PluginOutput::Deny("error".to_string());
        let deny_debug = format!("{:?}", deny_output);
        assert!(deny_debug.contains("Deny"));
        assert!(deny_debug.contains("error"));
    }
}
