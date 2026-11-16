//! This example is a separate crate: only pgcat's public library API is visible.
// Linking this consumer must fail if PgCat accidentally selects a global
// allocator in the library, even if its standalone allocator is also System.
#[global_allocator]
static GLOBAL: std::alloc::System = std::alloc::System;

use std::sync::Arc;

use async_trait::async_trait;
use pgcat::{
    errors::Error,
    messages::with_sqlstate,
    plugins::{
        registry::{PluginRegistry, RegisteredPlugin},
        BindParams, DataRow, ExecuteContext, PluginContext, PluginOutput, PluginResult,
        RowDescription, RowsContext, StatementHooks, StatementPlan, PLUGIN_DENIED, PLUGIN_FAILED,
    },
};
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

struct Synthetic;

#[async_trait]
impl RegisteredPlugin for Synthetic {
    async fn on_query(
        &self,
        _context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        // These sentinels model individual statements, not a multi-statement
        // policy. Refuse batches rather than dropping part of a query rewrite.
        if statements.len() > 1 {
            return Ok(PluginOutput::Deny(with_sqlstate(
                PLUGIN_DENIED,
                "synthetic plugin requires a single statement",
            ))
            .into());
        }
        for statement in statements {
            if statement.to_string().contains("registration_result") {
                let mut plan = StatementPlan::new(Arc::new(Synthetic), Box::new(()));
                plan.wants_response = true;
                plan.read_only = true;
                return Ok(PluginResult {
                    output: PluginOutput::Allow,
                    plan: Some(plan),
                });
            }
            match statement.to_string().as_str() {
                "SELECT 7001" | "SELECT 7006" => {
                    let rewritten = Parser::parse_sql(&PostgreSqlDialect {}, "SELECT 7002")
                        .map_err(|_| Error::PluginError("invalid synthetic rewrite".into()))?;
                    return Ok(PluginOutput::Overwrite(rewritten).into());
                }
                "SELECT 7003" => {
                    return Ok(PluginOutput::Deny(with_sqlstate(
                        PLUGIN_DENIED,
                        "synthetic plugin refused statement",
                    ))
                    .into());
                }
                "SELECT 7004" => return std::future::pending().await,
                "SELECT 7005" => {
                    return Err(Error::PluginError(with_sqlstate(
                        PLUGIN_FAILED,
                        "synthetic plugin failed",
                    )))
                }
                "SELECT 7008" => {
                    return Ok(PluginResult {
                        output: PluginOutput::Allow,
                        plan: Some(StatementPlan::new(Arc::new(Synthetic), Box::new(()))),
                    });
                }
                _ => {}
            }
        }
        Ok(PluginOutput::Allow.into())
    }

    async fn on_parse(
        &self,
        context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        if statements
            .iter()
            .any(|statement| statement.to_string().contains("registration_bind"))
        {
            let mut plan = StatementPlan::new(Arc::new(Synthetic), Box::new(()));
            plan.client_params = context.client_param_count(statements);
            plan.wants_response = true;
            plan.read_only = true;
            return Ok(PluginResult {
                output: PluginOutput::Allow,
                plan: Some(plan),
            });
        }
        self.on_query(context, statements).await
    }
}

#[async_trait]
impl StatementHooks for Synthetic {
    async fn on_bind(
        &self,
        _context: &mut ExecuteContext<'_>,
        _plan: &StatementPlan,
        params: &mut BindParams,
    ) -> Result<(), Error> {
        if params.len() != 1 {
            return Err(Error::PluginError(with_sqlstate(
                PLUGIN_FAILED,
                "unexpected parameter count",
            )));
        }
        params.set(0, 0, Some(b"42".to_vec()));
        Ok(())
    }

    fn on_row_description(
        &self,
        _plan: &StatementPlan,
        description: &mut RowDescription,
    ) -> Result<(), Error> {
        description.fields.pop();
        Ok(())
    }

    async fn on_rows(
        &self,
        _context: &mut ExecuteContext<'_>,
        _plan: &StatementPlan,
        _rows: &RowsContext<'_>,
        mut rows: Vec<DataRow>,
    ) -> Result<Vec<DataRow>, Error> {
        for row in &mut rows {
            row.values.pop();
        }
        Ok(rows)
    }
}

struct ConflictingTransform;

#[async_trait]
impl RegisteredPlugin for ConflictingTransform {
    async fn on_query(
        &self,
        _context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        match statements.first().map(ToString::to_string).as_deref() {
            Some("SELECT 7006") => Ok(PluginResult {
                output: PluginOutput::Allow,
                plan: Some(StatementPlan::new(Arc::new(Synthetic), Box::new(()))),
            }),
            Some("SELECT 7008") => Ok(PluginOutput::Overwrite(statements.to_vec()).into()),
            _ => Ok(PluginOutput::Allow.into()),
        }
    }
}

/// Rewrites between a table the policy pool allows and one it blocks. The
/// original filter matches no row, so a returned row proves the rewrite ran.
struct PolicyRewrite;

#[async_trait]
impl RegisteredPlugin for PolicyRewrite {
    async fn on_query(
        &self,
        _context: &PluginContext<'_>,
        statements: &[Statement],
    ) -> Result<PluginResult, Error> {
        let rewrite = match statements.first().map(ToString::to_string).as_deref() {
            Some("SELECT value FROM registration_policy_allowed WHERE value = 'to-blocked'") => {
                "SELECT value FROM registration_policy_blocked"
            }
            Some("SELECT value FROM registration_policy_blocked WHERE value = 'to-allowed'")
            | Some("SELECT value FROM registration_policy_allowed WHERE value = 'to-allowed'") => {
                "SELECT value FROM registration_policy_allowed"
            }
            _ => return Ok(PluginOutput::Allow.into()),
        };
        let rewritten = Parser::parse_sql(&PostgreSqlDialect {}, rewrite)
            .map_err(|_| Error::PluginError("invalid synthetic rewrite".into()))?;
        Ok(PluginOutput::Overwrite(rewritten).into())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut plugins = PluginRegistry::default();
    plugins
        .register("synthetic", Synthetic)
        .map_err(|_| "invalid synthetic plugin registration")?;
    plugins
        .register("conflicting-transform", ConflictingTransform)
        .map_err(|_| "invalid synthetic conflict registration")?;
    plugins
        .register("policy-rewrite", PolicyRewrite)
        .map_err(|_| "invalid synthetic policy registration")?;
    pgcat::runtime::run_with_plugins(plugins)
}
