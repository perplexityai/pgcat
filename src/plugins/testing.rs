//! Test-only plugin used to exercise the plugin hooks end to end.
//!
//! Only compiled for `cargo test` or with the `test-plugins` feature.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use sqlparser::ast::{
    visit_relations, Expr, Ident, ObjectName, SetExpr, Statement, TableObject, Value, ValueWithSpan,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::{
    config::{Testing as TestingConfig, TestingCopyParam},
    errors::Error,
    identity::{IdentityToken, VerifiedIdentity},
    messages::{command_complete, data_row_nullable, ready_for_query, row_description, DataType},
    plugins::{
        BindParams, DataRow, ExecuteContext, Plugin, PluginContext, PluginOutput, PluginResult,
        RowDescription, RowsContext, StatementHooks, StatementPlan,
    },
    query_router::QueryRouter,
};

/// Verify a test identity token, see `config::Testing::verify_identity`.
pub fn verify_identity(token: &IdentityToken) -> Result<VerifiedIdentity, String> {
    let mut rest = token.as_str();
    let mut verified = VerifiedIdentity::default();

    if rest.starts_with("invalid") {
        return Err("invalid identity token".into());
    }

    if let Some(expires) = rest.strip_prefix("expires:") {
        let (seconds, tail) = expires.split_once(':').ok_or("invalid identity token")?;
        let seconds: u64 = seconds.parse().map_err(|_| "invalid identity token")?;
        verified.expires_at = Some(UNIX_EPOCH + Duration::from_secs(seconds));
        rest = tail;
    }

    if let Some(tenant) = rest.strip_prefix("tenant:") {
        // `#...` makes tokens for the same tenant unique.
        let tenant = tenant.split('#').next().unwrap_or_default();
        verified.tenant = Some(tenant.to_string());
    }

    Ok(verified)
}

/// Postgres `text` type OID.
const TEXT_OID: i32 = 25;

pub struct Testing<'a> {
    pub enabled: bool,
    pub config: &'a TestingConfig,
}

#[async_trait]
impl<'a> Plugin for Testing<'a> {
    async fn run(
        &mut self,
        _query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error> {
        if !self.enabled || ast.is_empty() {
            return Ok(PluginOutput::Allow);
        }

        let mut rewritten = false;
        let mut output = Vec::with_capacity(ast.len());

        for statement in ast {
            if let Some(delay) = self
                .config
                .delays
                .get(&statement.to_string().to_ascii_lowercase())
            {
                tokio::time::sleep(std::time::Duration::from_millis(*delay)).await;
            }

            match self
                .config
                .rewrites
                .get(&statement.to_string().to_ascii_lowercase())
            {
                Some(replacement) => {
                    let replacement = Parser::parse_sql(&PostgreSqlDialect {}, replacement)
                        .map_err(|err| Error::QueryRouterParserError(err.to_string()))?;
                    output.extend(replacement);
                    rewritten = true;
                }
                None => output.push(statement.clone()),
            }
        }

        if rewritten {
            Ok(PluginOutput::Overwrite(output))
        } else {
            Ok(PluginOutput::Allow)
        }
    }

    async fn on_parse(
        &mut self,
        ctx: &PluginContext<'_>,
        ast: &Vec<Statement>,
    ) -> Result<PluginResult, Error> {
        if self.enabled {
            if let Some(output) = self.whoami(ctx, ast) {
                return Ok(output.into());
            }

            if let Some(result) = self.plan_drop_last_column(ctx, ast) {
                return Ok(result);
            }

            if let Some(ref copy) = self.config.copy_param {
                if let Some(result) = plan_copy_param(ctx, ast, copy) {
                    return Ok(result);
                }
            }
        }

        Ok(self.run(ctx.query_router, ast).await?.into())
    }

    async fn on_query(
        &mut self,
        ctx: &PluginContext<'_>,
        ast: &Vec<Statement>,
    ) -> Result<PluginResult, Error> {
        if self.enabled {
            if let Some(output) = self.whoami(ctx, ast) {
                return Ok(output.into());
            }

            if let Some(result) = self.plan_drop_last_column(ctx, ast) {
                return Ok(result);
            }
        }

        Ok(self.run(ctx.query_router, ast).await?.into())
    }
}

impl<'a> Testing<'a> {
    /// `SELECT 'whoami'` returns the caller identity, without asking the server.
    fn whoami(&self, ctx: &PluginContext<'_>, ast: &[Statement]) -> Option<PluginOutput> {
        if !self.config.whoami || ast.len() != 1 || ast[0].to_string() != "SELECT 'whoami'" {
            return None;
        }

        let mut response = BytesMut::new();
        response.put(row_description(&vec![("identity", DataType::Text)]));
        response.put(data_row_nullable(&vec![ctx
            .identity()
            .map(|identity| identity.token().as_str().to_string())]));
        response.put(command_complete("SELECT 1"));
        response.put(ready_for_query(false));

        Some(PluginOutput::Intercept(response))
    }

    /// Selects from the configured table lose their last column.
    fn plan_drop_last_column(
        &self,
        ctx: &PluginContext<'_>,
        ast: &[Statement],
    ) -> Option<PluginResult> {
        let table = self.config.drop_last_column.as_ref()?;

        if !matches!(ast, [Statement::Query(_)]) {
            return None;
        }

        let mut found = false;
        let _ = visit_relations(&ast[0], |relation| {
            found |= relation
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|ident| &ident.value)
                == Some(table);
            ControlFlow::<()>::Continue(())
        });

        if !found {
            return None;
        }

        Some(PluginResult {
            output: PluginOutput::Allow,
            plan: Some(
                StatementPlan::new(Arc::new(DropLastColumn), Box::new(()))
                    .with_client_params(ctx.client_param_count(ast))
                    .with_response_hooks()
                    .read_only(),
            ),
        })
    }
}

/// `INSERT INTO t (.., from, ..) VALUES (.., $k, ..), ...` becomes
/// `INSERT INTO t (.., from, .., to) VALUES (.., $k, .., $n+1), ...`.
fn plan_copy_param(
    ctx: &PluginContext<'_>,
    ast: &[Statement],
    copy: &TestingCopyParam,
) -> Option<PluginResult> {
    let mut insert = match ast {
        [Statement::Insert(insert)] => insert.clone(),
        _ => return None,
    };

    let TableObject::TableName(table_name) = &insert.table else {
        return None;
    };
    if table_name.0.last()?.as_ident()?.value != copy.table {
        return None;
    }

    let from = insert.columns.iter().position(|column| {
        matches!(column.0.as_slice(), [part] if part.as_ident().is_some_and(|ident| ident.value == copy.from))
    })?;

    let client_params = ctx.client_param_count(ast);
    let mut sources = Vec::new();

    let values = match insert.source.as_mut()?.body.as_mut() {
        SetExpr::Values(values) => values,
        _ => return None,
    };

    for row in values.rows.iter_mut() {
        let source = match row.get(from)? {
            Expr::Value(ValueWithSpan {
                value: Value::Placeholder(placeholder),
                ..
            }) => placeholder
                .strip_prefix('$')?
                .parse::<usize>()
                .ok()?
                .checked_sub(1)?,
            _ => return None,
        };

        sources.push(source);
        row.push(Expr::value(Value::Placeholder(format!(
            "${}",
            client_params + sources.len()
        ))));
    }

    insert
        .columns
        .push(ObjectName::from(vec![Ident::new(&copy.to)]));

    let appended = vec![TEXT_OID; sources.len()];

    Some(PluginResult {
        output: PluginOutput::Overwrite(vec![Statement::Insert(insert)]),
        plan: Some(
            StatementPlan::new(Arc::new(CopyParam), Box::new(sources))
                .with_client_params(client_params)
                .with_appended_params(appended),
        ),
    })
}

/// Fills the appended parameters with the values of their source parameters.
struct CopyParam;

#[async_trait]
impl StatementHooks for CopyParam {
    async fn before_checkout(
        &self,
        _ctx: &mut ExecuteContext<'_>,
        plan: &StatementPlan,
        params: &BindParams,
    ) -> Result<(), Error> {
        let sources = plan
            .state::<Vec<usize>>()
            .ok_or_else(|| Error::PluginError("bad testing plan".into()))?;
        match sources
            .iter()
            .any(|source| params.value(*source) == Some(b"no-server".as_slice()))
        {
            true => Err(Error::PluginError(
                "testing plugin refused the value before checkout".into(),
            )),
            false => Ok(()),
        }
    }

    async fn on_bind(
        &self,
        ctx: &mut ExecuteContext<'_>,
        plan: &StatementPlan,
        params: &mut BindParams,
    ) -> Result<(), Error> {
        let sources = plan
            .state::<Vec<usize>>()
            .ok_or_else(|| Error::PluginError("bad testing plan".into()))?;

        for source in sources {
            let value = params.value(*source).map(|value| value.to_vec());

            if value.as_deref() == Some(b"fail") {
                return Err(Error::PluginError(
                    "testing plugin refused the value".into(),
                ));
            }

            // Outlives the hook timeout, mid-query.
            if value.as_deref() == Some(b"slow-query") {
                ctx.query("SELECT pg_sleep(1)", &[]).await?;
            }

            params.push(params.format(*source), value);
        }

        Ok(())
    }
}

/// Hides the last column of the results.
struct DropLastColumn;

#[async_trait]
impl StatementHooks for DropLastColumn {
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
        _ctx: &mut ExecuteContext<'_>,
        _plan: &StatementPlan,
        _rows_ctx: &RowsContext<'_>,
        mut rows: Vec<DataRow>,
    ) -> Result<Vec<DataRow>, Error> {
        for row in rows.iter_mut() {
            if row.values.first().and_then(|value| value.as_deref()) == Some(b"slow-row") {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
            if row.values.first().and_then(|value| value.as_deref()) == Some(b"fail") {
                return Err(Error::PluginError("testing plugin refused the row".into()));
            }
            row.values.pop();
        }

        Ok(rows)
    }
}
