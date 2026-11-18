//! This query router plugin will check if the user can access a particular
//! table as part of their query. If they can't, the query will not be routed.

use async_trait::async_trait;
use sqlparser::ast::{
    visit_relations, visit_statements, AlterTableOperation, CopySource, CreateFunction,
    CreateFunctionBody, ObjectName, ObjectType, Statement, VacuumStatement,
};

use crate::{
    errors::Error,
    plugins::{Plugin, PluginOutput},
    query_router::QueryRouter,
};

use log::debug;

use core::ops::ControlFlow;

pub struct TableAccess<'a> {
    pub enabled: bool,
    pub tables: &'a Vec<String>,
}

#[async_trait]
impl<'a> Plugin for TableAccess<'a> {
    async fn run(
        &mut self,
        _query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error> {
        if !self.enabled {
            return Ok(PluginOutput::Allow);
        }

        let mut found = None;
        let _ = visit_relations(ast, |relation| {
            found = self.forbidden(relation);
            match found {
                Some(_) => ControlFlow::<()>::Break(()),
                None => ControlFlow::<()>::Continue(()),
            }
        });

        // `visit_relations` doesn't visit the table of `COPY t` or `VACUUM t`.
        let found = found.or_else(|| {
            ast.iter().find_map(|statement| match statement {
                Statement::Copy {
                    source: CopySource::Table { table_name, .. },
                    ..
                } => self.forbidden(table_name),
                Statement::Vacuum(vacuum) => vacuum_target(vacuum).and_then(|t| self.forbidden(t)),
                _ => None,
            })
        });

        // Nor the tables in `unvisited_relations`, in any statement.
        let found = found.or_else(|| {
            let mut found = None;
            let _ = visit_statements(ast, |statement| {
                found = unvisited_relations(statement)
                    .into_iter()
                    .find_map(|relation| self.forbidden(relation));
                match found {
                    Some(_) => ControlFlow::<()>::Break(()),
                    None => ControlFlow::<()>::Continue(()),
                }
            });
            found
        });

        // A VACUUM without a table processes every table, forbidden ones too.
        if found.is_none()
            && !self.tables.is_empty()
            && ast.iter().any(|statement| {
                matches!(statement, Statement::Vacuum(vacuum) if vacuum_target(vacuum).is_none())
            })
        {
            debug!("Blocking VACUUM of every table");

            return Ok(PluginOutput::Deny(
                "permission for VACUUM of every table denied".into(),
            ));
        }

        // A function body given as a string isn't parsed, so the tables it
        // names can't be checked. One given as SQL (`BEGIN ATOMIC`, `RETURN`)
        // is, above.
        if found.is_none() && !self.tables.is_empty() && has_string_body(ast) {
            debug!("Blocking a function body given as a string");

            return Ok(PluginOutput::Deny(
                "permission for a function body given as a string denied".into(),
            ));
        }

        if let Some(found) = found {
            debug!("Blocking access to table \"{}\"", found);

            Ok(PluginOutput::Deny(format!(
                "permission for table \"{}\" denied",
                found
            )))
        } else {
            Ok(PluginOutput::Allow)
        }
    }
}

/// Tables a statement names that `visit_relations` doesn't visit: the table an
/// ALTER TABLE attaches or detaches as a partition, the parent it adds or
/// removes with INHERIT or NO INHERIT, the parents a CREATE TABLE or CREATE
/// FOREIGN TABLE inherits from, and the tables a DROP FOREIGN TABLE drops.
fn unvisited_relations(statement: &Statement) -> Vec<&ObjectName> {
    match statement {
        Statement::AlterTable(alter) => alter
            .operations
            .iter()
            .filter_map(|operation| match operation {
                AlterTableOperation::AttachPartitionOf { partition, .. }
                | AlterTableOperation::DetachPartitionOf { partition, .. } => Some(partition),
                AlterTableOperation::Inherit { parent }
                | AlterTableOperation::NoInherit { parent } => Some(parent),
                _ => None,
            })
            .collect(),
        Statement::CreateTable(table) => table.inherits.iter().flatten().collect(),
        Statement::CreateForeignTable(table) => table.inherits.iter().flatten().collect(),
        Statement::Drop {
            object_type: ObjectType::ForeignTable,
            names,
            ..
        } => names.iter().collect(),
        _ => Vec::new(),
    }
}

/// Whether a statement, or one nested in it, creates a function whose body is
/// a string: `AS 'body'` or `AS $$body$$`, in any language, an object file's
/// too.
fn has_string_body(ast: &Vec<Statement>) -> bool {
    visit_statements(ast, |statement| match statement {
        Statement::CreateFunction(CreateFunction {
            function_body:
                Some(
                    CreateFunctionBody::AsBeforeOptions { .. }
                    | CreateFunctionBody::AsAfterOptions(_),
                ),
            ..
        }) => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

/// The table a VACUUM processes, or `None` when it processes every table.
fn vacuum_target(vacuum: &VacuumStatement) -> Option<&ObjectName> {
    let name = vacuum.table_name.as_ref()?;
    // The parser reads an option after VACUUM, as in `VACUUM VERBOSE`, as a
    // table name. Postgres reads these words there as options, never as an
    // unquoted table name.
    if let [part] = name.0.as_slice() {
        if let Some(ident) = part.as_ident() {
            let option = ["analyse", "analyze", "freeze", "verbose"]
                .contains(&ident.value.to_ascii_lowercase().as_str());
            if ident.quote_style.is_none() && option {
                return None;
            }
        }
    }
    Some(name)
}

impl TableAccess<'_> {
    /// The table's name, if it is one of the forbidden tables.
    fn forbidden(&self, relation: &ObjectName) -> Option<String> {
        let table = relation.0.last()?.as_ident()?;
        // Postgres folds a name to lower case unless it's quoted.
        let name = match table.quote_style {
            Some(_) => table.value.clone(),
            None => table.value.to_ascii_lowercase(),
        };
        self.tables.contains(&name).then_some(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    async fn refused(sql: &str) -> bool {
        let tables = vec!["secrets".to_string()];
        let mut plugin = TableAccess {
            enabled: true,
            tables: &tables,
        };
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        matches!(
            plugin.run(&QueryRouter::new(), &ast).await.unwrap(),
            PluginOutput::Deny(_)
        )
    }

    #[tokio::test]
    async fn test_copy_of_a_forbidden_table_is_refused() {
        for sql in [
            "SELECT * FROM secrets",
            "COPY secrets TO STDOUT",
            "COPY public.secrets (a) TO STDOUT",
            "COPY (SELECT a FROM secrets) TO STDOUT",
        ] {
            assert!(refused(sql).await, "{}", sql);
        }
        assert!(!refused("COPY other TO STDOUT").await);
    }

    #[tokio::test]
    async fn test_string_function_bodies_are_refused_while_a_table_is_forbidden() {
        for sql in [
            "CREATE FUNCTION f() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM secrets $$",
            "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1'",
            "CREATE OR REPLACE FUNCTION f() RETURNS int AS $body$ SELECT 1 $body$ LANGUAGE sql",
            "CREATE FUNCTION f() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END $$",
            "CREATE FUNCTION f() RETURNS int AS 'MODULE_PATHNAME', 'f' LANGUAGE c",
            "SELECT 1; CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1'",
            // A body given as SQL is checked by the tables it names.
            "CREATE FUNCTION f() RETURNS bigint LANGUAGE sql \
             RETURN (SELECT count(*) FROM secrets)",
            "CREATE FUNCTION f() RETURNS bigint LANGUAGE sql BEGIN ATOMIC \
             SELECT count(*) FROM secrets; END",
        ] {
            assert!(refused(sql).await, "{}", sql);
        }

        for sql in [
            "CREATE FUNCTION f() RETURNS bigint LANGUAGE sql \
             RETURN (SELECT count(*) FROM other)",
            "CREATE FUNCTION f() RETURNS bigint LANGUAGE sql BEGIN ATOMIC \
             SELECT count(*) FROM other; END",
            "CREATE PROCEDURE p() BEGIN ATOMIC SELECT 1; END",
        ] {
            assert!(!refused(sql).await, "{}", sql);
        }

        // Nothing to protect, nothing refused.
        let tables = vec![];
        let mut plugin = TableAccess {
            enabled: true,
            tables: &tables,
        };
        let ast = Parser::parse_sql(
            &PostgreSqlDialect {},
            "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1'",
        )
        .unwrap();
        assert_eq!(
            plugin.run(&QueryRouter::new(), &ast).await.unwrap(),
            PluginOutput::Allow
        );
    }

    #[tokio::test]
    async fn test_vacuum_of_a_forbidden_table_is_refused() {
        for sql in [
            "VACUUM secrets",
            "VACUUM FULL secrets",
            "VACUUM FULL public.secrets",
            r#"VACUUM "secrets""#,
            r#"VACUUM FULL "public"."secrets""#,
            "VACUUM SECRETS",
            "SELECT 1; VACUUM secrets",
        ] {
            assert!(refused(sql).await, "{}", sql);
        }
        for sql in [
            "VACUUM other",
            "VACUUM FULL public.other",
            r#"VACUUM "SECRETS""#,
        ] {
            assert!(!refused(sql).await, "{}", sql);
        }
    }

    #[tokio::test]
    async fn test_vacuum_of_every_table_is_refused() {
        // Without a table, VACUUM processes every table, so it's refused
        // while any table is forbidden. The parser reads the options in
        // `VACUUM VERBOSE` as a table name.
        for sql in [
            "VACUUM",
            "VACUUM FULL",
            "VACUUM;",
            "VACUUM VERBOSE",
            "VACUUM FULL FREEZE",
            "VACUUM analyze",
            "SELECT 1; VACUUM",
        ] {
            assert!(refused(sql).await, "{}", sql);
        }

        // A table named like an option is one when it's quoted.
        assert!(!refused(r#"VACUUM "verbose""#).await);

        let tables = vec![];
        let mut plugin = TableAccess {
            enabled: true,
            tables: &tables,
        };
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, "VACUUM FULL").unwrap();
        assert!(matches!(
            plugin.run(&QueryRouter::new(), &ast).await.unwrap(),
            PluginOutput::Allow
        ));
    }

    #[test]
    fn test_table_access_struct_creation() {
        let tables = vec!["secret_table".to_string()];
        let ta = TableAccess {
            enabled: true,
            tables: &tables,
        };
        assert!(ta.enabled);
        assert_eq!(ta.tables.len(), 1);
    }

    #[test]
    fn test_table_access_disabled() {
        let tables = vec!["secret_table".to_string()];
        let ta = TableAccess {
            enabled: false,
            tables: &tables,
        };
        assert!(!ta.enabled);
    }

    #[test]
    fn test_table_access_with_multiple_tables() {
        let tables = vec![
            "users".to_string(),
            "passwords".to_string(),
            "secrets".to_string(),
        ];
        let ta = TableAccess {
            enabled: true,
            tables: &tables,
        };
        assert_eq!(ta.tables.len(), 3);
        assert!(ta.tables.contains(&"users".to_string()));
        assert!(ta.tables.contains(&"passwords".to_string()));
        assert!(ta.tables.contains(&"secrets".to_string()));
    }

    #[test]
    fn test_table_access_empty_tables() {
        let tables: Vec<String> = vec![];
        let ta = TableAccess {
            enabled: true,
            tables: &tables,
        };
        assert!(ta.tables.is_empty());
    }
}
