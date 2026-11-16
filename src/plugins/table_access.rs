//! This query router plugin will check if the user can access a particular
//! table as part of their query. If they can't, the query will not be routed.

use async_trait::async_trait;
use sqlparser::ast::{visit_relations, CopySource, ObjectName, Statement};

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

        // `visit_relations` doesn't visit the table of `COPY t`.
        let found = found.or_else(|| {
            ast.iter().find_map(|statement| match statement {
                Statement::Copy {
                    source: CopySource::Table { table_name, .. },
                    ..
                } => self.forbidden(table_name),
                _ => None,
            })
        });

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

impl TableAccess<'_> {
    /// The table's name, if it is one of the forbidden tables.
    fn forbidden(&self, relation: &ObjectName) -> Option<String> {
        let table = relation.0.last()?;
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
