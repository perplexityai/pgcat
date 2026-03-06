//! This query router plugin will check if the user can access a particular
//! table as part of their query. If they can't, the query will not be routed.

use async_trait::async_trait;
use sqlparser::ast::{visit_relations, Statement};

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
            let relation = relation.to_string();
            let parts = relation.split('.').collect::<Vec<&str>>();
            let table_name = parts.last().unwrap();

            if self.tables.contains(&table_name.to_string()) {
                found = Some(table_name.to_string());
                ControlFlow::<()>::Break(())
            } else {
                ControlFlow::<()>::Continue(())
            }
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

#[cfg(test)]
mod tests {
    use super::*;

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
