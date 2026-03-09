//! Log all queries to stdout (or somewhere else, why not).

use crate::{
    errors::Error,
    plugins::{Plugin, PluginOutput},
    query_router::QueryRouter,
};
use async_trait::async_trait;
use log::info;
use sqlparser::ast::Statement;

pub struct QueryLogger<'a> {
    pub enabled: bool,
    pub user: &'a str,
    pub db: &'a str,
}

#[async_trait]
impl<'a> Plugin for QueryLogger<'a> {
    async fn run(
        &mut self,
        _query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error> {
        if !self.enabled {
            return Ok(PluginOutput::Allow);
        }

        let query = ast
            .iter()
            .map(|q| q.to_string())
            .collect::<Vec<String>>()
            .join("; ");
        info!("[pool: {}][user: {}] {}", self.db, self.user, query);

        Ok(PluginOutput::Allow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_logger_struct_creation() {
        let ql = QueryLogger {
            enabled: true,
            user: "test_user",
            db: "test_db",
        };
        assert!(ql.enabled);
        assert_eq!(ql.user, "test_user");
        assert_eq!(ql.db, "test_db");
    }

    #[test]
    fn test_query_logger_disabled() {
        let ql = QueryLogger {
            enabled: false,
            user: "user",
            db: "db",
        };
        assert!(!ql.enabled);
    }

    #[test]
    fn test_query_logger_user_and_db_strings() {
        let ql = QueryLogger {
            enabled: true,
            user: "admin",
            db: "production",
        };
        // Verify the strings are stored correctly
        assert_eq!(ql.user, "admin");
        assert_eq!(ql.db, "production");
    }
}
