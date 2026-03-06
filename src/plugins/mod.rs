//! The plugin ecosystem.
//!
//! Currently plugins only grant access or deny access to the database for a particual query.
//! Example use cases:
//!   - block known bad queries
//!   - block access to system catalogs
//!   - block dangerous modifications like `DROP TABLE`
//!   - etc
//!

pub mod intercept;
pub mod prewarmer;
pub mod query_logger;
pub mod table_access;

use crate::{errors::Error, query_router::QueryRouter};
use async_trait::async_trait;
use bytes::BytesMut;
use sqlparser::ast::Statement;

pub use intercept::Intercept;
pub use query_logger::QueryLogger;
pub use table_access::TableAccess;

#[derive(Clone, Debug, PartialEq)]
pub enum PluginOutput {
    Allow,
    Deny(String),
    Overwrite(Vec<Statement>),
    Intercept(BytesMut),
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

    // TODO: run after the result is returned
    // async fn callback(&mut self, query_router: &QueryRouter);
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

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
