//! The intercept plugin.
//!
//! It intercepts queries and returns fake results.

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;

use log::debug;

use crate::{
    config::Intercept as InterceptConfig,
    errors::Error,
    messages::{command_complete, data_row_nullable, row_description, DataType},
    plugins::{Plugin, PluginOutput},
    query_router::QueryRouter,
};

// TODO: use these structs for deserialization
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Rule {
    pub query: String,
    pub schema: Vec<Column>,
    pub result: Vec<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: String,
}

/// The intercept plugin.
pub struct Intercept<'a> {
    pub enabled: bool,
    pub config: &'a InterceptConfig,
}

#[async_trait]
impl<'a> Plugin for Intercept<'a> {
    async fn run(
        &mut self,
        query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error> {
        if !self.enabled || ast.is_empty() {
            return Ok(PluginOutput::Allow);
        }

        let mut config = self.config.clone();
        config.substitute(
            &query_router.pool_settings().db,
            &query_router.pool_settings().user.username,
        );

        let mut result = BytesMut::new();

        for q in ast {
            // Normalization
            let q = q.to_string().to_ascii_lowercase();

            for (_, target) in config.queries.iter() {
                if target.query.as_str() == q {
                    debug!("Intercepting query: {}", q);

                    let rd = target
                        .schema
                        .iter()
                        .map(|row| {
                            let name = &row[0];
                            let data_type = &row[1];
                            (
                                name.as_str(),
                                match data_type.as_str() {
                                    "text" => DataType::Text,
                                    "anyarray" => DataType::AnyArray,
                                    "oid" => DataType::Oid,
                                    "bool" => DataType::Bool,
                                    "int4" => DataType::Int4,
                                    _ => DataType::Any,
                                },
                            )
                        })
                        .collect::<Vec<(&str, DataType)>>();

                    result.put(row_description(&rd));

                    target.result.iter().for_each(|row| {
                        let row = row
                            .iter()
                            .map(|s| {
                                let s = s.as_str().to_string();

                                if s.is_empty() {
                                    None
                                } else {
                                    Some(s)
                                }
                            })
                            .collect::<Vec<Option<String>>>();
                        result.put(data_row_nullable(&row));
                    });

                    result.put(command_complete("SELECT"));
                }
            }
        }

        if !result.is_empty() {
            result.put_u8(b'Z');
            result.put_i32(5);
            result.put_u8(b'I');

            return Ok(PluginOutput::Intercept(result));
        } else {
            Ok(PluginOutput::Allow)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== Rule Tests ====================

    #[test]
    fn test_rule_struct() {
        let rule = Rule {
            query: "SELECT 1".to_string(),
            schema: vec![Column {
                name: "column1".to_string(),
                data_type: "text".to_string(),
            }],
            result: vec![vec!["value1".to_string()]],
        };

        assert_eq!(rule.query, "SELECT 1");
        assert_eq!(rule.schema.len(), 1);
        assert_eq!(rule.result.len(), 1);
    }

    #[test]
    fn test_rule_clone() {
        let rule = Rule {
            query: "SELECT 1".to_string(),
            schema: vec![],
            result: vec![],
        };
        let cloned = rule.clone();
        assert_eq!(rule, cloned);
    }

    // ==================== Column Tests ====================

    #[test]
    fn test_column_struct() {
        let column = Column {
            name: "id".to_string(),
            data_type: "int4".to_string(),
        };

        assert_eq!(column.name, "id");
        assert_eq!(column.data_type, "int4");
    }

    #[test]
    fn test_column_clone() {
        let column = Column {
            name: "name".to_string(),
            data_type: "text".to_string(),
        };
        let cloned = column.clone();
        assert_eq!(column, cloned);
    }

    #[test]
    fn test_column_equality() {
        let col1 = Column {
            name: "test".to_string(),
            data_type: "text".to_string(),
        };
        let col2 = Column {
            name: "test".to_string(),
            data_type: "text".to_string(),
        };
        let col3 = Column {
            name: "other".to_string(),
            data_type: "text".to_string(),
        };

        assert_eq!(col1, col2);
        assert_ne!(col1, col3);
    }

    // ==================== Intercept Tests ====================

    #[test]
    fn test_intercept_struct_disabled() {
        let config = InterceptConfig::default();
        let intercept = Intercept {
            enabled: false,
            config: &config,
        };

        assert!(!intercept.enabled);
    }

    #[test]
    fn test_intercept_struct_enabled() {
        let config = InterceptConfig {
            enabled: true,
            ..Default::default()
        };
        let intercept = Intercept {
            enabled: true,
            config: &config,
        };

        assert!(intercept.enabled);
    }

    // ==================== Serialization Tests ====================

    #[test]
    fn test_column_serialization() {
        let column = Column {
            name: "id".to_string(),
            data_type: "int4".to_string(),
        };

        let serialized = serde_json::to_string(&column).unwrap();
        assert!(serialized.contains("id"));
        assert!(serialized.contains("int4"));

        let deserialized: Column = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, column);
    }

    #[test]
    fn test_rule_serialization() {
        let rule = Rule {
            query: "SELECT * FROM users".to_string(),
            schema: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int4".to_string(),
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                },
            ],
            result: vec![
                vec!["1".to_string(), "Alice".to_string()],
                vec!["2".to_string(), "Bob".to_string()],
            ],
        };

        let serialized = serde_json::to_string(&rule).unwrap();
        let deserialized: Rule = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, rule);
    }
}
