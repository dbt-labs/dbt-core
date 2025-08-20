use crate::AdapterResult;
use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationMetadataKey,
    DatabricksRelationResults,
};

use dbt_schemas::schemas::InternalDbtNodeAttributes;
use minijinja::Value;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CommentConfig {
    pub comment: Option<String>,
    pub persist: bool,
}

impl CommentConfig {
    pub fn new(comment: Option<String>, persist: bool) -> Self {
        Self { comment, persist }
    }

    pub fn get_diff(&self, other: &Self) -> Option<Self> {
        if self.persist && self.comment != other.comment {
            Some(self.clone())
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct CommentProcessor;

impl DatabricksComponentProcessorProperties for CommentProcessor {
    fn name(&self) -> &'static str {
        "comment"
    }
}

/// https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/relation_configs/comment.py#L23
impl DatabricksComponentProcessor for CommentProcessor {
    #[allow(clippy::wrong_self_convention)]
    fn from_relation_results(
        &self,
        results: &DatabricksRelationResults,
    ) -> Option<DatabricksComponentConfig> {
        let describe_extended = results.get(&DatabricksRelationMetadataKey::DescribeExtended)?;

        for row in describe_extended.rows() {
            if let (Ok(key_val), Ok(value_val)) =
                (row.get_item(&Value::from(0)), row.get_item(&Value::from(1)))
            {
                if let (Some(key_str), Some(value_str)) = (key_val.as_str(), value_val.as_str()) {
                    if key_str == "Comment" {
                        let comment = if !value_str.is_empty() {
                            Some(value_str.to_string())
                        } else {
                            None
                        };
                        return Some(DatabricksComponentConfig::Comment(CommentConfig::new(
                            comment, false,
                        )));
                    }
                }
            }
        }

        Some(DatabricksComponentConfig::Comment(CommentConfig::new(
            None, false,
        )))
    }

    fn from_relation_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>> {
        let persist = relation_config
            .base()
            .persist_docs
            .as_ref()
            .map(|pd| pd.relation.unwrap_or(false))
            .unwrap_or(false);

        let comment = relation_config.common().description.clone();

        Ok(Some(DatabricksComponentConfig::Comment(
            CommentConfig::new(comment, persist),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::databricks::relation_configs::base::DatabricksRelationResultsBuilder;
    use dbt_agate::AgateTable;
    use dbt_schemas::schemas::{common::*, nodes::*, project::*};
    use std::collections::BTreeMap;

    fn create_mock_describe_extended_table(comment: Option<&str>) -> AgateTable {
        use arrow::csv::ReaderBuilder;
        use arrow_schema::{DataType, Field, Schema};
        use std::io;
        use std::sync::Arc;

        let mut csv_data = "key,value\n".to_string();
        csv_data.push_str("Table,test_table\n");
        csv_data.push_str("Owner,test_user\n");

        if let Some(comment_text) = comment {
            csv_data.push_str(&format!("Comment,{comment_text}\n"));
        }

        csv_data.push_str("# Detailed Table Information,\n");

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
        ]));

        let file = io::Cursor::new(csv_data);
        let mut reader = ReaderBuilder::new(schema)
            .with_header(true)
            .build(file)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        AgateTable::from_record_batch(Arc::new(batch))
    }

    fn create_mock_dbt_model(comment: Option<&str>, description: Option<&str>) -> DbtModel {
        use dbt_schemas::schemas::nodes::AdapterAttr;
        let _ = comment;

        // Use empty warehouse config for comment tests
        let warehouse_config = WarehouseSpecificNodeConfig::default();

        // Use the factory method to create adapter attributes
        let adapter_attr = AdapterAttr::from_config_and_dialect(&warehouse_config, "databricks");

        DbtModel {
            __common_attr__: CommonAttributes {
                name: "test_model".to_string(),
                fqn: vec!["test".to_string(), "test_model".to_string()],
                description: description.map(|s| s.to_string()),
                ..Default::default()
            },
            __base_attr__: NodeBaseAttributes {
                database: "test_db".to_string(),
                schema: "test_schema".to_string(),
                alias: "test_table".to_string(),
                relation_name: None,
                quoting: dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING,
                quoting_ignore_case: false,
                materialized: DbtMaterialization::StreamingTable,
                static_analysis: dbt_common::io_args::StaticAnalysisKind::On,
                enabled: true,
                extended_model: false,
                persist_docs: None,
                columns: BTreeMap::new(),
                refs: vec![],
                sources: vec![],
                metrics: vec![],
                depends_on: NodeDependsOn::default(),
            },
            __adapter_attr__: adapter_attr,
            deprecated_config: ModelConfig::default(),
            ..Default::default()
        }
    }

    #[test]
    fn test_processor_name() {
        let processor = CommentProcessor;
        assert_eq!(processor.name(), "comment");
    }

    #[test]
    fn test_from_relation_results_with_comment() {
        let processor = CommentProcessor;
        let table = create_mock_describe_extended_table(Some("Streaming table for events"));

        let results = DatabricksRelationResultsBuilder::new()
            .with_describe_extended(table)
            .build();

        let config = processor.from_relation_results(&results).unwrap();

        if let DatabricksComponentConfig::Comment(comment_config) = config {
            assert_eq!(
                comment_config.comment,
                Some("Streaming table for events".to_string())
            );
            assert!(!comment_config.persist);
        } else {
            panic!("Expected Comment config");
        }
    }

    #[test]
    fn test_from_relation_results_no_comment() {
        let processor = CommentProcessor;
        let table = create_mock_describe_extended_table(None);

        let results = DatabricksRelationResultsBuilder::new()
            .with_describe_extended(table)
            .build();

        let config = processor.from_relation_results(&results).unwrap();

        if let DatabricksComponentConfig::Comment(comment_config) = config {
            assert_eq!(comment_config.comment, None);
            assert!(!comment_config.persist);
        } else {
            panic!("Expected Comment config");
        }
    }

    #[test]
    fn test_from_relation_config_with_comment() {
        let processor = CommentProcessor;
        let model = create_mock_dbt_model(None, Some("Streaming table for testing"));

        let config = processor.from_relation_config(&model).unwrap().unwrap();

        if let DatabricksComponentConfig::Comment(comment_config) = config {
            assert_eq!(
                comment_config.comment,
                Some("Streaming table for testing".to_string())
            );
            assert!(!comment_config.persist);
        } else {
            panic!("Expected Comment config");
        }
    }

    #[test]
    fn test_from_relation_config_with_description() {
        let processor = CommentProcessor;
        let model = create_mock_dbt_model(None, Some("Table description"));

        let config = processor.from_relation_config(&model).unwrap().unwrap();

        if let DatabricksComponentConfig::Comment(comment_config) = config {
            assert_eq!(
                comment_config.comment,
                Some("Table description".to_string())
            );
            assert!(!comment_config.persist);
        } else {
            panic!("Expected Comment config");
        }
    }

    #[test]
    fn test_from_relation_config_only_uses_description() {
        let processor = CommentProcessor;
        let model = create_mock_dbt_model(Some("Ignored comment"), Some("Used description"));

        let config = processor.from_relation_config(&model).unwrap().unwrap();

        if let DatabricksComponentConfig::Comment(comment_config) = config {
            assert_eq!(comment_config.comment, Some("Used description".to_string()));
            assert!(!comment_config.persist);
        } else {
            panic!("Expected Comment config");
        }
    }

    #[test]
    fn test_from_relation_config_no_comment() {
        let processor = CommentProcessor;
        let model = create_mock_dbt_model(None, None);

        let config = processor.from_relation_config(&model).unwrap().unwrap();

        if let DatabricksComponentConfig::Comment(comment_config) = config {
            assert_eq!(comment_config.comment, None);
            assert!(!comment_config.persist);
        } else {
            panic!("Expected Comment config");
        }
    }

    #[test]
    fn test_comment_config_new() {
        let config = CommentConfig::new(Some("Test comment".to_string()), true);

        assert_eq!(config.comment, Some("Test comment".to_string()));
        assert!(config.persist);
    }
}
