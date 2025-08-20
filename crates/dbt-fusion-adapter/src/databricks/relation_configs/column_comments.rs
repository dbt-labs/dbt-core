// dbt/adapters/databricks/relation_configs/column_comments.py

use crate::AdapterResult;
use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationMetadataKey,
    DatabricksRelationResults,
};

use dbt_schemas::schemas::InternalDbtNodeAttributes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ColumnCommentsConfig {
    pub comments: BTreeMap<String, String>,
    pub quoted: BTreeMap<String, bool>,
    pub persist: bool,
}

impl ColumnCommentsConfig {
    pub fn new(
        comments: BTreeMap<String, String>,
        quoted: BTreeMap<String, bool>,
        persist: bool,
    ) -> Self {
        Self {
            comments,
            quoted,
            persist,
        }
    }

    pub fn get_diff(&self, other: &Self) -> Option<Self> {
        let mut comments = BTreeMap::new();

        if self.persist {
            for (column_name, comment) in &self.comments {
                if Some(comment) != other.comments.get(&column_name.to_lowercase()) {
                    let formatted_name = if self.quoted.get(column_name).copied().unwrap_or(false) {
                        format!("`{column_name}`")
                    } else {
                        column_name.clone()
                    };
                    comments.insert(formatted_name, comment.clone());
                }
            }

            if !comments.is_empty() {
                return Some(Self::new(comments, self.quoted.clone(), true));
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct ColumnCommentsProcessor;

impl DatabricksComponentProcessorProperties for ColumnCommentsProcessor {
    fn name(&self) -> &'static str {
        "column_comments"
    }
}

/// https://github.com/databricks/dbt-databricks/blob/4b5dcc534c74eba55ca75976277a82b94f5531ee/dbt/adapters/databricks/relation_configs/column_comments.py#L35
impl DatabricksComponentProcessor for ColumnCommentsProcessor {
    #[allow(clippy::wrong_self_convention)]
    fn from_relation_results(
        &self,
        results: &DatabricksRelationResults,
    ) -> Option<DatabricksComponentConfig> {
        // Get the describe_extended table from results
        let table = results.get(&DatabricksRelationMetadataKey::DescribeExtended)?;
        let mut comments = BTreeMap::new();

        // Iterate through rows looking for column information
        for row in table.rows().into_iter() {
            // Get col_name - if it starts with #, we've reached the end of columns
            if let Ok(col_name_value) = row.get_attr("col_name") {
                if let Some(col_name_str) = col_name_value.as_str() {
                    if col_name_str.starts_with('#') {
                        break;
                    }

                    // Skip empty column names (metadata rows)
                    if col_name_str.trim().is_empty() {
                        continue;
                    }

                    // Get the comment for this column (default to empty string if None)
                    let comment = if let Ok(comment_value) = row.get_attr("comment") {
                        comment_value.as_str().unwrap_or("").to_string()
                    } else {
                        String::new()
                    };

                    comments.insert(col_name_str.to_lowercase(), comment);
                }
            }
        }

        Some(DatabricksComponentConfig::ColumnComments(
            ColumnCommentsConfig::new(comments, BTreeMap::new(), false),
        ))
    }

    fn from_relation_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>> {
        let columns = &relation_config.base().columns;

        // Check if persist_docs.relation is enabled
        let persist = relation_config
            .base()
            .persist_docs
            .as_ref()
            .map(|pd| pd.relation.unwrap_or(false))
            .unwrap_or(false);

        let mut comments = BTreeMap::new();
        let mut quoted = BTreeMap::new();

        for (column_name, column) in columns {
            comments.insert(
                column_name.clone(),
                column
                    .description
                    .as_ref()
                    .unwrap_or(&String::new())
                    .clone(),
            );
            quoted.insert(column_name.clone(), column.quote.unwrap_or(false));
        }

        Ok(Some(DatabricksComponentConfig::ColumnComments(
            ColumnCommentsConfig::new(comments, quoted, persist),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::databricks::relation_configs::base::{
        DatabricksRelationChangeSet, DatabricksRelationResultsBuilder,
    };
    use arrow::array::{ArrayRef, RecordBatch, StringArray};
    use arrow::csv::ReaderBuilder;
    use arrow_schema::{DataType, Field, Schema};
    use dbt_agate::AgateTable;
    use dbt_schemas::schemas::DbtModel;
    use dbt_schemas::schemas::relations::relation_configs::{ComponentConfig, RelationChangeSet};
    use regex::Regex;
    use std::collections::BTreeMap;
    use std::io;
    use std::sync::{Arc, LazyLock};

    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("col_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("comment", DataType::Utf8, false),
        ]))
    });

    // treat empty strings in CSV as empty strings, not nulls
    static NULL: LazyLock<Regex> = LazyLock::new(|| Regex::new("NULL").unwrap());

    fn create_mock_describe_extended_table() -> AgateTable {
        let file = io::Cursor::new(
            "col_name,data_type,comment\n\
id,int,Primary key identifier\n\
name,string,User name\n\
email,string,\n\
# Detailed Table Information,,\n",
        );
        let mut reader = ReaderBuilder::new(Arc::clone(&SCHEMA))
            .with_header(true)
            .with_null_regex(NULL.clone())
            .build(file)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        AgateTable::from_record_batch(Arc::new(batch))
    }

    fn create_mock_dbt_model(
        columns: BTreeMap<String, dbt_schemas::schemas::dbt_column::DbtColumn>,
        meta: BTreeMap<String, serde_json::Value>,
    ) -> DbtModel {
        use dbt_schemas::schemas::project::*;
        use dbt_schemas::schemas::{common::*, nodes::*};

        // Extract persist_docs from meta
        let persist_docs = meta.get("persist_docs").map(|v| PersistDocsConfig {
            relation: v.get("relation").and_then(|r| r.as_bool()),
            columns: v.get("columns").and_then(|c| c.as_bool()),
        });

        let base_attrs = NodeBaseAttributes {
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
            alias: "test_table".to_string(),
            relation_name: None,
            quoting: dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING,
            quoting_ignore_case: false,
            materialized: DbtMaterialization::Table,
            static_analysis: dbt_common::io_args::StaticAnalysisKind::On,
            enabled: true,
            extended_model: false,
            persist_docs,
            columns,
            refs: vec![],
            sources: vec![],
            metrics: vec![],
            depends_on: NodeDependsOn::default(),
        };

        let deprecated_config = ModelConfig::default();

        DbtModel {
            __base_attr__: base_attrs,
            deprecated_config,
            ..Default::default()
        }
    }

    #[test]
    fn test_processor_name() {
        let processor = ColumnCommentsProcessor;
        assert_eq!(processor.name(), "column_comments");
    }

    #[test]
    fn test_from_relation_results() {
        let processor = ColumnCommentsProcessor;
        let table = create_mock_describe_extended_table();

        let results = DatabricksRelationResultsBuilder::new()
            .with_describe_extended(table)
            .build();

        let component = processor.from_relation_results(&results);
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::ColumnComments(config)) = component {
            assert_eq!(config.comments.len(), 3);
            assert_eq!(
                config.comments.get("id"),
                Some(&"Primary key identifier".to_string())
            );
            assert_eq!(config.comments.get("name"), Some(&"User name".to_string()));
            assert_eq!(config.comments.get("email"), Some(&"".to_string()));
            assert!(!config.persist);
        } else {
            panic!("Expected ColumnComments config");
        }
    }

    #[test]
    fn test_from_relation_results_missing_describe_extended() {
        let processor = ColumnCommentsProcessor;

        // Create results without describe_extended table
        let results = DatabricksRelationResultsBuilder::new().build();

        let component = processor.from_relation_results(&results);
        assert!(component.is_none());
    }

    #[test]
    fn test_from_relation_results_empty_table() {
        let processor = ColumnCommentsProcessor;

        let record_batch = RecordBatch::try_new(
            Arc::clone(&SCHEMA),
            vec![
                Arc::new(StringArray::new_null(0)) as ArrayRef,
                Arc::new(StringArray::new_null(0)) as ArrayRef,
                Arc::new(StringArray::new_null(0)) as ArrayRef,
            ],
        )
        .unwrap();

        let table = AgateTable::from_record_batch(Arc::new(record_batch));

        let results = DatabricksRelationResultsBuilder::new()
            .with_describe_extended(table)
            .build();

        let component = processor.from_relation_results(&results);
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::ColumnComments(config)) = component {
            assert_eq!(config.comments.len(), 0);
            assert!(!config.persist);
        } else {
            panic!("Expected ColumnComments config");
        }
    }

    #[test]
    fn test_from_relation_results_mixed_case_columns() {
        let processor = ColumnCommentsProcessor;

        let file = io::Cursor::new(
            "col_name,data_type,comment\n\
ID,int,Primary key\n\
Name,string,User name\n\
EMAIL,string,Email address\n\
# Detailed Table Information,,\n",
        );
        let mut reader = ReaderBuilder::new(Arc::clone(&SCHEMA))
            .with_header(true)
            .with_null_regex(NULL.clone())
            .build(file)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let table = AgateTable::from_record_batch(Arc::new(batch));

        let results = DatabricksRelationResultsBuilder::new()
            .with_describe_extended(table)
            .build();

        let component = processor.from_relation_results(&results);
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::ColumnComments(config)) = component {
            assert_eq!(config.comments.len(), 3);
            // All keys should be lowercase
            assert_eq!(config.comments.get("id"), Some(&"Primary key".to_string()));
            assert_eq!(config.comments.get("name"), Some(&"User name".to_string()));
            assert_eq!(
                config.comments.get("email"),
                Some(&"Email address".to_string())
            );
        } else {
            panic!("Expected ColumnComments config");
        }
    }

    #[test]
    fn test_from_relation_results_delimiter_variations() {
        let processor = ColumnCommentsProcessor;

        let file = io::Cursor::new(
            "col_name,data_type,comment\n\
id,int,Primary key\n\
#Detailed Table Information,,\n\
name,string,Should not be included\n",
        );
        let mut reader = ReaderBuilder::new(Arc::clone(&SCHEMA))
            .with_header(true)
            .with_null_regex(NULL.clone())
            .build(file)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let table = AgateTable::from_record_batch(Arc::new(batch));

        let results = DatabricksRelationResultsBuilder::new()
            .with_describe_extended(table)
            .build();

        let component = processor.from_relation_results(&results);
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::ColumnComments(config)) = component {
            // Should stop at first # delimiter, so only 'id' should be included
            assert_eq!(config.comments.len(), 1);
            assert_eq!(config.comments.get("id"), Some(&"Primary key".to_string()));
            assert!(!config.comments.contains_key("name"));
        } else {
            panic!("Expected ColumnComments config");
        }
    }

    #[test]
    fn test_from_relation_results_missing_comment_column() {
        let processor = ColumnCommentsProcessor;

        let file = io::Cursor::new(
            "col_name,data_type\n\
id,int\n\
name,string\n",
        );
        let mut reader = ReaderBuilder::new(Arc::new(Schema::new(
            [
                SCHEMA.fields()[0].clone(),
                SCHEMA.fields()[1].clone(),
                // Missing comment column
            ]
            .to_vec(),
        )))
        .with_header(true)
        .with_null_regex(NULL.clone())
        .build(file)
        .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let table = AgateTable::from_record_batch(Arc::new(batch));

        let results = DatabricksRelationResultsBuilder::new()
            .with_describe_extended(table)
            .build();

        let component = processor.from_relation_results(&results);
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::ColumnComments(config)) = component {
            assert_eq!(config.comments.len(), 2);
            // Should default to empty string when comment column is missing
            assert_eq!(config.comments.get("id"), Some(&"".to_string()));
            assert_eq!(config.comments.get("name"), Some(&"".to_string()));
        } else {
            panic!("Expected ColumnComments config");
        }
    }

    #[test]
    fn test_from_relation_results_skips_empty_column_names() {
        let processor = ColumnCommentsProcessor;

        let file = io::Cursor::new(
            "col_name,data_type,comment\n\
id,int,Primary key\n\
,,\n\
name,string,User name\n\
  ,,\n\
# Detailed Table Information,'',''\n",
        );
        let mut reader = ReaderBuilder::new(Arc::clone(&SCHEMA))
            .with_header(true)
            .with_null_regex(NULL.clone())
            .build(file)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let table = AgateTable::from_record_batch(Arc::new(batch));

        let results = DatabricksRelationResultsBuilder::new()
            .with_describe_extended(table)
            .build();

        let component = processor.from_relation_results(&results);
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::ColumnComments(config)) = component {
            // Should only have 2 valid columns, skipping empty and whitespace-only names
            assert_eq!(config.comments.len(), 2);
            assert_eq!(config.comments.get("id"), Some(&"Primary key".to_string()));
            assert_eq!(config.comments.get("name"), Some(&"User name".to_string()));
            // Should not contain empty column name
            assert!(!config.comments.contains_key(""));
        } else {
            panic!("Expected ColumnComments config");
        }
    }

    #[test]
    fn test_from_relation_config_with_persist() {
        use dbt_schemas::schemas::dbt_column::DbtColumn;
        use serde_json::json;

        let processor = ColumnCommentsProcessor;

        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            DbtColumn {
                name: "id".to_string(),
                description: Some("Primary key".to_string()),
                quote: Some(false),
                ..Default::default()
            },
        );
        columns.insert(
            "name".to_string(),
            DbtColumn {
                name: "name".to_string(),
                description: Some("User name".to_string()),
                quote: Some(true),
                ..Default::default()
            },
        );

        let mut meta = BTreeMap::new();
        meta.insert(
            "persist_docs".to_string(),
            json!({
                "relation": true
            }),
        );

        let mock_node = create_mock_dbt_model(columns, meta);
        let result = processor.from_relation_config(&mock_node);

        assert!(result.is_ok());
        let component = result.unwrap();
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::ColumnComments(config)) = component {
            assert!(config.persist);
            assert_eq!(config.comments.len(), 2);
            assert_eq!(config.comments.get("id"), Some(&"Primary key".to_string()));
            assert_eq!(config.comments.get("name"), Some(&"User name".to_string()));
            assert_eq!(config.quoted.get("id"), Some(&false));
            assert_eq!(config.quoted.get("name"), Some(&true));
        } else {
            panic!("Expected ColumnComments config");
        }
    }

    #[test]
    fn test_from_relation_config_without_persist() {
        use dbt_schemas::schemas::dbt_column::DbtColumn;

        let processor = ColumnCommentsProcessor;

        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            DbtColumn {
                name: "id".to_string(),
                description: Some("Primary key".to_string()),
                quote: Some(false),
                ..Default::default()
            },
        );

        let meta = BTreeMap::new(); // No persist_docs

        let mock_node = create_mock_dbt_model(columns, meta);
        let result = processor.from_relation_config(&mock_node);

        assert!(result.is_ok());
        let component = result.unwrap();
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::ColumnComments(config)) = component {
            assert!(!config.persist);
            assert_eq!(config.comments.len(), 1);
            assert_eq!(config.comments.get("id"), Some(&"Primary key".to_string()));
        } else {
            panic!("Expected ColumnComments config");
        }
    }

    #[test]
    fn test_from_relation_config_persist_false() {
        use dbt_schemas::schemas::dbt_column::DbtColumn;
        use serde_json::json;

        let processor = ColumnCommentsProcessor;

        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            DbtColumn {
                name: "id".to_string(),
                description: Some("Primary key".to_string()),
                quote: Some(false),
                ..Default::default()
            },
        );

        let mut meta = BTreeMap::new();
        meta.insert(
            "persist_docs".to_string(),
            json!({
                "relation": false
            }),
        );

        let mock_node = create_mock_dbt_model(columns, meta);
        let result = processor.from_relation_config(&mock_node);

        assert!(result.is_ok());
        let component = result.unwrap();
        assert!(component.is_some());

        if let Some(DatabricksComponentConfig::ColumnComments(config)) = component {
            assert!(!config.persist);
        } else {
            panic!("Expected ColumnComments config");
        }
    }

    #[test]
    fn test_column_comments_get_diff() {
        let mut new_comments = BTreeMap::new();
        new_comments.insert("id".to_string(), "Updated primary key".to_string());
        new_comments.insert("name".to_string(), "User full name".to_string());

        let mut quoted = BTreeMap::new();
        quoted.insert("id".to_string(), false);
        quoted.insert("name".to_string(), true);

        let new_config = ColumnCommentsConfig::new(new_comments, quoted.clone(), true);

        let mut old_comments = BTreeMap::new();
        old_comments.insert("id".to_string(), "Primary key".to_string());
        old_comments.insert("name".to_string(), "User name".to_string());

        let old_config = ColumnCommentsConfig::new(old_comments, quoted, false);

        let diff = new_config.get_diff(&old_config);
        assert!(diff.is_some());

        let diff_config = diff.unwrap();
        assert!(diff_config.persist);
        assert_eq!(diff_config.comments.len(), 2);
        assert_eq!(
            diff_config.comments.get("id"),
            Some(&"Updated primary key".to_string())
        );
        assert_eq!(
            diff_config.comments.get("`name`"),
            Some(&"User full name".to_string())
        );
    }

    #[test]
    fn test_changeset_integration() {
        let mut old_comments = BTreeMap::new();
        old_comments.insert("id".to_string(), "Old description".to_string());

        let old_config = DatabricksComponentConfig::ColumnComments(ColumnCommentsConfig::new(
            old_comments,
            BTreeMap::new(),
            true,
        ));

        let mut new_comments = BTreeMap::new();
        new_comments.insert("id".to_string(), "New description".to_string());
        new_comments.insert("email".to_string(), "New email field".to_string());

        let new_config = DatabricksComponentConfig::ColumnComments(ColumnCommentsConfig::new(
            new_comments,
            BTreeMap::new(),
            true,
        ));

        let diff = new_config.get_diff(&old_config);
        assert!(diff.is_some());

        let mut changes = BTreeMap::new();
        changes.insert("column_comments".to_string(), diff.unwrap());

        let changeset = DatabricksRelationChangeSet::new(changes, false);

        assert!(changeset.has_changes());
        assert!(!changeset.requires_full_refresh());
        assert!(changeset.get_change("column_comments").is_some());
    }
}
