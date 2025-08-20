// dbt/adapters/databricks/relation_configs/partitioning.py

use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationMetadataKey,
    DatabricksRelationResults,
};

use crate::AdapterResult;
use crate::errors::{AdapterError, AdapterErrorKind};
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use minijinja::Value;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PartitionedByConfig {
    pub partition_by: Vec<String>,
}

impl PartitionedByConfig {
    pub fn new(partition_by: Vec<String>) -> Self {
        Self { partition_by }
    }
}

#[derive(Debug)]
pub struct PartitionedByProcessor;

impl DatabricksComponentProcessorProperties for PartitionedByProcessor {
    fn name(&self) -> &'static str {
        "partitioned_by"
    }
}

/// https://github.com/databricks/dbt-databricks/blob/8fc69739c4885648bb95074e796c67a57fc9995f/dbt/adapters/databricks/relation_configs/partitioning.py#L19
impl DatabricksComponentProcessor for PartitionedByProcessor {
    fn from_relation_results(
        &self,
        results: &DatabricksRelationResults,
    ) -> Option<DatabricksComponentConfig> {
        let describe_extended = results.get(&DatabricksRelationMetadataKey::DescribeExtended)?;

        let mut partition_cols = Vec::new();
        let mut found_partition_section = false;

        // Find partition information section in describe_extended output
        for row in describe_extended.rows() {
            if let Ok(first_col) = row.get_item(&Value::from(0)) {
                if let Some(first_str) = first_col.as_str() {
                    if first_str == "# Partition Information" {
                        found_partition_section = true;
                        continue;
                    }

                    if found_partition_section {
                        if first_str.is_empty() {
                            break; // End of partition section
                        }
                        if !first_str.starts_with("# ") {
                            partition_cols.push(first_str.to_string());
                        }
                    }
                }
            }
        }

        Some(DatabricksComponentConfig::PartitionedBy(
            PartitionedByConfig::new(partition_cols),
        ))
    }

    // https://github.com/databricks/dbt-databricks/blob/92f1442faabe0fce6f0375b95e46ebcbfcea4c67/dbt/adapters/databricks/relation_configs/partitioning.py#L37
    fn from_relation_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>> {
        use dbt_schemas::schemas::DbtModel;
        use dbt_schemas::schemas::manifest::PartitionConfig;

        let partition_by_result = relation_config
            .as_any()
            .downcast_ref::<DbtModel>()
            .and_then(|model| model.__adapter_attr__.databricks_attr.as_ref())
            .and_then(|dbx_attr| dbx_attr.partition_by.as_ref())
            .map(|p| -> AdapterResult<Vec<String>> {
                match p {
                    PartitionConfig::String(s) => Ok(vec![s.clone()]),
                    PartitionConfig::List(list) => Ok(list.clone()),
                    _ => Err(AdapterError::new(
                        AdapterErrorKind::Configuration,
                        format!("Invalid partition config: {p:?}"),
                    )),
                }
            })
            .transpose()?
            .unwrap_or_default();

        Ok(Some(DatabricksComponentConfig::PartitionedBy(
            PartitionedByConfig::new(partition_by_result),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::databricks::relation_configs::base::DatabricksRelationResultsBuilder;
    use dbt_agate::AgateTable;
    use dbt_schemas::schemas::{common::*, nodes::*, project::*};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn create_mock_describe_extended_table(partition_columns: Vec<&str>) -> AgateTable {
        use arrow::csv::ReaderBuilder;
        use arrow_schema::{DataType, Field, Schema};
        use std::io;
        use std::sync::Arc;

        let mut csv_data = "key,value\n".to_string();

        // Add regular table info rows
        csv_data.push_str("Table,test_table\n");
        csv_data.push_str("Owner,test_user\n");

        // Add partition information section
        if !partition_columns.is_empty() {
            csv_data.push_str("# Partition Information,\n");
            csv_data.push_str("# col_name,data_type\n");
            for col in partition_columns {
                csv_data.push_str(&format!("{col},string\n"));
            }
            csv_data.push_str(",\n");
        }

        // Add remaining info
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

    fn create_mock_dbt_model(partition_by: Option<serde_json::Value>) -> DbtModel {
        use dbt_schemas::schemas::manifest::PartitionConfig;
        use dbt_schemas::schemas::nodes::AdapterAttr;
        use dbt_schemas::schemas::project::WarehouseSpecificNodeConfig;

        let partition_config = if let Some(partition_value) = partition_by {
            match partition_value {
                serde_json::Value::String(s) => Some(PartitionConfig::String(s)),
                serde_json::Value::Array(arr) => {
                    let strings: Vec<String> = arr
                        .into_iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                    Some(PartitionConfig::List(strings))
                }
                _ => None,
            }
        } else {
            None
        };

        let warehouse_config = WarehouseSpecificNodeConfig {
            partition_by: partition_config,
            ..Default::default()
        };

        let deprecated_config = ModelConfig {
            __warehouse_specific_config__: warehouse_config.clone(),
            ..Default::default()
        };

        // Use the factory method to create adapter attributes
        let adapter_attr = AdapterAttr::from_config_and_dialect(&warehouse_config, "databricks");

        DbtModel {
            __common_attr__: CommonAttributes {
                name: "test_model".to_string(),
                fqn: vec!["test".to_string(), "test_model".to_string()],
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
            deprecated_config,
            ..Default::default()
        }
    }

    #[test]
    fn test_processor_name() {
        let processor = PartitionedByProcessor;
        assert_eq!(processor.name(), "partitioned_by");
    }

    #[test]
    fn test_from_relation_results_with_partitions() {
        let processor = PartitionedByProcessor;
        let table = create_mock_describe_extended_table(vec!["event_name", "user_id"]);

        let results = DatabricksRelationResultsBuilder::new()
            .with_describe_extended(table)
            .build();

        let config = processor.from_relation_results(&results).unwrap();

        if let DatabricksComponentConfig::PartitionedBy(partition_config) = config {
            assert_eq!(partition_config.partition_by, vec!["event_name", "user_id"]);
        } else {
            panic!("Expected PartitionedBy config");
        }
    }

    #[test]
    fn test_from_relation_results_no_partitions() {
        let processor = PartitionedByProcessor;
        let table = create_mock_describe_extended_table(vec![]);

        let results = DatabricksRelationResultsBuilder::new()
            .with_describe_extended(table)
            .build();

        let config = processor.from_relation_results(&results).unwrap();

        if let DatabricksComponentConfig::PartitionedBy(partition_config) = config {
            assert!(partition_config.partition_by.is_empty());
        } else {
            panic!("Expected PartitionedBy config");
        }
    }

    #[test]
    fn test_from_relation_config_string() {
        let processor = PartitionedByProcessor;
        let model = create_mock_dbt_model(Some(json!("event_name")));

        let config = processor.from_relation_config(&model).unwrap().unwrap();

        if let DatabricksComponentConfig::PartitionedBy(partition_config) = config {
            assert_eq!(partition_config.partition_by, vec!["event_name"]);
        } else {
            panic!("Expected PartitionedBy config");
        }
    }

    #[test]
    fn test_from_relation_config_array() {
        let processor = PartitionedByProcessor;
        let model = create_mock_dbt_model(Some(json!(["event_name", "user_id"])));

        let config = processor.from_relation_config(&model).unwrap().unwrap();

        if let DatabricksComponentConfig::PartitionedBy(partition_config) = config {
            assert_eq!(partition_config.partition_by, vec!["event_name", "user_id"]);
        } else {
            panic!("Expected PartitionedBy config");
        }
    }

    #[test]
    fn test_from_relation_config_none() {
        let processor = PartitionedByProcessor;
        let model = create_mock_dbt_model(None);

        let config = processor.from_relation_config(&model).unwrap().unwrap();

        if let DatabricksComponentConfig::PartitionedBy(partition_config) = config {
            assert!(partition_config.partition_by.is_empty());
        } else {
            panic!("Expected PartitionedBy config ");
        }
    }
}
