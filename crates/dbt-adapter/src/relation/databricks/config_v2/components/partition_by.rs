// https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/partitioning.py

use crate::relation::config_v2::{
    ComponentConfig, ComponentConfigLoader, SimpleComponentConfigImpl, diff,
};
use crate::relation::databricks::config_v2::{
    DatabricksRelationMetadata, DatabricksRelationMetadataKey,
};
use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::manifest::PartitionConfig;
use minijinja::value::{Value, ValueMap};

pub(crate) const TYPE_NAME: &str = "partitioned_by";

// TODO(serramatutu): reuse this for `partition_by` in other warehouses
/// Component for Databricks partitioned by
///
/// Holds a vec of columns to partition by.
pub type PartitionBy = SimpleComponentConfigImpl<Vec<String>>;

fn to_jinja(v: &Vec<String>) -> Value {
    Value::from(ValueMap::from([(
        Value::from("partition_by"),
        Value::from_serialize(v),
    )]))
}

fn new(partition_by: Vec<String>) -> PartitionBy {
    PartitionBy {
        type_name: TYPE_NAME,
        diff_fn: diff::desired_state,
        to_jinja_fn: to_jinja,
        value: partition_by,
    }
}

fn from_remote_state(results: &DatabricksRelationMetadata) -> PartitionBy {
    let Some(describe_extended) = results.get(&DatabricksRelationMetadataKey::DescribeExtended)
    else {
        return new(Vec::new());
    };

    let mut partition_cols = Vec::new();
    let mut found_partition_section = false;

    // Find partition information section in describe_extended output
    for row in describe_extended.rows() {
        if let Ok(first_col) = row.get_item(&Value::from(0))
            && let Some(first_str) = first_col.as_str()
        {
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

    new(partition_cols)
}

fn from_local_config(relation_config: &dyn InternalDbtNodeAttributes) -> PartitionBy {
    new(relation_config
        .as_any()
        .downcast_ref::<DbtModel>()
        .and_then(|model| model.__adapter_attr__.databricks_attr.as_ref())
        .and_then(|dbx_attr| dbx_attr.partition_by.as_ref())
        .map(|p| {
            match p {
                PartitionConfig::String(s) => vec![s.clone()],
                PartitionConfig::List(list) => list.clone(),
                // FIXME(serramatutu): this will silently ignore BigQuery config
                // in Databricks. We should be able to just accept this to reduce
                // divergence between adapter types
                PartitionConfig::BigqueryPartitionConfig(_) => Vec::new(),
            }
        })
        .unwrap_or_default())
}

pub(crate) struct PartitionByLoader;

impl PartitionByLoader {
    pub fn new(partition_by: Vec<String>) -> Box<dyn ComponentConfig> {
        Box::new(new(partition_by))
    }

    pub fn type_name() -> &'static str {
        TYPE_NAME
    }
}

impl ComponentConfigLoader<DatabricksRelationMetadata> for PartitionByLoader {
    fn type_name(&self) -> &'static str {
        TYPE_NAME
    }

    fn from_remote_state(
        &self,
        remote_state: &DatabricksRelationMetadata,
    ) -> Box<dyn ComponentConfig> {
        Box::new(from_remote_state(remote_state))
    }

    fn from_local_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> Box<dyn ComponentConfig> {
        Box::new(from_local_config(relation_config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::databricks::config_v2::test_helpers;
    use dbt_agate::AgateTable;
    use indexmap::IndexMap;
    use std::sync::Arc;

    fn create_mock_describe_extended_table(partition_columns: Vec<&str>) -> AgateTable {
        use arrow::csv::ReaderBuilder;
        use arrow_schema::{DataType, Field, Schema};
        use std::io;

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

    fn create_mock_dbt_model(partition_by: &[&str]) -> DbtModel {
        let cfg = test_helpers::TestModelConfig {
            partition_by: partition_by.iter().map(|s| (*s).to_string()).collect(),
            ..Default::default()
        };
        test_helpers::create_mock_dbt_model(cfg)
    }

    #[test]
    fn test_from_remote_state_with_partitions() {
        let table = create_mock_describe_extended_table(vec!["event_name", "user_id"]);
        let results = IndexMap::from([(DatabricksRelationMetadataKey::DescribeExtended, table)]);
        let config = from_remote_state(&results);

        assert_eq!(config.value, vec!["event_name", "user_id"]);
    }

    #[test]
    fn test_from_remote_state_no_partitions() {
        let table = create_mock_describe_extended_table(vec![]);
        let results = IndexMap::from([(DatabricksRelationMetadataKey::DescribeExtended, table)]);
        let config = from_remote_state(&results);

        assert!(config.value.is_empty());
    }

    #[test]
    fn test_from_local_config() {
        let model = create_mock_dbt_model(&["event_name"]);
        let config = from_local_config(&model);

        assert_eq!(config.value, vec!["event_name"]);
    }

    #[test]
    fn test_from_local_config_none() {
        let model = create_mock_dbt_model(&[]);
        let config = from_local_config(&model);

        assert!(config.value.is_empty());
    }
}
