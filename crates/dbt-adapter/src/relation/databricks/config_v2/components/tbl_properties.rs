//! https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/tblproperties.py

use crate::relation::config_v2::{
    ComponentConfig, ComponentConfigLoader, SimpleComponentConfigImpl,
};
use crate::relation::databricks::config_v2::{
    DatabricksRelationMetadata, DatabricksRelationMetadataKey,
};
use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_serde_yaml::Value as YmlValue;
use indexmap::{IndexMap, IndexSet};
use minijinja::value::{Value, ValueMap};

pub(crate) const TYPE_NAME: &str = "tblproperties";

const PIPELINE_ID_KEY: &str = "pipelines.pipelineId";

/// All of the following keys are ignoring by the diffing function
///
/// These are generally set by databricks and cannot be modified by the user
const EQ_IGNORE_LIST: [&str; 24] = [
    PIPELINE_ID_KEY,
    "delta.enableChangeDataFeed",
    "delta.minReaderVersion",
    "delta.minWriterVersion",
    "pipeline_internal.catalogType",
    "pipelines.metastore.tableName",
    "pipeline_internal.enzymeMode",
    "clusterByAuto",
    "clusteringColumns",
    "delta.enableRowTracking",
    "delta.feature.appendOnly",
    "delta.feature.changeDataFeed",
    "delta.feature.checkConstraints",
    "delta.feature.domainMetadata",
    "delta.feature.generatedColumns",
    "delta.feature.invariants",
    "delta.feature.rowTracking",
    "delta.rowTracking.materializedRowCommitVersionColumnName",
    "delta.rowTracking.materializedRowIdColumnName",
    "spark.internal.pipelines.top_level_entry.user_specified_name",
    "delta.columnMapping.maxColumnId",
    "spark.sql.internal.pipelines.parentTableId",
    "delta.enableDeletionVectors",
    "delta.feature.deletionVectors",
];

/// Component for Databricks table properties
///
/// Holds a IndexMap of tag key and values.
pub type TblProperties = SimpleComponentConfigImpl<IndexMap<String, String>>;

fn to_jinja(v: &IndexMap<String, String>) -> Value {
    // FIXME: is there a way to ignore a key and serialize into Value without an extra allocation?
    let ignore_pipeline = v
        .iter()
        .filter(|(k, _v)| k.as_str() != PIPELINE_ID_KEY)
        .collect::<IndexMap<_, _>>();

    Value::from(ValueMap::from([
        (
            Value::from("tblproperties"),
            Value::from_serialize(ignore_pipeline),
        ),
        (
            Value::from("pipeline_id"),
            Value::from_serialize(v.get(PIPELINE_ID_KEY)),
        ),
    ]))
}

fn new(properties: IndexMap<String, String>) -> TblProperties {
    TblProperties {
        type_name: TYPE_NAME,
        diff_fn: diff,
        to_jinja_fn: to_jinja,
        value: properties,
    }
}

/// Takes the diff between two `TblProperties` by only comparing the non-ignored keys
fn diff(
    desired_state: &IndexMap<String, String>,
    current_state: &IndexMap<String, String>,
) -> Option<IndexMap<String, String>> {
    let all_keys: IndexSet<_> = desired_state
        .keys()
        .chain(current_state.keys())
        .filter(|key| !EQ_IGNORE_LIST.contains(&key.as_str()))
        .collect();

    let changed_keys: IndexMap<String, String> = all_keys
        .into_iter()
        .filter_map(|key| {
            let desired_val = desired_state.get(key.as_str());
            if desired_val.is_some() && desired_val != current_state.get(key.as_str()) {
                let desired_val = desired_val.cloned().unwrap_or_default();
                Some((key.clone(), desired_val))
            } else {
                None
            }
        })
        .collect();

    if !changed_keys.is_empty() {
        Some(changed_keys)
    } else {
        None
    }
}

fn from_remote_state(results: &DatabricksRelationMetadata) -> TblProperties {
    let Some(table) = results.get(&DatabricksRelationMetadataKey::ShowTblProperties) else {
        return new(IndexMap::new());
    };

    let mut tblproperties = IndexMap::new();
    for row in table.rows() {
        if let (Ok(key_val), Ok(value_val)) =
            (row.get_item(&Value::from(0)), row.get_item(&Value::from(1)))
            && let (Some(key_str), Some(value_str)) = (key_val.as_str(), value_val.as_str())
        {
            if key_str == PIPELINE_ID_KEY || !EQ_IGNORE_LIST.contains(&key_str) {
                tblproperties.insert(key_str.to_string(), value_str.to_string());
            }
        }
    }

    new(tblproperties)
}

fn from_local_config(relation_config: &dyn InternalDbtNodeAttributes) -> TblProperties {
    let Some(model) = relation_config.as_any().downcast_ref::<DbtModel>() else {
        return new(IndexMap::new());
    };

    let mut tblproperties = IndexMap::new();

    // Extract tblproperties from databricks_attr
    if let Some(databricks_attr) = &model.__adapter_attr__.databricks_attr
        && let Some(props_map) = &databricks_attr.tblproperties
    {
        for (key, value) in props_map {
            if let YmlValue::String(value_str, _) = value {
                tblproperties.insert(key.clone(), value_str.clone());
            }
        }
    }

    // Check for Iceberg table format using direct field
    let is_iceberg = model
        .deprecated_config
        .table_format
        .as_ref()
        .is_some_and(|s| s == "iceberg");

    if is_iceberg {
        tblproperties.insert(
            "delta.enableIcebergCompatV2".to_string(),
            "true".to_string(),
        );
        tblproperties.insert(
            "delta.universalFormat.enabledFormats".to_string(),
            "iceberg".to_string(),
        );
    }

    new(tblproperties)
}

pub(crate) struct TblPropertiesLoader;

impl TblPropertiesLoader {
    pub fn new(properties: IndexMap<String, String>) -> Box<dyn ComponentConfig> {
        Box::new(new(properties))
    }

    pub fn type_name() -> &'static str {
        TYPE_NAME
    }
}

impl ComponentConfigLoader<DatabricksRelationMetadata> for TblPropertiesLoader {
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
    use dbt_schemas::schemas::DbtModel;
    use indexmap::IndexMap;
    use std::sync::Arc;

    fn create_mock_show_tblproperties_table(properties: Vec<(&str, &str)>) -> AgateTable {
        use arrow::csv::ReaderBuilder;
        use arrow_schema::{DataType, Field, Schema};
        use std::io;

        let mut csv_data = "key,value\n".to_string();
        for (key, value) in properties {
            csv_data.push_str(&format!("{key},{value}\n"));
        }

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

    fn create_mock_dbt_model(
        tblproperties: IndexMap<&str, &str>,
        table_format: Option<&str>,
    ) -> DbtModel {
        let cfg = test_helpers::TestModelConfig {
            tbl_properties: tblproperties
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            table_format: table_format.map(|s| s.to_string()),
            ..Default::default()
        };
        test_helpers::create_mock_dbt_model(cfg)
    }

    #[test]
    fn test_diff_changed_databricks_keys() {
        let prev = IndexMap::from_iter([
            (
                "pipelines.pipelineId".to_string(),
                "pipeline123".to_string(),
            ),
            ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
        ]);
        let next = IndexMap::from_iter([
            (
                "pipelines.pipelineId".to_string(),
                "pipeline123456".to_string(),
            ),
            (
                "delta.enableChangeDataFeed".to_string(),
                "false".to_string(),
            ),
        ]);

        let diff = diff(&next, &prev);
        assert!(diff.is_none());
    }

    #[test]
    fn test_diff_changed_custom_keys() {
        let prev = IndexMap::from_iter([
            (
                "pipelines.pipelineId".to_string(),
                "pipeline123".to_string(),
            ),
            ("custom.change".to_string(), "old".to_string()),
            ("custom.drop".to_string(), "old".to_string()),
        ]);
        let next = IndexMap::from_iter([
            (
                "pipelines.pipelineId".to_string(),
                "pipeline123456".to_string(),
            ),
            ("custom.change".to_string(), "new".to_string()),
            ("custom.add".to_string(), "new".to_string()),
        ]);

        let diff = diff(&next, &prev).unwrap();

        assert_eq!(diff.len(), 2);
        assert_eq!(diff.get("custom.change").unwrap().as_str(), "new");
        assert_eq!(diff.get("custom.add").unwrap().as_str(), "new");
    }

    #[test]
    fn test_from_remote_state() {
        let table = create_mock_show_tblproperties_table(vec![
            ("streaming.checkpointLocation", "/tmp/checkpoint"),
            ("streaming.outputMode", "append"),
            ("custom.property", "test_value"),
            ("pipelines.pipelineId", "pipeline123"),
            ("delta.enableChangeDataFeed", "true"), // Should be ignored
        ]);

        let results = IndexMap::from([(DatabricksRelationMetadataKey::ShowTblProperties, table)]);
        let config = from_remote_state(&results);

        assert_eq!(config.value.len(), 4); // Ignores delta properties
        assert_eq!(
            config.value.get("streaming.checkpointLocation"),
            Some(&"/tmp/checkpoint".to_string())
        );
        assert_eq!(
            config.value.get("streaming.outputMode"),
            Some(&"append".to_string())
        );
        assert_eq!(
            config.value.get("custom.property"),
            Some(&"test_value".to_string())
        );
        assert_eq!(
            config.value.get(PIPELINE_ID_KEY),
            Some(&"pipeline123".to_string())
        );
        assert!(!config.value.contains_key("delta.enableChangeDataFeed"));
    }

    #[test]
    fn test_from_local_config() {
        let props = IndexMap::from_iter([
            ("streaming.checkpointLocation", "/tmp/checkpoint"),
            ("streaming.outputMode", "append"),
            ("custom.property", "test_value"),
        ]);
        let model = create_mock_dbt_model(props, None);
        let config = from_local_config(&model);

        assert_eq!(config.value.len(), 3);
        assert_eq!(
            config.value.get("streaming.checkpointLocation"),
            Some(&"/tmp/checkpoint".to_string())
        );
        assert_eq!(
            config.value.get("streaming.outputMode"),
            Some(&"append".to_string())
        );
        assert_eq!(
            config.value.get("custom.property"),
            Some(&"test_value".to_string())
        );
        assert!(!config.value.contains_key(PIPELINE_ID_KEY));
    }

    #[test]
    fn test_from_local_config_iceberg() {
        let props = IndexMap::from_iter([("custom.property", "test_value")]);
        let model = create_mock_dbt_model(props, Some("iceberg"));
        let config = from_local_config(&model);

        assert_eq!(config.value.len(), 3); // custom + 2 iceberg properties
        assert_eq!(
            config.value.get("custom.property"),
            Some(&"test_value".to_string())
        );
        assert_eq!(
            config.value.get("delta.enableIcebergCompatV2"),
            Some(&"true".to_string())
        );
        assert_eq!(
            config.value.get("delta.universalFormat.enabledFormats"),
            Some(&"iceberg".to_string())
        );
    }

    #[test]
    fn test_from_local_config_empty() {
        let model = create_mock_dbt_model(IndexMap::new(), None);
        let config = from_local_config(&model);

        assert!(config.value.is_empty());
    }
}
