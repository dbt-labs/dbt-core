//! https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/tblproperties.py

use crate::errors::AdapterResult;
use crate::relation::config_v2::{
    ComponentConfig, ComponentConfigLoader, SimpleComponentConfigImpl, impl_loader,
};
use crate::relation::databricks::config::{
    DatabricksRelationMetadata, DatabricksRelationMetadataKey,
};
use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_yaml::Value as YmlValue;
use indexmap::IndexMap;
use minijinja::value::{Value, ValueMap};

pub(crate) const TYPE_NAME: &str = "tblproperties";

pub(crate) const PIPELINE_ID_KEY: &str = "pipelines.pipelineId";

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

/// Component for Databricks table properties.
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

fn new_component(properties: IndexMap<String, String>) -> TblProperties {
    TblProperties {
        type_name: TYPE_NAME,
        diff_fn: diff,
        to_jinja_fn: to_jinja,
        value: properties,
    }
}

/// Takes the diff between two `TblProperties`, matching the Python dbt-databricks
/// `TblPropertiesConfig.get_diff` semantics:
///
/// ```python
/// def get_diff(self, other):
///     if self.tblproperties.items() - other.tblproperties.items():
///         return self
///     return None
/// ```
///
/// This is an asymmetric, one-directional comparison: a diff is reported only when the
/// *desired* state has a non-ignored key/value pair that is missing or different in the
/// *current* state. Keys present only in the current state (e.g. server-managed properties
/// Databricks sets automatically) are ignored — a model that declares no opinion on
/// `tblproperties` never produces a diff, regardless of what's already on the relation.
/// The returned value is the full desired state (matching Python's `get_diff` returning `self`).
fn diff(
    desired_state: &IndexMap<String, String>,
    current_state: &IndexMap<String, String>,
) -> Option<IndexMap<String, String>> {
    let has_diff = desired_state.iter().any(|(k, v)| {
        !EQ_IGNORE_LIST.contains(&k.as_str())
            && current_state.get(k).map(|cv| cv != v).unwrap_or(true)
    });

    if has_diff {
        Some(desired_state.clone())
    } else {
        None
    }
}

fn from_remote_state(results: &DatabricksRelationMetadata) -> AdapterResult<TblProperties> {
    let Some(table) = results.get(&DatabricksRelationMetadataKey::ShowTblProperties) else {
        return Ok(new_component(IndexMap::new()));
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

    Ok(new_component(tblproperties))
}

fn from_local_config(
    relation_config: &dyn InternalDbtNodeAttributes,
) -> AdapterResult<TblProperties> {
    let Some(model) = relation_config.as_any().downcast_ref::<DbtModel>() else {
        return Ok(new_component(IndexMap::new()));
    };

    let mut tblproperties = IndexMap::new();

    if let Some(databricks_attr) = &model.__adapter_attr__.databricks_attr
        && let Some(props_map) = &databricks_attr.tblproperties
    {
        for (key, value) in &props_map.0 {
            if let YmlValue::String(value_str, _) = value {
                tblproperties.insert(key.clone(), value_str.clone());
            }
        }
    }

    let is_iceberg = model
        .deprecated_config
        .table_format
        .as_ref()
        .is_some_and(|s| s.eq_ignore_ascii_case("iceberg"));

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

    Ok(new_component(tblproperties))
}

impl_loader!(TblProperties, DatabricksRelationMetadata);

impl TblPropertiesLoader {
    pub fn new_component_type_erased(
        properties: IndexMap<String, String>,
    ) -> Box<dyn ComponentConfig> {
        Box::new(new_component(properties))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::databricks::config::test_helpers;
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

        // diff returns the full desired state (matching Python's get_diff returning self)
        assert_eq!(diff.len(), 3);
        assert_eq!(
            diff.get("pipelines.pipelineId").unwrap().as_str(),
            "pipeline123456"
        );
        assert_eq!(diff.get("custom.change").unwrap().as_str(), "new");
        assert_eq!(diff.get("custom.add").unwrap().as_str(), "new");
    }

    /// The model config has tblproperties:
    ///   {"delta.enableChangeDataFeed": "true", "delta.columnMapping.mode": "name"}
    ///
    /// The existing table (SHOW TBLPROPERTIES) has those plus a few other properties
    /// like delta.checkpoint.*, delta.parquet.*, etc.
    ///
    /// Python dbt-databricks's `get_diff` is a one-directional set difference
    /// (`self.tblproperties.items() - other.tblproperties.items()`): it only reports a
    /// diff when the *desired* state has a non-ignored key/value pair missing or different
    /// in current. Extra keys present only in current (server-managed properties the model
    /// never declared an opinion on) must be ignored. Here every non-ignored desired key
    /// (`delta.columnMapping.mode`) matches current, so there is no diff even though current
    /// has additional keys.
    #[test]
    fn test_diff_extra_current_keys_are_ignored() {
        // Desired state: what from_local_config produces from the model config.
        // Note: delta.enableChangeDataFeed is in the model config but will be
        // filtered by EQ_IGNORE_LIST in the diff function.
        let desired = IndexMap::from_iter([
            ("delta.enableChangeDataFeed".to_string(), "true".to_string()),
            ("delta.columnMapping.mode".to_string(), "name".to_string()),
        ]);

        // Current state: what from_remote_state produces from SHOW TBLPROPERTIES.
        // from_remote_state already filters EQ_IGNORE_LIST, so delta.enableChangeDataFeed
        // is NOT here. But extra system properties (not in ignore list) ARE here.
        let current = IndexMap::from_iter([
            (
                "delta.checkpoint.writeStatsAsJson".to_string(),
                "false".to_string(),
            ),
            (
                "delta.checkpoint.writeStatsAsStruct".to_string(),
                "true".to_string(),
            ),
            ("delta.columnMapping.mode".to_string(), "name".to_string()),
            (
                "delta.parquet.compression.codec".to_string(),
                "zstd".to_string(),
            ),
        ]);

        // No diff: the only non-ignored desired key/value pair (delta.columnMapping.mode =
        // "name") is present and identical in current. Extra current-only keys don't count.
        let result = diff(&desired, &current);
        assert!(
            result.is_none(),
            "Extra non-ignored keys present only in current state must not trigger a diff"
        );
    }

    /// A model that declares no tblproperties opinion at all must never report a diff,
    /// even when the existing relation already has server-managed properties set
    /// (regression test for the bug that triggered spurious `apply_tblproperties` calls,
    /// e.g. unnecessary `adapter.is_uniform` checks, for models that never configured
    /// tblproperties).
    #[test]
    fn test_diff_empty_desired_never_reports_change() {
        let desired = IndexMap::new();

        let current = IndexMap::from_iter([
            (
                "delta.checkpoint.writeStatsAsJson".to_string(),
                "false".to_string(),
            ),
            (
                "delta.checkpoint.writeStatsAsStruct".to_string(),
                "true".to_string(),
            ),
            ("delta.minReaderVersion".to_string(), "1".to_string()),
            ("delta.minWriterVersion".to_string(), "2".to_string()),
        ]);

        assert!(diff(&desired, &current).is_none());
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
        let config = from_remote_state(&results).unwrap();

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
        let config = from_local_config(&model).unwrap();

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
        let config = from_local_config(&model).unwrap();

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
        let config = from_local_config(&model).unwrap();

        assert!(config.value.is_empty());
    }
}
