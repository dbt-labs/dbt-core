//! Liquid clustering component for Databricks incremental relation config.
//!
//! Reference: https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/liquid_clustering.py
//!
//! Databricks stores the live clustering state in two `SHOW TBLPROPERTIES` keys:
//!   - `clusteringColumns`: a JSON array of column names, e.g. `["event_date","user_id"]`
//!   - `clusterByAuto`:     a boolean string, `"true"` when `AUTO LIQUID CLUSTER` is in effect
//!
//! These keys are intentionally excluded from the `tblproperties` diff (see `EQ_IGNORE_LIST` in
//! `tbl_properties.rs`) so that this component owns the full liquid-clustering changeset.
//!
//! The local config is sourced from the model's `liquid_clustered_by` / `auto_liquid_cluster`
//! adapter attributes, which may be a single column string or an array.

use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::serde::StringOrArrayOfStrings;
use minijinja::Value;
use serde::Serialize;

use crate::errors::AdapterResult;
use crate::relation::config_v2::{
    ComponentConfig, ComponentConfigLoader, SimpleComponentConfigImpl, diff, impl_loader,
};
use crate::relation::databricks::config::{
    DatabricksRelationMetadata, DatabricksRelationMetadataKey,
};

pub(crate) const TYPE_NAME: &str = "liquid_clustering";

/// The Databricks tblproperty key that holds the live clustering column list as a JSON array.
const TBLPROP_CLUSTERING_COLUMNS: &str = "clusteringColumns";

/// The Databricks tblproperty key that signals automatic/predictive liquid clustering.
const TBLPROP_CLUSTER_BY_AUTO: &str = "clusterByAuto";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Config {
    pub auto_cluster: bool,
    pub cluster_by: Vec<String>,
}

/// Component for Databricks liquid clustering
pub(crate) type LiquidClustering = SimpleComponentConfigImpl<Config>;

fn new_component(auto_cluster: bool, cluster_by: Vec<String>) -> LiquidClustering {
    LiquidClustering {
        type_name: TYPE_NAME,
        diff_fn: diff::desired_state,
        to_jinja_fn: |v| Value::from_serialize(v),
        value: Config {
            auto_cluster,
            cluster_by,
        },
    }
}

/// Parses the live clustering state from `SHOW TBLPROPERTIES`.
///
/// Databricks represents the column list as a JSON array stored under
/// `clusteringColumns`, for example `["event_date","user_id"]`.  When the table was
/// created with `AUTO LIQUID CLUSTER`, the `clusterByAuto` key is `"true"`.
///
/// Returns an empty/disabled config when the metadata key is absent (non-table
/// relation types) or when neither property is present (table has no liquid
/// clustering configured).
fn from_remote_state(results: &DatabricksRelationMetadata) -> AdapterResult<LiquidClustering> {
    let Some(tblprops) = results.get(&DatabricksRelationMetadataKey::ShowTblProperties) else {
        return Ok(new_component(false, Vec::new()));
    };

    let mut cluster_by: Vec<String> = Vec::new();
    let mut auto_cluster = false;

    for row in tblprops.rows() {
        let Ok(key_val) = row.get_item(&Value::from(0)) else {
            continue;
        };
        let Ok(value_val) = row.get_item(&Value::from(1)) else {
            continue;
        };
        let (Some(key_str), Some(value_str)) = (key_val.as_str(), value_val.as_str()) else {
            continue;
        };

        match key_str {
            TBLPROP_CLUSTERING_COLUMNS => {
                // Databricks serialises the column list as a compact JSON array, e.g.
                // `["event_date","user_id"]`. We hand-roll a minimal parse rather than
                // pulling in a full JSON dep at this call-site.
                cluster_by = parse_clustering_columns_json(value_str);
            }
            TBLPROP_CLUSTER_BY_AUTO => {
                auto_cluster = value_str.eq_ignore_ascii_case("true");
            }
            _ => {}
        }
    }

    Ok(new_component(auto_cluster, cluster_by))
}

/// Reads liquid-clustering configuration from the compiled dbt model node.
///
/// Models may declare clustering via:
///   - `liquid_clustered_by: col`         — a single column as a plain string
///   - `liquid_clustered_by: [col1, col2]` — an array of columns
///   - `auto_liquid_cluster: true`         — delegate column selection to Databricks
///
/// Non-model node types (seeds, snapshots, etc.) are not expected to carry these
/// attributes and fall through to the empty default.
fn from_local_config(
    relation_config: &dyn InternalDbtNodeAttributes,
) -> AdapterResult<LiquidClustering> {
    let Some(model) = relation_config.as_any().downcast_ref::<DbtModel>() else {
        return Ok(new_component(false, Vec::new()));
    };

    let Some(databricks_attr) = &model.__adapter_attr__.databricks_attr else {
        return Ok(new_component(false, Vec::new()));
    };

    let auto_cluster = databricks_attr
        .auto_liquid_cluster
        .unwrap_or(false);

    let cluster_by = databricks_attr
        .liquid_clustered_by
        .as_ref()
        .map(string_or_array_to_vec)
        .unwrap_or_default();

    Ok(new_component(auto_cluster, cluster_by))
}

/// Normalises `StringOrArrayOfStrings` into a plain `Vec<String>`.
fn string_or_array_to_vec(value: &StringOrArrayOfStrings) -> Vec<String> {
    match value {
        StringOrArrayOfStrings::String(s) => vec![s.clone()],
        StringOrArrayOfStrings::ArrayOfStrings(cols) => cols.clone(),
    }
}

/// Minimal parser for the JSON array that Databricks stores in the
/// `clusteringColumns` tblproperty, e.g. `["event_date","user_id"]`.
///
/// We only need string elements; the format is tightly constrained by
/// Databricks itself (identifier names, no nesting), so a lightweight
/// hand-written parse is both sufficient and avoids an extra dependency.
fn parse_clustering_columns_json(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Vec::new();
    }

    let inner = &trimmed[1..trimmed.len() - 1];
    if inner.trim().is_empty() {
        return Vec::new();
    }

    inner.split(',').filter_map(parse_token).collect()
}

/// Trims a single token from the `clusteringColumns` JSON array and strips its
/// surrounding double-quotes.  Returns `None` for empty tokens (e.g. trailing
/// commas), so the caller can use `filter_map` directly.
fn parse_token(token: &str) -> Option<String> {
    let t = token.trim();
    if t.is_empty() {
        return None;
    }

    // Quoted JSON string — strip the surrounding quotes safely.
    if let Some(unquoted) = t.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        return Some(unquoted.to_string());
    }

    // Bare identifier — unexpected from Databricks but accept gracefully.
    Some(t.to_string())
}


impl_loader!(LiquidClustering, DatabricksRelationMetadata);

impl LiquidClusteringLoader {
    pub fn new_component_type_erased(
        auto_cluster: bool,
        cluster_by: Vec<String>,
    ) -> Box<dyn ComponentConfig> {
        Box::new(new_component(auto_cluster, cluster_by))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::databricks::config::test_helpers;
    use dbt_agate::AgateTable;
    use dbt_schemas::schemas::DbtModel;
    use dbt_schemas::schemas::serde::StringOrArrayOfStrings;
    use indexmap::IndexMap;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn make_tblprops_table(props: &[(&str, &str)]) -> AgateTable {
        use arrow::csv::ReaderBuilder;
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;
        use std::io;

        // RFC-4180: values that contain commas, double-quotes or newlines must
        // be enclosed in double-quotes, with any embedded double-quotes escaped
        // by doubling them.  The Databricks `clusteringColumns` value is a JSON
        // array such as `["event_date","user_id"]` which contains both commas
        // and double-quotes, so naïve interpolation produces a malformed CSV row.
        fn csv_field(v: &str) -> String {
            if v.contains([',', '"', '\n']) {
                format!("\"{}\"", v.replace('"', "\"\""))
            } else {
                v.to_string()
            }
        }

        let mut csv = "key,value\n".to_string();
        for (k, v) in props {
            csv.push_str(&format!("{},{}\n", csv_field(k), csv_field(v)));
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
        ]));

        let cursor = io::Cursor::new(csv);
        let batch = ReaderBuilder::new(schema)
            .with_header(true)
            .build(cursor)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        AgateTable::from_record_batch(Arc::new(batch))
    }

    fn make_dbt_model(
        liquid_clustered_by: Option<StringOrArrayOfStrings>,
        auto_liquid_cluster: Option<bool>,
    ) -> DbtModel {
        let cfg = test_helpers::TestModelConfig {
            cluster_by: liquid_clustered_by
                .as_ref()
                .map(string_or_array_to_vec)
                .unwrap_or_default(),
            auto_cluster: auto_liquid_cluster.unwrap_or(false),
            ..Default::default()
        };
        let mut model = test_helpers::create_mock_dbt_model(cfg);

        // Wire liquid_clustered_by / auto_liquid_cluster into the adapter attribute,
        // since create_mock_dbt_model doesn't yet populate those specific fields.
        if let Some(ref mut dbx) = model.__adapter_attr__.databricks_attr {
            dbx.liquid_clustered_by = liquid_clustered_by;
            dbx.auto_liquid_cluster = auto_liquid_cluster;
        }

        model
    }

    // ── parse_clustering_columns_json ─────────────────────────────────────────

    #[test]
    fn test_parse_clustering_columns_json_multi_column() {
        let result = parse_clustering_columns_json(r#"["event_date","user_id"]"#);
        assert_eq!(result, vec!["event_date", "user_id"]);
    }

    #[test]
    fn test_parse_clustering_columns_json_single_column() {
        let result = parse_clustering_columns_json(r#"["partition_col"]"#);
        assert_eq!(result, vec!["partition_col"]);
    }

    #[test]
    fn test_parse_clustering_columns_json_empty_array() {
        let result = parse_clustering_columns_json("[]");
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_clustering_columns_json_invalid_format() {
        let result = parse_clustering_columns_json("not_an_array");
        assert!(result.is_empty());
    }

    // ── from_remote_state ─────────────────────────────────────────────────────

    #[test]
    fn test_from_remote_state_explicit_cluster_columns() {
        let table = make_tblprops_table(&[
            ("clusteringColumns", r#"["event_date","user_id"]"#),
            ("clusterByAuto", "false"),
        ]);
        let results =
            IndexMap::from([(DatabricksRelationMetadataKey::ShowTblProperties, table)]);

        let config = from_remote_state(&results).unwrap();
        assert_eq!(config.value.cluster_by, vec!["event_date", "user_id"]);
        assert!(!config.value.auto_cluster);
    }

    #[test]
    fn test_from_remote_state_auto_cluster_enabled() {
        let table = make_tblprops_table(&[("clusterByAuto", "true")]);
        let results =
            IndexMap::from([(DatabricksRelationMetadataKey::ShowTblProperties, table)]);

        let config = from_remote_state(&results).unwrap();
        assert!(config.value.auto_cluster);
        assert!(config.value.cluster_by.is_empty());
    }

    #[test]
    fn test_from_remote_state_no_liquid_clustering_properties() {
        // Table has properties but none related to liquid clustering.
        let table = make_tblprops_table(&[("some.other.key", "value")]);
        let results =
            IndexMap::from([(DatabricksRelationMetadataKey::ShowTblProperties, table)]);

        let config = from_remote_state(&results).unwrap();
        assert!(config.value.cluster_by.is_empty());
        assert!(!config.value.auto_cluster);
    }

    #[test]
    fn test_from_remote_state_missing_metadata_key() {
        // No ShowTblProperties key at all (e.g. view or streaming table).
        let results: DatabricksRelationMetadata = IndexMap::new();
        let config = from_remote_state(&results).unwrap();
        assert!(config.value.cluster_by.is_empty());
        assert!(!config.value.auto_cluster);
    }

    // ── from_local_config ──────────────────────────────────────────────────────

    #[test]
    fn test_from_local_config_array_of_columns() {
        let model = make_dbt_model(
            Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "event_date".to_string(),
                "user_id".to_string(),
            ])),
            None,
        );
        let config = from_local_config(&model).unwrap();
        assert_eq!(config.value.cluster_by, vec!["event_date", "user_id"]);
        assert!(!config.value.auto_cluster);
    }

    #[test]
    fn test_from_local_config_single_column_string() {
        let model = make_dbt_model(
            Some(StringOrArrayOfStrings::String("partition_col".to_string())),
            None,
        );
        let config = from_local_config(&model).unwrap();
        assert_eq!(config.value.cluster_by, vec!["partition_col"]);
        assert!(!config.value.auto_cluster);
    }

    #[test]
    fn test_from_local_config_auto_liquid_cluster() {
        let model = make_dbt_model(None, Some(true));
        let config = from_local_config(&model).unwrap();
        assert!(config.value.auto_cluster);
        assert!(config.value.cluster_by.is_empty());
    }

    #[test]
    fn test_from_local_config_no_liquid_clustering() {
        let model = make_dbt_model(None, None);
        let config = from_local_config(&model).unwrap();
        assert!(config.value.cluster_by.is_empty());
        assert!(!config.value.auto_cluster);
    }

    // ── diff / changeset computation ───────────────────────────────────────────

    #[test]
    fn test_diff_returns_some_when_cluster_columns_change() {
        let old = new_component(false, vec!["col_a".to_string()]);
        let new = new_component(false, vec!["col_a".to_string(), "col_b".to_string()]);
        assert!(LiquidClustering::diff_from(&new, Some(&old)).is_some());
    }

    #[test]
    fn test_diff_returns_none_when_config_is_identical() {
        let config = new_component(false, vec!["col_a".to_string()]);
        assert!(LiquidClustering::diff_from(&config, Some(&config)).is_none());
    }

    #[test]
    fn test_diff_returns_some_when_auto_cluster_flag_changes() {
        let old = new_component(false, Vec::new());
        let new = new_component(true, Vec::new());
        assert!(LiquidClustering::diff_from(&new, Some(&old)).is_some());
    }

    #[test]
    fn test_diff_returns_some_against_no_prior_state() {
        let config = new_component(false, vec!["col_a".to_string()]);
        assert!(LiquidClustering::diff_from(&config, None).is_some());
    }
}
