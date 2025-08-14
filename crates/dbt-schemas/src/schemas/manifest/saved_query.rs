use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::{collections::BTreeMap, path::PathBuf};

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

use crate::schemas::project::SavedQueriesConfig;
use crate::schemas::serde::bool_or_string_bool;

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct DbtSavedQuery {
    pub name: String,
    pub package_name: String,
    pub path: PathBuf,
    pub original_file_path: PathBuf,
    pub unique_id: String,
    pub fqn: Vec<String>,
    pub query_params: DbtSavedQueryParams,
    pub exports: Vec<DbtSavedQueryExport>,
    pub description: Option<String>,
    pub label: Option<String>,
    pub metadata: Option<YmlValue>,
    pub config: SavedQueriesConfig,
    pub unrendered_config: BTreeMap<String, YmlValue>,
    pub depends_on: SavedQueryDependsOn,
    pub created_at: f64,
    pub refs: Vec<YmlValue>,
    pub tags: Option<Vec<String>>,
    pub __other__: BTreeMap<String, YmlValue>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct SavedQueryDependsOn {
    pub nodes: Vec<String>,
    pub macros: Vec<String>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct DbtSavedQueryParams {
    pub metrics: Vec<String>,
    pub group_by: Vec<YmlValue>,
    pub where_clause: Option<YmlValue>,
    #[serde(rename = "where")]
    pub where_condition: Option<YmlValue>,
    // According to the the V12 JSON schema the `order_by` field is required, however in reality
    // many manifests omit it. To allow these to be parsed the `order_by` field needs to be optional
    pub order_by: Option<Vec<YmlValue>>,
    pub limit: Option<YmlValue>,
    pub __other__: BTreeMap<String, YmlValue>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct DbtSavedQueryExport {
    pub name: String,
    pub config: DbtSavedQueryExportConfig,
    pub unrendered_config: BTreeMap<String, YmlValue>,
    pub __other__: BTreeMap<String, YmlValue>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct DbtSavedQueryExportConfig {
    pub export_as: String,
    #[serde(rename = "schema_name")]
    pub schema: String,
    pub alias: String,
    pub database: Option<String>,
    pub __other__: BTreeMap<String, YmlValue>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct DbtSavedQueryConfig {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub group: Option<String>,
    pub meta: BTreeMap<String, YmlValue>,
    pub export_as: Option<String>,
    pub schema: Option<String>,
    pub cache: Option<DbtSavedQueryCacheConfig>,
    pub __other__: BTreeMap<String, YmlValue>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct DbtSavedQueryCacheConfig {
    pub enabled: bool,
    pub __other__: BTreeMap<String, YmlValue>,
}
