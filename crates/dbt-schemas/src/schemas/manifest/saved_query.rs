use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::{collections::BTreeMap, path::PathBuf};

// Type aliases for clarity
type JsonValue = serde_json::Value;

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
    pub metadata: Option<JsonValue>,
    pub config: SavedQueriesConfig,
    pub unrendered_config: BTreeMap<String, JsonValue>,
    pub depends_on: SavedQueryDependsOn,
    pub created_at: f64,
    pub refs: Vec<JsonValue>,
    pub tags: Option<Vec<String>>,
    #[serde(flatten)]
    pub other: BTreeMap<String, JsonValue>,
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
    pub group_by: Vec<JsonValue>,
    pub where_clause: Option<JsonValue>,
    #[serde(rename = "where")]
    pub where_condition: Option<JsonValue>,
    // According to the the V12 JSON schema the `order_by` field is required, however in reality
    // many manifests omit it. To allow these to be parsed the `order_by` field needs to be optional
    pub order_by: Option<Vec<JsonValue>>,
    pub limit: Option<JsonValue>,
    #[serde(flatten)]
    pub other: BTreeMap<String, JsonValue>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct DbtSavedQueryExport {
    pub name: String,
    pub config: DbtSavedQueryExportConfig,
    pub unrendered_config: BTreeMap<String, JsonValue>,
    #[serde(flatten)]
    pub other: BTreeMap<String, JsonValue>,
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
    #[serde(flatten)]
    pub other: BTreeMap<String, JsonValue>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct DbtSavedQueryConfig {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub group: Option<String>,
    pub meta: BTreeMap<String, JsonValue>,
    pub export_as: Option<String>,
    pub schema: Option<String>,
    pub cache: Option<DbtSavedQueryCacheConfig>,
    #[serde(flatten)]
    pub other: BTreeMap<String, JsonValue>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct DbtSavedQueryCacheConfig {
    pub enabled: bool,
    #[serde(flatten)]
    pub other: BTreeMap<String, JsonValue>,
}
