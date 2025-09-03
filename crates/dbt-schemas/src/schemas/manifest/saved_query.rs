use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

use crate::schemas::{
    CommonAttributes,
    common::NodeDependsOn,
    manifest::common::SourceFileMetadata,
    project::{ExportConfigExportAs, SavedQueryConfig},
    ref_and_source::DbtRef,
};

use super::common::WhereFilterIntersection;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DbtSavedQuery {
    pub __common_attr__: CommonAttributes,
    pub __saved_query_attr__: DbtSavedQueryAttr,

    // To be deprecated
    pub deprecated_config: SavedQueryConfig,

    pub __other__: BTreeMap<String, YmlValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DbtSavedQueryAttr {
    pub query_params: SavedQueryParams,
    pub exports: Vec<SavedQueryExport>,
    pub label: Option<String>,
    pub metadata: Option<SourceFileMetadata>,
    pub unrendered_config: BTreeMap<String, YmlValue>,
    pub depends_on: NodeDependsOn,
    pub refs: Vec<DbtRef>,
    pub created_at: f64,
    pub group: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SavedQueryParams {
    pub metrics: Vec<String>,
    pub group_by: Vec<String>,
    #[serde(rename = "where")]
    pub where_: Option<WhereFilterIntersection>,
    pub order_by: Vec<String>,
    pub limit: Option<i32>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SavedQueryExport {
    pub name: String,
    pub config: SavedQueryExportConfig,
    pub unrendered_config: BTreeMap<String, YmlValue>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SavedQueryExportConfig {
    pub export_as: ExportConfigExportAs,
    pub schema_name: Option<String>,
    pub alias: Option<String>,
    pub database: Option<String>,
}
