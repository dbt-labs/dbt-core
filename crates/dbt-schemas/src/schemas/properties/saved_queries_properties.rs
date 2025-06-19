use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::schemas::project::ExportConfigExportAs;
use crate::schemas::project::SavedQueriesConfig;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct SavedQueriesProperties {
    pub config: Option<SavedQueriesConfig>,
    pub description: Option<String>,
    pub exports: Option<Vec<Export>>,
    pub label: Option<String>,
    pub name: String,
    pub query_params: Verbatim<SavedQueriesQueryParams>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct SavedQueriesQueryParams {
    pub dimensions: Option<Vec<String>>,
    pub group_by: Option<Vec<String>>,
    pub metrics: Option<Vec<String>>,
    #[serde(rename = "where")]
    pub where_: Option<Vec<String>>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct Export {
    pub name: String,
    pub config: Option<ExportConfig>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ExportConfig {
    pub alias: Option<String>,
    pub export_as: Option<ExportConfigExportAs>,
    pub schema: Option<String>,
}
