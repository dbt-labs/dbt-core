use serde::{Deserialize, Serialize};

use crate::schemas::{
    dbt_column::Granularity,
    manifest::{common::SourceFileMetadata, semantic_model::TimeSpine},
};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SemanticManifestProjectConfiguration {
    pub time_spine_table_configurations: Vec<TimeSpineTableConfiguration>,
    pub metadata: Option<SourceFileMetadata>,
    pub dsi_package_version: SemanticVersion,
    pub time_spines: Vec<TimeSpine>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TimeSpineTableConfiguration {
    pub location: String,
    pub column_name: String,
    pub grain: Granularity,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SemanticVersion {
    pub major: String,
    pub minor: String,
    pub patch: Option<String>,
}

impl Default for SemanticVersion {
    fn default() -> Self {
        Self {
            major: "0".to_string(),
            minor: "0".to_string(),
            patch: Some("0".to_string()),
        }
    }
}
