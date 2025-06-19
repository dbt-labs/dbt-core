use std::{collections::BTreeMap, path::PathBuf};

use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;

use crate::schemas::{manifest::common::DbtOwner, project::ExposureConfig, ref_and_source::DbtRef};

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ExposureProperties {
    pub name: String,
    pub package_name: String,
    pub path: PathBuf,
    pub original_file_path: PathBuf,
    pub unique_id: String,
    pub fqn: Vec<String>,
    pub owner: DbtOwner,
    pub description: String,
    pub label: Option<String>,
    pub maturity: Option<String>,
    pub config: ExposureConfig,
    pub unrendered_config: BTreeMap<String, Value>,
    pub url: Option<String>,
    pub depends_on: ExposureDependsOn,
    pub refs: Vec<DbtRef>,
    pub sources: Vec<Vec<String>>,
    pub metrics: Vec<Vec<String>>,
    pub created_at: Option<f64>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ExposureDependsOn {
    pub macros: Vec<String>,
    pub nodes: Vec<String>,
}
