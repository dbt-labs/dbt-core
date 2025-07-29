use crate::schemas::manifest::common::DbtOwner;
use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::schemas::project::ExposureConfig;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ExposureProperties {
    pub config: Option<ExposureConfig>,
    pub depends_on: Option<Vec<String>>,
    pub description: Option<String>,
    pub label: Option<String>,
    pub maturity: Option<String>,
    pub name: String,
    pub owner: DbtOwner,
    #[serde(rename = "type")]
    pub type_: String,
    pub url: Option<String>,
}
