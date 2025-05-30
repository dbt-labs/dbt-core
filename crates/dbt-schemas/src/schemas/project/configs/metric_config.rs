use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::HashMap;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectMetricConfigs {
    #[serde(rename = "+meta")]
    pub meta: Option<serde_json::Value>,
    pub enabled: Option<bool>,
    #[serde(flatten)]
    pub additional_properties: HashMap<String, serde_json::Value>,
}
