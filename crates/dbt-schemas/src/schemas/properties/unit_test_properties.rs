use std::collections::BTreeMap;

use crate::schemas::{
    common::{Expect, Given},
    project::UnitTestConfig,
};
use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct UnitTestProperties {
    pub config: Option<UnitTestConfig>,
    pub description: Option<String>,
    pub expect: Expect,
    pub given: Option<Vec<Given>>,
    pub model: String,
    pub name: String,
    pub overrides: Option<UnitTestOverrides>,
    pub versions: Option<serde_json::Value>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct UnitTestOverrides {
    pub env_vars: Option<BTreeMap<String, serde_json::Value>>,
    pub macros: Option<BTreeMap<String, serde_json::Value>>,
    pub vars: Option<BTreeMap<String, serde_json::Value>>,
}
