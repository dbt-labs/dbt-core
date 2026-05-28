use std::collections::BTreeMap;

// Type aliases for clarity
type YmlValue = dbt_yaml::Value;

use crate::schemas::{
    common::{Expect, Given},
    project::UnitTestConfig,
};
use dbt_common::io_args::StaticAnalysisOffReason;
use dbt_yaml::DbtSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct UnitTestProperties {
    pub config: Option<UnitTestConfig>,
    pub description: Option<String>,
    pub expect: Expect,
    pub given: Option<Vec<Given>>,
    pub model: String,
    pub name: String,
    pub overrides: Option<UnitTestOverrides>,
    #[serde(skip_deserializing, default)]
    pub static_analysis_off_reason: Option<StaticAnalysisOffReason>,
    pub versions: Option<YmlValue>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct UnitTestOverrides {
    pub env_vars: Option<BTreeMap<String, YmlValue>>,
    pub macros: Option<BTreeMap<String, YmlValue>>,
    pub vars: Option<BTreeMap<String, YmlValue>>,
}
