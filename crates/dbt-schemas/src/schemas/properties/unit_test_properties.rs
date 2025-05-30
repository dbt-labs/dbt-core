use crate::schemas::{
    common::{Expect, Given},
    manifest::DbtConfig,
    serde::{try_from_value, StringOrArrayOfStrings},
};
use dbt_common::FsError;
use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct UnitTestProperties {
    pub config: Option<UnitTestPropertiesConfig>,
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
pub struct UnitTestPropertiesConfig {
    pub enabled: Option<bool>,
    pub meta: Option<serde_json::Value>,
    pub tags: Option<StringOrArrayOfStrings>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct UnitTestOverrides {
    pub env_vars: Option<serde_json::Value>,
    pub macros: Option<serde_json::Value>,
    pub vars: Option<serde_json::Value>,
}

impl TryFrom<&UnitTestPropertiesConfig> for DbtConfig {
    type Error = Box<FsError>;

    fn try_from(unit_test_configs: &UnitTestPropertiesConfig) -> Result<Self, Self::Error> {
        Ok(DbtConfig {
            enabled: unit_test_configs.enabled,
            meta: try_from_value(unit_test_configs.meta.clone())?,
            tags: match &unit_test_configs.tags {
                Some(StringOrArrayOfStrings::String(tags)) => {
                    Some(tags.split(',').map(|s| s.to_string()).collect())
                }
                Some(StringOrArrayOfStrings::ArrayOfStrings(tags)) => Some(tags.clone()),
                None => None,
            },
            ..Default::default()
        })
    }
}
