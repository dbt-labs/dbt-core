use crate::schemas::{
    common::{DbtQuoting, StoreFailuresAs},
    manifest::DbtConfig,
    serde::{try_from_value, StringOrArrayOfStrings},
};
use dbt_common::FsError;
use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct TestProperties {
    pub config: Option<TestPropertiesConfig>,
    pub description: Option<String>,
    pub name: String,
}

impl TestProperties {
    pub fn empty(model_name: String) -> Self {
        Self {
            config: None,
            description: None,
            name: model_name,
        }
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct TestPropertiesConfig {
    pub alias: Option<String>,
    pub database: Option<String>,
    pub enabled: Option<bool>,
    pub error_if: Option<String>,
    pub fail_calc: Option<String>,
    pub group: Option<String>,
    pub limit: Option<i32>,
    pub meta: Option<serde_json::Value>,
    pub schema: Option<String>,
    pub severity: Option<String>,
    pub store_failures: Option<bool>,
    pub store_failures_as: Option<StoreFailuresAs>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub warn_if: Option<String>,
    pub quoting: Option<DbtQuoting>,
    #[serde(rename = "where")]
    pub where_: Option<String>,
}

impl TryFrom<&TestPropertiesConfig> for DbtConfig {
    type Error = Box<FsError>;

    fn try_from(test_configs: &TestPropertiesConfig) -> Result<Self, Self::Error> {
        Ok(DbtConfig {
            enabled: test_configs.enabled,
            meta: try_from_value(test_configs.meta.clone())?,
            tags: match &test_configs.tags {
                Some(StringOrArrayOfStrings::String(tags)) => {
                    Some(tags.split(',').map(|s| s.to_string()).collect())
                }
                Some(StringOrArrayOfStrings::ArrayOfStrings(tags)) => Some(tags.clone()),
                None => None,
            },
            severity: test_configs.severity.clone(),
            limit: test_configs.limit,
            warn_if: test_configs.warn_if.clone(),
            error_if: test_configs.error_if.clone(),
            fail_calc: test_configs.fail_calc.clone(),
            store_failures: test_configs.store_failures,
            ..Default::default()
        })
    }
}
