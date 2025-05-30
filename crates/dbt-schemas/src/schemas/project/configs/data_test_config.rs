use dbt_common::FsError;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use crate::schemas::common::DbtQuoting;
use crate::schemas::manifest::DbtConfig;
use crate::schemas::serde::try_from_value;
use crate::schemas::serde::StringOrArrayOfStrings;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectDataTestConfig {
    #[serde(rename = "+alias")]
    pub alias: Option<String>,
    #[serde(rename = "+database")]
    pub database: Option<String>,
    #[serde(rename = "+enabled")]
    pub enabled: Option<bool>,
    #[serde(rename = "+error_if")]
    pub error_if: Option<String>,
    #[serde(rename = "+fail_calc")]
    pub fail_calc: Option<String>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    #[serde(rename = "+limit")]
    pub limit: Option<i32>,
    #[serde(rename = "+meta")]
    pub meta: Option<serde_json::Value>,
    #[serde(rename = "+schema")]
    pub schema: Option<String>,
    #[serde(rename = "+severity")]
    pub severity: Option<String>,
    #[serde(rename = "+store_failures")]
    pub store_failures: Option<bool>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+warn_if")]
    pub warn_if: Option<String>,
    #[serde(rename = "+quoting")]
    pub quoting: Option<DbtQuoting>,
    // Flattened field:
    pub __additional_properties__: Verbatim<BTreeMap<String, dbt_serde_yaml::Value>>,
}

impl TryFrom<&ProjectDataTestConfig> for DbtConfig {
    type Error = Box<FsError>;

    fn try_from(test_configs: &ProjectDataTestConfig) -> Result<Self, Self::Error> {
        Ok(DbtConfig {
            alias: test_configs.alias.clone(),
            database: test_configs.database.clone(),
            schema: test_configs.schema.clone(),
            enabled: test_configs.enabled,
            group: test_configs.group.clone(),
            meta: try_from_value(test_configs.meta.clone())?,
            tags: match &test_configs.tags {
                Some(StringOrArrayOfStrings::String(tags)) => {
                    Some(tags.split(',').map(|s| s.to_string()).collect())
                }
                Some(StringOrArrayOfStrings::ArrayOfStrings(tags)) => Some(tags.clone()),
                None => None,
            },
            error_if: test_configs.error_if.clone(),
            warn_if: test_configs.warn_if.clone(),
            fail_calc: test_configs.fail_calc.clone(),
            limit: test_configs.limit,
            severity: test_configs.severity.clone(),
            store_failures: test_configs.store_failures,
            ..Default::default()
        })
    }
}
