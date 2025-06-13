use dbt_common::FsError;
use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use crate::schemas::{
    manifest::DbtConfig,
    serde::{try_from_value, StringOrArrayOfStrings},
};

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectExposureConfig {
    #[serde(rename = "+meta")]
    pub meta: Option<serde_json::Value>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+enabled")]
    pub enabled: Option<bool>,
    #[serde(flatten)]
    pub __additional_properties__: BTreeMap<String, dbt_serde_yaml::Value>,
}

impl TryFrom<&ProjectExposureConfig> for DbtConfig {
    type Error = Box<FsError>;

    fn try_from(exposure_configs: &ProjectExposureConfig) -> Result<Self, Self::Error> {
        Ok(DbtConfig {
            enabled: exposure_configs.enabled,
            meta: try_from_value(exposure_configs.meta.clone())?,
            tags: match &exposure_configs.tags {
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
