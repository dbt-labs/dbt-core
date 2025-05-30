use dbt_common::FsError;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use crate::schemas::common::DbtQuoting;
use crate::schemas::manifest::DbtConfig;
use crate::schemas::serde::StringOrArrayOfStrings;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectSourceConfig {
    #[serde(rename = "+enabled")]
    pub enabled: Option<bool>,
    #[serde(rename = "+event_time")]
    pub event_time: Option<String>,
    #[serde(rename = "+meta")]
    pub meta: Option<serde_json::Value>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+quoting")]
    pub quoting: Option<DbtQuoting>,
    #[serde(flatten)]
    pub __additional_properties__: Verbatim<BTreeMap<String, dbt_serde_yaml::Value>>,
}

impl TryFrom<&ProjectSourceConfig> for DbtConfig {
    type Error = Box<FsError>;

    fn try_from(source_configs: &ProjectSourceConfig) -> Result<Self, Self::Error> {
        Ok(DbtConfig {
            enabled: source_configs.enabled,
            tags: match &source_configs.tags {
                Some(StringOrArrayOfStrings::String(tags)) => {
                    Some(tags.split(',').map(|s| s.to_string()).collect())
                }
                Some(StringOrArrayOfStrings::ArrayOfStrings(tags)) => Some(tags.clone()),
                None => None,
            },
            meta: source_configs
                .meta
                .as_ref()
                .and_then(|v| v.as_object().map(|obj| obj.clone().into_iter().collect())),
            event_time: source_configs.event_time.clone(),
            ..Default::default()
        })
    }
}
