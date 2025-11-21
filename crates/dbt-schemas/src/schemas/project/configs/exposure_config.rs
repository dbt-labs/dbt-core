use crate::schemas::project::TypedRecursiveConfig;
use dbt_serde_yaml::{JsonSchema, ShouldBe};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::btree_map::Iter;

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

use crate::{
    default_to,
    schemas::{
        project::{DefaultTo, configs::common::default_meta_and_tags},
        serde::{StringOrArrayOfStrings, bool_or_string_bool},
    },
};

// NOTE: No #[skip_serializing_none] - we handle None serialization in serialize_with_mode
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq)]
pub struct ProjectExposureConfig {
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectExposureConfig>>,
}

impl TypedRecursiveConfig for ProjectExposureConfig {
    fn type_name() -> &'static str {
        "exposure"
    }

    fn iter_children(&self) -> Iter<'_, String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

// NOTE: No #[skip_serializing_none] - we handle None serialization in serialize_with_mode
#[derive(Deserialize, Serialize, Debug, Clone, Default, JsonSchema, PartialEq)]
pub struct ExposureConfig {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub meta: Option<BTreeMap<String, YmlValue>>,
    pub tags: Option<StringOrArrayOfStrings>,
}

impl From<ProjectExposureConfig> for ExposureConfig {
    fn from(config: ProjectExposureConfig) -> Self {
        Self {
            enabled: config.enabled,
            meta: config.meta,
            tags: config.tags,
        }
    }
}

impl From<ExposureConfig> for ProjectExposureConfig {
    fn from(config: ExposureConfig) -> Self {
        Self {
            meta: config.meta,
            tags: config.tags,
            enabled: config.enabled,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl DefaultTo<ExposureConfig> for ExposureConfig {
    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn default_to(&mut self, parent: &ExposureConfig) {
        let ExposureConfig {
            meta,
            tags,
            enabled,
        } = self;

        #[allow(unused, clippy::let_unit_value)]
        let meta = default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        #[allow(unused)]
        let tags = ();

        default_to!(parent, [enabled]);
    }
}
