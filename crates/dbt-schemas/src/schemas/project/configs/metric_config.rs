use dbt_serde_yaml::{JsonSchema, ShouldBe};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::{BTreeMap, btree_map::Iter};

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

use crate::{
    default_to,
    schemas::{
        project::{DefaultTo, IterChildren, configs::common::default_meta_and_tags},
        serde::{StringOrArrayOfStrings, bool_or_string_bool},
    },
};

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectMetricConfigs {
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    // Flattened fields
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectMetricConfigs>>,
}

impl IterChildren<ProjectMetricConfigs> for ProjectMetricConfigs {
    fn iter_children(&self) -> Iter<String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, Default, JsonSchema)]
pub struct MetricConfig {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub meta: Option<BTreeMap<String, YmlValue>>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub group: Option<String>,
}

impl From<ProjectMetricConfigs> for MetricConfig {
    fn from(config: ProjectMetricConfigs) -> Self {
        Self {
            enabled: config.enabled,
            meta: config.meta,
            tags: config.tags,
            group: config.group,
        }
    }
}

impl From<MetricConfig> for ProjectMetricConfigs {
    fn from(config: MetricConfig) -> Self {
        Self {
            enabled: config.enabled,
            meta: config.meta,
            tags: config.tags,
            group: config.group,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl DefaultTo<MetricConfig> for MetricConfig {
    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn default_to(&mut self, parent: &MetricConfig) {
        let MetricConfig {
            enabled,
            meta,
            tags,
            group,
        } = self;

        #[allow(unused, clippy::let_unit_value)]
        let meta = default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        #[allow(unused)]
        let tags = ();

        default_to!(parent, [enabled, group]);
    }
}
