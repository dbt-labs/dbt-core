use dbt_proc_macros::Resolvable;
use dbt_yaml::{DbtSchema, ShouldBe};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::{BTreeMap, btree_map::Iter};

// Type aliases for clarity
type YmlValue = dbt_yaml::Value;

use crate::{
    default_to,
    schemas::{
        project::{ResolvableConfig, TypedRecursiveConfig, configs::common::default_meta_and_tags},
        serde::{StringOrArrayOfStrings, bool_or_string_bool},
    },
};

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct ProjectMetricConfigs {
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+meta")]
    pub meta: Option<IndexMap<String, YmlValue>>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    // Flattened fields
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectMetricConfigs>>,
}

impl TypedRecursiveConfig for ProjectMetricConfigs {
    fn type_name() -> &'static str {
        "metric"
    }

    fn iter_children(&self) -> Iter<'_, String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[derive(Resolvable, Deserialize, Serialize, Debug, Clone, DbtSchema, PartialEq)]
pub struct MetricConfig {
    #[resolved(promote, method = get_enabled_with_default)]
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub meta: Option<IndexMap<String, YmlValue>>,
    #[serde(
        default,
        serialize_with = "crate::schemas::nodes::serialize_none_as_empty_list"
    )]
    pub tags: Option<StringOrArrayOfStrings>,
    pub group: Option<String>,
}

impl Default for MetricConfig {
    fn default() -> Self {
        Self {
            enabled: Some(true),
            meta: Some(IndexMap::new()),
            tags: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![])),
            group: None,
        }
    }
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

impl ResolvableConfig<MetricConfig> for MetricConfig {
    type Resolved = ResolvedMetricConfig;
    type PackageDefaults = ();
    type ResolveDefaults = ();

    fn get_enabled_with_default(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    fn disable(&mut self) {
        self.enabled = Some(false);
    }

    fn apply_package_defaults(&mut self, _: ()) {}

    fn finalize(self) -> ResolvedMetricConfig {
        self.finalize_resolved()
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
