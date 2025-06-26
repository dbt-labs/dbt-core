use dbt_common::io_args::StaticAnalysisKind;
use dbt_serde_yaml::{JsonSchema, ShouldBe};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::btree_map::Iter;
use std::collections::BTreeMap;

use crate::default_to;
use crate::schemas::common::{DbtQuoting, FreshnessDefinition};
use crate::schemas::project::configs::common::{default_meta_and_tags, default_quoting};
use crate::schemas::project::{DefaultTo, IterChildren};
use crate::schemas::serde::{bool_or_string_bool, StringOrArrayOfStrings};

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectSourceConfig {
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+event_time")]
    pub event_time: Option<String>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
    #[serde(rename = "+freshness")]
    pub freshness: Option<FreshnessDefinition>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+quoting")]
    pub quoting: Option<DbtQuoting>,
    #[serde(rename = "+loaded_at_query")]
    pub loaded_at_query: Option<String>,
    #[serde(rename = "+loaded_at_field")]
    pub loaded_at_field: Option<String>,
    #[serde(rename = "+static_analysis")]
    pub static_analysis: Option<StaticAnalysisKind>,
    // Flattened fields
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectSourceConfig>>,
}

impl IterChildren<ProjectSourceConfig> for ProjectSourceConfig {
    fn iter_children(&self) -> Iter<String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema)]
pub struct SourceConfig {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub event_time: Option<String>,
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
    pub freshness: Option<FreshnessDefinition>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub quoting: Option<DbtQuoting>,
    pub loaded_at_field: Option<String>,
    pub loaded_at_query: Option<String>,
    pub static_analysis: Option<StaticAnalysisKind>,
}

impl From<ProjectSourceConfig> for SourceConfig {
    fn from(config: ProjectSourceConfig) -> Self {
        Self {
            enabled: config.enabled,
            event_time: config.event_time,
            meta: config.meta,
            freshness: config.freshness,
            tags: config.tags,
            quoting: config.quoting,
            loaded_at_field: config.loaded_at_field,
            loaded_at_query: config.loaded_at_query,
            static_analysis: config.static_analysis,
        }
    }
}

impl From<SourceConfig> for ProjectSourceConfig {
    fn from(config: SourceConfig) -> Self {
        Self {
            enabled: config.enabled,
            event_time: config.event_time,
            meta: config.meta,
            freshness: config.freshness,
            tags: config.tags,
            quoting: config.quoting,
            loaded_at_field: config.loaded_at_field,
            loaded_at_query: config.loaded_at_query,
            static_analysis: config.static_analysis,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl DefaultTo<SourceConfig> for SourceConfig {
    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn default_to(&mut self, parent: &SourceConfig) {
        let SourceConfig {
            ref mut enabled,
            ref mut event_time,
            ref mut meta,
            ref mut freshness,
            ref mut tags,
            ref mut quoting,
            ref mut loaded_at_field,
            ref mut loaded_at_query,
            ref mut static_analysis,
        } = self;

        #[allow(unused, clippy::let_unit_value)]
        let quoting = default_quoting(quoting, &parent.quoting);
        #[allow(unused, clippy::let_unit_value)]
        let meta = default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        #[allow(unused, clippy::let_unit_value)]
        let tags = ();

        default_to!(
            parent,
            [
                enabled,
                event_time,
                freshness,
                loaded_at_field,
                loaded_at_query,
                static_analysis
            ]
        );
    }
}
