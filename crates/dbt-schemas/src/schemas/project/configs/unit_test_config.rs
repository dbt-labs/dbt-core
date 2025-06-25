use dbt_common::io_args::StaticAnalysisKind;
use dbt_serde_yaml::{JsonSchema, ShouldBe};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::{btree_map::Iter, BTreeMap};

use crate::{
    default_to,
    schemas::{
        project::{configs::common::default_meta_and_tags, DefaultTo, IterChildren},
        serde::{bool_or_string_bool, StringOrArrayOfStrings},
    },
};

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectUnitTestConfig {
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+static_analysis")]
    pub static_analysis: Option<StaticAnalysisKind>,
    // Flattened fields
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectUnitTestConfig>>,
}

impl IterChildren<ProjectUnitTestConfig> for ProjectUnitTestConfig {
    fn iter_children(&self) -> Iter<String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema)]
pub struct UnitTestConfig {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub static_analysis: Option<StaticAnalysisKind>,
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
    pub tags: Option<StringOrArrayOfStrings>,
}

impl From<ProjectUnitTestConfig> for UnitTestConfig {
    fn from(config: ProjectUnitTestConfig) -> Self {
        Self {
            enabled: config.enabled,
            static_analysis: config.static_analysis,
            meta: config.meta,
            tags: config.tags,
        }
    }
}

impl From<UnitTestConfig> for ProjectUnitTestConfig {
    fn from(config: UnitTestConfig) -> Self {
        Self {
            enabled: config.enabled,
            static_analysis: config.static_analysis,
            meta: config.meta,
            tags: config.tags,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl DefaultTo<UnitTestConfig> for UnitTestConfig {
    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn default_to(&mut self, parent: &UnitTestConfig) {
        let UnitTestConfig {
            ref mut enabled,
            ref mut static_analysis,
            ref mut meta,
            ref mut tags,
        } = self;

        #[allow(unused, clippy::let_unit_value)]
        let meta = default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        #[allow(unused, clippy::let_unit_value)]
        let tags = ();

        default_to!(parent, [enabled, static_analysis]);
    }
}
