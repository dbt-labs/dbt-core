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
        serde::StringOrArrayOfStrings,
    },
};

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectSemanticModelConfig {
    pub enabled: Option<bool>,
    pub group: Option<String>,
    pub meta: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectSemanticModelConfig>>,
}

impl IterChildren<ProjectSemanticModelConfig> for ProjectSemanticModelConfig {
    fn iter_children(&self) -> Iter<String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema)]
pub struct SemanticModelConfig {
    pub enabled: Option<bool>,
    pub group: Option<String>,
    pub meta: Option<BTreeMap<String, YmlValue>>,
    pub tags: Option<StringOrArrayOfStrings>,
}

impl From<ProjectSemanticModelConfig> for SemanticModelConfig {
    fn from(config: ProjectSemanticModelConfig) -> Self {
        Self {
            enabled: config.enabled,
            group: config.group,
            meta: config.meta,
            tags: config.tags,
        }
    }
}

impl From<SemanticModelConfig> for ProjectSemanticModelConfig {
    fn from(config: SemanticModelConfig) -> Self {
        Self {
            enabled: config.enabled,
            group: config.group,
            meta: config.meta,
            tags: config.tags,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl DefaultTo<SemanticModelConfig> for SemanticModelConfig {
    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn default_to(&mut self, parent: &SemanticModelConfig) {
        let SemanticModelConfig {
            enabled,
            group,
            meta,
            tags,
        } = self;

        #[allow(unused, clippy::let_unit_value)]
        let meta = default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        #[allow(unused)]
        let tags = ();

        default_to!(parent, [enabled, group]);
    }
}
