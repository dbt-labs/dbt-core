use std::collections::{BTreeMap, btree_map::Iter};

use dbt_serde_yaml::{JsonSchema, ShouldBe};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

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
pub struct ProjectSavedQueryConfig {
    #[serde(rename = "+cache")]
    pub cache: Option<SavedQueryCache>,
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+export_as")]
    pub export_as: Option<ExportConfigExportAs>,
    #[serde(rename = "+schema")]
    pub schema: Option<String>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    // Flattened fields
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectSavedQueryConfig>>,
}

impl IterChildren<ProjectSavedQueryConfig> for ProjectSavedQueryConfig {
    fn iter_children(&self) -> Iter<String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, JsonSchema)]
pub struct SavedQueryConfig {
    pub cache: Option<SavedQueryCache>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub export_as: Option<ExportConfigExportAs>,
    pub schema: Option<String>,
    pub group: Option<String>,
    pub meta: Option<BTreeMap<String, YmlValue>>,
    pub tags: Option<StringOrArrayOfStrings>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, JsonSchema)]
pub struct SavedQueryCache {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default, PartialEq, JsonSchema)]
#[allow(non_camel_case_types)]
pub enum ExportConfigExportAs {
    #[default]
    table,
    view,
    cache,
}

impl Default for SavedQueryConfig {
    fn default() -> Self {
        Self {
            cache: Some(SavedQueryCache {
                enabled: Some(false),
            }),
            enabled: Some(true),
            export_as: None,
            schema: None,
            group: None,
            meta: Some(BTreeMap::new()),
            tags: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![])),
        }
    }
}

impl From<ProjectSavedQueryConfig> for SavedQueryConfig {
    fn from(config: ProjectSavedQueryConfig) -> Self {
        Self {
            cache: config.cache,
            enabled: config.enabled,
            export_as: config.export_as,
            schema: config.schema,
            group: config.group,
            meta: config.meta,
            tags: config.tags,
        }
    }
}

impl From<SavedQueryConfig> for ProjectSavedQueryConfig {
    fn from(config: SavedQueryConfig) -> Self {
        Self {
            cache: config.cache,
            enabled: config.enabled,
            export_as: config.export_as,
            schema: config.schema,
            group: config.group,
            meta: config.meta,
            tags: config.tags,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl DefaultTo<SavedQueryConfig> for SavedQueryConfig {
    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn default_to(&mut self, parent: &SavedQueryConfig) {
        let SavedQueryConfig {
            cache,
            enabled,
            export_as,
            schema,
            group,
            meta,
            tags,
        } = self;

        #[allow(unused, clippy::let_unit_value)]
        let meta = default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        #[allow(unused)]
        let tags = ();

        default_to!(parent, [cache, enabled, export_as, schema, group]);
    }
}
