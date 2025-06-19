use std::collections::{btree_map::Iter, BTreeMap};

use dbt_serde_yaml::{JsonSchema, ShouldBe};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::{
    default_to,
    schemas::{
        project::{configs::common::default_meta_and_tags, DefaultTo, IterChildren},
        serde::{bool_or_string_bool, StringOrArrayOfStrings},
    },
};

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectSavedQueriesConfig {
    #[serde(rename = "+cache")]
    pub cache: Option<SavedQueriesConfigCache>,
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+export_as")]
    pub export_as: Option<ExportConfigExportAs>,
    #[serde(rename = "+schema")]
    pub schema: Option<String>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    // Flattened fields
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectSavedQueriesConfig>>,
}

impl IterChildren<ProjectSavedQueriesConfig> for ProjectSavedQueriesConfig {
    fn iter_children(&self) -> Iter<String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, Default, PartialEq, JsonSchema)]
pub struct SavedQueriesConfig {
    pub cache: Option<SavedQueriesConfigCache>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub export_as: Option<ExportConfigExportAs>,
    pub schema: Option<String>,
    pub group: Option<String>,
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
    pub tags: Option<StringOrArrayOfStrings>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, JsonSchema)]
pub struct SavedQueriesConfigCache {
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

impl From<ProjectSavedQueriesConfig> for SavedQueriesConfig {
    fn from(config: ProjectSavedQueriesConfig) -> Self {
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

impl From<SavedQueriesConfig> for ProjectSavedQueriesConfig {
    fn from(config: SavedQueriesConfig) -> Self {
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

impl DefaultTo<SavedQueriesConfig> for SavedQueriesConfig {
    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn default_to(&mut self, parent: &SavedQueriesConfig) {
        let SavedQueriesConfig {
            ref mut cache,
            ref mut enabled,
            ref mut export_as,
            ref mut schema,
            ref mut group,
            ref mut meta,
            ref mut tags,
        } = self;

        #[allow(unused, clippy::let_unit_value)]
        let meta = default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        #[allow(unused)]
        let tags = ();

        default_to!(parent, [cache, enabled, export_as, schema, group]);
    }
}
