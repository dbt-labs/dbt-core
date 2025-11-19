use dbt_serde_yaml::{JsonSchema, Spanned};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

use crate::schemas::common::DocsConfig;
use crate::schemas::project::{DefaultTo, IterChildren};
use crate::schemas::serde::{StringOrArrayOfStrings, bool_or_string_bool};
use dbt_common::io_args::StaticAnalysisKind;
use dbt_serde_yaml::ShouldBe;
use std::collections::btree_map::Iter;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectAnalysisConfig {
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+static_analysis")]
    pub static_analysis: Option<Spanned<StaticAnalysisKind>>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+docs")]
    pub docs: Option<DocsConfig>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    pub __additional_properties__: BTreeMap<String, ShouldBe<Self>>,
}

impl Default for ProjectAnalysisConfig {
    fn default() -> Self {
        Self {
            enabled: Some(true),
            static_analysis: Some(StaticAnalysisKind::Off.into()),
            meta: None,
            tags: None,
            docs: None,
            group: None,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl IterChildren<ProjectAnalysisConfig> for ProjectAnalysisConfig {
    fn iter_children(&'_ self) -> Iter<'_, String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Default, Clone, PartialEq, Eq, JsonSchema)]
pub struct AnalysesConfig {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub static_analysis: Option<Spanned<StaticAnalysisKind>>,
    pub meta: Option<BTreeMap<String, YmlValue>>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub description: Option<String>,
    pub docs: Option<DocsConfig>,
    pub group: Option<String>,
}

impl From<ProjectAnalysisConfig> for AnalysesConfig {
    fn from(config: ProjectAnalysisConfig) -> Self {
        Self {
            enabled: config.enabled,
            static_analysis: config.static_analysis,
            meta: config.meta,
            tags: config.tags,
            description: None,
            docs: config.docs,
            group: config.group,
        }
    }
}

impl DefaultTo<AnalysesConfig> for AnalysesConfig {
    fn default_to(&mut self, other: &AnalysesConfig) {
        if self.enabled.is_none() {
            self.enabled = other.enabled;
        }
        if self.static_analysis.is_none() {
            self.static_analysis = other.static_analysis.clone();
        }
        if self.meta.is_none() {
            self.meta = other.meta.clone();
        }
        if self.tags.is_none() {
            self.tags = other.tags.clone();
        }
        if self.description.is_none() {
            self.description = other.description.clone();
        }
        if self.group.is_none() {
            self.group = other.group.clone();
        }
    }

    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }
}
