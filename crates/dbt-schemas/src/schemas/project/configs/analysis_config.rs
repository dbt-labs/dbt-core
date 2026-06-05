use dbt_proc_macros::Resolvable;
use dbt_yaml::{DbtSchema, Spanned};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

// Type aliases for clarity
type YmlValue = dbt_yaml::Value;

use crate::schemas::common::DocsConfig;
use crate::schemas::project::{ResolvableConfig, TypedRecursiveConfig};
use crate::schemas::serde::{StringOrArrayOfStrings, bool_or_string_bool};
use dbt_common::io_args::StaticAnalysisKind;
use dbt_yaml::ShouldBe;
use std::collections::btree_map::Iter;

// NOTE: No #[skip_serializing_none] - we handle None serialization in serialize_with_mode
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct ProjectAnalysisConfig {
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+static_analysis")]
    pub static_analysis: Option<Spanned<StaticAnalysisKind>>,
    #[serde(rename = "+meta")]
    pub meta: Option<IndexMap<String, YmlValue>>,
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
            static_analysis: Some(AnalysesConfig::default_static_analysis()),
            meta: None,
            tags: None,
            docs: None,
            group: None,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl TypedRecursiveConfig for ProjectAnalysisConfig {
    fn type_name() -> &'static str {
        "analysis"
    }

    fn iter_children(&'_ self) -> Iter<'_, String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Resolvable, Deserialize, Serialize, Debug, Default, Clone, PartialEq, Eq, DbtSchema)]
pub struct AnalysesConfig {
    #[resolved(promote, method = get_enabled_with_default)]
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    // We don't want to do static analysis for analysis nodes unless they are explicitly enabled
    #[resolved(promote, default = StaticAnalysisKind::Off.into())]
    pub static_analysis: Option<Spanned<StaticAnalysisKind>>,
    pub meta: Option<IndexMap<String, YmlValue>>,
    #[serde(
        default,
        serialize_with = "crate::schemas::nodes::serialize_none_as_empty_list"
    )]
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

impl ResolvableConfig<AnalysesConfig> for AnalysesConfig {
    type Resolved = ResolvedAnalysesConfig;
    type PackageDefaults = ();
    type ResolveDefaults = ();

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

    fn get_enabled_with_default(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    fn disable(&mut self) {
        self.enabled = Some(false);
    }

    fn apply_package_defaults(&mut self, _: ()) {}

    fn finalize(self) -> ResolvedAnalysesConfig {
        self.finalize_resolved()
    }
}
