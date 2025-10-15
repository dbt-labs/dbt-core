use dbt_common::io_args::StaticAnalysisKind;
use dbt_common::serde_utils::Omissible;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::ShouldBe;
use serde::{Deserialize, Serialize};
// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;
use std::collections::btree_map::Iter;

use super::omissible_utils::handle_omissible_override;

use crate::default_to;
use crate::schemas::common::DocsConfig;
use crate::schemas::common::{Access, DbtQuoting};
use crate::schemas::project::configs::common::WarehouseSpecificNodeConfig;
use crate::schemas::project::configs::common::{
    default_meta_and_tags, default_quoting, default_to_grants,
};
use crate::schemas::project::dbt_project::DefaultTo;
use crate::schemas::project::dbt_project::IterChildren;
use crate::schemas::serde::StringOrArrayOfStrings;
use crate::schemas::serde::{bool_or_string_bool, default_type};

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectFunctionConfig {
    #[serde(rename = "+access")]
    pub access: Option<Access>,
    #[serde(rename = "+alias")]
    pub alias: Option<String>,
    #[serde(rename = "+database", alias = "+project")]
    pub database: Omissible<Option<String>>,
    #[serde(rename = "+description")]
    pub description: Option<String>,
    #[serde(rename = "+docs")]
    pub docs: Option<DocsConfig>,
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+grants")]
    pub grants: Option<BTreeMap<String, StringOrArrayOfStrings>>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    #[serde(rename = "+language")]
    pub language: Option<String>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+on_configuration_change")]
    pub on_configuration_change: Option<String>,
    #[serde(rename = "+quoting")]
    pub quoting: Option<DbtQuoting>,
    #[serde(rename = "+schema")]
    pub schema: Omissible<Option<String>>,
    #[serde(rename = "+static_analysis")]
    pub static_analysis: Option<StaticAnalysisKind>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,

    // Additional properties for directory structure
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectFunctionConfig>>,
}

impl Default for ProjectFunctionConfig {
    fn default() -> Self {
        Self {
            access: None,
            alias: None,
            database: Omissible::Omitted,
            description: None,
            docs: None,
            enabled: None,
            grants: None,
            group: None,
            language: None,
            meta: None,
            on_configuration_change: None,
            quoting: None,
            schema: Omissible::Omitted,
            static_analysis: None,
            tags: None,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl DefaultTo<ProjectFunctionConfig> for ProjectFunctionConfig {
    fn default_to(&mut self, parent: &ProjectFunctionConfig) {
        let ProjectFunctionConfig {
            access,
            alias,
            database,
            description,
            docs,
            enabled,
            grants,
            group,
            language,
            meta,
            on_configuration_change,
            quoting,
            schema,
            static_analysis,
            tags,
            __additional_properties__: _,
        } = self;

        // Handle special cases
        default_quoting(quoting, &parent.quoting);
        default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        default_to_grants(grants, &parent.grants);
        handle_omissible_override(database, &parent.database);
        handle_omissible_override(schema, &parent.schema);

        default_to!(
            parent,
            [
                access,
                alias,
                description,
                docs,
                enabled,
                group,
                language,
                on_configuration_change,
                static_analysis,
            ]
        );
    }
}

impl IterChildren<ProjectFunctionConfig> for ProjectFunctionConfig {
    fn iter_children(&self) -> Iter<'_, String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct FunctionConfig {
    pub access: Option<Access>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub alias: Option<String>,
    pub database: Omissible<Option<String>>,
    pub schema: Omissible<Option<String>>,
    pub tags: Option<StringOrArrayOfStrings>,
    // need default to ensure None if field is not set
    #[serde(default, deserialize_with = "default_type")]
    pub meta: Option<BTreeMap<String, YmlValue>>,
    pub group: Option<String>,
    pub docs: Option<DocsConfig>,
    pub grants: Option<BTreeMap<String, StringOrArrayOfStrings>>,
    pub quoting: Option<DbtQuoting>,
    pub language: Option<String>,
    pub on_configuration_change: Option<String>,
    pub static_analysis: Option<StaticAnalysisKind>,

    // Warehouse-specific configurations
    pub __warehouse_specific_config__: WarehouseSpecificNodeConfig,
}

impl DefaultTo<FunctionConfig> for FunctionConfig {
    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn database(&self) -> Option<String> {
        self.database.clone().into_inner().unwrap_or(None)
    }

    fn schema(&self) -> Option<String> {
        self.schema.clone().into_inner().unwrap_or(None)
    }

    fn alias(&self) -> Option<String> {
        self.alias.clone()
    }

    fn default_to(&mut self, parent: &FunctionConfig) {
        let FunctionConfig {
            access,
            enabled,
            alias,
            database,
            schema,
            tags,
            meta,
            group,
            docs,
            grants,
            quoting,
            language,
            on_configuration_change,
            static_analysis,
            __warehouse_specific_config__: warehouse_config,
        } = self;

        // Handle warehouse config
        warehouse_config.default_to(&parent.__warehouse_specific_config__);

        // Handle omissible database and schema fields separately
        handle_omissible_override(database, &parent.database);
        handle_omissible_override(schema, &parent.schema);

        default_to!(
            parent,
            [
                access,
                enabled,
                alias,
                tags,
                meta,
                group,
                docs,
                grants,
                quoting,
                language,
                on_configuration_change,
                static_analysis,
            ]
        );
    }
}

impl From<ProjectFunctionConfig> for FunctionConfig {
    fn from(config: ProjectFunctionConfig) -> Self {
        Self {
            access: config.access,
            enabled: config.enabled,
            alias: config.alias,
            database: config.database,
            schema: config.schema,
            tags: config.tags,
            meta: config.meta,
            group: config.group,
            docs: config.docs,
            grants: config.grants,
            quoting: config.quoting,
            language: config.language,
            on_configuration_change: config.on_configuration_change,
            static_analysis: config.static_analysis,
            __warehouse_specific_config__: WarehouseSpecificNodeConfig::default(),
        }
    }
}
