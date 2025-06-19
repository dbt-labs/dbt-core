use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::ShouldBe;
use dbt_serde_yaml::Spanned;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;
use std::collections::btree_map::Iter;
use std::collections::BTreeMap;

use crate::default_to;
use crate::schemas::common::DbtQuoting;
use crate::schemas::common::DocsConfig;
use crate::schemas::common::Hooks;
use crate::schemas::common::PersistDocsConfig;
use crate::schemas::project::configs::common::default_column_types;
use crate::schemas::project::configs::common::default_hooks;
use crate::schemas::project::configs::common::default_meta_and_tags;
use crate::schemas::project::configs::common::default_quoting;
use crate::schemas::project::DefaultTo;
use crate::schemas::project::IterChildren;
use crate::schemas::serde::bool_or_string_bool;
use crate::schemas::serde::StringOrArrayOfStrings;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectSeedConfig {
    #[serde(rename = "+column_types")]
    pub column_types: Option<BTreeMap<String, String>>,
    #[serde(rename = "+copy_grants")]
    pub copy_grants: Option<bool>,
    #[serde(rename = "+database")]
    pub database: Option<String>,
    #[serde(rename = "+alias")]
    pub alias: Option<String>,
    #[serde(rename = "+docs")]
    pub docs: Option<DocsConfig>,
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+event_time")]
    pub event_time: Option<String>,
    #[serde(rename = "+full_refresh")]
    pub full_refresh: Option<bool>,
    #[serde(rename = "+grants")]
    pub grants: Option<BTreeMap<String, Value>>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, Value>>,
    #[serde(rename = "+persist_docs")]
    pub persist_docs: Option<PersistDocsConfig>,
    #[serde(rename = "+post-hook")]
    pub post_hook: Verbatim<Option<Hooks>>,
    #[serde(rename = "+pre-hook")]
    pub pre_hook: Verbatim<Option<Hooks>>,
    #[serde(
        default,
        rename = "+quote_columns",
        deserialize_with = "bool_or_string_bool"
    )]
    pub quote_columns: Option<bool>,
    #[serde(rename = "+schema")]
    pub schema: Option<String>,
    #[serde(rename = "+snowflake_warehouse")]
    pub snowflake_warehouse: Option<String>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+transient")]
    pub transient: Option<bool>,
    #[serde(rename = "+quoting")]
    pub quoting: Option<DbtQuoting>,
    #[serde(rename = "+delimiter")]
    pub delimiter: Option<Spanned<String>>,
    // Flattened fields
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectSeedConfig>>,
}

impl IterChildren<ProjectSeedConfig> for ProjectSeedConfig {
    fn iter_children(&self) -> Iter<String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq, Clone, JsonSchema)]
pub struct SeedConfig {
    pub column_types: Option<BTreeMap<String, String>>,
    pub copy_grants: Option<bool>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub alias: Option<String>,
    pub docs: Option<DocsConfig>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub grants: Option<BTreeMap<String, Value>>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub quote_columns: Option<bool>,
    pub delimiter: Option<Spanned<String>>,
    pub event_time: Option<String>,
    pub full_refresh: Option<bool>,
    pub group: Option<String>,
    pub meta: Option<BTreeMap<String, Value>>,
    pub persist_docs: Option<PersistDocsConfig>,
    pub post_hook: Verbatim<Option<Hooks>>,
    pub pre_hook: Verbatim<Option<Hooks>>,
    pub snowflake_warehouse: Option<String>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub transient: Option<bool>,
    pub quoting: Option<DbtQuoting>,
}

impl From<ProjectSeedConfig> for SeedConfig {
    fn from(config: ProjectSeedConfig) -> Self {
        Self {
            column_types: config.column_types,
            copy_grants: config.copy_grants,
            database: config.database,
            schema: config.schema,
            alias: config.alias,
            docs: config.docs,
            enabled: config.enabled,
            grants: config.grants,
            quote_columns: config.quote_columns,
            delimiter: config.delimiter,
            event_time: config.event_time,
            full_refresh: config.full_refresh,
            group: config.group,
            meta: config.meta,
            persist_docs: config.persist_docs,
            post_hook: config.post_hook,
            pre_hook: config.pre_hook,
            snowflake_warehouse: config.snowflake_warehouse,
            tags: config.tags,
            transient: config.transient,
            quoting: config.quoting,
        }
    }
}

impl From<SeedConfig> for ProjectSeedConfig {
    fn from(config: SeedConfig) -> Self {
        Self {
            column_types: config.column_types,
            copy_grants: config.copy_grants,
            database: config.database,
            schema: config.schema,
            alias: config.alias,
            docs: config.docs,
            enabled: config.enabled,
            grants: config.grants,
            quote_columns: config.quote_columns,
            delimiter: config.delimiter,
            event_time: config.event_time,
            full_refresh: config.full_refresh,
            group: config.group,
            meta: config.meta,
            persist_docs: config.persist_docs,
            post_hook: config.post_hook,
            pre_hook: config.pre_hook,
            snowflake_warehouse: config.snowflake_warehouse,
            tags: config.tags,
            transient: config.transient,
            quoting: config.quoting,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl DefaultTo<SeedConfig> for SeedConfig {
    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn default_to(&mut self, parent: &SeedConfig) {
        // Handle simple fields - using a pattern that ensures all fields are covered
        let SeedConfig {
            ref mut post_hook,
            ref mut pre_hook,
            ref mut meta,
            ref mut tags,
            ref mut quoting,
            ref mut column_types,
            ref mut copy_grants,
            ref mut database,
            ref mut schema,
            ref mut alias,
            ref mut docs,
            ref mut enabled,
            ref mut grants,
            ref mut quote_columns,
            ref mut delimiter,
            ref mut event_time,
            ref mut full_refresh,
            ref mut group,
            ref mut persist_docs,
            ref mut snowflake_warehouse,
            ref mut transient,
        } = self;

        #[allow(unused, clippy::let_unit_value)]
        let pre_hook = default_hooks(pre_hook, &parent.pre_hook);
        #[allow(unused, clippy::let_unit_value)]
        let post_hook = default_hooks(post_hook, &parent.post_hook);
        #[allow(unused, clippy::let_unit_value)]
        let quoting = default_quoting(quoting, &parent.quoting);
        #[allow(unused, clippy::let_unit_value)]
        let meta = default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        #[allow(unused, clippy::let_unit_value)]
        let tags = ();
        #[allow(unused, clippy::let_unit_value)]
        let column_types = default_column_types(column_types, &parent.column_types);

        default_to!(
            parent,
            [
                copy_grants,
                database,
                schema,
                alias,
                docs,
                enabled,
                grants,
                quote_columns,
                delimiter,
                event_time,
                full_refresh,
                group,
                persist_docs,
                snowflake_warehouse,
                transient,
            ]
        );
    }

    fn database(&self) -> Option<String> {
        self.database.clone()
    }

    fn schema(&self) -> Option<String> {
        self.schema.clone()
    }

    fn alias(&self) -> Option<String> {
        self.alias.clone()
    }
}
