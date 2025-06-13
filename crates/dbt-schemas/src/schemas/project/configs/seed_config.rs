use dbt_common::FsError;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Spanned;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use crate::dbt_utils::validate_delimeter;
use crate::schemas::common::DbtQuoting;
use crate::schemas::common::DocsConfig;
use crate::schemas::common::Hooks;
use crate::schemas::common::PersistDocsConfig;
use crate::schemas::manifest::DbtConfig;
use crate::schemas::serde::try_from_value;
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
    #[serde(rename = "+enabled")]
    pub enabled: Option<bool>,
    #[serde(rename = "+full_refresh")]
    pub full_refresh: Option<bool>,
    #[serde(rename = "+grants")]
    pub grants: Option<serde_json::Value>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    #[serde(rename = "+meta")]
    pub meta: Option<serde_json::Value>,
    #[serde(rename = "+persist_docs")]
    pub persist_docs: Option<PersistDocsConfig>,
    #[serde(rename = "+post-hook")]
    pub post_hook: Verbatim<Option<Hooks>>,
    #[serde(rename = "+pre-hook")]
    pub pre_hook: Verbatim<Option<Hooks>>,
    #[serde(rename = "+quote_columns")]
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
    pub delimiter: Spanned<Option<String>>,
    // Flattened field:
    pub __additional_properties__: Verbatim<BTreeMap<String, dbt_serde_yaml::Value>>,
}

impl TryFrom<&ProjectSeedConfig> for DbtConfig {
    type Error = Box<FsError>;

    fn try_from(seed_configs: &ProjectSeedConfig) -> Result<Self, Self::Error> {
        Ok(DbtConfig {
            column_types: seed_configs.column_types.clone(),
            copy_grants: seed_configs.copy_grants,
            database: seed_configs.database.clone(),
            schema: seed_configs.schema.clone(),
            docs: seed_configs.docs.clone(),
            enabled: seed_configs.enabled,
            group: seed_configs.group.clone(),
            meta: try_from_value(seed_configs.meta.clone())?,
            tags: match &seed_configs.tags {
                Some(StringOrArrayOfStrings::String(tags)) => {
                    Some(tags.split(',').map(|s| s.to_string()).collect())
                }
                Some(StringOrArrayOfStrings::ArrayOfStrings(tags)) => Some(tags.clone()),
                None => None,
            },
            full_refresh: seed_configs.full_refresh,
            grants: try_from_value(seed_configs.grants.clone())?,
            persist_docs: seed_configs.persist_docs.clone(),
            pre_hook: (*seed_configs.pre_hook).clone(),
            post_hook: (*seed_configs.post_hook).clone(),
            quote_columns: seed_configs.quote_columns,
            snowflake_warehouse: seed_configs.snowflake_warehouse.clone(),
            transient: seed_configs.transient,
            alias: seed_configs.alias.clone(),
            delimiter: validate_delimeter(&seed_configs.delimiter)?,
            ..Default::default()
        })
    }
}
