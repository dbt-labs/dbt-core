use crate::dbt_utils::validate_delimeter;
use crate::schemas::common::DocsConfig;
use crate::schemas::common::Hooks;
use crate::schemas::common::PersistDocsConfig;
use crate::schemas::data_tests::DataTests;
use crate::schemas::dbt_column::ColumnProperties;
use crate::schemas::manifest::DbtConfig;
use crate::schemas::serde::try_from_value;
use crate::schemas::serde::StringOrArrayOfStrings;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Spanned;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct SeedProperties {
    pub columns: Option<Vec<ColumnProperties>>,
    pub config: Option<SeedsConfig>,
    pub data_tests: Verbatim<Option<Vec<DataTests>>>,
    pub description: Option<String>,
    pub name: String,
    pub tests: Option<Vec<DataTests>>,
}

impl SeedProperties {
    pub fn empty(name: String) -> Self {
        Self {
            name,
            columns: None,
            config: None,
            data_tests: Verbatim(None),
            description: None,
            tests: None,
        }
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct SeedsConfig {
    pub column_types: Option<BTreeMap<String, String>>,
    pub copy_grants: Option<bool>,
    pub database: Option<String>,
    pub docs: Option<DocsConfig>,
    pub enabled: Option<bool>,
    pub grants: Option<serde_json::Value>,
    pub quote_columns: Option<bool>,
    pub schema: Option<String>,
    pub alias: Option<String>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub persist_docs: Option<PersistDocsConfig>,
    pub delimiter: Spanned<Option<String>>,
    pub event_time: Option<String>,
    pub full_refresh: Option<bool>,
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
    pub post_hook: Verbatim<Option<Hooks>>,
    pub pre_hook: Verbatim<Option<Hooks>>,
    pub group: Option<String>,
}

impl TryFrom<&SeedsConfig> for DbtConfig {
    type Error = Box<dyn std::error::Error>;
    fn try_from(config: &SeedsConfig) -> Result<Self, Self::Error> {
        Ok(DbtConfig {
            enabled: config.enabled,
            database: config.database.clone(),
            schema: config.schema.clone(),
            column_types: config.column_types.clone(),
            grants: try_from_value(config.grants.clone())?,
            quote_columns: config.quote_columns,
            alias: config.alias.clone(),
            persist_docs: config.persist_docs.clone(),
            tags: match &config.tags {
                Some(StringOrArrayOfStrings::String(tags)) => {
                    Some(tags.split(',').map(|s| s.to_string()).collect())
                }
                Some(StringOrArrayOfStrings::ArrayOfStrings(tags)) => Some(tags.clone()),
                None => None,
            },
            delimiter: validate_delimeter(&config.delimiter)?,
            ..Default::default()
        })
    }
}
