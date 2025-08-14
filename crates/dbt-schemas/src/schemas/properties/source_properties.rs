use crate::schemas::common::DbtQuoting;
use crate::schemas::common::FreshnessDefinition;
use crate::schemas::data_tests::DataTests;
use crate::schemas::dbt_column::ColumnProperties;
use crate::schemas::project::SourceConfig;
use crate::schemas::serde::StringOrArrayOfStrings;
use crate::schemas::serde::bool_or_string_bool;
use dbt_common::serde_utils::Omissible;
use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct SourceProperties {
    pub config: Option<SourceConfig>,
    pub database: Option<String>,
    // TODO: support alias then we can remove this field and use #[serde[alias = "catalog"]] on database
    pub catalog: Option<String>,
    pub description: Option<String>,
    pub loader: Option<String>,
    pub name: String,
    pub quoting: Option<DbtQuoting>,
    pub schema: Option<String>,
    pub tables: Option<Vec<Tables>>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct Tables {
    pub columns: Option<Vec<ColumnProperties>>,
    pub config: Option<TablesConfig>,
    pub data_tests: Option<Vec<DataTests>>,
    pub description: Option<String>,
    pub external: Option<YmlValue>,
    pub identifier: Option<String>,
    pub loader: Option<String>,
    pub name: String,
    pub quoting: Option<DbtQuoting>,
    pub tests: Option<Vec<DataTests>>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, Default)]
pub struct TablesConfig {
    pub event_time: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub meta: Option<BTreeMap<String, YmlValue>>,
    pub freshness: Omissible<Option<FreshnessDefinition>>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub loaded_at_field: Option<String>,
    pub loaded_at_query: Option<String>,
}
