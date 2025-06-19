use crate::schemas::common::DbtQuoting;
use crate::schemas::common::FreshnessDefinition;
use crate::schemas::data_tests::DataTests;
use crate::schemas::dbt_column::ColumnProperties;
use crate::schemas::project::SourceConfig;
use crate::schemas::serde::bool_or_string_bool;
use crate::schemas::serde::StringOrArrayOfStrings;
use dbt_common::err;
use dbt_common::serde_utils::Omissible;
use dbt_common::ErrorCode;
use dbt_common::FsResult;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Spanned;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::{BTreeMap, HashMap};

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct SourceProperties {
    pub config: Option<SourceConfig>,
    pub database: Option<String>,
    // TODO: support alias then we can remove this field and use #[serde[alias = "catalog"]] on database
    pub catalog: Option<String>,
    pub description: Option<String>,
    pub loaded_at_field: Option<String>,
    pub loaded_at_query: Option<String>,
    pub loader: Option<String>,
    pub name: String,
    pub overrides: Spanned<Option<String>>,
    pub quoting: Option<DbtQuoting>,
    pub schema: Option<String>,
    pub tables: Option<Vec<Tables>>,
}

impl SourceProperties {
    pub fn err_on_deprecated_overrides_for_source_properties(&self) -> FsResult<()> {
        if self.overrides.is_some() {
            return err!(
                code => ErrorCode::DeprecatedOption,
                loc => self.overrides.span().clone(),
                "The `overrides` field is deprecated. Please remove it from your project to continue.",
            );
        }
        Ok(())
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct Tables {
    pub columns: Option<Vec<ColumnProperties>>,
    pub config: Option<TablesConfig>,
    pub data_tests: Option<Vec<DataTests>>,
    pub description: Option<String>,
    pub external: Option<serde_json::Value>,
    pub identifier: Option<String>,
    pub loaded_at_field: Option<String>,
    pub loaded_at_query: Option<String>,
    pub loader: Option<String>,
    pub name: String,
    pub quoting: Option<DbtQuoting>,
    pub tests: Option<Vec<DataTests>>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, Default)]
pub struct TablesConfig {
    #[serde(flatten)]
    pub additional_properties: HashMap<String, serde_json::Value>,
    pub event_time: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
    pub freshness: Omissible<Option<FreshnessDefinition>>,
    pub tags: Option<StringOrArrayOfStrings>,
}
