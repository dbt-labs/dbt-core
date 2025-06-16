use std::collections::BTreeMap;

use dbt_common::FsError;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::schemas::common::DbtMaterialization;
use crate::schemas::common::DbtUniqueKey;
use crate::schemas::common::DocsConfig;
use crate::schemas::common::Hooks;
use crate::schemas::common::PersistDocsConfig;
use crate::schemas::data_tests::DataTests;
use crate::schemas::dbt_column::ColumnProperties;
use crate::schemas::manifest::DbtConfig;
use crate::schemas::serde::try_from_value;
use crate::schemas::serde::try_string_to_type;
use crate::schemas::serde::StringOrArrayOfStrings;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct SnapshotProperties {
    pub name: String,
    pub relation: Option<String>,
    pub columns: Option<Vec<ColumnProperties>>,
    pub config: Option<SnapshotsConfig>,
    pub data_tests: Option<Vec<DataTests>>,
    pub description: Option<String>,
    pub tests: Option<Vec<DataTests>>,
}

impl SnapshotProperties {
    pub fn empty(name: String) -> Self {
        Self {
            name,
            relation: None,
            columns: None,
            config: None,
            data_tests: None,
            description: None,
            tests: None,
        }
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct SnapshotsConfigSnapshotMetaColumnNames {
    #[serde(default = "default_dbt_scd_id")]
    pub dbt_scd_id: String,
    #[serde(default = "default_dbt_updated_at")]
    pub dbt_updated_at: String,
    #[serde(default = "default_dbt_valid_from")]
    pub dbt_valid_from: String,
    #[serde(default = "default_dbt_valid_to")]
    pub dbt_valid_to: String,
    #[serde(default = "default_dbt_is_deleted")]
    pub dbt_is_deleted: String,
}

fn default_dbt_scd_id() -> String {
    "DBT_SCD_ID".to_string()
}

fn default_dbt_updated_at() -> String {
    "DBT_UPDATED_AT".to_string()
}

fn default_dbt_valid_from() -> String {
    "DBT_VALID_FROM".to_string()
}

fn default_dbt_valid_to() -> String {
    "DBT_VALID_TO".to_string()
}

fn default_dbt_is_deleted() -> String {
    "DBT_IS_DELETED".to_string()
}

impl From<SnapshotsConfigSnapshotMetaColumnNames> for BTreeMap<String, String> {
    fn from(value: SnapshotsConfigSnapshotMetaColumnNames) -> Self {
        let mut map = BTreeMap::new();
        map.insert("dbt_scd_id".to_string(), value.dbt_scd_id);
        map.insert("dbt_updated_at".to_string(), value.dbt_updated_at);
        map.insert("dbt_valid_from".to_string(), value.dbt_valid_from);
        map.insert("dbt_valid_to".to_string(), value.dbt_valid_to);
        map.insert("dbt_is_deleted".to_string(), value.dbt_is_deleted);
        map
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct SnapshotsConfig {
    pub alias: Option<String>,
    pub check_cols: Option<StringOrArrayOfStrings>,
    pub dbt_valid_to_current: Option<String>,
    pub enabled: Option<bool>,
    pub grants: Option<serde_json::Value>,
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
    pub persist_docs: Option<PersistDocsConfig>,
    pub group: Option<String>,
    #[serde(rename = "post-hook")]
    pub post_hook: Verbatim<Option<Hooks>>,
    #[serde(rename = "pre-hook")]
    pub pre_hook: Verbatim<Option<Hooks>>,
    pub quote_columns: Option<bool>,
    pub snapshot_meta_column_names: Option<SnapshotsConfigSnapshotMetaColumnNames>,
    pub strategy: Option<String>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub unique_key: Option<StringOrArrayOfStrings>,
    pub updated_at: Option<String>,
    pub hard_deletes: Option<String>,
    pub invalidate_hard_deletes: Option<bool>,
    pub docs: Option<DocsConfig>,
    pub event_time: Option<String>,
}

impl TryFrom<&SnapshotsConfig> for DbtConfig {
    type Error = Box<FsError>;
    fn try_from(config: &SnapshotsConfig) -> Result<Self, Self::Error> {
        Ok(DbtConfig {
            alias: config.alias.clone(),
            check_cols: config
                .check_cols
                .clone()
                .map(|check_cols| check_cols.try_into())
                .transpose()?,
            enabled: config.enabled,
            grants: try_from_value(config.grants.clone())?,
            persist_docs: config.persist_docs.clone(),
            post_hook: (*config.post_hook).clone(),
            pre_hook: (*config.pre_hook).clone(),
            quote_columns: config.quote_columns,
            strategy: try_string_to_type(&config.strategy)?,
            tags: match &config.tags {
                Some(StringOrArrayOfStrings::String(tags)) => {
                    Some(tags.split(',').map(|s| s.to_string()).collect())
                }
                Some(StringOrArrayOfStrings::ArrayOfStrings(tags)) => Some(tags.clone()),
                None => None,
            },
            database: config.database.clone(),
            schema: config.schema.clone(),
            unique_key: match &config.unique_key {
                Some(StringOrArrayOfStrings::String(unique_key)) => {
                    Some(DbtUniqueKey::Single(unique_key.clone()))
                }
                Some(StringOrArrayOfStrings::ArrayOfStrings(unique_key)) => {
                    Some(DbtUniqueKey::Multiple(unique_key.clone()))
                }
                None => None,
            },
            updated_at: config.updated_at.clone(),
            snapshot_table_column_names: config
                .snapshot_meta_column_names
                .clone()
                .map(|s| s.into()),
            hard_deletes: config
                .hard_deletes
                .clone()
                .map(|s| s.try_into())
                .transpose()?,
            materialized: Some(DbtMaterialization::Snapshot),
            ..Default::default()
        })
    }
}
