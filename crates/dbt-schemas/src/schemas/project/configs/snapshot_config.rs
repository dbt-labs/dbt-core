use dbt_common::FsError;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use crate::schemas::common::DbtQuoting;
use crate::schemas::common::Hooks;
use crate::schemas::common::PersistDocsConfig;
use crate::schemas::manifest::DbtConfig;
use crate::schemas::serde::try_from_value;
use crate::schemas::serde::StringOrArrayOfStrings;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectSnapshotConfig {
    // Snapshot-specific Configuration
    #[serde(rename = "+database")]
    pub database: Option<String>,
    #[serde(rename = "+schema")]
    pub schema: Option<String>,
    #[serde(rename = "+alias")]
    pub alias: Option<String>,
    #[serde(rename = "+strategy")]
    pub strategy: Option<String>,
    #[serde(rename = "+unique_key")]
    pub unique_key: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+check_cols")]
    pub check_cols: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+updated_at")]
    pub updated_at: Option<String>,
    #[serde(rename = "+dbt_valid_to_current")]
    pub dbt_valid_to_current: Option<String>,
    #[serde(rename = "+snapshot_meta_column_names")]
    pub snapshot_meta_column_names: Option<BTreeMap<String, String>>,
    #[serde(rename = "+hard_deletes")]
    pub hard_deletes: Option<String>,
    // General Configuration
    #[serde(rename = "+enabled")]
    pub enabled: Option<bool>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+pre-hook")]
    pub pre_hook: Verbatim<Option<Hooks>>,
    #[serde(rename = "+post-hook")]
    pub post_hook: Verbatim<Option<Hooks>>,
    #[serde(rename = "+persist_docs")]
    pub persist_docs: Option<PersistDocsConfig>,
    #[serde(rename = "+grants")]
    pub grants: Option<serde_json::Value>,
    #[serde(rename = "+event_time")]
    pub event_time: Option<String>,
    #[serde(rename = "+target_schema")]
    pub target_schema: Option<String>,
    #[serde(rename = "+quoting")]
    pub quoting: Option<DbtQuoting>,
    // Flattened field:
    pub __additional_properties__: Verbatim<BTreeMap<String, dbt_serde_yaml::Value>>,
}

impl TryFrom<&ProjectSnapshotConfig> for DbtConfig {
    type Error = Box<FsError>;

    fn try_from(snapshot_configs: &ProjectSnapshotConfig) -> Result<Self, Self::Error> {
        Ok(DbtConfig {
            database: snapshot_configs.database.clone(),
            schema: snapshot_configs.schema.clone(),
            alias: snapshot_configs.alias.clone(),
            strategy: snapshot_configs.strategy.clone(),
            unique_key: snapshot_configs
                .unique_key
                .clone()
                .map(|unique_key| unique_key.into()),
            check_cols: snapshot_configs
                .check_cols
                .clone()
                .map(|check_cols| check_cols.try_into())
                .transpose()?,
            updated_at: snapshot_configs.updated_at.clone(),
            dbt_valid_to_current: snapshot_configs.dbt_valid_to_current.clone(),
            snapshot_table_column_names: snapshot_configs.snapshot_meta_column_names.clone(),
            hard_deletes: snapshot_configs
                .hard_deletes
                .clone()
                .map(|v| v.try_into())
                .transpose()?,
            enabled: snapshot_configs.enabled,
            tags: match &snapshot_configs.tags {
                Some(StringOrArrayOfStrings::String(tags)) => {
                    Some(tags.split(',').map(|s| s.to_string()).collect())
                }
                Some(StringOrArrayOfStrings::ArrayOfStrings(tags)) => Some(tags.clone()),
                None => None,
            },
            pre_hook: (*snapshot_configs.pre_hook).clone(),
            post_hook: (*snapshot_configs.post_hook).clone(),
            persist_docs: snapshot_configs.persist_docs.clone(),
            grants: try_from_value(snapshot_configs.grants.clone())?,
            ..Default::default()
        })
    }
}
