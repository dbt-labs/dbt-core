use dbt_common::FsResult;
use dbt_common::{err, FsError};
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Spanned;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use crate::schemas::common::{Access, DbtQuoting};
use crate::schemas::common::{DocsConfig, OnConfigurationChange};
use crate::schemas::common::{FreshnessRules, PersistDocsConfig};
use crate::schemas::common::{Hooks, OnSchemaChange};
use crate::schemas::manifest::{BigqueryClusterConfig, BigqueryPartitionConfigLegacy, DbtConfig};
use crate::schemas::properties::ModelFreshness;
use crate::schemas::serde::{try_from_value, try_string_to_type, StringOrArrayOfStrings};

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectModelConfig {
    #[serde(rename = "+access")]
    pub access: Option<Access>,
    #[serde(rename = "+alias")]
    pub alias: Option<String>,
    #[serde(rename = "+automatic_clustering")]
    pub automatic_clustering: Option<bool>,
    #[serde(rename = "+auto_refresh")]
    pub auto_refresh: Option<bool>,
    #[serde(rename = "+auto_liquid_cluster")]
    pub auto_liquid_cluster: Option<bool>,
    #[serde(rename = "+backup")]
    pub backup: Option<bool>,
    #[serde(rename = "+base_location_root")]
    pub base_location_root: Option<String>,
    #[serde(rename = "+base_location_subpath")]
    pub base_location_subpath: Option<String>,
    #[serde(rename = "+batch_size")]
    pub batch_size: Option<String>,
    #[serde(rename = "+begin")]
    pub begin: Option<String>,
    #[serde(rename = "+bind")]
    pub bind: Option<bool>,
    #[serde(rename = "+buckets")]
    pub buckets: Option<i64>,
    #[serde(rename = "+cluster_by")]
    pub cluster_by: Option<BigqueryClusterConfig>,
    #[serde(rename = "+clustered_by")]
    pub clustered_by: Option<String>,
    #[serde(rename = "+concurrent_batches")]
    pub concurrent_batches: Option<bool>,
    #[serde(rename = "+contract")]
    pub contract: Option<Contract>,
    #[serde(rename = "+compression")]
    pub compression: Option<String>,
    #[serde(rename = "+copy_grants")]
    pub copy_grants: Option<bool>,
    #[serde(rename = "+database")]
    pub database: Option<String>,
    #[serde(rename = "+databricks_compute")]
    pub databricks_compute: Option<String>,
    #[serde(rename = "+databricks_tags")]
    pub databricks_tags: Option<serde_json::Value>,
    #[serde(rename = "+description")]
    pub description: Option<String>,
    #[serde(rename = "+docs")]
    pub docs: Option<DocsConfig>,
    #[serde(rename = "+enable_refresh")]
    pub enable_refresh: Option<bool>,
    #[serde(rename = "+enabled")]
    pub enabled: Option<bool>,
    #[serde(rename = "+event_time")]
    pub event_time: Option<String>,
    #[serde(rename = "+external_volume")]
    pub external_volume: Option<String>,
    #[serde(rename = "+file_format")]
    pub file_format: Option<String>,
    #[serde(rename = "+freshness")]
    pub freshness: Option<ModelFreshness>,
    #[serde(rename = "+full_refresh")]
    pub full_refresh: Option<bool>,
    #[serde(rename = "+grant_access_to")]
    pub grant_access_to: Option<Vec<serde_json::Value>>,
    #[serde(rename = "+grants")]
    pub grants: Option<serde_json::Value>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    #[serde(rename = "+hours_to_expiration")]
    pub hours_to_expiration: Option<f32>,
    #[serde(rename = "+include_full_name_in_path")]
    pub include_full_name_in_path: Option<bool>,
    #[serde(rename = "+incremental_predicates")]
    pub incremental_predicates: Option<Vec<String>>,
    #[serde(rename = "+incremental_strategy")]
    pub incremental_strategy: Option<String>,
    #[serde(rename = "+initialize")]
    pub initialize: Option<String>,
    #[serde(rename = "+kms_key_name")]
    pub kms_key_name: Option<String>,
    #[serde(rename = "+labels")]
    pub labels: Option<serde_json::Value>,
    #[serde(rename = "+liquid_clustered_by")]
    pub liquid_clustered_by: Option<String>,
    #[serde(rename = "+location")]
    pub location: Option<String>,
    #[serde(rename = "+location_root")]
    pub location_root: Option<String>,
    #[serde(rename = "+lookback")]
    pub lookback: Option<f32>,
    #[serde(rename = "+materialized")]
    pub materialized: Option<String>,
    #[serde(rename = "+max_staleness")]
    pub max_staleness: Option<String>,
    #[serde(rename = "+merge_exclude_columns")]
    pub merge_exclude_columns: Spanned<Option<StringOrArrayOfStrings>>,
    #[serde(rename = "+merge_update_columns")]
    pub merge_update_columns: Spanned<Option<StringOrArrayOfStrings>>,
    #[serde(rename = "+meta")]
    pub meta: Option<serde_json::Value>,
    #[serde(rename = "+on_configuration_change")]
    pub on_configuration_change: Option<OnConfigurationChange>,
    #[serde(rename = "+on_schema_change")]
    pub on_schema_change: Option<OnSchemaChange>,
    #[serde(rename = "+partition_by")]
    pub partition_by: Option<BigqueryPartitionConfigLegacy>,
    #[serde(rename = "+partitions")]
    pub partitions: Option<Vec<String>>,
    #[serde(rename = "+persist_docs")]
    pub persist_docs: Option<PersistDocsConfig>,
    #[serde(rename = "+post-hook")]
    pub post_hook: Verbatim<Option<Hooks>>,
    #[serde(rename = "+pre-hook")]
    pub pre_hook: Verbatim<Option<Hooks>>,
    #[serde(rename = "+predicates")]
    pub predicates: Option<Vec<String>>,
    #[serde(rename = "+query_tag")]
    pub query_tag: Option<String>,
    #[serde(rename = "+quoting")]
    pub quoting: Option<DbtQuoting>,
    #[serde(rename = "+refresh_mode")]
    pub refresh_mode: Option<String>,
    #[serde(rename = "+refresh_interval_minutes")]
    pub refresh_interval_minutes: Option<u64>,
    #[serde(rename = "+schema")]
    pub schema: Option<String>,
    #[serde(rename = "+secure")]
    pub secure: Option<bool>,
    #[serde(rename = "+snowflake_warehouse")]
    pub snowflake_warehouse: Option<String>,
    #[serde(rename = "+sql_header")]
    pub sql_header: Option<String>,
    #[serde(rename = "+table_format")]
    pub table_format: Option<String>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+target_lag")]
    pub target_lag: Option<String>,
    #[serde(rename = "+tblproperties")]
    pub tblproperties: Option<serde_json::Value>,
    #[serde(rename = "+tmp_relation_type")]
    pub tmp_relation_type: Option<String>,
    #[serde(rename = "+transient")]
    pub transient: Option<bool>,
    #[serde(rename = "+unique_key")]
    pub unique_key: Option<StringOrArrayOfStrings>,
    // Flattened field:
    pub __additional_properties__: Verbatim<BTreeMap<String, dbt_serde_yaml::Value>>,
}

impl ProjectModelConfig {
    pub fn validate_merge_update_columns_xor(self) -> FsResult<()> {
        if self.merge_update_columns.is_some() && self.merge_exclude_columns.is_some() {
            let loc = self.merge_update_columns.span();
            return err!(
                code => dbt_common::ErrorCode::InvalidConfig,
                loc => loc.clone(),
                "merge_update_columns and merge_exclude_columns cannot both be set",
            );
        }
        Ok(())
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct Contract {
    pub alias_types: Option<bool>,
    pub enforced: bool,
}

#[allow(clippy::cognitive_complexity)]
impl TryFrom<&ProjectModelConfig> for DbtConfig {
    type Error = Box<FsError>;

    fn try_from(model_configs: &ProjectModelConfig) -> Result<Self, Self::Error> {
        model_configs.clone().validate_merge_update_columns_xor()?;
        if let Some(freshness) = &model_configs.freshness {
            FreshnessRules::validate(freshness.build_after.as_ref())?;
        }
        Ok(DbtConfig {
            access: model_configs.access.clone(),
            alias: model_configs.alias.clone(),
            database: model_configs.database.clone(),
            schema: model_configs.schema.clone(),
            docs: model_configs.docs.clone(),
            enabled: model_configs.enabled,
            full_refresh: model_configs.full_refresh,
            grants: try_from_value(model_configs.grants.clone())?,
            group: model_configs.group.clone(),
            incremental_strategy: try_string_to_type(&model_configs.incremental_strategy)?,
            materialized: try_string_to_type(&model_configs.materialized)?,
            meta: try_from_value(model_configs.meta.clone())?,
            on_configuration_change: model_configs.on_configuration_change.clone(),
            on_schema_change: model_configs.on_schema_change.clone(),
            persist_docs: model_configs.persist_docs.clone(),
            post_hook: (*model_configs.post_hook).clone(),
            pre_hook: (*model_configs.pre_hook).clone(),
            snowflake_warehouse: model_configs.snowflake_warehouse.clone(),
            sql_header: model_configs.sql_header.clone(),
            tags: match &model_configs.tags {
                Some(StringOrArrayOfStrings::String(tags)) => {
                    Some(tags.split(',').map(|s| s.to_string()).collect())
                }
                Some(StringOrArrayOfStrings::ArrayOfStrings(tags)) => Some(tags.clone()),
                None => None,
            },
            file_format: model_configs.file_format.clone(),
            table_format: model_configs.table_format.clone(),
            location_root: model_configs.location_root.clone(),
            tblproperties: try_from_value(model_configs.tblproperties.clone())?,
            include_full_name_in_path: model_configs.include_full_name_in_path,
            copy_grants: model_configs.copy_grants,
            merge_update_columns: match &*model_configs.merge_update_columns {
                Some(StringOrArrayOfStrings::String(tags)) => {
                    Some(tags.split(',').map(|s| s.to_string()).collect())
                }
                Some(StringOrArrayOfStrings::ArrayOfStrings(tags)) => Some(tags.clone()),
                None => None,
            },
            merge_exclude_columns: match &*model_configs.merge_exclude_columns {
                Some(StringOrArrayOfStrings::String(tags)) => {
                    Some(tags.split(',').map(|s| s.to_string()).collect())
                }
                Some(StringOrArrayOfStrings::ArrayOfStrings(tags)) => Some(tags.clone()),
                None => None,
            },
            model_freshness: model_configs.freshness.clone(),
            auto_liquid_cluster: model_configs.auto_liquid_cluster,
            buckets: model_configs.buckets,
            clustered_by: model_configs.clustered_by.clone(),
            compression: model_configs.compression.clone(),
            databricks_tags: try_from_value(model_configs.databricks_tags.clone())?,
            databricks_compute: model_configs.databricks_compute.clone(),
            liquid_clustered_by: model_configs.liquid_clustered_by.clone(),
            ..Default::default()
        })
    }
}
