use crate::default_to;
use crate::schemas::common::{
    merge_meta, merge_tags, Access, DbtQuoting, DocsConfig, FreshnessDefinition, Hooks,
    OnConfigurationChange, OnSchemaChange, PersistDocsConfig,
};
use crate::schemas::properties::ModelFreshness;
use crate::schemas::{
    common::{
        DbtBatchSize, DbtCheckColsSpec, DbtContract, DbtIncrementalStrategy, DbtMaterialization,
        DbtUniqueKey, HardDeletes,
    },
    manifest::{BigqueryPartitionConfigLegacy, GrantAccessToTarget},
    serde::{bool_or_string_bool, string_or_array, u64_or_string_u64},
};
use dbt_common::io_args::StaticAnalysisKind;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use super::BigqueryClusterConfig;

// TODO: This config is too general - it is all encompassing of models + tests
// We need to ensure configs are Node level - this config is useful
// when propagating configs from parent to child nodes (since it is a superset)
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DbtConfig {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub alias: Option<String>,
    pub schema: Option<String>,
    pub database: Option<String>,
    #[serde(default, deserialize_with = "string_or_array")]
    pub tags: Option<Vec<String>>,
    pub meta: Option<BTreeMap<String, Value>>,
    pub source_meta: Option<BTreeMap<String, Value>>,
    pub group: Option<String>,
    pub materialized: Option<DbtMaterialization>,
    pub incremental_strategy: Option<DbtIncrementalStrategy>,
    pub persist_docs: Option<PersistDocsConfig>,
    pub post_hook: Option<Hooks>,
    pub pre_hook: Option<Hooks>,
    pub quoting: Option<DbtQuoting>,
    pub column_types: Option<BTreeMap<String, String>>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub full_refresh: Option<bool>,
    pub unique_key: Option<DbtUniqueKey>,
    pub on_schema_change: Option<OnSchemaChange>,
    pub on_configuration_change: Option<OnConfigurationChange>,
    pub grants: Option<BTreeMap<String, Value>>,
    #[serde(default, deserialize_with = "string_or_array")]
    pub packages: Option<Vec<String>>,
    pub docs: Option<DocsConfig>,
    pub access: Option<Access>,
    pub delimiter: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub quote_columns: Option<bool>,
    pub strategy: Option<String>,
    pub updated_at: Option<String>,
    pub check_cols: Option<DbtCheckColsSpec>,
    #[serde(default, deserialize_with = "string_or_array")]
    pub merge_exclude_columns: Option<Vec<String>>,
    #[serde(default, deserialize_with = "string_or_array")]
    pub merge_update_columns: Option<Vec<String>>,
    pub sql_header: Option<String>,
    // Additional Test Config Parameters
    pub error_if: Option<String>,
    pub warn_if: Option<String>,
    pub fail_calc: Option<String>,
    pub limit: Option<i32>,
    pub severity: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub store_failures: Option<bool>,
    pub batch_size: Option<DbtBatchSize>,
    pub lookback: Option<i32>,
    pub begin: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub concurrent_batches: Option<bool>,
    pub contract: Option<DbtContract>,
    pub event_time: Option<String>,
    pub model_freshness: Option<ModelFreshness>,
    // Additional Source Config Parameters
    pub freshness: Option<FreshnessDefinition>,
    // below are configs for Snowflake
    pub external_volume: Option<String>,
    pub base_location_root: Option<String>,
    pub base_location_subpath: Option<String>,
    pub target_lag: Option<String>,
    pub snowflake_warehouse: Option<String>,
    pub refresh_mode: Option<String>,
    pub initialize: Option<String>,
    pub tmp_relation_type: Option<String>,
    pub query_tag: Option<String>,
    pub automatic_clustering: Option<bool>,
    pub secure: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub copy_grants: Option<bool>,
    // below are configs for BigQuery
    pub partition_by: Option<BigqueryPartitionConfigLegacy>,
    pub cluster_by: Option<BigqueryClusterConfig>,
    #[serde(default, deserialize_with = "u64_or_string_u64")]
    pub hours_to_expiration: Option<u64>,
    pub labels: Option<BTreeMap<String, String>>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub labels_from_meta: Option<bool>,
    pub kms_key_name: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub require_partition_filter: Option<bool>,
    #[serde(default, deserialize_with = "u64_or_string_u64")]
    pub partition_expiration_days: Option<u64>,
    pub grant_access_to: Option<Vec<GrantAccessToTarget>>,
    pub location: Option<String>,
    pub partitions: Option<Vec<String>>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enable_refresh: Option<bool>,
    #[serde(default, deserialize_with = "u64_or_string_u64")]
    pub refresh_interval_minutes: Option<u64>,
    pub description: Option<String>,
    pub max_staleness: Option<String>,
    // below are configs for Databricks
    pub file_format: Option<String>,
    pub table_format: Option<String>,
    pub location_root: Option<String>,
    pub tblproperties: Option<BTreeMap<String, Value>>,
    pub auto_liquid_cluster: Option<bool>,
    pub buckets: Option<i64>,
    pub clustered_by: Option<String>,
    pub compression: Option<String>,
    pub catalog: Option<String>,
    pub databricks_tags: Option<BTreeMap<String, Value>>,
    pub databricks_compute: Option<String>,
    pub liquid_clustered_by: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub include_full_name_in_path: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub transient: Option<bool>,
    pub dbt_valid_to_current: Option<String>,
    pub snapshot_table_column_names: Option<BTreeMap<String, String>>,
    pub hard_deletes: Option<HardDeletes>,
    pub expected_rows: Option<Value>,
    // Unsafe Designation
    #[serde(rename = "unsafe")]
    pub unsafe_: Option<bool>,
    pub skip_compile: Option<bool>,
    #[serde(rename = "where")]
    pub where_: Option<String>,
    pub auto_refresh: Option<bool>,
    pub backup: Option<bool>,
    pub static_analysis: Option<StaticAnalysisKind>,
}

impl DbtConfig {
    #[allow(clippy::cognitive_complexity)]
    pub fn default_to(&mut self, parent_config: &DbtConfig) {
        if let Some(quoting) = &mut self.quoting {
            if let Some(parent_quoting) = &parent_config.quoting {
                quoting.default_to(parent_quoting);
            }
        } else {
            self.quoting = parent_config.quoting;
        }
        if let Some(parent_pre_hooks) = &parent_config.pre_hook {
            if let Some(pre_hooks) = &mut self.pre_hook {
                pre_hooks.extend(parent_pre_hooks);
            } else {
                self.pre_hook = Some(parent_pre_hooks.clone());
            }
        }
        if let Some(parent_post_hooks) = &parent_config.post_hook {
            if let Some(post_hooks) = &mut self.post_hook {
                post_hooks.extend(parent_post_hooks);
            } else {
                self.post_hook = Some(parent_post_hooks.clone());
            }
        }

        // Handle meta and tags separately using merge functions
        self.meta = merge_meta(self.meta.take(), parent_config.meta.clone());
        self.tags = merge_tags(self.tags.take(), parent_config.tags.clone());
        default_to!(
            self,
            parent_config,
            [
                query_tag,
                alias,
                schema,
                database,
                group,
                incremental_strategy,
                materialized,
                persist_docs,
                full_refresh,
                unique_key,
                on_schema_change,
                on_configuration_change,
                grants,
                packages,
                docs,
                access,
                delimiter,
                quote_columns,
                strategy,
                updated_at,
                check_cols,
                snowflake_warehouse,
                merge_exclude_columns,
                merge_update_columns,
                sql_header,
                error_if,
                warn_if,
                fail_calc,
                limit,
                severity,
                store_failures,
                enabled,
                snapshot_table_column_names,
                hard_deletes,
                file_format,
                table_format,
                location_root,
                tblproperties,
                include_full_name_in_path,
                copy_grants,
                transient,
                dbt_valid_to_current,
                concurrent_batches,
                contract,
                event_time,
                partition_by,
                hours_to_expiration,
                labels,
                labels_from_meta,
                partition_expiration_days,
                grant_access_to,
                location,
                target_lag,
                kms_key_name,
                column_types,
                static_analysis,
                model_freshness
            ]
        );
    }

    pub fn to_btree_map(&self) -> BTreeMap<String, Value> {
        let value = serde_json::to_value(self).unwrap();
        serde_json::from_value(value).unwrap()
    }
    pub fn from_serde_json_value(value: Value) -> Self {
        serde_json::from_value(value).unwrap()
    }
    pub fn show_existing_fields(&self) -> String {
        let value = serde_json::to_value(self).unwrap();

        // Convert to a map and filter out None values
        if let Value::Object(map) = value {
            let filtered_map: serde_json::Map<String, Value> =
                map.into_iter().filter(|(_, v)| !v.is_null()).collect();

            serde_json::to_string(&Value::Object(filtered_map)).unwrap()
        } else {
            // Fallback in case the value is not an object (shouldn't happen)
            serde_json::to_string(&value).unwrap()
        }
    }
    pub fn is_enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }
}
