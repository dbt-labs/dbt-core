use crate::schemas::common::Access;
use crate::schemas::common::Constraint;
use crate::schemas::common::DbtContract;
use crate::schemas::common::DbtUniqueKey;
use crate::schemas::common::DocsConfig;
use crate::schemas::common::FreshnessRules;
use crate::schemas::common::Hooks;
use crate::schemas::common::PersistDocsConfig;
use crate::schemas::common::Versions;
use crate::schemas::data_tests::DataTests;
use crate::schemas::dbt_column::ColumnProperties;
use crate::schemas::manifest::BigqueryClusterConfig;
use crate::schemas::manifest::BigqueryPartitionConfigLegacy;
use crate::schemas::manifest::DbtConfig;
use crate::schemas::manifest::GrantAccessToTarget;
use crate::schemas::serde::try_from_value;
use crate::schemas::serde::try_string_to_type;
use crate::schemas::serde::FloatOrString;
use crate::schemas::serde::StringOrArrayOfStrings;
use dbt_common::io_args::StaticAnalysisKind;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ModelProperties {
    pub columns: Option<Vec<ColumnProperties>>,
    pub config: Option<ModelPropertiesConfigs>,
    pub constraints: Option<Vec<Constraint>>,
    pub data_tests: Verbatim<Option<Vec<DataTests>>>,
    pub deprecation_date: Option<String>,
    pub description: Option<String>,
    pub identifier: Option<String>,
    pub latest_version: Option<FloatOrString>,
    pub name: String,
    pub tests: Verbatim<Option<Vec<DataTests>>>,
    pub time_spine: Option<ModelsTimeSpine>,
    pub versions: Option<Vec<Versions>>,
}

impl ModelProperties {
    pub fn empty(name: String) -> Self {
        Self {
            name,
            columns: None,
            config: None,
            constraints: None,
            data_tests: Verbatim(None),
            deprecation_date: None,
            description: None,
            identifier: None,
            latest_version: None,
            tests: Verbatim(None),
            time_spine: None,
            versions: None,
        }
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ModelPropertiesConfigs {
    pub access: Option<Access>,
    pub alias: Option<String>,
    pub auto_liquid_cluster: Option<bool>,
    pub auto_refresh: Option<bool>,
    pub automatic_clustering: Option<bool>,
    pub backup: Option<bool>,
    pub base_location_root: Option<String>,
    pub base_location_subpath: Option<String>,
    pub batch_size: Option<String>,
    pub begin: Option<String>,
    pub buckets: Option<i64>,
    pub cluster_by: Option<BigqueryClusterConfig>,
    pub clustered_by: Option<String>,
    pub compression: Option<String>,
    pub concurrent_batches: Option<bool>,
    pub contract: Option<DbtContract>,
    pub copy_grants: Option<bool>,
    pub database: Option<String>,
    pub databricks_tags: Option<serde_json::Value>,
    pub databricks_compute: Option<String>,
    pub description: Option<String>,
    pub docs: Option<DocsConfig>,
    pub enable_refresh: Option<bool>,
    pub enabled: Option<bool>,
    pub event_time: Option<String>,
    pub external_volume: Option<String>,
    pub file_format: Option<String>,
    pub freshness: Option<ModelFreshness>,
    pub full_refresh: Option<bool>,
    pub grant_access_to: Option<Vec<GrantAccessToTarget>>,
    pub grants: Option<serde_json::Value>,
    pub group: Option<String>,
    pub hours_to_expiration: Option<f32>,
    pub include_full_name_in_path: Option<bool>,
    pub incremental_strategy: Option<String>,
    pub initialize: Option<String>,
    pub kms_key_name: Option<String>,
    pub labels: Option<serde_json::Value>,
    pub liquid_clustered_by: Option<String>,
    pub location: Option<String>,
    pub location_root: Option<String>,
    pub lookback: Option<f32>,
    pub materialized: Option<String>,
    pub max_staleness: Option<String>,
    pub meta: Option<serde_json::Value>,
    pub on_configuration_change: Option<String>,
    pub on_schema_change: Option<String>,
    pub partition_by: Option<BigqueryPartitionConfigLegacy>,
    pub partitions: Option<Vec<String>>,
    pub persist_docs: Option<PersistDocsConfig>,
    pub post_hook: Verbatim<Option<Hooks>>,
    pub pre_hook: Verbatim<Option<Hooks>>,
    pub refresh_mode: Option<String>,
    pub refresh_interval_minutes: Option<u64>,
    pub schema: Option<String>,
    pub secure: Option<bool>,
    pub snowflake_warehouse: Option<String>,
    pub sql_header: Option<String>,
    pub table_format: Option<String>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub target_lag: Option<String>,
    pub tblproperties: Option<serde_json::Value>,
    pub tmp_relation_type: Option<String>,
    pub unique_key: Option<StringOrArrayOfStrings>,
    pub query_tag: Option<String>,
    pub static_analysis: Option<StaticAnalysisKind>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ModelsTimeSpine {
    pub custom_granularities: Option<Vec<CustomGranularity>>,
    pub standard_granularity_column: String,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct CustomGranularity {
    pub column_name: Option<String>,
    pub name: String,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
pub struct ModelFreshness {
    pub build_after: Option<FreshnessRules>,
}

impl TryFrom<&ModelPropertiesConfigs> for DbtConfig {
    type Error = Box<dyn std::error::Error>;
    fn try_from(config: &ModelPropertiesConfigs) -> Result<Self, Self::Error> {
        Ok(DbtConfig {
            access: config.access.clone(),
            alias: config.alias.clone(),
            auto_liquid_cluster: config.auto_liquid_cluster,
            auto_refresh: config.auto_refresh,
            automatic_clustering: config.automatic_clustering,
            backup: config.backup,
            base_location_root: config.base_location_root.clone(),
            base_location_subpath: config.base_location_subpath.clone(),
            batch_size: try_string_to_type(&config.batch_size)?,
            begin: config.begin.clone(),
            buckets: config.buckets,
            cluster_by: config.cluster_by.clone(),
            clustered_by: config.clustered_by.clone(),
            compression: config.compression.clone(),
            concurrent_batches: config.concurrent_batches,
            contract: config.contract.clone(),
            copy_grants: config.copy_grants,
            database: config.database.clone(),
            docs: config.docs.clone(),
            enabled: config.enabled,
            event_time: config.event_time.clone(),
            external_volume: config.external_volume.clone(),
            file_format: config.file_format.clone(),
            full_refresh: config.full_refresh,
            grant_access_to: config.grant_access_to.clone(),
            grants: try_from_value(config.grants.clone())?,
            group: config.group.clone(),
            // Cast from f32 to u64
            hours_to_expiration: config.hours_to_expiration.map(|f| f as u64),
            incremental_strategy: try_string_to_type(&config.incremental_strategy)?,
            kms_key_name: config.kms_key_name.clone(),
            labels: try_from_value(config.labels.clone())?,
            location: config.location.clone(),
            lookback: config.lookback.map(|f| f as i32),
            materialized: try_string_to_type(&config.materialized)?,
            on_configuration_change: try_string_to_type(&config.on_configuration_change)?,
            on_schema_change: try_string_to_type(&config.on_schema_change)?,
            schema: config.schema.clone(),
            secure: config.secure,
            snowflake_warehouse: config.snowflake_warehouse.clone(),
            sql_header: config.sql_header.clone(),
            tags: match &config.tags {
                Some(StringOrArrayOfStrings::String(tags)) => {
                    Some(tags.split(',').map(|s| s.to_string()).collect())
                }
                Some(StringOrArrayOfStrings::ArrayOfStrings(tags)) => Some(tags.clone()),
                None => None,
            },
            target_lag: config.target_lag.clone(),
            unique_key: match &config.unique_key {
                Some(StringOrArrayOfStrings::String(unique_key)) => {
                    Some(DbtUniqueKey::Single(unique_key.clone()))
                }
                Some(StringOrArrayOfStrings::ArrayOfStrings(unique_key)) => {
                    Some(DbtUniqueKey::Multiple(unique_key.clone()))
                }
                None => None,
            },
            persist_docs: config.persist_docs.clone(),
            post_hook: (*config.post_hook).clone(),
            pre_hook: (*config.pre_hook).clone(),
            model_freshness: config.freshness.clone(),
            static_analysis: config.static_analysis,
            ..Default::default()
        })
    }
}
