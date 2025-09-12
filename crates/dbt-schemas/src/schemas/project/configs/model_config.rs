use dbt_common::io_args::StaticAnalysisKind;
use dbt_common::serde_utils::Omissible;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;
use std::collections::btree_map::Iter;

use super::omissible_utils::handle_omissible_override;

use crate::default_to;
use crate::schemas::common::DbtBatchSize;
use crate::schemas::common::DbtContract;
use crate::schemas::common::DbtIncrementalStrategy;
use crate::schemas::common::DbtMaterialization;
use crate::schemas::common::DbtUniqueKey;
use crate::schemas::common::PersistDocsConfig;
use crate::schemas::common::{Access, DbtQuoting, ScheduleConfig};
use crate::schemas::common::{DocsConfig, OnConfigurationChange};
use crate::schemas::common::{Hooks, OnSchemaChange};
use crate::schemas::manifest::GrantAccessToTarget;
use crate::schemas::manifest::postgres::PostgresIndex;
use crate::schemas::manifest::{BigqueryClusterConfig, PartitionConfig};
use crate::schemas::project::configs::common::WarehouseSpecificNodeConfig;
use crate::schemas::project::configs::common::default_column_types;
use crate::schemas::project::configs::common::default_hooks;
use crate::schemas::project::configs::common::default_meta_and_tags;
use crate::schemas::project::configs::common::default_quoting;
use crate::schemas::project::configs::common::default_to_grants;
use crate::schemas::project::dbt_project::DefaultTo;
use crate::schemas::project::dbt_project::IterChildren;
use crate::schemas::properties::ModelFreshness;
use crate::schemas::serde::StringOrArrayOfStrings;
use crate::schemas::serde::{bool_or_string_bool, default_type, u64_or_string_u64};
use dbt_serde_yaml::ShouldBe;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectModelConfig {
    #[serde(rename = "+access")]
    pub access: Option<Access>,
    #[serde(rename = "+alias")]
    pub alias: Option<String>,
    #[serde(
        default,
        rename = "+automatic_clustering",
        deserialize_with = "bool_or_string_bool"
    )]
    pub automatic_clustering: Option<bool>,
    #[serde(
        default,
        rename = "+auto_refresh",
        deserialize_with = "bool_or_string_bool"
    )]
    pub auto_refresh: Option<bool>,
    #[serde(
        default,
        rename = "+auto_liquid_cluster",
        deserialize_with = "bool_or_string_bool"
    )]
    pub auto_liquid_cluster: Option<bool>,
    #[serde(default, rename = "+backup", deserialize_with = "bool_or_string_bool")]
    pub backup: Option<bool>,
    #[serde(rename = "+base_location_root")]
    pub base_location_root: Option<String>,
    #[serde(rename = "+base_location_subpath")]
    pub base_location_subpath: Option<String>,
    #[serde(rename = "+batch_size")]
    pub batch_size: Option<DbtBatchSize>,
    #[serde(rename = "+begin")]
    pub begin: Option<String>,
    #[serde(default, rename = "+bind", deserialize_with = "bool_or_string_bool")]
    pub bind: Option<bool>,
    #[serde(rename = "+buckets")]
    pub buckets: Option<i64>,
    #[serde(rename = "+catalog")]
    pub catalog: Option<String>,
    #[serde(rename = "+catalog_name")]
    pub catalog_name: Option<String>,
    #[serde(rename = "+cluster_by")]
    pub cluster_by: Option<BigqueryClusterConfig>,
    #[serde(rename = "+clustered_by")]
    pub clustered_by: Option<String>,
    #[serde(rename = "+column_types")]
    pub column_types: Option<BTreeMap<String, String>>,
    #[serde(
        default,
        rename = "+concurrent_batches",
        deserialize_with = "bool_or_string_bool"
    )]
    pub concurrent_batches: Option<bool>,
    #[serde(rename = "+contract")]
    pub contract: Option<DbtContract>,
    #[serde(rename = "+compression")]
    pub compression: Option<String>,
    #[serde(
        default,
        rename = "+copy_grants",
        deserialize_with = "bool_or_string_bool"
    )]
    pub copy_grants: Option<bool>,
    #[serde(rename = "+database", alias = "+project")]
    pub database: Omissible<Option<String>>,
    #[serde(rename = "+databricks_compute")]
    pub databricks_compute: Option<String>,
    #[serde(rename = "+databricks_tags")]
    pub databricks_tags: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+description")]
    pub description: Option<String>,
    #[serde(rename = "+dist")]
    pub dist: Option<String>,
    #[serde(rename = "+docs")]
    pub docs: Option<DocsConfig>,
    #[serde(
        default,
        rename = "+enable_refresh",
        deserialize_with = "bool_or_string_bool"
    )]
    pub enable_refresh: Option<bool>,
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+event_time")]
    pub event_time: Option<String>,
    #[serde(rename = "+external_volume")]
    pub external_volume: Option<String>,
    #[serde(rename = "+file_format")]
    pub file_format: Option<String>,
    #[serde(rename = "+freshness")]
    pub freshness: Option<ModelFreshness>,
    #[serde(
        default,
        rename = "+full_refresh",
        deserialize_with = "bool_or_string_bool"
    )]
    pub full_refresh: Option<bool>,
    #[serde(rename = "+grant_access_to")]
    pub grant_access_to: Option<Vec<GrantAccessToTarget>>,
    #[serde(rename = "+grants")]
    pub grants: Option<BTreeMap<String, StringOrArrayOfStrings>>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    #[serde(
        default,
        rename = "+hours_to_expiration",
        deserialize_with = "u64_or_string_u64"
    )]
    pub hours_to_expiration: Option<u64>,
    #[serde(
        default,
        rename = "+include_full_name_in_path",
        deserialize_with = "bool_or_string_bool"
    )]
    pub include_full_name_in_path: Option<bool>,
    #[serde(rename = "+incremental_predicates")]
    pub incremental_predicates: Option<Vec<String>>,
    #[serde(rename = "+incremental_strategy")]
    pub incremental_strategy: Option<DbtIncrementalStrategy>,
    #[serde(rename = "+initialize")]
    pub initialize: Option<String>,
    #[serde(rename = "+kms_key_name")]
    pub kms_key_name: Option<String>,
    #[serde(rename = "+labels")]
    pub labels: Option<BTreeMap<String, String>>,
    #[serde(
        default,
        rename = "+labels_from_meta",
        deserialize_with = "bool_or_string_bool"
    )]
    pub labels_from_meta: Option<bool>,
    #[serde(rename = "+liquid_clustered_by")]
    pub liquid_clustered_by: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+location")]
    pub location: Option<String>,
    #[serde(rename = "+location_root")]
    pub location_root: Option<String>,
    #[serde(rename = "+lookback")]
    pub lookback: Option<i32>,
    #[serde(rename = "+matched_condition")]
    pub matched_condition: Option<String>,
    #[serde(rename = "+materialized")]
    pub materialized: Option<DbtMaterialization>,
    #[serde(rename = "+max_staleness")]
    pub max_staleness: Option<String>,
    #[serde(rename = "+merge_exclude_columns")]
    pub merge_exclude_columns: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+merge_update_columns")]
    pub merge_update_columns: Option<StringOrArrayOfStrings>,
    #[serde(
        default,
        rename = "+merge_with_schema_evolution",
        deserialize_with = "bool_or_string_bool"
    )]
    pub merge_with_schema_evolution: Option<bool>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+not_matched_by_source_action")]
    pub not_matched_by_source_action: Option<String>,
    #[serde(rename = "+not_matched_by_source_condition")]
    pub not_matched_by_source_condition: Option<String>,
    #[serde(rename = "+not_matched_condition")]
    pub not_matched_condition: Option<String>,
    #[serde(rename = "+source_alias")]
    pub source_alias: Option<String>,
    #[serde(rename = "+target_alias")]
    pub target_alias: Option<String>,
    #[serde(rename = "+on_configuration_change")]
    pub on_configuration_change: Option<OnConfigurationChange>,
    #[serde(rename = "+on_schema_change")]
    pub on_schema_change: Option<OnSchemaChange>,
    #[serde(rename = "+packages")]
    pub packages: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+partition_by")]
    pub partition_by: Option<PartitionConfig>,
    #[serde(
        default,
        rename = "+partition_expiration_days",
        deserialize_with = "u64_or_string_u64"
    )]
    pub partition_expiration_days: Option<u64>,
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
    #[serde(rename = "+table_tag")]
    pub table_tag: Option<String>,
    #[serde(rename = "+row_access_policy")]
    pub row_access_policy: Option<String>,
    #[serde(rename = "+quoting")]
    pub quoting: Option<DbtQuoting>,
    #[serde(rename = "+refresh_mode")]
    pub refresh_mode: Option<String>,
    #[serde(
        default,
        rename = "+refresh_interval_minutes",
        deserialize_with = "u64_or_string_u64"
    )]
    pub refresh_interval_minutes: Option<u64>,
    #[serde(
        default,
        rename = "+require_partition_filter",
        deserialize_with = "bool_or_string_bool"
    )]
    pub require_partition_filter: Option<bool>,
    #[serde(rename = "+schema", alias = "+dataset")]
    pub schema: Omissible<Option<String>>,
    #[serde(
        default,
        rename = "+skip_matched_step",
        deserialize_with = "bool_or_string_bool"
    )]
    pub skip_matched_step: Option<bool>,
    #[serde(
        default,
        rename = "+skip_not_matched_step",
        deserialize_with = "bool_or_string_bool"
    )]
    pub skip_not_matched_step: Option<bool>,
    #[serde(default, rename = "+secure", deserialize_with = "bool_or_string_bool")]
    pub secure: Option<bool>,
    #[serde(rename = "+sort")]
    pub sort: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+sort_type")]
    pub sort_type: Option<String>,
    #[serde(rename = "+snowflake_warehouse")]
    pub snowflake_warehouse: Option<String>,
    #[serde(rename = "+sql_header")]
    pub sql_header: Option<String>,
    #[serde(rename = "+static_analysis")]
    pub static_analysis: Option<StaticAnalysisKind>,
    #[serde(rename = "+table_format")]
    pub table_format: Option<String>,
    #[serde(rename = "+tags")]
    pub tags: Omissible<StringOrArrayOfStrings>,
    #[serde(rename = "+target_lag")]
    pub target_lag: Option<String>,
    #[serde(rename = "+tblproperties")]
    pub tblproperties: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+tmp_relation_type")]
    pub tmp_relation_type: Option<String>,
    #[serde(
        default,
        rename = "+transient",
        deserialize_with = "bool_or_string_bool"
    )]
    pub transient: Option<bool>,
    #[serde(rename = "+unique_key")]
    pub unique_key: Option<DbtUniqueKey>,
    #[serde(
        default,
        rename = "+as_columnstore",
        deserialize_with = "bool_or_string_bool"
    )]
    pub as_columnstore: Option<bool>,

    #[serde(default, rename = "+table_type")]
    pub table_type: Option<String>,

    #[serde(default, rename = "+indexes")]
    pub indexes: Option<Vec<PostgresIndex>>,

    // Schedule (Databricks streaming tables)
    #[serde(rename = "+schedule")]
    pub schedule: Option<ScheduleConfig>,

    // Primary Key (Salesforce)
    #[serde(rename = "+primary_key")]
    pub primary_key: Option<String>,
    #[serde(rename = "+category")]
    pub category: Option<DataLakeObjectCategory>,
    // Flattened field:
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectModelConfig>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "PascalCase")]
/// See `category` from https://developer.salesforce.com/docs/data/connectapi/references/spec?meta=postDataLakeObject
pub enum DataLakeObjectCategory {
    Profile,
    Engagement,
    #[serde(rename = "Directory_Table")]
    DirectoryTable,
    Insights,
    Other,
}

impl IterChildren<ProjectModelConfig> for ProjectModelConfig {
    fn iter_children(&self) -> Iter<String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone, PartialEq, Eq, JsonSchema)]
pub struct ModelConfig {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub alias: Option<String>,
    pub schema: Omissible<Option<String>>,
    pub database: Omissible<Option<String>>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub catalog_name: Option<String>,
    // need default to ensure None if field is not set
    #[serde(default, deserialize_with = "default_type")]
    pub meta: Option<BTreeMap<String, YmlValue>>,
    pub group: Option<String>,
    pub materialized: Option<DbtMaterialization>,
    pub incremental_strategy: Option<DbtIncrementalStrategy>,
    pub incremental_predicates: Option<Vec<String>>,
    pub batch_size: Option<DbtBatchSize>,
    pub lookback: Option<i32>,
    pub begin: Option<String>,
    pub persist_docs: Option<PersistDocsConfig>,
    pub post_hook: Verbatim<Option<Hooks>>,
    pub pre_hook: Verbatim<Option<Hooks>>,
    pub quoting: Option<DbtQuoting>,
    pub column_types: Option<BTreeMap<String, String>>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub full_refresh: Option<bool>,
    pub unique_key: Option<DbtUniqueKey>,
    pub on_schema_change: Option<OnSchemaChange>,
    pub on_configuration_change: Option<OnConfigurationChange>,
    pub grants: Option<BTreeMap<String, StringOrArrayOfStrings>>,
    pub packages: Option<StringOrArrayOfStrings>,
    pub docs: Option<DocsConfig>,
    pub contract: Option<DbtContract>,
    pub event_time: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub concurrent_batches: Option<bool>,
    pub merge_update_columns: Option<StringOrArrayOfStrings>,
    pub merge_exclude_columns: Option<StringOrArrayOfStrings>,
    pub access: Option<Access>,
    pub table_format: Option<String>,
    pub static_analysis: Option<StaticAnalysisKind>,
    pub freshness: Option<ModelFreshness>,
    pub sql_header: Option<String>,
    pub location: Option<String>,
    pub predicates: Option<Vec<String>>,
    pub description: Option<String>,
    // Adapter specific configs
    pub __warehouse_specific_config__: WarehouseSpecificNodeConfig,
}

impl From<ProjectModelConfig> for ModelConfig {
    fn from(config: ProjectModelConfig) -> Self {
        Self {
            access: config.access,
            alias: config.alias,
            batch_size: config.batch_size,
            begin: config.begin,
            catalog_name: config.catalog_name,
            column_types: config.column_types,
            concurrent_batches: config.concurrent_batches,
            contract: config.contract,
            database: config.database,
            docs: config.docs,
            enabled: config.enabled,
            event_time: config.event_time,
            freshness: config.freshness,
            full_refresh: config.full_refresh,
            grants: config.grants,
            group: config.group,
            incremental_predicates: config.incremental_predicates,
            incremental_strategy: config.incremental_strategy,
            location: config.location,
            lookback: config.lookback,
            materialized: config.materialized,
            merge_exclude_columns: config.merge_exclude_columns,
            merge_update_columns: config.merge_update_columns,
            meta: config.meta,
            on_configuration_change: config.on_configuration_change,
            on_schema_change: config.on_schema_change,
            packages: config.packages,
            persist_docs: config.persist_docs,
            post_hook: config.post_hook,
            pre_hook: config.pre_hook,
            predicates: config.predicates,
            quoting: config.quoting,
            schema: config.schema,
            sql_header: config.sql_header,
            static_analysis: config.static_analysis,
            table_format: config.table_format,
            tags: config.tags.into_inner(),
            unique_key: config.unique_key,
            description: config.description,
            __warehouse_specific_config__: WarehouseSpecificNodeConfig {
                external_volume: config.external_volume,
                base_location_root: config.base_location_root,
                base_location_subpath: config.base_location_subpath,
                target_lag: config.target_lag,
                snowflake_warehouse: config.snowflake_warehouse,
                refresh_mode: config.refresh_mode,
                initialize: config.initialize,
                tmp_relation_type: config.tmp_relation_type,
                query_tag: config.query_tag,
                table_tag: config.table_tag,
                row_access_policy: config.row_access_policy,
                automatic_clustering: config.automatic_clustering,
                copy_grants: config.copy_grants,
                secure: config.secure,
                transient: config.transient,

                partition_by: config.partition_by,
                cluster_by: config.cluster_by,
                hours_to_expiration: config.hours_to_expiration,
                labels: config.labels,
                labels_from_meta: config.labels_from_meta,
                kms_key_name: config.kms_key_name,
                require_partition_filter: config.require_partition_filter,
                partition_expiration_days: config.partition_expiration_days,
                grant_access_to: config.grant_access_to,
                partitions: config.partitions,
                enable_refresh: config.enable_refresh,
                refresh_interval_minutes: config.refresh_interval_minutes,
                max_staleness: config.max_staleness,

                file_format: config.file_format,
                location_root: config.location_root,
                tblproperties: config.tblproperties,
                include_full_name_in_path: config.include_full_name_in_path,
                liquid_clustered_by: config.liquid_clustered_by,
                auto_liquid_cluster: config.auto_liquid_cluster,
                clustered_by: config.clustered_by,
                buckets: config.buckets,
                catalog: config.catalog,
                databricks_tags: config.databricks_tags,
                compression: config.compression,
                databricks_compute: config.databricks_compute,
                target_alias: config.target_alias,
                source_alias: config.source_alias,
                matched_condition: config.matched_condition,
                not_matched_condition: config.not_matched_condition,
                not_matched_by_source_condition: config.not_matched_by_source_condition,
                not_matched_by_source_action: config.not_matched_by_source_action,
                merge_with_schema_evolution: config.merge_with_schema_evolution,
                skip_matched_step: config.skip_matched_step,
                skip_not_matched_step: config.skip_not_matched_step,
                schedule: config.schedule,

                auto_refresh: config.auto_refresh,
                backup: config.backup,
                bind: config.bind,
                dist: config.dist,
                sort: config.sort,
                sort_type: config.sort_type,

                as_columnstore: config.as_columnstore,

                table_type: config.table_type,
                indexes: config.indexes,

                primary_key: config.primary_key,
                category: config.category,
            },
        }
    }
}

impl From<ModelConfig> for ProjectModelConfig {
    fn from(config: ModelConfig) -> Self {
        Self {
            access: config.access,
            alias: config.alias,
            auto_refresh: config.__warehouse_specific_config__.auto_refresh,
            backup: config.__warehouse_specific_config__.backup,
            batch_size: config.batch_size,
            begin: config.begin,
            bind: config.__warehouse_specific_config__.bind,
            catalog_name: config.catalog_name,
            column_types: config.column_types,
            concurrent_batches: config.concurrent_batches,
            contract: config.contract,
            database: config.database,
            docs: config.docs,
            enabled: config.enabled,
            event_time: config.event_time,
            freshness: config.freshness,
            full_refresh: config.full_refresh,
            grants: config.grants,
            group: config.group,
            incremental_predicates: config.incremental_predicates,
            incremental_strategy: config.incremental_strategy,
            location: config.location,
            lookback: config.lookback,
            materialized: config.materialized,
            merge_exclude_columns: config.merge_exclude_columns,
            merge_update_columns: config.merge_update_columns,
            meta: config.meta,
            on_configuration_change: config.on_configuration_change,
            on_schema_change: config.on_schema_change,
            packages: config.packages,
            persist_docs: config.persist_docs,
            post_hook: config.post_hook,
            pre_hook: config.pre_hook,
            predicates: config.predicates,
            quoting: config.quoting,
            schema: config.schema,
            sql_header: config.sql_header,
            static_analysis: config.static_analysis,
            table_format: config.table_format,
            tags: config.tags.into(),
            transient: config.__warehouse_specific_config__.transient,
            unique_key: config.unique_key,
            external_volume: config.__warehouse_specific_config__.external_volume,
            base_location_root: config.__warehouse_specific_config__.base_location_root,
            base_location_subpath: config.__warehouse_specific_config__.base_location_subpath,
            target_lag: config.__warehouse_specific_config__.target_lag,
            snowflake_warehouse: config.__warehouse_specific_config__.snowflake_warehouse,
            refresh_mode: config.__warehouse_specific_config__.refresh_mode,
            initialize: config.__warehouse_specific_config__.initialize,
            tmp_relation_type: config.__warehouse_specific_config__.tmp_relation_type,
            query_tag: config.__warehouse_specific_config__.query_tag,
            table_tag: config.__warehouse_specific_config__.table_tag,
            row_access_policy: config.__warehouse_specific_config__.row_access_policy,
            automatic_clustering: config.__warehouse_specific_config__.automatic_clustering,
            copy_grants: config.__warehouse_specific_config__.copy_grants,
            secure: config.__warehouse_specific_config__.secure,
            partition_by: config.__warehouse_specific_config__.partition_by,
            cluster_by: config.__warehouse_specific_config__.cluster_by,
            hours_to_expiration: config.__warehouse_specific_config__.hours_to_expiration,
            labels: config.__warehouse_specific_config__.labels,
            labels_from_meta: config.__warehouse_specific_config__.labels_from_meta,
            kms_key_name: config.__warehouse_specific_config__.kms_key_name,
            require_partition_filter: config
                .__warehouse_specific_config__
                .require_partition_filter,
            partition_expiration_days: config
                .__warehouse_specific_config__
                .partition_expiration_days,
            grant_access_to: config.__warehouse_specific_config__.grant_access_to,
            partitions: config.__warehouse_specific_config__.partitions,
            enable_refresh: config.__warehouse_specific_config__.enable_refresh,
            refresh_interval_minutes: config
                .__warehouse_specific_config__
                .refresh_interval_minutes,
            max_staleness: config.__warehouse_specific_config__.max_staleness,
            file_format: config.__warehouse_specific_config__.file_format,
            location_root: config.__warehouse_specific_config__.location_root,
            tblproperties: config.__warehouse_specific_config__.tblproperties,
            include_full_name_in_path: config
                .__warehouse_specific_config__
                .include_full_name_in_path,
            liquid_clustered_by: config.__warehouse_specific_config__.liquid_clustered_by,
            auto_liquid_cluster: config.__warehouse_specific_config__.auto_liquid_cluster,
            clustered_by: config.__warehouse_specific_config__.clustered_by,
            buckets: config.__warehouse_specific_config__.buckets,
            catalog: config.__warehouse_specific_config__.catalog,
            databricks_tags: config.__warehouse_specific_config__.databricks_tags,
            compression: config.__warehouse_specific_config__.compression,
            databricks_compute: config.__warehouse_specific_config__.databricks_compute,
            dist: config.__warehouse_specific_config__.dist,
            sort: config.__warehouse_specific_config__.sort,
            sort_type: config.__warehouse_specific_config__.sort_type,
            matched_condition: config.__warehouse_specific_config__.matched_condition,
            merge_with_schema_evolution: config
                .__warehouse_specific_config__
                .merge_with_schema_evolution,
            not_matched_by_source_action: config
                .__warehouse_specific_config__
                .not_matched_by_source_action,
            not_matched_by_source_condition: config
                .__warehouse_specific_config__
                .not_matched_by_source_condition,
            not_matched_condition: config.__warehouse_specific_config__.not_matched_condition,
            source_alias: config.__warehouse_specific_config__.source_alias,
            target_alias: config.__warehouse_specific_config__.target_alias,
            skip_matched_step: config.__warehouse_specific_config__.skip_matched_step,
            skip_not_matched_step: config.__warehouse_specific_config__.skip_not_matched_step,
            as_columnstore: config.__warehouse_specific_config__.as_columnstore,
            table_type: config.__warehouse_specific_config__.table_type,
            indexes: config.__warehouse_specific_config__.indexes,
            schedule: config.__warehouse_specific_config__.schedule,
            description: config.description,
            primary_key: config.__warehouse_specific_config__.primary_key,
            category: config.__warehouse_specific_config__.category,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl DefaultTo<ModelConfig> for ModelConfig {
    /// Default this config to the parent config
    ///
    /// This method ensures that:
    /// 1. All fields are explicitly handled
    /// 2. Custom merge logic is applied where needed
    /// 3. Compile-time safety through exhaustive pattern matching
    #[allow(clippy::cognitive_complexity)]
    fn default_to(&mut self, parent: &ModelConfig) {
        // Handle simple fields - using a pattern that ensures all fields are covered
        let ModelConfig {
            // Custom fields (already handled above)
            post_hook,
            pre_hook,
            meta,
            tags,
            quoting,

            // Flattened config (already handled above)
            __warehouse_specific_config__: warehouse_specific_config,

            // Simple fields (handle with macro)
            enabled,
            alias,
            schema,
            database,
            catalog_name,
            group,
            materialized,
            incremental_strategy,
            incremental_predicates,
            batch_size,
            lookback,
            begin,
            persist_docs,
            column_types,
            full_refresh,
            unique_key,
            on_schema_change,
            on_configuration_change,
            grants,
            packages,
            docs,
            contract,
            event_time,
            concurrent_batches,
            merge_update_columns,
            merge_exclude_columns,
            access,
            table_format,
            static_analysis,
            freshness,
            sql_header,
            location,
            predicates,
            description,
        } = self;

        // Handle flattened configs
        #[allow(unused, clippy::let_unit_value)]
        let warehouse_specific_config =
            warehouse_specific_config.default_to(&parent.__warehouse_specific_config__);

        // Protect the mutable refs from being used in the default_to macro
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
        #[allow(unused, clippy::let_unit_value)]
        let grants = default_to_grants(grants, &parent.grants);

        // Handle Omissible fields for hierarchical overrides
        handle_omissible_override(schema, &parent.schema);
        handle_omissible_override(database, &parent.database);

        default_to!(
            parent,
            [
                enabled,
                alias,
                catalog_name,
                group,
                materialized,
                incremental_strategy,
                incremental_predicates,
                batch_size,
                lookback,
                begin,
                persist_docs,
                full_refresh,
                unique_key,
                on_schema_change,
                on_configuration_change,
                packages,
                docs,
                contract,
                event_time,
                concurrent_batches,
                merge_update_columns,
                merge_exclude_columns,
                access,
                table_format,
                static_analysis,
                freshness,
                sql_header,
                location,
                predicates,
                description,
            ]
        );
    }

    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn is_incremental(&self) -> bool {
        self.incremental_strategy.is_some()
    }

    fn database(&self) -> Option<String> {
        self.database.clone().into_inner().unwrap_or(None)
    }

    fn schema(&self) -> Option<String> {
        self.schema.clone().into_inner().unwrap_or(None)
    }

    fn alias(&self) -> Option<String> {
        self.alias.clone()
    }

    fn get_pre_hook(&self) -> Option<&Hooks> {
        (*self.pre_hook).as_ref()
    }

    fn get_post_hook(&self) -> Option<&Hooks> {
        (*self.post_hook).as_ref()
    }
}

impl ModelConfig {
    /// Custom comparison that treats Omitted and Present(None) as equivalent for schema/database fields
    pub fn same_config(&self, other: &ModelConfig) -> bool {
        // Compare all fields,
        self.enabled == other.enabled
            && omissible_option_eq(&self.schema, &other.schema)
            && omissible_option_eq(&self.database, &other.database)
            && self.catalog_name == other.catalog_name
            && meta_eq(&self.meta, &other.meta)  // Custom comparison for meta
            && self.materialized == other.materialized
            && self.incremental_strategy == other.incremental_strategy
            && self.incremental_predicates == other.incremental_predicates
            && self.batch_size == other.batch_size
            && lookback_eq(&self.lookback, &other.lookback)  // Custom comparison for lookback
            && self.begin == other.begin
            && persist_docs_eq(&self.persist_docs, &other.persist_docs)  // Custom comparison for persist_docs
            && self.post_hook == other.post_hook
            && self.pre_hook == other.pre_hook
            // && self.quoting == other.quoting // TODO: re-enable when no longer using mantle/core manifests in IA
            && column_types_eq(&self.column_types, &other.column_types)  // Custom comparison for column_types
            && self.full_refresh == other.full_refresh
            && self.unique_key == other.unique_key
            && on_schema_change_eq(&self.on_schema_change, &other.on_schema_change)  // Custom comparison for on_schema_change
            && on_configuration_change_eq(&self.on_configuration_change, &other.on_configuration_change)  // Custom comparison for on_configuration_change
            && grants_eq(&self.grants, &other.grants)  // Custom comparison for grants
            && packages_eq(&self.packages, &other.packages)  // Custom comparison for packages
            && docs_eq(&self.docs, &other.docs)  // Custom comparison for docs
            && self.event_time == other.event_time
            && self.concurrent_batches == other.concurrent_batches
            && self.merge_update_columns == other.merge_update_columns
            && self.merge_exclude_columns == other.merge_exclude_columns
            && access_eq(&self.access, &other.access)  // Custom comparison for access
            && self.table_format == other.table_format
            && self.static_analysis == other.static_analysis
            && self.freshness == other.freshness
            && self.sql_header == other.sql_header
            && self.location == other.location
            && self.predicates == other.predicates
            && self.description == other.description
            && self.__warehouse_specific_config__ == other.__warehouse_specific_config__
    }
}
// Helper function to compare Omissible<Option<T>> fields
fn omissible_option_eq<T: PartialEq>(a: &Omissible<Option<T>>, b: &Omissible<Option<T>>) -> bool {
    match (a, b) {
        // Both omitted
        (Omissible::Omitted, Omissible::Omitted) => true,
        // Both present
        (Omissible::Present(a_val), Omissible::Present(b_val)) => a_val == b_val,
        // One omitted, one present with None - treat as equivalent
        (Omissible::Omitted, Omissible::Present(None)) => true,
        (Omissible::Present(None), Omissible::Omitted) => true,
        // Any other combination is not equal
        _ => false,
    }
}

// Helper function to compare docs fields, treating None and default DocsConfig as equivalent
fn docs_eq(a: &Option<DocsConfig>, b: &Option<DocsConfig>) -> bool {
    use crate::schemas::common::DocsConfig;
    // Default value in dbt-core
    // See https://github.com/dbt-labs/dbt-core/blob/b75d5e701ef4dc2d7a98c5301ef63ecfc02eae15/core/dbt/artifacts/resources/base.py#L65
    let default_docs = DocsConfig {
        show: true,
        node_color: None,
    };

    match (a, b) {
        // Both None
        (None, None) => true,
        // Both Some - direct comparison
        (Some(a_docs), Some(b_docs)) => a_docs == b_docs,
        // One None, one Some - check if the Some value equals default
        (None, Some(b_docs)) => b_docs == &default_docs,
        (Some(a_docs), None) => a_docs == &default_docs,
    }
}

// Helper function to compare on_schema_change fields, treating None and default OnSchemaChange as equivalent
fn on_schema_change_eq(a: &Option<OnSchemaChange>, b: &Option<OnSchemaChange>) -> bool {
    use crate::schemas::common::OnSchemaChange;
    // Default value in dbt-core is "ignore"
    // See https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/artifacts/resources/v1/config.py#L109
    let default_on_schema_change = OnSchemaChange::Ignore;

    match (a, b) {
        // Both None
        (None, None) => true,
        // Both Some - direct comparison
        (Some(a_val), Some(b_val)) => a_val == b_val,
        // One None, one Some - check if the Some value equals default
        (None, Some(b_val)) => b_val == &default_on_schema_change,
        (Some(a_val), None) => a_val == &default_on_schema_change,
    }
}

// Helper function to compare access fields, treating None and default Access as equivalent
fn access_eq(a: &Option<Access>, b: &Option<Access>) -> bool {
    use crate::schemas::common::Access;
    // Default value in dbt-core is "protected"
    // See https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/artifacts/resources/v1/model.py#L72-L75
    let default_access = Access::Protected;

    match (a, b) {
        // Both None
        (None, None) => true,
        // Both Some - direct comparison
        (Some(a_val), Some(b_val)) => a_val == b_val,
        // One None, one Some - check if the Some value equals default
        (None, Some(b_val)) => b_val == &default_access,
        (Some(a_val), None) => a_val == &default_access,
    }
}
// Helper function to compare persist_docs fields, treating None and default PersistDocsConfig as equivalent
fn persist_docs_eq(a: &Option<PersistDocsConfig>, b: &Option<PersistDocsConfig>) -> bool {
    use crate::schemas::common::PersistDocsConfig;
    // Default value in dbt-core is empty dict {}
    // See https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/artifacts/resources/v1/config.py#L86
    let default_persist_docs = PersistDocsConfig {
        columns: None,
        relation: None,
    };

    match (a, b) {
        // Both None
        (None, None) => true,
        // Both Some - direct comparison
        (Some(a_val), Some(b_val)) => a_val == b_val,
        // One None, one Some - check if the Some value equals default
        (None, Some(b_val)) => b_val == &default_persist_docs,
        (Some(a_val), None) => a_val == &default_persist_docs,
    }
}

// Helper function to compare lookback fields, treating None and default lookback as equivalent
fn lookback_eq(a: &Option<i32>, b: &Option<i32>) -> bool {
    // Default value in dbt-core is 1
    // See https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/artifacts/resources/v1/config.py#L84
    let default_lookback = 1;

    match (a, b) {
        // Both None
        (None, None) => true,
        // Both Some - direct comparison
        (Some(a_val), Some(b_val)) => a_val == b_val,
        // One None, one Some - check if the Some value equals default
        (None, Some(b_val)) => b_val == &default_lookback,
        (Some(a_val), None) => a_val == &default_lookback,
    }
}

// Helper function to compare meta fields, treating None and empty BTreeMap as equivalent
fn meta_eq(a: &Option<BTreeMap<String, YmlValue>>, b: &Option<BTreeMap<String, YmlValue>>) -> bool {
    match (a, b) {
        // Both None
        (None, None) => true,
        // Both Some - direct comparison
        (Some(a_val), Some(b_val)) => a_val == b_val,
        // One None, one Some - check if the Some value is empty (equals default)
        (None, Some(b_val)) => b_val.is_empty(),
        (Some(a_val), None) => a_val.is_empty(),
    }
}

// Helper function to compare column_types fields, treating None and empty BTreeMap as equivalent
fn column_types_eq(
    a: &Option<BTreeMap<String, String>>,
    b: &Option<BTreeMap<String, String>>,
) -> bool {
    match (a, b) {
        // Both None
        (None, None) => true,
        // Both Some - direct comparison
        (Some(a_val), Some(b_val)) => a_val == b_val,
        // One None, one Some - check if the Some value is empty (equals default)
        (None, Some(b_val)) => b_val.is_empty(),
        (Some(a_val), None) => a_val.is_empty(),
    }
}

// Helper function to compare grants fields, treating None and empty BTreeMap as equivalent
fn grants_eq(
    a: &Option<BTreeMap<String, StringOrArrayOfStrings>>,
    b: &Option<BTreeMap<String, StringOrArrayOfStrings>>,
) -> bool {
    match (a, b) {
        // Both None
        (None, None) => true,
        // Both Some - direct comparison
        (Some(a_val), Some(b_val)) => a_val == b_val,
        // One None, one Some - check if the Some value is empty (equals default)
        (None, Some(b_val)) => b_val.is_empty(),
        (Some(a_val), None) => a_val.is_empty(),
    }
}

// Helper function to compare packages fields, treating None and empty ArrayOfStrings as equivalent
fn packages_eq(a: &Option<StringOrArrayOfStrings>, b: &Option<StringOrArrayOfStrings>) -> bool {
    use crate::schemas::serde::StringOrArrayOfStrings;

    match (a, b) {
        // Both None
        (None, None) => true,
        // Both Some - direct comparison
        (Some(a_val), Some(b_val)) => a_val == b_val,
        // One None, one Some - check if the Some value is an empty ArrayOfStrings
        (None, Some(StringOrArrayOfStrings::ArrayOfStrings(b_vec))) => b_vec.is_empty(),
        (Some(StringOrArrayOfStrings::ArrayOfStrings(a_vec)), None) => a_vec.is_empty(),
        // If one is None and the other is Some(String), they are not equal
        (None, Some(StringOrArrayOfStrings::String(_))) => false,
        (Some(StringOrArrayOfStrings::String(_)), None) => false,
    }
}
// Helper function to compare on_configuration_change fields, treating None and default OnConfigurationChange as equivalent
fn on_configuration_change_eq(
    a: &Option<OnConfigurationChange>,
    b: &Option<OnConfigurationChange>,
) -> bool {
    use crate::schemas::common::OnConfigurationChange;
    // Default value in dbt-core is "apply"
    // See https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/artifacts/resources/v1/config.py#L110-L112
    // and https://github.com/dbt-labs/dbt-common/blob/eb6b6f4a155f94d4863d8f503f8eb997ab6226d3/dbt_common/contracts/config/materialization.py#L4-L11
    let default_on_configuration_change = OnConfigurationChange::Apply;

    match (a, b) {
        // Both None
        (None, None) => true,
        // Both Some - direct comparison
        (Some(a_val), Some(b_val)) => a_val == b_val,
        // One None, one Some - check if the Some value equals default
        (None, Some(b_val)) => b_val == &default_on_configuration_change,
        (Some(a_val), None) => a_val == &default_on_configuration_change,
    }
}
