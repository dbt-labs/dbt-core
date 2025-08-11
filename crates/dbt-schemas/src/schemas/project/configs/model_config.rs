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
use crate::schemas::common::{Access, DbtQuoting};
use crate::schemas::common::{DocsConfig, OnConfigurationChange};
use crate::schemas::common::{Hooks, OnSchemaChange};
use crate::schemas::manifest::GrantAccessToTarget;
use crate::schemas::manifest::{BigqueryClusterConfig, BigqueryPartitionConfigLegacy};
use crate::schemas::project::configs::common::BigQueryNodeConfig;
use crate::schemas::project::configs::common::DatabricksNodeConfig;
use crate::schemas::project::configs::common::RedshiftNodeConfig;
use crate::schemas::project::configs::common::SnowflakeNodeConfig;
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
    pub partition_by: Option<BigqueryPartitionConfigLegacy>,
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
    // Flattened field:
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectModelConfig>>,
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
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub auto_refresh: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub backup: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub bind: Option<bool>,
    pub location: Option<String>,
    pub predicates: Option<Vec<String>>,
    // Adapter specific configs
    #[serde(flatten)]
    pub snowflake_node_config: SnowflakeNodeConfig,
    #[serde(flatten)]
    pub bigquery_node_config: BigQueryNodeConfig,
    #[serde(flatten)]
    pub databricks_node_config: DatabricksNodeConfig,
    #[serde(flatten)]
    pub redshift_node_config: RedshiftNodeConfig,
}

impl From<ProjectModelConfig> for ModelConfig {
    fn from(config: ProjectModelConfig) -> Self {
        Self {
            access: config.access,
            alias: config.alias,
            auto_refresh: config.auto_refresh,
            backup: config.backup,
            batch_size: config.batch_size,
            begin: config.begin,
            bind: config.bind,
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
            snowflake_node_config: SnowflakeNodeConfig {
                external_volume: config.external_volume,
                base_location_root: config.base_location_root,
                base_location_subpath: config.base_location_subpath,
                target_lag: config.target_lag,
                snowflake_warehouse: config.snowflake_warehouse,
                refresh_mode: config.refresh_mode,
                initialize: config.initialize,
                tmp_relation_type: config.tmp_relation_type,
                query_tag: config.query_tag,
                automatic_clustering: config.automatic_clustering,
                copy_grants: config.copy_grants,
                secure: config.secure,
                transient: config.transient,
            },
            bigquery_node_config: BigQueryNodeConfig {
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
                description: config.description,
                max_staleness: config.max_staleness,
            },
            databricks_node_config: DatabricksNodeConfig {
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
            },
            redshift_node_config: RedshiftNodeConfig {
                auto_refresh: config.auto_refresh,
                backup: config.backup,
                bind: config.bind,
                dist: config.dist,
                sort: config.sort,
                sort_type: config.sort_type,
            },
        }
    }
}

impl From<ModelConfig> for ProjectModelConfig {
    fn from(config: ModelConfig) -> Self {
        Self {
            access: config.access,
            alias: config.alias,
            auto_refresh: config.auto_refresh,
            backup: config.backup,
            batch_size: config.batch_size,
            begin: config.begin,
            bind: config.bind,
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
            transient: config.snowflake_node_config.transient,
            unique_key: config.unique_key,
            external_volume: config.snowflake_node_config.external_volume,
            base_location_root: config.snowflake_node_config.base_location_root,
            base_location_subpath: config.snowflake_node_config.base_location_subpath,
            target_lag: config.snowflake_node_config.target_lag,
            snowflake_warehouse: config.snowflake_node_config.snowflake_warehouse,
            refresh_mode: config.snowflake_node_config.refresh_mode,
            initialize: config.snowflake_node_config.initialize,
            tmp_relation_type: config.snowflake_node_config.tmp_relation_type,
            query_tag: config.snowflake_node_config.query_tag,
            automatic_clustering: config.snowflake_node_config.automatic_clustering,
            copy_grants: config.snowflake_node_config.copy_grants,
            secure: config.snowflake_node_config.secure,
            partition_by: config.bigquery_node_config.partition_by,
            cluster_by: config.bigquery_node_config.cluster_by,
            hours_to_expiration: config.bigquery_node_config.hours_to_expiration,
            labels: config.bigquery_node_config.labels,
            labels_from_meta: config.bigquery_node_config.labels_from_meta,
            kms_key_name: config.bigquery_node_config.kms_key_name,
            require_partition_filter: config.bigquery_node_config.require_partition_filter,
            partition_expiration_days: config.bigquery_node_config.partition_expiration_days,
            grant_access_to: config.bigquery_node_config.grant_access_to,
            partitions: config.bigquery_node_config.partitions,
            enable_refresh: config.bigquery_node_config.enable_refresh,
            refresh_interval_minutes: config.bigquery_node_config.refresh_interval_minutes,
            description: config.bigquery_node_config.description,
            max_staleness: config.bigquery_node_config.max_staleness,
            file_format: config.databricks_node_config.file_format,
            location_root: config.databricks_node_config.location_root,
            tblproperties: config.databricks_node_config.tblproperties,
            include_full_name_in_path: config.databricks_node_config.include_full_name_in_path,
            liquid_clustered_by: config.databricks_node_config.liquid_clustered_by,
            auto_liquid_cluster: config.databricks_node_config.auto_liquid_cluster,
            clustered_by: config.databricks_node_config.clustered_by,
            buckets: config.databricks_node_config.buckets,
            catalog: config.databricks_node_config.catalog,
            databricks_tags: config.databricks_node_config.databricks_tags,
            compression: config.databricks_node_config.compression,
            databricks_compute: config.databricks_node_config.databricks_compute,
            dist: config.redshift_node_config.dist,
            sort: config.redshift_node_config.sort,
            sort_type: config.redshift_node_config.sort_type,
            matched_condition: config.databricks_node_config.matched_condition,
            merge_with_schema_evolution: config.databricks_node_config.merge_with_schema_evolution,
            not_matched_by_source_action: config
                .databricks_node_config
                .not_matched_by_source_action,
            not_matched_by_source_condition: config
                .databricks_node_config
                .not_matched_by_source_condition,
            not_matched_condition: config.databricks_node_config.not_matched_condition,
            source_alias: config.databricks_node_config.source_alias,
            target_alias: config.databricks_node_config.target_alias,
            skip_matched_step: config.databricks_node_config.skip_matched_step,
            skip_not_matched_step: config.databricks_node_config.skip_not_matched_step,
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

            // Flattened configs (already handled above)
            snowflake_node_config: snowflake_model_config,
            bigquery_node_config: bigquery_model_config,
            databricks_node_config: databricks_model_config,
            redshift_node_config: redshift_model_config,

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
            auto_refresh,
            backup,
            bind,
            location,
            predicates,
        } = self;

        // Handle flattened configs
        #[allow(unused, clippy::let_unit_value)]
        let snowflake_model_config =
            snowflake_model_config.default_to(&parent.snowflake_node_config);
        #[allow(unused, clippy::let_unit_value)]
        let bigquery_model_config = bigquery_model_config.default_to(&parent.bigquery_node_config);
        #[allow(unused, clippy::let_unit_value)]
        let databricks_model_config =
            databricks_model_config.default_to(&parent.databricks_node_config);
        #[allow(unused, clippy::let_unit_value)]
        let redshift_model_config = redshift_model_config.default_to(&parent.redshift_node_config);

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
                auto_refresh,
                backup,
                bind,
                location,
                predicates
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
