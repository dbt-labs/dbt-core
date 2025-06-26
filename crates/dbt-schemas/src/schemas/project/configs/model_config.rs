use dbt_common::io_args::StaticAnalysisKind;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;
use std::collections::btree_map::Iter;
use std::collections::BTreeMap;

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
use crate::schemas::project::configs::common::default_column_types;
use crate::schemas::project::configs::common::default_hooks;
use crate::schemas::project::configs::common::default_meta_and_tags;
use crate::schemas::project::configs::common::default_quoting;
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
    pub batch_size: Option<DbtBatchSize>,
    #[serde(rename = "+begin")]
    pub begin: Option<String>,
    #[serde(rename = "+bind")]
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
    #[serde(rename = "+database")]
    pub database: Option<String>,
    #[serde(rename = "+databricks_compute")]
    pub databricks_compute: Option<String>,
    #[serde(rename = "+databricks_tags")]
    pub databricks_tags: Option<BTreeMap<String, Value>>,
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
    pub grants: Option<BTreeMap<String, Value>>,
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
    #[serde(rename = "+merge_with_schema_evolution")]
    pub merge_with_schema_evolution: Option<bool>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, Value>>,
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
    #[serde(rename = "+schema")]
    pub schema: Option<String>,
    #[serde(rename = "+skip_matched_step")]
    pub skip_matched_step: Option<bool>,
    #[serde(rename = "+skip_not_matched_step")]
    pub skip_not_matched_step: Option<bool>,
    #[serde(rename = "+secure")]
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
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+target_lag")]
    pub target_lag: Option<String>,
    #[serde(rename = "+tblproperties")]
    pub tblproperties: Option<BTreeMap<String, Value>>,
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
    pub schema: Option<String>,
    pub database: Option<String>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub catalog_name: Option<String>,
    // need default to ensure None if field is not set
    #[serde(default, deserialize_with = "default_type")]
    pub meta: Option<BTreeMap<String, Value>>,
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
    pub grants: Option<BTreeMap<String, Value>>,
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
    #[serde(flatten)]
    pub snowflake_model_config: SnowflakeModelConfig,
    #[serde(flatten)]
    pub bigquery_model_config: BigQueryModelConfig,
    #[serde(flatten)]
    pub databricks_model_config: DatabricksModelConfig,
    #[serde(flatten)]
    pub redshift_model_config: RedshiftModelConfig,
    pub static_analysis: Option<StaticAnalysisKind>,
    pub freshness: Option<ModelFreshness>,
    pub sql_header: Option<String>,
    pub auto_refresh: Option<bool>,
    pub backup: Option<bool>,
    pub bind: Option<bool>,
    pub location: Option<String>,
    pub predicates: Option<Vec<String>>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub transient: Option<bool>,
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
            tags: config.tags,
            transient: config.transient,
            unique_key: config.unique_key,
            snowflake_model_config: SnowflakeModelConfig {
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
            },
            bigquery_model_config: BigQueryModelConfig {
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
            databricks_model_config: DatabricksModelConfig {
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
            redshift_model_config: RedshiftModelConfig {
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
            tags: config.tags,
            transient: config.transient,
            unique_key: config.unique_key,
            external_volume: config.snowflake_model_config.external_volume,
            base_location_root: config.snowflake_model_config.base_location_root,
            base_location_subpath: config.snowflake_model_config.base_location_subpath,
            target_lag: config.snowflake_model_config.target_lag,
            snowflake_warehouse: config.snowflake_model_config.snowflake_warehouse,
            refresh_mode: config.snowflake_model_config.refresh_mode,
            initialize: config.snowflake_model_config.initialize,
            tmp_relation_type: config.snowflake_model_config.tmp_relation_type,
            query_tag: config.snowflake_model_config.query_tag,
            automatic_clustering: config.snowflake_model_config.automatic_clustering,
            copy_grants: config.snowflake_model_config.copy_grants,
            secure: config.snowflake_model_config.secure,
            partition_by: config.bigquery_model_config.partition_by,
            cluster_by: config.bigquery_model_config.cluster_by,
            hours_to_expiration: config.bigquery_model_config.hours_to_expiration,
            labels: config.bigquery_model_config.labels,
            labels_from_meta: config.bigquery_model_config.labels_from_meta,
            kms_key_name: config.bigquery_model_config.kms_key_name,
            require_partition_filter: config.bigquery_model_config.require_partition_filter,
            partition_expiration_days: config.bigquery_model_config.partition_expiration_days,
            grant_access_to: config.bigquery_model_config.grant_access_to,
            partitions: config.bigquery_model_config.partitions,
            enable_refresh: config.bigquery_model_config.enable_refresh,
            refresh_interval_minutes: config.bigquery_model_config.refresh_interval_minutes,
            description: config.bigquery_model_config.description,
            max_staleness: config.bigquery_model_config.max_staleness,
            file_format: config.databricks_model_config.file_format,
            location_root: config.databricks_model_config.location_root,
            tblproperties: config.databricks_model_config.tblproperties,
            include_full_name_in_path: config.databricks_model_config.include_full_name_in_path,
            liquid_clustered_by: config.databricks_model_config.liquid_clustered_by,
            auto_liquid_cluster: config.databricks_model_config.auto_liquid_cluster,
            clustered_by: config.databricks_model_config.clustered_by,
            buckets: config.databricks_model_config.buckets,
            catalog: config.databricks_model_config.catalog,
            databricks_tags: config.databricks_model_config.databricks_tags,
            compression: config.databricks_model_config.compression,
            databricks_compute: config.databricks_model_config.databricks_compute,
            dist: config.redshift_model_config.dist,
            sort: config.redshift_model_config.sort,
            sort_type: config.redshift_model_config.sort_type,
            matched_condition: config.databricks_model_config.matched_condition,
            merge_with_schema_evolution: config.databricks_model_config.merge_with_schema_evolution,
            not_matched_by_source_action: config
                .databricks_model_config
                .not_matched_by_source_action,
            not_matched_by_source_condition: config
                .databricks_model_config
                .not_matched_by_source_condition,
            not_matched_condition: config.databricks_model_config.not_matched_condition,
            source_alias: config.databricks_model_config.source_alias,
            target_alias: config.databricks_model_config.target_alias,
            skip_matched_step: config.databricks_model_config.skip_matched_step,
            skip_not_matched_step: config.databricks_model_config.skip_not_matched_step,
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
            ref mut post_hook,
            ref mut pre_hook,
            ref mut meta,
            ref mut tags,
            ref mut quoting,

            // Flattened configs (already handled above)
            ref mut snowflake_model_config,
            ref mut bigquery_model_config,
            ref mut databricks_model_config,
            ref mut redshift_model_config,

            // Simple fields (handle with macro)
            ref mut enabled,
            ref mut alias,
            ref mut schema,
            ref mut database,
            ref mut catalog_name,
            ref mut group,
            ref mut materialized,
            ref mut incremental_strategy,
            ref mut incremental_predicates,
            ref mut batch_size,
            ref mut lookback,
            ref mut begin,
            ref mut persist_docs,
            ref mut column_types,
            ref mut full_refresh,
            ref mut unique_key,
            ref mut on_schema_change,
            ref mut on_configuration_change,
            ref mut grants,
            ref mut packages,
            ref mut docs,
            ref mut contract,
            ref mut event_time,
            ref mut concurrent_batches,
            ref mut merge_update_columns,
            ref mut merge_exclude_columns,
            ref mut access,
            ref mut table_format,
            ref mut static_analysis,
            ref mut freshness,
            ref mut sql_header,
            ref mut auto_refresh,
            ref mut backup,
            ref mut bind,
            ref mut location,
            ref mut predicates,
            ref mut transient,
        } = self;

        // Handle flattened configs
        #[allow(unused, clippy::let_unit_value)]
        let snowflake_model_config =
            snowflake_model_config.default_to(&parent.snowflake_model_config);
        #[allow(unused, clippy::let_unit_value)]
        let bigquery_model_config = bigquery_model_config.default_to(&parent.bigquery_model_config);
        #[allow(unused, clippy::let_unit_value)]
        let databricks_model_config =
            databricks_model_config.default_to(&parent.databricks_model_config);
        #[allow(unused, clippy::let_unit_value)]
        let redshift_model_config = redshift_model_config.default_to(&parent.redshift_model_config);

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

        default_to!(
            parent,
            [
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
                transient
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
        self.database.clone()
    }

    fn schema(&self) -> Option<String> {
        self.schema.clone()
    }

    fn alias(&self) -> Option<String> {
        self.alias.clone()
    }

    fn get_pre_hook(&self) -> Option<&Hooks> {
        self.pre_hook.as_ref()
    }

    fn get_post_hook(&self) -> Option<&Hooks> {
        self.post_hook.as_ref()
    }
}

// Implement default_to for the flattened configs
impl DefaultTo<SnowflakeModelConfig> for SnowflakeModelConfig {
    fn default_to(&mut self, parent: &SnowflakeModelConfig) {
        // Exhaustive destructuring ensures all fields are handled
        let SnowflakeModelConfig {
            ref mut external_volume,
            ref mut base_location_root,
            ref mut base_location_subpath,
            ref mut target_lag,
            ref mut snowflake_warehouse,
            ref mut refresh_mode,
            ref mut initialize,
            ref mut tmp_relation_type,
            ref mut query_tag,
            ref mut automatic_clustering,
            ref mut copy_grants,
            ref mut secure,
        } = self;

        default_to!(
            parent,
            [
                external_volume,
                base_location_root,
                base_location_subpath,
                target_lag,
                snowflake_warehouse,
                refresh_mode,
                initialize,
                tmp_relation_type,
                query_tag,
                automatic_clustering,
                copy_grants,
                secure
            ]
        );
    }
}

impl DefaultTo<BigQueryModelConfig> for BigQueryModelConfig {
    fn default_to(&mut self, parent: &BigQueryModelConfig) {
        // Exhaustive destructuring ensures all fields are handled
        let BigQueryModelConfig {
            ref mut partition_by,
            ref mut cluster_by,
            ref mut hours_to_expiration,
            ref mut labels,
            ref mut labels_from_meta,
            ref mut kms_key_name,
            ref mut require_partition_filter,
            ref mut partition_expiration_days,
            ref mut grant_access_to,
            ref mut partitions,
            ref mut enable_refresh,
            ref mut refresh_interval_minutes,
            ref mut description,
            ref mut max_staleness,
        } = self;

        default_to!(
            parent,
            [
                partition_by,
                cluster_by,
                hours_to_expiration,
                labels,
                labels_from_meta,
                kms_key_name,
                require_partition_filter,
                partition_expiration_days,
                grant_access_to,
                partitions,
                enable_refresh,
                refresh_interval_minutes,
                description,
                max_staleness
            ]
        );
    }
}

impl DefaultTo<DatabricksModelConfig> for DatabricksModelConfig {
    fn default_to(&mut self, parent: &DatabricksModelConfig) {
        // Exhaustive destructuring ensures all fields are handled
        let DatabricksModelConfig {
            ref mut file_format,
            ref mut location_root,
            ref mut tblproperties,
            ref mut include_full_name_in_path,
            ref mut liquid_clustered_by,
            ref mut auto_liquid_cluster,
            ref mut clustered_by,
            ref mut buckets,
            ref mut catalog,
            ref mut databricks_tags,
            ref mut compression,
            ref mut databricks_compute,
            ref mut target_alias,
            ref mut source_alias,
            ref mut matched_condition,
            ref mut not_matched_condition,
            ref mut not_matched_by_source_condition,
            ref mut not_matched_by_source_action,
            ref mut merge_with_schema_evolution,
            ref mut skip_matched_step,
            ref mut skip_not_matched_step,
        } = self;

        default_to!(
            parent,
            [
                file_format,
                location_root,
                tblproperties,
                include_full_name_in_path,
                liquid_clustered_by,
                auto_liquid_cluster,
                clustered_by,
                buckets,
                catalog,
                databricks_tags,
                compression,
                databricks_compute,
                target_alias,
                source_alias,
                matched_condition,
                not_matched_condition,
                not_matched_by_source_condition,
                not_matched_by_source_action,
                merge_with_schema_evolution,
                skip_matched_step,
                skip_not_matched_step,
            ]
        );
    }
}

impl DefaultTo<RedshiftModelConfig> for RedshiftModelConfig {
    fn default_to(&mut self, parent: &RedshiftModelConfig) {
        let RedshiftModelConfig {
            ref mut auto_refresh,
            ref mut backup,
            ref mut bind,
            ref mut dist,
            ref mut sort,
            ref mut sort_type,
        } = self;

        default_to!(parent, [auto_refresh, backup, bind, dist, sort, sort_type]);
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, JsonSchema)]
pub struct DatabricksModelConfig {
    pub file_format: Option<String>,
    pub location_root: Option<String>,
    pub tblproperties: Option<BTreeMap<String, Value>>,
    // this config is introduced here https://github.com/databricks/dbt-databricks/pull/823
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub include_full_name_in_path: Option<bool>,
    pub liquid_clustered_by: Option<StringOrArrayOfStrings>,
    pub auto_liquid_cluster: Option<bool>,
    pub clustered_by: Option<String>,
    pub buckets: Option<i64>,
    pub catalog: Option<String>,
    pub databricks_tags: Option<BTreeMap<String, Value>>,
    pub compression: Option<String>,
    pub databricks_compute: Option<String>,
    pub target_alias: Option<String>,
    pub source_alias: Option<String>,
    pub matched_condition: Option<String>,
    pub not_matched_condition: Option<String>,
    pub not_matched_by_source_condition: Option<String>,
    pub not_matched_by_source_action: Option<String>,
    pub merge_with_schema_evolution: Option<bool>,
    pub skip_matched_step: Option<bool>,
    pub skip_not_matched_step: Option<bool>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, JsonSchema)]
pub struct SnowflakeModelConfig {
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
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub copy_grants: Option<bool>,
    pub secure: Option<bool>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, JsonSchema)]
pub struct RedshiftModelConfig {
    pub auto_refresh: Option<bool>,
    pub backup: Option<bool>,
    pub bind: Option<bool>,
    pub dist: Option<String>,
    pub sort: Option<StringOrArrayOfStrings>,
    pub sort_type: Option<String>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, JsonSchema)]
pub struct BigQueryModelConfig {
    pub partition_by: Option<BigqueryPartitionConfigLegacy>,
    pub cluster_by: Option<BigqueryClusterConfig>,
    #[serde(default, deserialize_with = "u64_or_string_u64")]
    pub hours_to_expiration: Option<u64>,
    pub labels: Option<BTreeMap<String, String>>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub labels_from_meta: Option<bool>,
    pub kms_key_name: Option<String>,
    #[serde(default = "default_false", deserialize_with = "bool_or_string_bool")]
    pub require_partition_filter: Option<bool>,
    #[serde(default, deserialize_with = "u64_or_string_u64")]
    pub partition_expiration_days: Option<u64>,
    pub grant_access_to: Option<Vec<GrantAccessToTarget>>,
    pub partitions: Option<Vec<String>>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enable_refresh: Option<bool>,
    #[serde(default, deserialize_with = "u64_or_string_u64")]
    pub refresh_interval_minutes: Option<u64>,
    pub description: Option<String>,
    pub max_staleness: Option<String>,
}

fn default_false() -> Option<bool> {
    Some(false)
}
