use crate::schemas::common::ClusterConfig;
use crate::schemas::serde::OmissibleGrantConfig;
use crate::schemas::serde::QueryTag;
use dbt_common::io_args::StaticAnalysisKind;
use dbt_common::serde_utils::Omissible;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Spanned;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;
use indexmap::IndexMap;
use std::collections::btree_map::Iter;
use std::collections::{BTreeMap, HashSet};

use super::config_keys::ConfigKeys;
use super::omissible_utils::handle_omissible_override;

use crate::default_to;
use crate::schemas::common::DbtBatchSize;
use crate::schemas::common::DbtContract;
use crate::schemas::common::DbtIncrementalStrategy;
use crate::schemas::common::DbtMaterialization;
use crate::schemas::common::DbtUniqueKey;
use crate::schemas::common::PartitionConfig;
use crate::schemas::common::PersistDocsConfig;
use crate::schemas::common::SyncConfig;
use crate::schemas::common::{Access, DbtQuoting, Schedule};
use crate::schemas::common::{DocsConfig, OnConfigurationChange};
use crate::schemas::common::{Hooks, OnSchemaChange, hooks_equal};
use crate::schemas::manifest::GrantAccessToTarget;
use crate::schemas::project::configs::common::default_column_types;
use crate::schemas::project::configs::common::default_hooks;
use crate::schemas::project::configs::common::default_meta_and_tags;
use crate::schemas::project::configs::common::default_packages;
use crate::schemas::project::configs::common::default_quoting;
use crate::schemas::project::configs::common::default_to_grants;
use crate::schemas::project::configs::common::log_state_mod_diff;
use crate::schemas::project::configs::common::{
    WarehouseSpecificNodeConfig, access_eq, docs_eq, grants_eq, meta_eq, omissible_option_eq,
    same_warehouse_config,
};
use crate::schemas::project::dbt_project::DefaultTo;
use crate::schemas::project::dbt_project::TypedRecursiveConfig;
use crate::schemas::properties::ModelFreshness;
use crate::schemas::serde::StringOrArrayOfStrings;
use crate::schemas::serde::{
    IndexesConfig, PrimaryKeyConfig, bool_or_string_bool, default_type, f64_or_string_f64,
    u64_or_string_u64,
};
use dbt_serde_yaml::ShouldBe;

// NOTE: No #[skip_serializing_none] - we handle None serialization in serialize_with_mode
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectModelConfig {
    #[serde(rename = "+access")]
    pub access: Option<Access>,
    #[serde(rename = "+adapter_properties")]
    pub adapter_properties: Option<BTreeMap<String, YmlValue>>,
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
    pub cluster_by: Option<ClusterConfig>,
    #[serde(rename = "+clustered_by")]
    pub clustered_by: Option<String>,
    #[serde(rename = "+column_types")]
    pub column_types: Option<BTreeMap<Spanned<String>, String>>,
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
    #[serde(rename = "+database", alias = "+project", alias = "+data_space")]
    pub database: Omissible<Option<String>>,
    #[serde(rename = "+databricks_compute")]
    pub databricks_compute: Option<String>,
    #[serde(rename = "+databricks_tags")]
    pub databricks_tags: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+submission_method")]
    pub submission_method: Option<String>,
    #[serde(rename = "+job_cluster_config")]
    pub job_cluster_config: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+python_job_config")]
    pub python_job_config: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+cluster_id")]
    pub cluster_id: Option<String>,
    #[serde(rename = "+http_path")]
    pub http_path: Option<String>,
    #[serde(
        default,
        rename = "+create_notebook",
        deserialize_with = "bool_or_string_bool"
    )]
    pub create_notebook: Option<bool>,
    #[serde(rename = "+index_url")]
    pub index_url: Option<String>,
    #[serde(rename = "+additional_libs")]
    pub additional_libs: Option<Vec<YmlValue>>,
    #[serde(
        default,
        rename = "+user_folder_for_python",
        deserialize_with = "bool_or_string_bool"
    )]
    pub user_folder_for_python: Option<bool>,

    // NOTE: This is only for BigQuery materialized views
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
    pub grants: OmissibleGrantConfig,
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
    #[serde(rename = "+jar_file_uri")]
    pub jar_file_uri: Option<String>,
    #[serde(rename = "+timeout")]
    pub timeout: Option<u64>,
    #[serde(rename = "+batch_id")]
    pub batch_id: Option<String>,
    #[serde(rename = "+dataproc_cluster_name")]
    pub dataproc_cluster_name: Option<String>,
    #[serde(
        default,
        rename = "+notebook_template_id",
        deserialize_with = "u64_or_string_u64"
    )]
    pub notebook_template_id: Option<u64>,
    #[serde(
        default,
        rename = "+enable_list_inference",
        deserialize_with = "bool_or_string_bool"
    )]
    pub enable_list_inference: Option<bool>,
    #[serde(rename = "+intermediate_format")]
    pub intermediate_format: Option<String>,
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
    pub meta: Verbatim<Option<IndexMap<String, YmlValue>>>,
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
    #[serde(rename = "+python_version")]
    pub python_version: Option<String>,
    #[serde(rename = "+imports")]
    pub imports: Option<StringOrArrayOfStrings>,
    /// Snowflake Python model config: secrets to pass to stored procedure
    #[serde(rename = "+secrets")]
    pub secrets: Option<BTreeMap<String, YmlValue>>,
    /// Snowflake Python model config: external access integrations for network access
    #[serde(rename = "+external_access_integrations")]
    pub external_access_integrations: Option<StringOrArrayOfStrings>,
    /// Snowflake Python model config: use anonymous stored procedure (default: true)
    #[serde(
        default,
        rename = "+use_anonymous_sproc",
        deserialize_with = "bool_or_string_bool"
    )]
    pub use_anonymous_sproc: Option<bool>,
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
    pub query_tag: Option<QueryTag>,
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
        deserialize_with = "f64_or_string_f64"
    )]
    pub refresh_interval_minutes: Option<f64>,
    #[serde(rename = "+resource_tags")]
    pub resource_tags: Option<BTreeMap<String, String>>,
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
    pub static_analysis: Option<Spanned<StaticAnalysisKind>>,
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
    pub indexes: IndexesConfig,

    // Schedule (Databricks streaming tables)
    #[serde(rename = "+schedule")]
    pub schedule: Option<Schedule>,

    // Primary Key (Salesforce)
    #[serde(default, rename = "+primary_key")]
    pub primary_key: PrimaryKeyConfig,
    #[serde(rename = "+category")]
    pub category: Option<DataLakeObjectCategory>,

    /// Schema synchronization configuration
    #[serde(rename = "+sync")]
    pub sync: Option<SyncConfig>,

    // Flattened field:
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectModelConfig>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
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

impl TypedRecursiveConfig for ProjectModelConfig {
    fn type_name() -> &'static str {
        "model"
    }

    fn iter_children(&self) -> Iter<'_, String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone, PartialEq, JsonSchema)]
pub struct ModelConfig {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub alias: Option<String>,
    #[serde(alias = "project", alias = "data_space")]
    pub database: Omissible<Option<String>>,
    #[serde(alias = "dataset")]
    pub schema: Omissible<Option<String>>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub catalog_name: Option<String>,
    // need default to ensure None if field is not set
    // serialize_with ensures meta is always present (as {} when None) for Jinja macros
    // that access node.config.meta.get(...)
    #[serde(
        default,
        deserialize_with = "default_type",
        serialize_with = "crate::schemas::nodes::serialize_none_as_empty_map"
    )]
    pub meta: Option<IndexMap<String, YmlValue>>,
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
    pub column_types: Option<BTreeMap<Spanned<String>, String>>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub full_refresh: Option<bool>,
    pub unique_key: Option<DbtUniqueKey>,
    pub on_schema_change: Option<OnSchemaChange>,
    pub on_configuration_change: Option<OnConfigurationChange>,
    pub grants: OmissibleGrantConfig,
    pub packages: Option<StringOrArrayOfStrings>,
    pub python_version: Option<String>,
    pub docs: Option<DocsConfig>,
    pub imports: Option<StringOrArrayOfStrings>,
    pub secrets: Option<BTreeMap<String, YmlValue>>,
    pub external_access_integrations: Option<StringOrArrayOfStrings>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub use_anonymous_sproc: Option<bool>,
    pub contract: Option<DbtContract>,
    pub event_time: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub concurrent_batches: Option<bool>,
    pub merge_update_columns: Option<StringOrArrayOfStrings>,
    pub merge_exclude_columns: Option<StringOrArrayOfStrings>,
    pub access: Option<Access>,
    pub table_format: Option<String>,
    pub static_analysis: Option<Spanned<StaticAnalysisKind>>,
    pub freshness: Option<ModelFreshness>,
    pub sql_header: Option<String>,
    pub location: Option<String>,
    pub predicates: Option<Vec<String>>,
    /// Schema synchronization configuration
    pub sync: Option<SyncConfig>,
    // Adapter specific configs
    pub __warehouse_specific_config__: WarehouseSpecificNodeConfig,
    pub submission_method: Option<String>,
    pub job_cluster_config: Option<BTreeMap<String, YmlValue>>,
    pub python_job_config: Option<BTreeMap<String, YmlValue>>,
    pub cluster_id: Option<String>,
    pub http_path: Option<String>,
    pub create_notebook: Option<bool>,
    pub index_url: Option<String>,
    pub additional_libs: Option<Vec<YmlValue>>,
    pub user_folder_for_python: Option<bool>,
    /// Config keys accessed via dbt.config.get() in Python models
    /// Used to populate config_dict at runtime
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(skip)]
    pub config_keys_used: Option<Vec<String>>,
    /// Default values for config keys in the same order as config_keys_used
    /// Stored as minijinja Values which render as Python literals
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(skip)]
    pub config_keys_defaults: Option<Vec<minijinja::value::Value>>,
}

impl From<ProjectModelConfig> for ModelConfig {
    fn from(config: ProjectModelConfig) -> Self {
        Self {
            access: config.access,
            alias: config.alias,
            batch_size: config.batch_size,
            begin: config.begin,
            submission_method: config.submission_method.clone(),
            job_cluster_config: config.job_cluster_config.clone(),
            python_job_config: config.python_job_config.clone(),
            cluster_id: config.cluster_id.clone(),
            http_path: config.http_path.clone(),
            create_notebook: config.create_notebook,
            index_url: config.index_url.clone(),
            additional_libs: config.additional_libs.clone(),
            user_folder_for_python: config.user_folder_for_python,
            catalog_name: config.catalog_name.clone(),
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
            meta: (*config.meta).clone(),
            on_configuration_change: config.on_configuration_change,
            on_schema_change: config.on_schema_change,
            packages: config.packages,
            python_version: config.python_version,
            imports: config.imports,
            secrets: config.secrets,
            external_access_integrations: config.external_access_integrations,
            use_anonymous_sproc: config.use_anonymous_sproc,
            persist_docs: config.persist_docs,
            post_hook: config.post_hook,
            pre_hook: config.pre_hook,
            predicates: config.predicates,
            quoting: config.quoting,
            schema: config.schema,
            sql_header: config.sql_header,
            static_analysis: config.static_analysis,
            sync: config.sync,
            table_format: config.table_format,
            tags: config.tags.into_inner(),
            unique_key: config.unique_key,
            __warehouse_specific_config__: WarehouseSpecificNodeConfig {
                description: config.description,
                adapter_properties: config.adapter_properties,
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
                resource_tags: config.resource_tags,
                refresh_interval_minutes: config.refresh_interval_minutes,
                max_staleness: config.max_staleness,
                jar_file_uri: config.jar_file_uri,
                timeout: config.timeout,
                batch_id: config.batch_id,
                dataproc_cluster_name: config.dataproc_cluster_name,
                notebook_template_id: config.notebook_template_id,
                enable_list_inference: config.enable_list_inference,
                intermediate_format: config.intermediate_format,

                file_format: config.file_format,
                catalog_name: config.catalog_name,
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
            // Python-specific fields - initialized to None here, set during Python AST analysis
            config_keys_used: None,
            config_keys_defaults: None,
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
            description: config.__warehouse_specific_config__.description,
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
            meta: Verbatim::from(config.meta),
            submission_method: config.submission_method.clone(),
            job_cluster_config: config.job_cluster_config.clone(),
            python_job_config: config.python_job_config.clone(),
            cluster_id: config.cluster_id.clone(),
            http_path: config.http_path.clone(),
            create_notebook: config.create_notebook,
            index_url: config.index_url.clone(),
            additional_libs: config.additional_libs.clone(),
            user_folder_for_python: config.user_folder_for_python,
            on_configuration_change: config.on_configuration_change,
            on_schema_change: config.on_schema_change,
            packages: config.packages,
            python_version: config.python_version,
            imports: config.imports,
            secrets: config.secrets,
            external_access_integrations: config.external_access_integrations,
            use_anonymous_sproc: config.use_anonymous_sproc,
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
            adapter_properties: config.__warehouse_specific_config__.adapter_properties,
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
            jar_file_uri: config.__warehouse_specific_config__.jar_file_uri,
            timeout: config.__warehouse_specific_config__.timeout,
            batch_id: config.__warehouse_specific_config__.batch_id,
            dataproc_cluster_name: config.__warehouse_specific_config__.dataproc_cluster_name,
            notebook_template_id: config.__warehouse_specific_config__.notebook_template_id,
            enable_list_inference: config.__warehouse_specific_config__.enable_list_inference,
            intermediate_format: config.__warehouse_specific_config__.intermediate_format,
            copy_grants: config.__warehouse_specific_config__.copy_grants,
            secure: config.__warehouse_specific_config__.secure,
            partition_by: config.__warehouse_specific_config__.partition_by,
            cluster_by: config.__warehouse_specific_config__.cluster_by,
            hours_to_expiration: config.__warehouse_specific_config__.hours_to_expiration,
            labels: config.__warehouse_specific_config__.labels,
            labels_from_meta: config.__warehouse_specific_config__.labels_from_meta,
            resource_tags: config.__warehouse_specific_config__.resource_tags,
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
            primary_key: config.__warehouse_specific_config__.primary_key,
            category: config.__warehouse_specific_config__.category,
            sync: config.sync,
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
            python_version,
            imports,
            secrets,
            external_access_integrations,
            use_anonymous_sproc,
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
            submission_method,
            job_cluster_config,
            python_job_config,
            cluster_id,
            http_path,
            create_notebook,
            index_url,
            additional_libs,
            user_folder_for_python,
            config_keys_used,
            config_keys_defaults,
            sync,
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
        #[allow(unused, clippy::let_unit_value)]
        let packages = default_packages(packages, &parent.packages);

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
                python_version,
                use_anonymous_sproc,
                secrets,
                external_access_integrations,
                imports,
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
                submission_method,
                job_cluster_config,
                python_job_config,
                cluster_id,
                http_path,
                create_notebook,
                index_url,
                additional_libs,
                user_folder_for_python,
                config_keys_used,
                config_keys_defaults,
                sync,
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

    fn get_static_analysis(&self) -> Option<StaticAnalysisKind> {
        self.static_analysis
            .as_ref()
            .map(|s| s.clone().into_inner())
    }
}

impl ModelConfig {
    pub fn same_database_representation(&self, other: &ModelConfig) -> bool {
        let database_eq = omissible_option_eq(&self.database, &other.database);
        let alias_eq = self.alias == other.alias;

        let result = database_eq && alias_eq;

        if !result {
            log_state_mod_diff(
                "unique_id in next model_config log",
                "model_database_representation",
                [
                    (
                        "database",
                        database_eq,
                        Some((
                            format!("{:?}", &self.database),
                            format!("{:?}", &other.database),
                        )),
                    ),
                    (
                        "alias",
                        alias_eq,
                        Some((format!("{:?}", &self.alias), format!("{:?}", &other.alias))),
                    ),
                ],
            );
        }

        result
    }

    /// Custom comparison that treats Omitted and Present(None) as equivalent for schema/database fields
    pub fn same_config(&self, other: &ModelConfig) -> bool {
        // Compare all fields.
        let enabled_eq = self.enabled == other.enabled;
        let catalog_name_eq = self.catalog_name == other.catalog_name;
        let meta_eq_result = meta_eq(&self.meta, &other.meta); // Custom comparison for meta
        let materialized_eq_result = materialized_eq(&self.materialized, &other.materialized);
        let incremental_strategy_eq = self.incremental_strategy == other.incremental_strategy;
        // incremental_predicates can differ because of environment, i.e. dev vs prod
        // so we don't compare them. To compare them we will need a SQL AST whose
        // shape and node types can be compared rather contents of each node.
        // && self.incremental_predicates == other.incremental_predicates
        let batch_size_eq = self.batch_size == other.batch_size;
        let lookback_eq_result = lookback_eq(&self.lookback, &other.lookback); // Custom comparison for lookback
        let begin_eq = self.begin == other.begin;
        let persist_docs_eq_result = persist_docs_eq(&self.persist_docs, &other.persist_docs); // Custom comparison for persist_docs
        let post_hook_eq = hooks_equal(&self.post_hook, &other.post_hook);
        let pre_hook_eq = hooks_equal(&self.pre_hook, &other.pre_hook);
        // let quoting_eq = self.quoting == other.quoting // TODO: re-enable when no longer using mantle/core manifests in IA
        let column_types_eq_result = column_types_eq(&self.column_types, &other.column_types); // Custom comparison for column_types
        let full_refresh_eq = self.full_refresh == other.full_refresh;
        let unique_key_eq = self.unique_key == other.unique_key;
        let on_schema_change_eq_result =
            on_schema_change_eq(&self.on_schema_change, &other.on_schema_change); // Custom comparison for on_schema_change
        let on_configuration_change_eq_result = on_configuration_change_eq(
            &self.on_configuration_change,
            &other.on_configuration_change,
        ); // Custom comparison for on_configuration_change
        let grants_eq_result = grants_eq(&self.grants, &other.grants); // Custom comparison for grants
        let packages_eq = packages_and_imports_eq(&self.packages, &other.packages); // Custom comparison for packages
        let imports_eq = packages_and_imports_eq(&self.imports, &other.imports); // Custom comparison for imports (same function as packages)
        let python_version_eq = self.python_version == other.python_version;
        let docs_eq_result = docs_eq(&self.docs, &other.docs); // Custom comparison for docs
        // This is a project level config that can differ between environments,
        // so we don't compare them.
        // && self.event_time == other.event_time
        let concurrent_batches_eq = self.concurrent_batches == other.concurrent_batches;
        let merge_update_columns_eq = self.merge_update_columns == other.merge_update_columns;
        let merge_exclude_columns_eq = self.merge_exclude_columns == other.merge_exclude_columns;
        let access_eq_result = access_eq(&self.access, &other.access); // Custom comparison for access
        let table_format_eq = self.table_format == other.table_format;
        let static_analysis_eq = self.static_analysis == other.static_analysis;
        let freshness_eq = self.freshness == other.freshness;
        let sql_header_eq = self.sql_header == other.sql_header;
        let location_eq = self.location == other.location;
        let predicates_eq = self.predicates == other.predicates;
        let warehouse_config_eq = same_warehouse_config(
            &self.__warehouse_specific_config__,
            &other.__warehouse_specific_config__,
        );

        let result = enabled_eq
            && catalog_name_eq
            && meta_eq_result
            && materialized_eq_result
            && incremental_strategy_eq
            && batch_size_eq
            && lookback_eq_result
            && begin_eq
            && persist_docs_eq_result
            && post_hook_eq
            && pre_hook_eq
            // && quoting_eq
            && column_types_eq_result
            && full_refresh_eq
            && unique_key_eq
            && on_schema_change_eq_result
            && on_configuration_change_eq_result
            && grants_eq_result
            && packages_eq
            && imports_eq
            && python_version_eq
            && docs_eq_result
            // && event_time_eq
            && concurrent_batches_eq
            && merge_update_columns_eq
            && merge_exclude_columns_eq
            && access_eq_result
            && table_format_eq
            && static_analysis_eq
            && freshness_eq
            && sql_header_eq
            && location_eq
            && predicates_eq
            && warehouse_config_eq;

        if !result {
            log_state_mod_diff(
                "unique_id in next model_config log",
                "model_config",
                [
                    (
                        "enabled",
                        enabled_eq,
                        Some((
                            format!("{:?}", &self.enabled),
                            format!("{:?}", &other.enabled),
                        )),
                    ),
                    (
                        "catalog_name",
                        catalog_name_eq,
                        Some((
                            format!("{:?}", &self.catalog_name),
                            format!("{:?}", &other.catalog_name),
                        )),
                    ),
                    (
                        "meta",
                        meta_eq_result,
                        Some((format!("{:?}", &self.meta), format!("{:?}", &other.meta))),
                    ),
                    (
                        "materialized",
                        materialized_eq_result,
                        Some((
                            format!("{:?}", &self.materialized),
                            format!("{:?}", &other.materialized),
                        )),
                    ),
                    (
                        "incremental_strategy",
                        incremental_strategy_eq,
                        Some((
                            format!("{:?}", &self.incremental_strategy),
                            format!("{:?}", &other.incremental_strategy),
                        )),
                    ),
                    (
                        "batch_size",
                        batch_size_eq,
                        Some((
                            format!("{:?}", &self.batch_size),
                            format!("{:?}", &other.batch_size),
                        )),
                    ),
                    (
                        "lookback",
                        lookback_eq_result,
                        Some((
                            format!("{:?}", &self.lookback),
                            format!("{:?}", &other.lookback),
                        )),
                    ),
                    (
                        "begin",
                        begin_eq,
                        Some((format!("{:?}", &self.begin), format!("{:?}", &other.begin))),
                    ),
                    ("persist_docs", persist_docs_eq_result, None),
                    (
                        "post_hook",
                        post_hook_eq,
                        Some((
                            format!("{:?}", &self.post_hook),
                            format!("{:?}", &other.post_hook),
                        )),
                    ),
                    (
                        "pre_hook",
                        pre_hook_eq,
                        Some((
                            format!("{:?}", &self.pre_hook),
                            format!("{:?}", &other.pre_hook),
                        )),
                    ),
                    (
                        "column_types",
                        column_types_eq_result,
                        Some((
                            format!("{:?}", &self.column_types),
                            format!("{:?}", &other.column_types),
                        )),
                    ),
                    (
                        "full_refresh",
                        full_refresh_eq,
                        Some((
                            format!("{:?}", &self.full_refresh),
                            format!("{:?}", &other.full_refresh),
                        )),
                    ),
                    (
                        "unique_key",
                        unique_key_eq,
                        Some((
                            format!("{:?}", &self.unique_key),
                            format!("{:?}", &other.unique_key),
                        )),
                    ),
                    (
                        "on_schema_change",
                        on_schema_change_eq_result,
                        Some((
                            format!("{:?}", &self.on_schema_change),
                            format!("{:?}", &other.on_schema_change),
                        )),
                    ),
                    (
                        "on_configuration_change",
                        on_configuration_change_eq_result,
                        Some((
                            format!("{:?}", &self.on_configuration_change),
                            format!("{:?}", &other.on_configuration_change),
                        )),
                    ),
                    (
                        "grants",
                        grants_eq_result,
                        Some((
                            format!("{:?}", &self.grants),
                            format!("{:?}", &other.grants),
                        )),
                    ),
                    (
                        "packages",
                        packages_eq,
                        Some((
                            format!("{:?}", &self.packages),
                            format!("{:?}", &other.packages),
                        )),
                    ),
                    (
                        "imports",
                        imports_eq,
                        Some((
                            format!("{:?}", &self.imports),
                            format!("{:?}", &other.imports),
                        )),
                    ),
                    (
                        "python_version",
                        python_version_eq,
                        Some((
                            format!("{:?}", &self.python_version),
                            format!("{:?}", &other.python_version),
                        )),
                    ),
                    (
                        "docs",
                        docs_eq_result,
                        Some((format!("{:?}", &self.docs), format!("{:?}", &other.docs))),
                    ),
                    (
                        "concurrent_batches",
                        concurrent_batches_eq,
                        Some((
                            format!("{:?}", &self.concurrent_batches),
                            format!("{:?}", &other.concurrent_batches),
                        )),
                    ),
                    (
                        "merge_update_columns",
                        merge_update_columns_eq,
                        Some((
                            format!("{:?}", &self.merge_update_columns),
                            format!("{:?}", &other.merge_update_columns),
                        )),
                    ),
                    (
                        "merge_exclude_columns",
                        merge_exclude_columns_eq,
                        Some((
                            format!("{:?}", &self.merge_exclude_columns),
                            format!("{:?}", &other.merge_exclude_columns),
                        )),
                    ),
                    (
                        "access",
                        access_eq_result,
                        Some((
                            format!("{:?}", &self.access),
                            format!("{:?}", &other.access),
                        )),
                    ),
                    (
                        "table_format",
                        table_format_eq,
                        Some((
                            format!("{:?}", &self.table_format),
                            format!("{:?}", &other.table_format),
                        )),
                    ),
                    (
                        "static_analysis",
                        static_analysis_eq,
                        Some((
                            format!("{:?}", &self.static_analysis),
                            format!("{:?}", &other.static_analysis),
                        )),
                    ),
                    (
                        "freshness",
                        freshness_eq,
                        Some((
                            format!("{:?}", &self.freshness),
                            format!("{:?}", &other.freshness),
                        )),
                    ),
                    (
                        "sql_header",
                        sql_header_eq,
                        Some((
                            format!("{:?}", &self.sql_header),
                            format!("{:?}", &other.sql_header),
                        )),
                    ),
                    (
                        "location",
                        location_eq,
                        Some((
                            format!("{:?}", &self.location),
                            format!("{:?}", &other.location),
                        )),
                    ),
                    (
                        "predicates",
                        predicates_eq,
                        Some((
                            format!("{:?}", &self.predicates),
                            format!("{:?}", &other.predicates),
                        )),
                    ),
                    ("warehouse_config", warehouse_config_eq, None),
                ],
            );
        }

        result
    }
}

impl ConfigKeys for ModelConfig {
    fn valid_field_names() -> HashSet<String> {
        let default_instance = Self::default();
        let serialized = dbt_serde_yaml::to_value(&default_instance)
            .expect("Failed to serialize ModelConfig for field extraction");

        let mut field_names = HashSet::new();

        if let YmlValue::Mapping(map, _) = serialized {
            for (key, value) in map {
                if let YmlValue::String(key_str, _) = key {
                    // Extract top-level fields
                    field_names.insert(key_str.clone());

                    // Also include warehouse-specific fields from __warehouse_specific_config__
                    if key_str == "__warehouse_specific_config__" {
                        if let YmlValue::Mapping(warehouse_map, _) = value {
                            for (warehouse_key, _) in warehouse_map {
                                if let YmlValue::String(warehouse_key_str, _) = warehouse_key {
                                    field_names.insert(warehouse_key_str);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Add known aliases that might not show up in serialization
        field_names.insert("project".to_string()); // alias for database
        field_names.insert("data_space".to_string()); // alias for database
        field_names.insert("dataset".to_string()); // alias for schema
        field_names.insert("post-hook".to_string()); // might be serialized as post_hook
        field_names.insert("pre-hook".to_string()); // might be serialized as pre_hook

        field_names
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

// Helper function to compare column_types fields, treating None and empty BTreeMap as equivalent
fn column_types_eq(
    a: &Option<BTreeMap<Spanned<String>, String>>,
    b: &Option<BTreeMap<Spanned<String>, String>>,
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

// Helper function to compare packages and imports fields, treating None and empty ArrayOfStrings as equivalent
fn packages_and_imports_eq(
    a: &Option<StringOrArrayOfStrings>,
    b: &Option<StringOrArrayOfStrings>,
) -> bool {
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

// Helper function to compare materialized fields, treating None as View (the default)
fn materialized_eq(a: &Option<DbtMaterialization>, b: &Option<DbtMaterialization>) -> bool {
    let default_materialized = DbtMaterialization::View;

    match (a, b) {
        (None, None) => true,
        (Some(a_val), Some(b_val)) => a_val == b_val,
        (None, Some(b_val)) => b_val == &default_materialized,
        (Some(a_val), None) => a_val == &default_materialized,
    }
}

#[cfg(test)]
mod tests {
    use super::ModelConfig;

    #[test]
    fn test_packages_append() {
        use crate::schemas::project::dbt_project::DefaultTo;
        use crate::schemas::serde::StringOrArrayOfStrings;

        let parent = ModelConfig {
            packages: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "numpy".to_string(),
                "pandas".to_string(),
            ])),
            ..Default::default()
        };

        let mut child = ModelConfig {
            packages: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "matplotlib".to_string(),
            ])),
            ..Default::default()
        };

        child.default_to(&parent);

        // Should have parent packages first, then child packages (no dedup/sort, matches dbt-core)
        assert_eq!(
            child.packages,
            Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "numpy".to_string(),
                "pandas".to_string(),
                "matplotlib".to_string(),
            ]))
        );
    }

    #[test]
    fn test_packages_append_with_string_variant() {
        use crate::schemas::project::dbt_project::DefaultTo;
        use crate::schemas::serde::StringOrArrayOfStrings;

        let parent = ModelConfig {
            packages: Some(StringOrArrayOfStrings::String("numpy".to_string())),
            ..Default::default()
        };

        let mut child = ModelConfig {
            packages: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "pandas".to_string(),
            ])),
            ..Default::default()
        };

        child.default_to(&parent);

        // Should convert String to ArrayOfStrings and merge
        assert_eq!(
            child.packages,
            Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "numpy".to_string(),
                "pandas".to_string(),
            ]))
        );
    }

    #[test]
    fn test_packages_none_child_inherits_parent() {
        use crate::schemas::project::dbt_project::DefaultTo;
        use crate::schemas::serde::StringOrArrayOfStrings;

        let parent = ModelConfig {
            packages: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "numpy".to_string(),
            ])),
            ..Default::default()
        };

        let mut child = ModelConfig {
            packages: None,
            ..Default::default()
        };

        child.default_to(&parent);

        // Child should inherit parent's packages
        assert_eq!(
            child.packages,
            Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "numpy".to_string(),
            ]))
        );
    }

    #[test]
    fn test_packages_no_deduplication() {
        use crate::schemas::project::dbt_project::DefaultTo;
        use crate::schemas::serde::StringOrArrayOfStrings;

        let parent = ModelConfig {
            packages: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "numpy".to_string(),
                "pandas".to_string(),
            ])),
            ..Default::default()
        };

        let mut child = ModelConfig {
            packages: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "numpy".to_string(),
                "matplotlib".to_string(),
            ])),
            ..Default::default()
        };

        child.default_to(&parent);

        // Should preserve duplicates (no dedup/sort, matches dbt-core behavior)
        assert_eq!(
            child.packages,
            Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "numpy".to_string(),
                "pandas".to_string(),
                "numpy".to_string(),
                "matplotlib".to_string(),
            ]))
        );
    }
}
