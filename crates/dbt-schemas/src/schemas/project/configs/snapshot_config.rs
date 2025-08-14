use dbt_common::io_args::StaticAnalysisKind;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::ShouldBe;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;
use std::collections::btree_map::Iter;

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

use crate::default_to;
use crate::schemas::common::DbtMaterialization;
use crate::schemas::common::DbtQuoting;
use crate::schemas::common::DocsConfig;
use crate::schemas::common::HardDeletes;
use crate::schemas::common::Hooks;
use crate::schemas::common::PersistDocsConfig;
use crate::schemas::manifest::GrantAccessToTarget;
use crate::schemas::manifest::{BigqueryClusterConfig, BigqueryPartitionConfigLegacy};
use crate::schemas::project::BigQueryNodeConfig;
use crate::schemas::project::DatabricksNodeConfig;
use crate::schemas::project::DefaultTo;
use crate::schemas::project::IterChildren;
use crate::schemas::project::SnowflakeNodeConfig;
use crate::schemas::project::configs::common::MsSqlNodeConfig;
use crate::schemas::project::configs::common::RedshiftNodeConfig;
use crate::schemas::project::configs::common::default_hooks;
use crate::schemas::project::configs::common::default_meta_and_tags;
use crate::schemas::project::configs::common::default_quoting;
use crate::schemas::project::configs::common::default_to_grants;
use crate::schemas::serde::StringOrArrayOfStrings;
use crate::schemas::serde::bool_or_string_bool;
use crate::schemas::serde::u64_or_string_u64;

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
    #[serde(rename = "+materialized")]
    pub materialized: Option<DbtMaterialization>,
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
    pub snapshot_meta_column_names: Option<SnapshotMetaColumnNames>,
    #[serde(rename = "+hard_deletes")]
    pub hard_deletes: Option<HardDeletes>,
    // General Configuration
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
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
    pub grants: Option<BTreeMap<String, StringOrArrayOfStrings>>,
    #[serde(rename = "+event_time")]
    pub event_time: Option<String>,
    #[serde(rename = "+quoting")]
    pub quoting: Option<DbtQuoting>,
    #[serde(rename = "+static_analysis")]
    pub static_analysis: Option<StaticAnalysisKind>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+group")]
    pub group: Option<String>,
    #[serde(
        default,
        rename = "+quote_columns",
        deserialize_with = "bool_or_string_bool"
    )]
    pub quote_columns: Option<bool>,
    #[serde(rename = "+invalidate_hard_deletes")]
    pub invalidate_hard_deletes: Option<bool>,
    #[serde(rename = "+docs")]
    pub docs: Option<DocsConfig>,
    // Adapter-specific fields (Snowflake)
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
    #[serde(default, rename = "+backup", deserialize_with = "bool_or_string_bool")]
    pub backup: Option<bool>,
    #[serde(rename = "+base_location_root")]
    pub base_location_root: Option<String>,
    #[serde(rename = "+base_location_subpath")]
    pub base_location_subpath: Option<String>,
    #[serde(
        default,
        rename = "+copy_grants",
        deserialize_with = "bool_or_string_bool"
    )]
    pub copy_grants: Option<bool>,
    #[serde(rename = "+external_volume")]
    pub external_volume: Option<String>,
    #[serde(rename = "+initialize")]
    pub initialize: Option<String>,
    #[serde(rename = "+query_tag")]
    pub query_tag: Option<String>,
    #[serde(rename = "+refresh_mode")]
    pub refresh_mode: Option<String>,
    #[serde(default, rename = "+secure", deserialize_with = "bool_or_string_bool")]
    pub secure: Option<bool>,
    #[serde(rename = "+snowflake_warehouse")]
    pub snowflake_warehouse: Option<String>,
    #[serde(rename = "+target_lag")]
    pub target_lag: Option<String>,
    #[serde(rename = "+tmp_relation_type")]
    pub tmp_relation_type: Option<String>,
    #[serde(
        default,
        rename = "+transient",
        deserialize_with = "bool_or_string_bool"
    )]
    pub transient: Option<bool>,
    // Adapter-specific fields (BigQuery)
    #[serde(rename = "+cluster_by")]
    pub cluster_by: Option<BigqueryClusterConfig>,
    #[serde(rename = "+description")]
    pub description: Option<String>,
    #[serde(
        default,
        rename = "+enable_refresh",
        deserialize_with = "bool_or_string_bool"
    )]
    pub enable_refresh: Option<bool>,
    #[serde(rename = "+grant_access_to")]
    pub grant_access_to: Option<Vec<GrantAccessToTarget>>,
    #[serde(
        default,
        rename = "+hours_to_expiration",
        deserialize_with = "u64_or_string_u64"
    )]
    pub hours_to_expiration: Option<u64>,
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
    #[serde(rename = "+max_staleness")]
    pub max_staleness: Option<String>,
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
    // Adapter-specific fields (Databricks)
    #[serde(
        default,
        rename = "+auto_liquid_cluster",
        deserialize_with = "bool_or_string_bool"
    )]
    pub auto_liquid_cluster: Option<bool>,
    #[serde(rename = "+buckets")]
    pub buckets: Option<i64>,
    #[serde(rename = "+catalog")]
    pub catalog: Option<String>,
    #[serde(rename = "+clustered_by")]
    pub clustered_by: Option<String>,
    #[serde(rename = "+compression")]
    pub compression: Option<String>,
    #[serde(rename = "+databricks_compute")]
    pub databricks_compute: Option<String>,
    #[serde(rename = "+databricks_tags")]
    pub databricks_tags: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+file_format")]
    pub file_format: Option<String>,
    #[serde(
        default,
        rename = "+include_full_name_in_path",
        deserialize_with = "bool_or_string_bool"
    )]
    pub include_full_name_in_path: Option<bool>,
    #[serde(rename = "+liquid_clustered_by")]
    pub liquid_clustered_by: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+location_root")]
    pub location_root: Option<String>,
    #[serde(rename = "+matched_condition")]
    pub matched_condition: Option<String>,
    #[serde(
        default,
        rename = "+merge_with_schema_evolution",
        deserialize_with = "bool_or_string_bool"
    )]
    pub merge_with_schema_evolution: Option<bool>,
    #[serde(rename = "+not_matched_by_source_action")]
    pub not_matched_by_source_action: Option<String>,
    #[serde(rename = "+not_matched_by_source_condition")]
    pub not_matched_by_source_condition: Option<String>,
    #[serde(rename = "+not_matched_condition")]
    pub not_matched_condition: Option<String>,
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
    #[serde(rename = "+source_alias")]
    pub source_alias: Option<String>,
    #[serde(rename = "+target_alias")]
    pub target_alias: Option<String>,
    #[serde(rename = "+tblproperties")]
    pub tblproperties: Option<BTreeMap<String, YmlValue>>,
    // Adapter-specific fields (Redshift)
    #[serde(default, rename = "+bind", deserialize_with = "bool_or_string_bool")]
    pub bind: Option<bool>,
    #[serde(rename = "+dist")]
    pub dist: Option<String>,
    #[serde(rename = "+sort")]
    pub sort: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+sort_type")]
    pub sort_type: Option<String>,
    // Adapter-specific fields (MSSQL)
    #[serde(
        default,
        rename = "+as_columnstore",
        deserialize_with = "bool_or_string_bool"
    )]
    pub as_columnstore: Option<bool>,
    // Flattened field:
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectSnapshotConfig>>,
}

impl IterChildren<ProjectSnapshotConfig> for ProjectSnapshotConfig {
    fn iter_children(&self) -> Iter<String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, Default, PartialEq, Eq)]
pub struct SnapshotConfig {
    // Snapshot-specific Configuration
    pub database: Option<String>,
    pub schema: Option<String>,
    pub alias: Option<String>,
    pub materialized: Option<DbtMaterialization>,
    pub strategy: Option<String>,
    pub unique_key: Option<StringOrArrayOfStrings>,
    pub check_cols: Option<StringOrArrayOfStrings>,
    pub updated_at: Option<String>,
    pub dbt_valid_to_current: Option<String>,
    pub snapshot_meta_column_names: Option<SnapshotMetaColumnNames>,
    pub hard_deletes: Option<HardDeletes>,
    // General Configuration
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub pre_hook: Verbatim<Option<Hooks>>,
    pub post_hook: Verbatim<Option<Hooks>>,
    pub persist_docs: Option<PersistDocsConfig>,
    pub grants: Option<BTreeMap<String, StringOrArrayOfStrings>>,
    pub event_time: Option<String>,
    pub quoting: Option<DbtQuoting>,
    pub static_analysis: Option<StaticAnalysisKind>,
    pub meta: Option<BTreeMap<String, YmlValue>>,
    pub group: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub quote_columns: Option<bool>,
    pub invalidate_hard_deletes: Option<bool>,
    pub docs: Option<DocsConfig>,
    // Adapter specific configs
    pub __snowflake_node_config__: SnowflakeNodeConfig,
    pub __bigquery_node_config__: BigQueryNodeConfig,
    pub __databricks_node_config__: DatabricksNodeConfig,
    pub __redshift_node_config__: RedshiftNodeConfig,
    pub __mssql_node_config__: MsSqlNodeConfig,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
pub struct SnapshotMetaColumnNames {
    #[serde(default = "default_dbt_scd_id")]
    pub dbt_scd_id: Option<String>,
    #[serde(default = "default_dbt_updated_at")]
    pub dbt_updated_at: Option<String>,
    #[serde(default = "default_dbt_valid_from")]
    pub dbt_valid_from: Option<String>,
    #[serde(default = "default_dbt_valid_to")]
    pub dbt_valid_to: Option<String>,
    #[serde(default = "default_dbt_is_deleted")]
    pub dbt_is_deleted: Option<String>,
}

impl Default for SnapshotMetaColumnNames {
    fn default() -> Self {
        Self {
            dbt_scd_id: default_dbt_scd_id(),
            dbt_updated_at: default_dbt_updated_at(),
            dbt_valid_from: default_dbt_valid_from(),
            dbt_valid_to: default_dbt_valid_to(),
            dbt_is_deleted: default_dbt_is_deleted(),
        }
    }
}

fn default_dbt_scd_id() -> Option<String> {
    Some("DBT_SCD_ID".to_string())
}

fn default_dbt_updated_at() -> Option<String> {
    Some("DBT_UPDATED_AT".to_string())
}

fn default_dbt_valid_from() -> Option<String> {
    Some("DBT_VALID_FROM".to_string())
}

fn default_dbt_valid_to() -> Option<String> {
    Some("DBT_VALID_TO".to_string())
}

fn default_dbt_is_deleted() -> Option<String> {
    Some("DBT_IS_DELETED".to_string())
}

impl From<ProjectSnapshotConfig> for SnapshotConfig {
    fn from(config: ProjectSnapshotConfig) -> Self {
        Self {
            database: config.database,
            schema: config.schema,
            alias: config.alias,
            materialized: config.materialized,
            strategy: config.strategy,
            unique_key: config.unique_key,
            check_cols: config.check_cols,
            updated_at: config.updated_at,
            dbt_valid_to_current: config.dbt_valid_to_current,
            snapshot_meta_column_names: config.snapshot_meta_column_names,
            hard_deletes: config.hard_deletes,
            enabled: config.enabled,
            tags: config.tags,
            pre_hook: config.pre_hook,
            post_hook: config.post_hook,
            persist_docs: config.persist_docs,
            grants: config.grants,
            event_time: config.event_time,
            quoting: config.quoting,
            static_analysis: config.static_analysis,
            meta: config.meta,
            group: config.group,
            quote_columns: config.quote_columns,
            invalidate_hard_deletes: config.invalidate_hard_deletes,
            docs: config.docs,
            __snowflake_node_config__: SnowflakeNodeConfig {
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
            __bigquery_node_config__: BigQueryNodeConfig {
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
            __databricks_node_config__: DatabricksNodeConfig {
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
            __redshift_node_config__: RedshiftNodeConfig {
                auto_refresh: config.auto_refresh,
                backup: config.backup,
                bind: config.bind,
                dist: config.dist,
                sort: config.sort,
                sort_type: config.sort_type,
            },
            __mssql_node_config__: MsSqlNodeConfig {
                as_columnstore: config.as_columnstore,
            },
        }
    }
}

impl From<SnapshotConfig> for ProjectSnapshotConfig {
    fn from(config: SnapshotConfig) -> Self {
        Self {
            database: config.database,
            schema: config.schema,
            alias: config.alias,
            materialized: config.materialized,
            strategy: config.strategy,
            unique_key: config.unique_key,
            check_cols: config.check_cols,
            updated_at: config.updated_at,
            dbt_valid_to_current: config.dbt_valid_to_current,
            snapshot_meta_column_names: config.snapshot_meta_column_names,
            hard_deletes: config.hard_deletes,
            enabled: config.enabled,
            tags: config.tags,
            pre_hook: config.pre_hook,
            post_hook: config.post_hook,
            persist_docs: config.persist_docs,
            grants: config.grants,
            event_time: config.event_time,
            quoting: config.quoting,
            static_analysis: config.static_analysis,
            meta: config.meta,
            group: config.group,
            quote_columns: config.quote_columns,
            invalidate_hard_deletes: config.invalidate_hard_deletes,
            docs: config.docs,
            // Snowflake fields
            external_volume: config.__snowflake_node_config__.external_volume,
            base_location_root: config.__snowflake_node_config__.base_location_root,
            base_location_subpath: config.__snowflake_node_config__.base_location_subpath,
            target_lag: config.__snowflake_node_config__.target_lag,
            snowflake_warehouse: config.__snowflake_node_config__.snowflake_warehouse,
            refresh_mode: config.__snowflake_node_config__.refresh_mode,
            initialize: config.__snowflake_node_config__.initialize,
            tmp_relation_type: config.__snowflake_node_config__.tmp_relation_type,
            query_tag: config.__snowflake_node_config__.query_tag,
            automatic_clustering: config.__snowflake_node_config__.automatic_clustering,
            copy_grants: config.__snowflake_node_config__.copy_grants,
            secure: config.__snowflake_node_config__.secure,
            // BigQuery fields
            partition_by: config.__bigquery_node_config__.partition_by,
            cluster_by: config.__bigquery_node_config__.cluster_by,
            hours_to_expiration: config.__bigquery_node_config__.hours_to_expiration,
            labels: config.__bigquery_node_config__.labels,
            labels_from_meta: config.__bigquery_node_config__.labels_from_meta,
            kms_key_name: config.__bigquery_node_config__.kms_key_name,
            require_partition_filter: config.__bigquery_node_config__.require_partition_filter,
            partition_expiration_days: config.__bigquery_node_config__.partition_expiration_days,
            grant_access_to: config.__bigquery_node_config__.grant_access_to,
            partitions: config.__bigquery_node_config__.partitions,
            enable_refresh: config.__bigquery_node_config__.enable_refresh,
            refresh_interval_minutes: config.__bigquery_node_config__.refresh_interval_minutes,
            description: config.__bigquery_node_config__.description,
            max_staleness: config.__bigquery_node_config__.max_staleness,
            // Databricks fields
            file_format: config.__databricks_node_config__.file_format,
            location_root: config.__databricks_node_config__.location_root,
            tblproperties: config.__databricks_node_config__.tblproperties,
            include_full_name_in_path: config.__databricks_node_config__.include_full_name_in_path,
            liquid_clustered_by: config.__databricks_node_config__.liquid_clustered_by,
            auto_liquid_cluster: config.__databricks_node_config__.auto_liquid_cluster,
            clustered_by: config.__databricks_node_config__.clustered_by,
            buckets: config.__databricks_node_config__.buckets,
            catalog: config.__databricks_node_config__.catalog,
            databricks_tags: config.__databricks_node_config__.databricks_tags,
            compression: config.__databricks_node_config__.compression,
            databricks_compute: config.__databricks_node_config__.databricks_compute,
            matched_condition: config.__databricks_node_config__.matched_condition,
            merge_with_schema_evolution: config
                .__databricks_node_config__
                .merge_with_schema_evolution,
            not_matched_by_source_action: config
                .__databricks_node_config__
                .not_matched_by_source_action,
            not_matched_by_source_condition: config
                .__databricks_node_config__
                .not_matched_by_source_condition,
            not_matched_condition: config.__databricks_node_config__.not_matched_condition,
            source_alias: config.__databricks_node_config__.source_alias,
            target_alias: config.__databricks_node_config__.target_alias,
            skip_matched_step: config.__databricks_node_config__.skip_matched_step,
            skip_not_matched_step: config.__databricks_node_config__.skip_not_matched_step,
            // Redshift fields
            auto_refresh: config.__redshift_node_config__.auto_refresh,
            backup: config.__redshift_node_config__.backup,
            bind: config.__redshift_node_config__.bind,
            dist: config.__redshift_node_config__.dist,
            sort: config.__redshift_node_config__.sort,
            sort_type: config.__redshift_node_config__.sort_type,
            transient: config.__snowflake_node_config__.transient,
            // MSSQL fields
            as_columnstore: config.__mssql_node_config__.as_columnstore,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl DefaultTo<SnapshotConfig> for SnapshotConfig {
    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn get_pre_hook(&self) -> Option<&Hooks> {
        (*self.pre_hook).as_ref()
    }

    fn get_post_hook(&self) -> Option<&Hooks> {
        (*self.post_hook).as_ref()
    }

    fn default_to(&mut self, parent: &SnapshotConfig) {
        let SnapshotConfig {
            database,
            schema,
            alias,
            materialized,
            strategy,
            unique_key,
            check_cols,
            updated_at,
            dbt_valid_to_current,
            snapshot_meta_column_names,
            hard_deletes,
            enabled,
            tags,
            pre_hook,
            post_hook,
            persist_docs,
            grants,
            event_time,
            quoting,
            meta,
            group,
            quote_columns,
            invalidate_hard_deletes,
            docs,
            static_analysis,
            // Flattened configs
            __snowflake_node_config__: snowflake_model_config,
            __bigquery_node_config__: bigquery_model_config,
            __databricks_node_config__: databricks_model_config,
            __redshift_node_config__: redshift_model_config,
            __mssql_node_config__: mssql_model_config,
        } = self;

        // Handle flattened configs
        #[allow(unused, clippy::let_unit_value)]
        let snowflake_model_config =
            snowflake_model_config.default_to(&parent.__snowflake_node_config__);
        #[allow(unused, clippy::let_unit_value)]
        let bigquery_model_config =
            bigquery_model_config.default_to(&parent.__bigquery_node_config__);
        #[allow(unused, clippy::let_unit_value)]
        let databricks_model_config =
            databricks_model_config.default_to(&parent.__databricks_node_config__);
        #[allow(unused, clippy::let_unit_value)]
        let redshift_model_config =
            redshift_model_config.default_to(&parent.__redshift_node_config__);
        #[allow(unused, clippy::let_unit_value)]
        let mssql_model_config = mssql_model_config.default_to(&parent.__mssql_node_config__);

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
        let grants = default_to_grants(grants, &parent.grants);

        // Use the improved default_to macro for simple fields
        default_to!(
            parent,
            [
                enabled,
                alias,
                schema,
                database,
                materialized,
                group,
                persist_docs,
                unique_key,
                docs,
                event_time,
                quote_columns,
                invalidate_hard_deletes,
                strategy,
                updated_at,
                dbt_valid_to_current,
                snapshot_meta_column_names,
                hard_deletes,
                check_cols,
                static_analysis,
            ]
        );
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
}
