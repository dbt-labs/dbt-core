use dbt_common::io_args::StaticAnalysisKind;
use dbt_serde_yaml::{JsonSchema, ShouldBe};
use serde::{Deserialize, Serialize};
// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;
use std::collections::btree_map::Iter;

use crate::default_to;
use crate::schemas::common::{DbtQuoting, FreshnessDefinition};
use crate::schemas::manifest::GrantAccessToTarget;
use crate::schemas::manifest::{BigqueryClusterConfig, BigqueryPartitionConfigLegacy};
use crate::schemas::project::configs::common::DatabricksNodeConfig;
use crate::schemas::project::configs::common::RedshiftNodeConfig;
use crate::schemas::project::configs::common::SnowflakeNodeConfig;
use crate::schemas::project::configs::common::{BigQueryNodeConfig, MsSqlNodeConfig};
use crate::schemas::project::configs::common::{default_meta_and_tags, default_quoting};
use crate::schemas::project::{DefaultTo, IterChildren};
use crate::schemas::serde::{StringOrArrayOfStrings, bool_or_string_bool, u64_or_string_u64};

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectSourceConfig {
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+event_time")]
    pub event_time: Option<String>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+freshness")]
    pub freshness: Option<FreshnessDefinition>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+quoting")]
    pub quoting: Option<DbtQuoting>,
    #[serde(rename = "+loaded_at_query")]
    pub loaded_at_query: Option<String>,
    #[serde(rename = "+loaded_at_field")]
    pub loaded_at_field: Option<String>,
    #[serde(rename = "+static_analysis")]
    pub static_analysis: Option<StaticAnalysisKind>,

    // Snowflake specific fields
    #[serde(rename = "+external_volume")]
    pub external_volume: Option<String>,
    #[serde(rename = "+base_location_root")]
    pub base_location_root: Option<String>,
    #[serde(rename = "+base_location_subpath")]
    pub base_location_subpath: Option<String>,
    #[serde(rename = "+target_lag")]
    pub target_lag: Option<String>,
    #[serde(rename = "+snowflake_warehouse")]
    pub snowflake_warehouse: Option<String>,
    #[serde(rename = "+refresh_mode")]
    pub refresh_mode: Option<String>,
    #[serde(rename = "+initialize")]
    pub initialize: Option<String>,
    #[serde(rename = "+tmp_relation_type")]
    pub tmp_relation_type: Option<String>,
    #[serde(rename = "+query_tag")]
    pub query_tag: Option<String>,
    #[serde(
        default,
        rename = "+automatic_clustering",
        deserialize_with = "bool_or_string_bool"
    )]
    pub automatic_clustering: Option<bool>,
    #[serde(
        default,
        rename = "+copy_grants",
        deserialize_with = "bool_or_string_bool"
    )]
    pub copy_grants: Option<bool>,
    #[serde(default, rename = "+secure", deserialize_with = "bool_or_string_bool")]
    pub secure: Option<bool>,
    #[serde(
        default,
        rename = "+transient",
        deserialize_with = "bool_or_string_bool"
    )]
    pub transient: Option<bool>,

    // BigQuery specific fields
    #[serde(rename = "+partition_by")]
    pub partition_by: Option<BigqueryPartitionConfigLegacy>,
    #[serde(rename = "+cluster_by")]
    pub cluster_by: Option<BigqueryClusterConfig>,
    #[serde(
        default,
        rename = "+hours_to_expiration",
        deserialize_with = "u64_or_string_u64"
    )]
    pub hours_to_expiration: Option<u64>,
    #[serde(rename = "+labels")]
    pub labels: Option<BTreeMap<String, String>>,
    #[serde(
        default,
        rename = "+labels_from_meta",
        deserialize_with = "bool_or_string_bool"
    )]
    pub labels_from_meta: Option<bool>,
    #[serde(rename = "+kms_key_name")]
    pub kms_key_name: Option<String>,
    #[serde(
        default,
        rename = "+require_partition_filter",
        deserialize_with = "bool_or_string_bool"
    )]
    pub require_partition_filter: Option<bool>,
    #[serde(
        default,
        rename = "+partition_expiration_days",
        deserialize_with = "u64_or_string_u64"
    )]
    pub partition_expiration_days: Option<u64>,
    #[serde(rename = "+grant_access_to")]
    pub grant_access_to: Option<Vec<GrantAccessToTarget>>,
    #[serde(rename = "+partitions")]
    pub partitions: Option<Vec<String>>,
    #[serde(
        default,
        rename = "+enable_refresh",
        deserialize_with = "bool_or_string_bool"
    )]
    pub enable_refresh: Option<bool>,
    #[serde(
        default,
        rename = "+refresh_interval_minutes",
        deserialize_with = "u64_or_string_u64"
    )]
    pub refresh_interval_minutes: Option<u64>,
    #[serde(rename = "+description")]
    pub description: Option<String>,
    #[serde(rename = "+max_staleness")]
    pub max_staleness: Option<String>,

    // Databricks specific fields
    #[serde(rename = "+file_format")]
    pub file_format: Option<String>,
    #[serde(rename = "+location_root")]
    pub location_root: Option<String>,
    #[serde(rename = "+tblproperties")]
    pub tblproperties: Option<BTreeMap<String, YmlValue>>,
    #[serde(
        default,
        rename = "+include_full_name_in_path",
        deserialize_with = "bool_or_string_bool"
    )]
    pub include_full_name_in_path: Option<bool>,
    #[serde(rename = "+liquid_clustered_by")]
    pub liquid_clustered_by: Option<StringOrArrayOfStrings>,
    #[serde(
        default,
        rename = "+auto_liquid_cluster",
        deserialize_with = "bool_or_string_bool"
    )]
    pub auto_liquid_cluster: Option<bool>,
    #[serde(rename = "+clustered_by")]
    pub clustered_by: Option<String>,
    #[serde(rename = "+buckets")]
    pub buckets: Option<i64>,
    #[serde(rename = "+catalog")]
    pub catalog: Option<String>,
    #[serde(rename = "+databricks_tags")]
    pub databricks_tags: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+compression")]
    pub compression: Option<String>,
    #[serde(rename = "+databricks_compute")]
    pub databricks_compute: Option<String>,
    #[serde(rename = "+target_alias")]
    pub target_alias: Option<String>,
    #[serde(rename = "+source_alias")]
    pub source_alias: Option<String>,
    #[serde(rename = "+matched_condition")]
    pub matched_condition: Option<String>,
    #[serde(rename = "+not_matched_condition")]
    pub not_matched_condition: Option<String>,
    #[serde(rename = "+not_matched_by_source_condition")]
    pub not_matched_by_source_condition: Option<String>,
    #[serde(rename = "+not_matched_by_source_action")]
    pub not_matched_by_source_action: Option<String>,
    #[serde(
        default,
        rename = "+merge_with_schema_evolution",
        deserialize_with = "bool_or_string_bool"
    )]
    pub merge_with_schema_evolution: Option<bool>,
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

    // Redshift specific fields
    #[serde(
        default,
        rename = "+auto_refresh",
        deserialize_with = "bool_or_string_bool"
    )]
    pub auto_refresh: Option<bool>,
    #[serde(default, rename = "+backup", deserialize_with = "bool_or_string_bool")]
    pub backup: Option<bool>,
    #[serde(default, rename = "+bind", deserialize_with = "bool_or_string_bool")]
    pub bind: Option<bool>,
    #[serde(rename = "+dist")]
    pub dist: Option<String>,
    #[serde(rename = "+sort")]
    pub sort: Option<StringOrArrayOfStrings>,
    #[serde(rename = "+sort_type")]
    pub sort_type: Option<String>,

    // MSSQL specific fields
    #[serde(
        default,
        rename = "+as_columnstore",
        deserialize_with = "bool_or_string_bool"
    )]
    pub as_columnstore: Option<bool>,

    // Flattened fields
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectSourceConfig>>,
}

impl IterChildren<ProjectSourceConfig> for ProjectSourceConfig {
    fn iter_children(&self) -> Iter<String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema)]
pub struct SourceConfig {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub event_time: Option<String>,
    pub meta: Option<BTreeMap<String, YmlValue>>,
    pub freshness: Option<FreshnessDefinition>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub quoting: Option<DbtQuoting>,
    pub loaded_at_field: Option<String>,
    pub loaded_at_query: Option<String>,
    pub static_analysis: Option<StaticAnalysisKind>,
    // Adapter specific configs
    pub __snowflake_node_config__: SnowflakeNodeConfig,
    pub __bigquery_node_config__: BigQueryNodeConfig,
    pub __databricks_node_config__: DatabricksNodeConfig,
    pub __redshift_node_config__: RedshiftNodeConfig,
    pub __mssql_node_config__: MsSqlNodeConfig,
}

impl From<ProjectSourceConfig> for SourceConfig {
    fn from(config: ProjectSourceConfig) -> Self {
        Self {
            enabled: config.enabled,
            event_time: config.event_time,
            meta: config.meta,
            freshness: config.freshness,
            tags: config.tags,
            quoting: config.quoting,
            loaded_at_field: config.loaded_at_field,
            loaded_at_query: config.loaded_at_query,
            static_analysis: config.static_analysis,
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

impl From<SourceConfig> for ProjectSourceConfig {
    fn from(config: SourceConfig) -> Self {
        Self {
            enabled: config.enabled,
            event_time: config.event_time,
            meta: config.meta,
            freshness: config.freshness,
            tags: config.tags,
            quoting: config.quoting,
            loaded_at_field: config.loaded_at_field,
            loaded_at_query: config.loaded_at_query,
            static_analysis: config.static_analysis,
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
            transient: config.__snowflake_node_config__.transient,
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
            target_alias: config.__databricks_node_config__.target_alias,
            source_alias: config.__databricks_node_config__.source_alias,
            matched_condition: config.__databricks_node_config__.matched_condition,
            not_matched_condition: config.__databricks_node_config__.not_matched_condition,
            not_matched_by_source_condition: config
                .__databricks_node_config__
                .not_matched_by_source_condition,
            not_matched_by_source_action: config
                .__databricks_node_config__
                .not_matched_by_source_action,
            merge_with_schema_evolution: config
                .__databricks_node_config__
                .merge_with_schema_evolution,
            skip_matched_step: config.__databricks_node_config__.skip_matched_step,
            skip_not_matched_step: config.__databricks_node_config__.skip_not_matched_step,
            // Redshift fields
            auto_refresh: config.__redshift_node_config__.auto_refresh,
            backup: config.__redshift_node_config__.backup,
            bind: config.__redshift_node_config__.bind,
            dist: config.__redshift_node_config__.dist,
            sort: config.__redshift_node_config__.sort,
            sort_type: config.__redshift_node_config__.sort_type,
            // MSSQL fields
            as_columnstore: config.__mssql_node_config__.as_columnstore,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl DefaultTo<SourceConfig> for SourceConfig {
    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn default_to(&mut self, parent: &SourceConfig) {
        let SourceConfig {
            enabled,
            event_time,
            meta,
            freshness,
            tags,
            quoting,
            loaded_at_field,
            loaded_at_query,
            static_analysis,
            __snowflake_node_config__: snowflake_source_config,
            __bigquery_node_config__: bigquery_source_config,
            __databricks_node_config__: databricks_source_config,
            __redshift_node_config__: redshift_source_config,
            __mssql_node_config__: mssql_source_config,
        } = self;

        // Handle flattened configs
        #[allow(unused, clippy::let_unit_value)]
        let snowflake_source_config =
            snowflake_source_config.default_to(&parent.__snowflake_node_config__);
        #[allow(unused, clippy::let_unit_value)]
        let bigquery_source_config =
            bigquery_source_config.default_to(&parent.__bigquery_node_config__);
        #[allow(unused, clippy::let_unit_value)]
        let databricks_source_config =
            databricks_source_config.default_to(&parent.__databricks_node_config__);
        #[allow(unused, clippy::let_unit_value)]
        let redshift_source_config =
            redshift_source_config.default_to(&parent.__redshift_node_config__);
        #[allow(unused, clippy::let_unit_value)]
        let mssql_source_config = mssql_source_config.default_to(&parent.__mssql_node_config__);

        #[allow(unused, clippy::let_unit_value)]
        let quoting = default_quoting(quoting, &parent.quoting);
        #[allow(unused, clippy::let_unit_value)]
        let meta = default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        #[allow(unused, clippy::let_unit_value)]
        let tags = ();

        default_to!(
            parent,
            [
                enabled,
                event_time,
                freshness,
                loaded_at_field,
                loaded_at_query,
                static_analysis
            ]
        );
    }
}
