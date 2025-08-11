use dbt_common::io_args::StaticAnalysisKind;
use dbt_serde_yaml::{JsonSchema, ShouldBe};
use serde::{Deserialize, Serialize};
// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;
use serde_with::skip_serializing_none;
use std::collections::{BTreeMap, btree_map::Iter};

use crate::{
    default_to,
    schemas::{
        manifest::GrantAccessToTarget,
        manifest::{BigqueryClusterConfig, BigqueryPartitionConfigLegacy},
        project::{
            DefaultTo, IterChildren, configs::common::BigQueryNodeConfig,
            configs::common::DatabricksNodeConfig, configs::common::RedshiftNodeConfig,
            configs::common::SnowflakeNodeConfig, configs::common::default_meta_and_tags,
        },
        serde::{StringOrArrayOfStrings, bool_or_string_bool, u64_or_string_u64},
    },
};

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ProjectUnitTestConfig {
    #[serde(default, rename = "+enabled", deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    #[serde(rename = "+meta")]
    pub meta: Option<BTreeMap<String, YmlValue>>,
    #[serde(rename = "+tags")]
    pub tags: Option<StringOrArrayOfStrings>,
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

    // Flattened fields
    pub __additional_properties__: BTreeMap<String, ShouldBe<ProjectUnitTestConfig>>,
}

impl IterChildren<ProjectUnitTestConfig> for ProjectUnitTestConfig {
    fn iter_children(&self) -> Iter<String, ShouldBe<Self>> {
        self.__additional_properties__.iter()
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema)]
pub struct UnitTestConfig {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enabled: Option<bool>,
    pub static_analysis: Option<StaticAnalysisKind>,
    pub meta: Option<BTreeMap<String, YmlValue>>,
    pub tags: Option<StringOrArrayOfStrings>,
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

impl From<ProjectUnitTestConfig> for UnitTestConfig {
    fn from(config: ProjectUnitTestConfig) -> Self {
        Self {
            enabled: config.enabled,
            static_analysis: config.static_analysis,
            meta: config.meta,
            tags: config.tags,
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

impl From<UnitTestConfig> for ProjectUnitTestConfig {
    fn from(config: UnitTestConfig) -> Self {
        Self {
            enabled: config.enabled,
            static_analysis: config.static_analysis,
            meta: config.meta,
            tags: config.tags,
            // Snowflake fields
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
            transient: config.snowflake_node_config.transient,
            // BigQuery fields
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
            // Databricks fields
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
            target_alias: config.databricks_node_config.target_alias,
            source_alias: config.databricks_node_config.source_alias,
            matched_condition: config.databricks_node_config.matched_condition,
            not_matched_condition: config.databricks_node_config.not_matched_condition,
            not_matched_by_source_condition: config
                .databricks_node_config
                .not_matched_by_source_condition,
            not_matched_by_source_action: config
                .databricks_node_config
                .not_matched_by_source_action,
            merge_with_schema_evolution: config.databricks_node_config.merge_with_schema_evolution,
            skip_matched_step: config.databricks_node_config.skip_matched_step,
            skip_not_matched_step: config.databricks_node_config.skip_not_matched_step,
            // Redshift fields
            auto_refresh: config.redshift_node_config.auto_refresh,
            backup: config.redshift_node_config.backup,
            bind: config.redshift_node_config.bind,
            dist: config.redshift_node_config.dist,
            sort: config.redshift_node_config.sort,
            sort_type: config.redshift_node_config.sort_type,
            __additional_properties__: BTreeMap::new(),
        }
    }
}

impl DefaultTo<UnitTestConfig> for UnitTestConfig {
    fn get_enabled(&self) -> Option<bool> {
        self.enabled
    }

    fn default_to(&mut self, parent: &UnitTestConfig) {
        let UnitTestConfig {
            enabled,
            static_analysis,
            meta,
            tags,
            snowflake_node_config: snowflake_config,
            bigquery_node_config: bigquery_config,
            databricks_node_config: databricks_config,
            redshift_node_config: redshift_config,
        } = self;

        // Handle adapter-specific configs
        #[allow(unused, clippy::let_unit_value)]
        let snowflake_config = snowflake_config.default_to(&parent.snowflake_node_config);
        #[allow(unused, clippy::let_unit_value)]
        let bigquery_config = bigquery_config.default_to(&parent.bigquery_node_config);
        #[allow(unused, clippy::let_unit_value)]
        let databricks_config = databricks_config.default_to(&parent.databricks_node_config);
        #[allow(unused, clippy::let_unit_value)]
        let redshift_config = redshift_config.default_to(&parent.redshift_node_config);

        #[allow(unused, clippy::let_unit_value)]
        let meta = default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        #[allow(unused, clippy::let_unit_value)]
        let tags = ();

        default_to!(parent, [enabled, static_analysis]);
    }
}
