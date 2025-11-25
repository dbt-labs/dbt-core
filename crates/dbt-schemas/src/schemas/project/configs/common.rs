use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Spanned;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use crate::default_to;
use crate::schemas::common::Hooks;
use crate::schemas::common::merge_meta;
use crate::schemas::common::merge_tags;
use crate::schemas::common::{DbtQuoting, DocsConfig, ScheduleConfig};
use crate::schemas::manifest::GrantAccessToTarget;
use crate::schemas::manifest::postgres::PostgresIndex;
use crate::schemas::manifest::{BigqueryClusterConfig, PartitionConfig};
use crate::schemas::project::configs::model_config::DataLakeObjectCategory;
use crate::schemas::project::dbt_project::DefaultTo;
use crate::schemas::serde::StringOrArrayOfStrings;
use crate::schemas::serde::{bool_or_string_bool, f64_or_string_f64, u64_or_string_u64};

/// Helper function to handle default_to logic for hooks (pre_hook/post_hook)
/// Hooks should be extended, not replaced when merging configs
pub fn default_hooks(
    child_hooks: &mut Verbatim<Option<Hooks>>,
    parent_hooks: &Verbatim<Option<Hooks>>,
) {
    if let Some(parent_hooks) = &**parent_hooks {
        if let Some(child_hooks) = &mut **child_hooks {
            child_hooks.extend(parent_hooks);
        } else {
            *child_hooks = Verbatim::from(Some(parent_hooks.clone()));
        }
    }
}

/// Helper function to handle default_to logic for quoting configs
/// Quoting has its own default_to method that should be called
pub fn default_quoting(
    child_quoting: &mut Option<DbtQuoting>,
    parent_quoting: &Option<DbtQuoting>,
) {
    if let Some(quoting) = child_quoting {
        if let Some(parent_quoting) = parent_quoting {
            quoting.default_to(parent_quoting);
        }
    } else {
        *child_quoting = *parent_quoting;
    }
}

/// Helper function to handle default_to logic for meta and tags
/// Uses the existing merge functions for proper merging behavior
pub fn default_meta_and_tags(
    child_meta: &mut Option<BTreeMap<String, YmlValue>>,
    parent_meta: &Option<BTreeMap<String, YmlValue>>,
    child_tags: &mut Option<StringOrArrayOfStrings>,
    parent_tags: &Option<StringOrArrayOfStrings>,
) {
    // Handle meta using existing merge function
    *child_meta = merge_meta(parent_meta.clone(), child_meta.take());

    // Handle tags using existing merge function
    let child_tags_vec = child_tags.take().map(|tags| tags.into());
    let parent_tags_vec = parent_tags.clone().map(|tags| tags.into());
    *child_tags =
        merge_tags(child_tags_vec, parent_tags_vec).map(StringOrArrayOfStrings::ArrayOfStrings);
}

/// Helper function to handle default_to logic for column_types
/// Column types should be merged, with parent values filling in missing keys
pub fn default_column_types(
    child_column_types: &mut Option<BTreeMap<Spanned<String>, String>>,
    parent_column_types: &Option<BTreeMap<Spanned<String>, String>>,
) {
    match (child_column_types, parent_column_types) {
        (Some(inner_column_types), Some(parent_column_types)) => {
            for (key, value) in parent_column_types {
                inner_column_types
                    .entry(key.clone())
                    .or_insert_with(|| value.clone());
            }
        }
        (column_types, Some(parent_column_types)) => {
            *column_types = Some(parent_column_types.clone())
        }
        (_, None) => {}
    }
}

/// helper function to handle default_to for grants
/// if the key of a grant starts with a + append the child grant to the parents, otherwise replace the parent grant
pub fn default_to_grants(
    child_grants: &mut Option<BTreeMap<String, StringOrArrayOfStrings>>,
    parent_grants: &Option<BTreeMap<String, StringOrArrayOfStrings>>,
) {
    match (child_grants, parent_grants) {
        (Some(child_grants_map), Some(parent_grants_map)) => {
            // Collect keys that need to be processed to avoid borrow conflicts
            let keys_to_process: Vec<String> = child_grants_map
                .keys()
                .filter(|key| key.starts_with('+'))
                .cloned()
                .collect();

            // Process each + prefixed key
            // Can you ever have more than one key in a grant?
            // TODO: Validate above assumption
            for child_key in keys_to_process {
                // Remove the + prefix to get the actual key
                let actual_key = child_key.trim_start_matches('+');

                // Get the value and remove the + prefixed key
                if let Some(value) = child_grants_map.remove(&child_key) {
                    // Append parent value to child value if parent has this key
                    if let Some(parent_value) = parent_grants_map.get(actual_key) {
                        let mut child_array: Vec<String> = value.clone().into();
                        let parent_array: Vec<String> = parent_value.clone().into();

                        child_array.extend(parent_array.iter().cloned());
                        child_grants_map.insert(
                            actual_key.to_string(),
                            StringOrArrayOfStrings::ArrayOfStrings(child_array),
                        );
                    } else {
                        // If parent doesn't have this key, just insert the child value
                        child_grants_map.insert(actual_key.to_string(), value);
                    }
                }
            }
        }
        // no child, set child to parent
        (child_grants, Some(parent_grants_map)) => {
            // If only parent exists, set child to parent
            *child_grants = Some(parent_grants_map.clone());
        }
        (Some(child_grants_map), None) => {
            // Parent doesn't exist but child does - still need to strip + prefixes
            let keys_to_process: Vec<String> = child_grants_map
                .keys()
                .filter(|key| key.starts_with('+'))
                .cloned()
                .collect();

            for child_key in keys_to_process {
                // Remove the + prefix to get the actual key
                let actual_key = child_key.trim_start_matches('+');

                // Get the value and remove the + prefixed key
                if let Some(value) = child_grants_map.remove(&child_key) {
                    // No parent to merge with, just insert the child value with stripped prefix
                    child_grants_map.insert(actual_key.to_string(), value);
                }
            }
        }
        (None, None) => {
            // Neither child nor parent exists, nothing to do
        }
    }
}

/// This configuration is a superset of all warehouse specific configurations
/// that users can set
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
pub struct WarehouseSpecificNodeConfig {
    // Shared
    pub partition_by: Option<PartitionConfig>,
    pub adapter_properties: Option<BTreeMap<String, YmlValue>>,

    // BigQuery
    pub description: Option<String>,
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
    pub partitions: Option<Vec<String>>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enable_refresh: Option<bool>,
    #[serde(default, deserialize_with = "f64_or_string_f64")]
    pub refresh_interval_minutes: Option<f64>,
    pub resource_tags: Option<BTreeMap<String, String>>,
    pub max_staleness: Option<String>,
    pub jar_file_uri: Option<String>,
    pub timeout: Option<u64>,
    pub batch_id: Option<String>,
    pub dataproc_cluster_name: Option<String>,

    // Used by both Databricks and Bigquery
    pub file_format: Option<String>,

    // Databricks
    pub catalog_name: Option<String>,
    pub location_root: Option<String>,
    pub tblproperties: Option<BTreeMap<String, YmlValue>>,
    // this config is introduced here https://github.com/databricks/dbt-databricks/pull/823
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub include_full_name_in_path: Option<bool>,
    pub liquid_clustered_by: Option<StringOrArrayOfStrings>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub auto_liquid_cluster: Option<bool>,
    pub clustered_by: Option<String>,
    pub buckets: Option<i64>,
    pub catalog: Option<String>,
    pub databricks_tags: Option<BTreeMap<String, YmlValue>>,
    pub compression: Option<String>,
    pub databricks_compute: Option<String>,
    pub target_alias: Option<String>,
    pub source_alias: Option<String>,
    pub matched_condition: Option<String>,
    pub not_matched_condition: Option<String>,
    pub not_matched_by_source_condition: Option<String>,
    pub not_matched_by_source_action: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub merge_with_schema_evolution: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub skip_matched_step: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub skip_not_matched_step: Option<bool>,
    pub schedule: Option<ScheduleConfig>,

    // Snowflake
    pub table_tag: Option<String>,
    pub row_access_policy: Option<String>,
    pub external_volume: Option<String>,
    pub base_location_root: Option<String>,
    pub base_location_subpath: Option<String>,
    pub target_lag: Option<String>,
    pub snowflake_warehouse: Option<String>,
    pub refresh_mode: Option<String>,
    pub initialize: Option<String>,
    pub tmp_relation_type: Option<String>,
    pub query_tag: Option<String>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub automatic_clustering: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub copy_grants: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub secure: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub transient: Option<bool>,

    // Redshift
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub auto_refresh: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub backup: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub bind: Option<bool>,
    pub dist: Option<String>,
    pub sort: Option<StringOrArrayOfStrings>,
    pub sort_type: Option<String>,

    // MsSql
    // XXX: This is an incomplete set of configs
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub as_columnstore: Option<bool>,

    // Athena
    // XXX: This is an incomplete set of configs
    pub table_type: Option<String>,

    // Postgres
    // XXX: This is an incomplete set of configs
    pub indexes: Option<Vec<PostgresIndex>>,

    // Salesforce
    pub primary_key: Option<String>,
    pub category: Option<DataLakeObjectCategory>,
}

impl DefaultTo<WarehouseSpecificNodeConfig> for WarehouseSpecificNodeConfig {
    #[allow(clippy::cognitive_complexity)]
    fn default_to(&mut self, parent: &WarehouseSpecificNodeConfig) {
        // Exhaustive destructuring ensures all fields are handled
        let WarehouseSpecificNodeConfig {
            // Shared
            partition_by,

            // BigQuery
            description,
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
            resource_tags,
            max_staleness,
            jar_file_uri,
            timeout,
            batch_id,
            dataproc_cluster_name,

            // Databricks
            file_format,
            catalog_name,
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
            schedule,

            // Snowflake
            adapter_properties,
            table_tag,
            row_access_policy,
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
            secure,
            transient,

            // Redshift
            auto_refresh,
            backup,
            bind,
            dist,
            sort,
            sort_type,

            // MsSql
            as_columnstore,

            // Athena
            table_type,

            // Postgres
            indexes,

            // Salesforce
            primary_key,
            category,
        } = self;

        default_to!(
            parent,
            [
                // Shared
                partition_by,
                // BigQuery
                description,
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
                resource_tags,
                max_staleness,
                // Databricks
                file_format,
                catalog_name,
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
                schedule,
                jar_file_uri,
                timeout,
                batch_id,
                dataproc_cluster_name,
                // Snowflake
                table_tag,
                row_access_policy,
                adapter_properties,
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
                secure,
                transient,
                // Redshift
                auto_refresh,
                backup,
                bind,
                dist,
                sort,
                sort_type,
                // MsSql
                as_columnstore,
                // Athena
                table_type,
                // Postgres
                indexes,
                // Salesforce
                primary_key,
                category,
            ]
        );
    }
}

// Shared comparison helper functions
use crate::schemas::common::Access;
use dbt_common::serde_utils::Omissible;

/// Helper function to compare Omissible<Option<T>> fields
pub fn omissible_option_eq<T: PartialEq>(
    a: &Omissible<Option<T>>,
    b: &Omissible<Option<T>>,
) -> bool {
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

/// Helper function to compare docs fields, treating None and default DocsConfig as equivalent
pub fn docs_eq(a: &Option<DocsConfig>, b: &Option<DocsConfig>) -> bool {
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

/// Helper function to compare access fields, treating None and default Access as equivalent
pub fn access_eq(a: &Option<Access>, b: &Option<Access>) -> bool {
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

/// Helper function to compare meta fields, treating None and empty BTreeMap as equivalent
pub fn meta_eq(
    a: &Option<BTreeMap<String, YmlValue>>,
    b: &Option<BTreeMap<String, YmlValue>>,
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

/// Helper function to compare grants fields, treating None and empty BTreeMap as equivalent
pub fn grants_eq(
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

/// Compare warehouse-specific configurations field by field
pub fn same_warehouse_config(
    self_wh: &WarehouseSpecificNodeConfig,
    other_wh: &WarehouseSpecificNodeConfig,
) -> bool {
    // Shared
    self_wh.partition_by == other_wh.partition_by
        // BigQuery
        && self_wh.cluster_by == other_wh.cluster_by
        && self_wh.hours_to_expiration == other_wh.hours_to_expiration
        && self_wh.labels == other_wh.labels
        && self_wh.labels_from_meta == other_wh.labels_from_meta
        && self_wh.kms_key_name == other_wh.kms_key_name
        && self_wh.require_partition_filter == other_wh.require_partition_filter
        && self_wh.partition_expiration_days == other_wh.partition_expiration_days
        && self_wh.grant_access_to == other_wh.grant_access_to
        && self_wh.partitions == other_wh.partitions
        && self_wh.enable_refresh == other_wh.enable_refresh
        && self_wh.refresh_interval_minutes == other_wh.refresh_interval_minutes
        && self_wh.max_staleness == other_wh.max_staleness
        // Databricks
        && self_wh.file_format == other_wh.file_format
        && self_wh.catalog_name == other_wh.catalog_name
        && self_wh.location_root == other_wh.location_root
        && self_wh.tblproperties == other_wh.tblproperties
        && self_wh.include_full_name_in_path == other_wh.include_full_name_in_path
        && self_wh.liquid_clustered_by == other_wh.liquid_clustered_by
        && self_wh.auto_liquid_cluster == other_wh.auto_liquid_cluster
        && self_wh.clustered_by == other_wh.clustered_by
        && self_wh.buckets == other_wh.buckets
        && self_wh.catalog == other_wh.catalog
        && self_wh.databricks_tags == other_wh.databricks_tags
        && self_wh.compression == other_wh.compression
        && self_wh.databricks_compute == other_wh.databricks_compute
        && self_wh.target_alias == other_wh.target_alias
        && self_wh.source_alias == other_wh.source_alias
        && self_wh.matched_condition == other_wh.matched_condition
        && self_wh.not_matched_condition == other_wh.not_matched_condition
        && self_wh.not_matched_by_source_condition == other_wh.not_matched_by_source_condition
        && self_wh.not_matched_by_source_action == other_wh.not_matched_by_source_action
        && self_wh.merge_with_schema_evolution == other_wh.merge_with_schema_evolution
        && self_wh.skip_matched_step == other_wh.skip_matched_step
        && self_wh.skip_not_matched_step == other_wh.skip_not_matched_step
        && self_wh.schedule == other_wh.schedule
        // Snowflake
        && self_wh.adapter_properties == other_wh.adapter_properties
        && self_wh.table_tag == other_wh.table_tag
        && self_wh.row_access_policy == other_wh.row_access_policy
        && self_wh.external_volume == other_wh.external_volume
        && self_wh.base_location_root == other_wh.base_location_root
        && self_wh.base_location_subpath == other_wh.base_location_subpath
        && self_wh.target_lag == other_wh.target_lag
        && self_wh.refresh_mode == other_wh.refresh_mode
        && self_wh.initialize == other_wh.initialize
        && self_wh.tmp_relation_type == other_wh.tmp_relation_type
        && self_wh.query_tag == other_wh.query_tag
        && self_wh.automatic_clustering == other_wh.automatic_clustering
        && self_wh.copy_grants == other_wh.copy_grants
        && self_wh.secure == other_wh.secure
        && self_wh.transient == other_wh.transient
        // Redshift
        && self_wh.auto_refresh == other_wh.auto_refresh
        && self_wh.backup == other_wh.backup
        && self_wh.bind == other_wh.bind
        && self_wh.dist == other_wh.dist
        && self_wh.sort == other_wh.sort
        && self_wh.sort_type == other_wh.sort_type
        // Fabric
        && self_wh.as_columnstore == other_wh.as_columnstore
        && self_wh.table_type == other_wh.table_type
        // Postgres
        && self_wh.indexes == other_wh.indexes
        // Salesforce
        && self_wh.primary_key == other_wh.primary_key
        && self_wh.category == other_wh.category
}
