use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use crate::default_to;
use crate::schemas::common::DbtQuoting;
use crate::schemas::common::Hooks;
use crate::schemas::common::merge_meta;
use crate::schemas::common::merge_tags;
use crate::schemas::manifest::GrantAccessToTarget;
use crate::schemas::manifest::{BigqueryClusterConfig, BigqueryPartitionConfigLegacy};
use crate::schemas::project::dbt_project::DefaultTo;
use crate::schemas::serde::StringOrArrayOfStrings;
use crate::schemas::serde::{bool_or_string_bool, u64_or_string_u64};

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
    *child_meta = merge_meta(child_meta.take(), parent_meta.clone());

    // Handle tags using existing merge function
    let child_tags_vec = child_tags.take().map(|tags| tags.into());
    let parent_tags_vec = parent_tags.clone().map(|tags| tags.into());
    *child_tags =
        merge_tags(child_tags_vec, parent_tags_vec).map(StringOrArrayOfStrings::ArrayOfStrings);
}

/// Helper function to handle default_to logic for column_types
/// Column types should be merged, with parent values filling in missing keys
pub fn default_column_types(
    child_column_types: &mut Option<BTreeMap<String, String>>,
    parent_column_types: &Option<BTreeMap<String, String>>,
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
        (_, None) => {
            // no parent, leave child as is
        }
    }
}

// Implement default_to for the flattened configs
impl DefaultTo<SnowflakeNodeConfig> for SnowflakeNodeConfig {
    fn default_to(&mut self, parent: &SnowflakeNodeConfig) {
        // Exhaustive destructuring ensures all fields are handled
        let SnowflakeNodeConfig {
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
                secure,
                transient
            ]
        );
    }
}

impl DefaultTo<BigQueryNodeConfig> for BigQueryNodeConfig {
    fn default_to(&mut self, parent: &BigQueryNodeConfig) {
        // Exhaustive destructuring ensures all fields are handled
        let BigQueryNodeConfig {
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
            max_staleness,
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

impl DefaultTo<DatabricksNodeConfig> for DatabricksNodeConfig {
    fn default_to(&mut self, parent: &DatabricksNodeConfig) {
        // Exhaustive destructuring ensures all fields are handled
        let DatabricksNodeConfig {
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

impl DefaultTo<RedshiftNodeConfig> for RedshiftNodeConfig {
    fn default_to(&mut self, parent: &RedshiftNodeConfig) {
        let RedshiftNodeConfig {
            auto_refresh,
            backup,
            bind,
            dist,
            sort,
            sort_type,
        } = self;

        default_to!(parent, [auto_refresh, backup, bind, dist, sort, sort_type]);
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, JsonSchema)]
pub struct DatabricksNodeConfig {
    pub file_format: Option<String>,
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
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, JsonSchema)]
pub struct SnowflakeNodeConfig {
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
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, JsonSchema)]
pub struct RedshiftNodeConfig {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub auto_refresh: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub backup: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub bind: Option<bool>,
    pub dist: Option<String>,
    pub sort: Option<StringOrArrayOfStrings>,
    pub sort_type: Option<String>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, JsonSchema)]
pub struct BigQueryNodeConfig {
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
    pub partitions: Option<Vec<String>>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub enable_refresh: Option<bool>,
    #[serde(default, deserialize_with = "u64_or_string_u64")]
    pub refresh_interval_minutes: Option<u64>,
    pub description: Option<String>,
    pub max_staleness: Option<String>,
}
