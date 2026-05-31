use crate::relation::config_v2::ComponentConfigChange;
use indexmap::IndexMap;

pub(crate) mod incremental_table;
pub(crate) mod materialized_view;
pub(crate) mod streaming_table;
pub(crate) mod view;

/// All Databricks materialization types
///
/// This is only used for the `requires_full_refresh` function below
pub(super) enum MaterializationType {
    IncrementalTable,
    MaterializedView,
    StreamingTable,
    View,
}

/// Whether a changeset requires a full refresh given the materialization type
///
/// I made this as one function instead of a bunch of little scattered functions for each
/// materialization type so we can have it all in one place. It makes it easier to see possible
/// optimizations.
pub(super) fn requires_full_refresh(
    materialization_type: MaterializationType,
    components: &IndexMap<&'static str, ComponentConfigChange>,
) -> bool {
    use crate::relation::databricks::config::components::*;

    match materialization_type {
        // https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/incremental.py
        MaterializationType::IncrementalTable => false,
        // https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/materialized_view.py
        MaterializationType::MaterializedView => {
            const REFRESH_ON: [&str; 5] = [
                liquid_clustering::TYPE_NAME,
                partition_by::TYPE_NAME,
                query::TYPE_NAME,
                relation_comment::TYPE_NAME,
                tbl_properties::TYPE_NAME,
            ];
            REFRESH_ON.iter().any(|k| components.contains_key(k))
        }
        // https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/streaming_table.py
        MaterializationType::StreamingTable => components.contains_key(partition_by::TYPE_NAME),
        // https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/view.py
        MaterializationType::View => components.contains_key(relation_comment::TYPE_NAME),
    }
}
