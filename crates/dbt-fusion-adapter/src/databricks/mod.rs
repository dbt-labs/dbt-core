use dbt_schemas::schemas::{DbtModel, DbtSnapshot};
use minijinja::State;
use serde::Deserialize;

pub mod adapter;
/// Databricks constraint utilities
pub mod constraints;
pub mod metadata;
/// Databricks relation
pub mod relation;
/// Databricks relation config
pub mod relation_configs;
/// serde schemas for Databricks
pub mod schemas;

pub mod describe_table;
pub(crate) mod version;

// Get the Databricks compute engine configured for this model/snapshot
// https://docs.getdbt.com/reference/resource-configs/databricks-configs#selecting-compute-per-model
pub fn databricks_compute_from_state(state: &State) -> Option<String> {
    let yaml_node = dbt_serde_yaml::to_value(state.lookup("model").as_ref()?).ok()?;

    if let Ok(model) = DbtModel::deserialize(&yaml_node) {
        if let Some(databricks_attr) = &model.__adapter_attr__.databricks_attr {
            databricks_attr.databricks_compute.clone()
        } else {
            None
        }
    } else if let Ok(snapshot) = DbtSnapshot::deserialize(&yaml_node) {
        if let Some(databricks_attr) = &snapshot.__adapter_attr__.databricks_attr {
            databricks_attr.databricks_compute.clone()
        } else {
            None
        }
    } else {
        None
    }
}
