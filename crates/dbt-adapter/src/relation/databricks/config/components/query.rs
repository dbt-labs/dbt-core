//! https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/query.py

use crate::errors::AdapterResult;
use crate::relation::config_v2::{
    ComponentConfig, ComponentConfigLoader, SimpleComponentConfigImpl, diff, impl_loader,
};
use crate::relation::databricks::config::DatabricksRelationMetadata;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use minijinja::Value;

pub(crate) const TYPE_NAME: &str = "query";

// TODO(serramatutu): reuse this for `query` or `sql` in other warehouses
/// Component for Databricks query
///
/// Holds a string representing the SQL query.
pub type Query = SimpleComponentConfigImpl<String>;

fn new_component(query: &str) -> Query {
    Query {
        type_name: TYPE_NAME,
        diff_fn: diff::desired_state,
        to_jinja_fn: |v| Value::from_serialize(v),
        value: query.trim().to_string(),
    }
}

fn from_remote_state(_results: &DatabricksRelationMetadata) -> AdapterResult<Query> {
    // TODO: implement
    Ok(new_component(""))
}

fn from_local_config(_relation_config: &dyn InternalDbtNodeAttributes) -> AdapterResult<Query> {
    // TODO: implement
    Ok(new_component(""))
}

impl_loader!(Query, DatabricksRelationMetadata);

impl QueryLoader {
    pub fn new_component_type_erased(query: &str) -> Box<dyn ComponentConfig> {
        Box::new(new_component(query))
    }
}
