//! https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/query.py

use crate::relation::config_v2::{
    ComponentConfig, ComponentConfigLoader, SimpleComponentConfigImpl, diff,
};
use crate::relation::databricks::config_v2::DatabricksRelationMetadata;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use minijinja::Value;

pub(crate) const TYPE_NAME: &str = "query";

// TODO(serramatutu): reuse this for `query` or `sql` in other warehouses
/// Component for Databricks query
///
/// Holds a string representing the SQL query.
pub type Query = SimpleComponentConfigImpl<String>;

fn new(query: &str) -> Query {
    Query {
        type_name: TYPE_NAME,
        diff_fn: diff::desired_state,
        to_jinja_fn: |v| Value::from_serialize(v),
        value: query.trim().to_string(),
    }
}

fn from_remote_state(_results: &DatabricksRelationMetadata) -> Query {
    // TODO: implement
    new("")
}

fn from_local_config(_relation_config: &dyn InternalDbtNodeAttributes) -> Query {
    // TODO: implement
    new("")
}

pub(crate) struct QueryLoader;

impl QueryLoader {
    pub fn new(query: &str) -> Box<dyn ComponentConfig> {
        Box::new(new(query))
    }

    pub fn type_name() -> &'static str {
        TYPE_NAME
    }
}

impl ComponentConfigLoader<DatabricksRelationMetadata> for QueryLoader {
    fn type_name(&self) -> &'static str {
        TYPE_NAME
    }

    fn from_remote_state(
        &self,
        remote_state: &DatabricksRelationMetadata,
    ) -> Box<dyn ComponentConfig> {
        Box::new(from_remote_state(remote_state))
    }

    fn from_local_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> Box<dyn ComponentConfig> {
        Box::new(from_local_config(relation_config))
    }
}
