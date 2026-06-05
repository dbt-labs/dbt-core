//! https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/liquid_clustering.py

use dbt_schemas::schemas::InternalDbtNodeAttributes;
use minijinja::Value;
use serde::Serialize;

use crate::errors::AdapterResult;
use crate::relation::config_v2::{
    ComponentConfig, ComponentConfigLoader, SimpleComponentConfigImpl, diff, impl_loader,
};
use crate::relation::databricks::config::DatabricksRelationMetadata;

pub(crate) const TYPE_NAME: &str = "liquid_clustering";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Config {
    pub auto_cluster: bool,
    pub cluster_by: Vec<String>,
}

/// Component for Databricks liquid clustering
pub(crate) type LiquidClustering = SimpleComponentConfigImpl<Config>;

fn new_component(auto_cluster: bool, cluster_by: Vec<String>) -> LiquidClustering {
    LiquidClustering {
        type_name: TYPE_NAME,
        diff_fn: diff::desired_state,
        to_jinja_fn: |v| Value::from_serialize(v),
        value: Config {
            auto_cluster,
            cluster_by,
        },
    }
}

fn from_remote_state(_state: &DatabricksRelationMetadata) -> AdapterResult<LiquidClustering> {
    // TODO: this currently just returns an empty config
    Ok(new_component(false, Vec::new()))
}

fn from_local_config(
    _relation_config: &dyn InternalDbtNodeAttributes,
) -> AdapterResult<LiquidClustering> {
    // TODO: this currently just returns an empty config
    Ok(new_component(false, Vec::new()))
}

impl_loader!(LiquidClustering, DatabricksRelationMetadata);

impl LiquidClusteringLoader {
    pub fn new_component_type_erased(
        auto_cluster: bool,
        cluster_by: Vec<String>,
    ) -> Box<dyn ComponentConfig> {
        Box::new(new_component(auto_cluster, cluster_by))
    }
}
