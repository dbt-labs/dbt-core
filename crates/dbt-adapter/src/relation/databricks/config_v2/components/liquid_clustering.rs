//! https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/liquid_clustering.py

use dbt_schemas::schemas::InternalDbtNodeAttributes;
use minijinja::Value;
use serde::Serialize;

use crate::relation::config_v2::{
    ComponentConfig, ComponentConfigLoader, SimpleComponentConfigImpl, diff,
};
use crate::relation::databricks::config_v2::DatabricksRelationMetadata;

pub(crate) const TYPE_NAME: &str = "liquid_clustering";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Config {
    pub auto_cluster: bool,
    pub cluster_by: Vec<String>,
}

/// Component for Databricks liquid clustering
pub(crate) type LiquidClustering = SimpleComponentConfigImpl<Config>;

fn new(auto_cluster: bool, cluster_by: Vec<String>) -> LiquidClustering {
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

fn from_remote_state(_state: &DatabricksRelationMetadata) -> LiquidClustering {
    // TODO: this currently just returns an empty config
    new(false, Vec::new())
}

fn from_local_config(_relation_config: &dyn InternalDbtNodeAttributes) -> LiquidClustering {
    // TODO: this currently just returns an empty config
    new(false, Vec::new())
}

pub(crate) struct LiquidClusteringLoader;

impl LiquidClusteringLoader {
    pub fn new(auto_cluster: bool, cluster_by: Vec<String>) -> Box<dyn ComponentConfig> {
        Box::new(new(auto_cluster, cluster_by))
    }

    pub fn type_name() -> &'static str {
        TYPE_NAME
    }
}

impl ComponentConfigLoader<DatabricksRelationMetadata> for LiquidClusteringLoader {
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
