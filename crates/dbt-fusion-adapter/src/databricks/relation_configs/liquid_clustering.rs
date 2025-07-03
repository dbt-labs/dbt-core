//! dbt/adapters/databricks/relation_configs/liquid_clustering.py

use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationResults,
};

use crate::AdapterResult;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LiquidClusteringConfig {
    pub auto_cluster: bool,
    pub cluster_by: Vec<String>,
}

impl LiquidClusteringConfig {
    pub fn new(auto_cluster: bool, cluster_by: Vec<String>) -> Self {
        Self {
            auto_cluster,
            cluster_by,
        }
    }
}

#[derive(Debug)]
pub struct LiquidClusteringProcessor;

impl DatabricksComponentProcessorProperties for LiquidClusteringProcessor {
    fn name(&self) -> &'static str {
        "liquid_clustering"
    }
}

/// https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/relation_configs/liquid_clustering.py#L19
impl DatabricksComponentProcessor for LiquidClusteringProcessor {
    #[allow(clippy::wrong_self_convention)]
    fn from_relation_results(
        &self,
        _row: &DatabricksRelationResults,
    ) -> Option<DatabricksComponentConfig> {
        // TODO: implement
        None
    }

    fn from_relation_config(
        &self,
        _relation_config: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>> {
        // TODO: implement
        Ok(None)
    }
}
