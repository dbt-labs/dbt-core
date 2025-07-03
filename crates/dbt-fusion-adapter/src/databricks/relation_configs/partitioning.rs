// dbt/adapters/databricks/relation_configs/partitioning.py

use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationResults,
};

use crate::AdapterResult;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PartitionedByConfig {
    pub partition_by: Vec<String>,
}

impl PartitionedByConfig {
    pub fn new(partition_by: Vec<String>) -> Self {
        Self { partition_by }
    }
}

#[derive(Debug)]
pub struct PartitionedByProcessor;

impl DatabricksComponentProcessorProperties for PartitionedByProcessor {
    fn name(&self) -> &'static str {
        "partitioned_by"
    }
}

/// https://github.com/databricks/dbt-databricks/blob/8fc69739c4885648bb95074e796c67a57fc9995f/dbt/adapters/databricks/relation_configs/partitioning.py#L19
impl DatabricksComponentProcessor for PartitionedByProcessor {
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
        todo!()
    }
}
