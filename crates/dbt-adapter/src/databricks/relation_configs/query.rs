use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationResults,
};

use crate::AdapterResult;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryConfig {
    query: String,
}

impl QueryConfig {
    pub fn new(query: String) -> Self {
        Self { query }
    }

    pub fn get_diff(&self, other: &Self) -> Option<Self> {
        if self.query.trim() != other.query.trim() {
            Some(self.clone())
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct QueryProcessor;

impl DatabricksComponentProcessorProperties for QueryProcessor {
    fn name(&self) -> &'static str {
        "query"
    }
}

/// https://github.com/databricks/dbt-databricks/blob/65ff2dfdf6df5b6ae125cf94b6fab5a065e51676/dbt/adapters/databricks/relation_configs/query.py#L25
impl DatabricksComponentProcessor for QueryProcessor {
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
