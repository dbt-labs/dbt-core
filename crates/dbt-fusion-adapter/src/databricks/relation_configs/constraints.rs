//! dbt/adapters/databricks/relation_configs/constraints.py

use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationResults,
};

use crate::AdapterResult;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ConstraintsConfig {
    pub set_non_nulls: BTreeSet<String>,
    pub unset_non_nulls: BTreeSet<String>,
    // TODO: support constraints
    // reference: https://github.com/databricks/dbt-databricks/blob/e7099a2c75a92fa5240989b19d246a0ca8a313ef/dbt/adapters/databricks/relation_configs/constraints.py#L27
    // pub set_constraints: BTreeSet<TypedConstraint>,
    // pub unset_constraints: BTreeSet<TypedConstraint>,
}

impl ConstraintsConfig {
    pub fn new(set_non_nulls: BTreeSet<String>, unset_non_nulls: BTreeSet<String>) -> Self {
        Self {
            set_non_nulls,
            unset_non_nulls,
        }
    }

    pub fn get_diff(&self, _other: &Self) -> Option<Self> {
        // TODO: implement
        None
    }
}

#[derive(Debug)]
pub struct ConstraintsProcessor;

impl DatabricksComponentProcessorProperties for ConstraintsProcessor {
    fn name(&self) -> &'static str {
        "constraints"
    }
}

/// https://github.com/databricks/dbt-databricks/blob/e7099a2c75a92fa5240989b19d246a0ca8a313ef/dbt/adapters/databricks/relation_configs/constraints.py#L50
impl DatabricksComponentProcessor for ConstraintsProcessor {
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
