use crate::databricks::relation_configs::{base::from_relation_config, DatabricksRelationConfig};

use crate::AdapterResult;
use dbt_schemas::schemas::InternalDbtNodeAttributes;

use std::fmt::Debug;

/// Given a model, parse and build its configurations
/// reference: https://github.com/databricks/dbt-databricks/blob/13686739eb59566c7a90ee3c357d12fe52ec02ea/dbt/adapters/databricks/impl.py#L881
pub fn get_from_relation_config<T: Debug + DatabricksRelationConfig>(
    model: &dyn InternalDbtNodeAttributes,
) -> AdapterResult<T> {
    let config = from_relation_config::<T>(model)?;
    Ok(T::new(config))
}
