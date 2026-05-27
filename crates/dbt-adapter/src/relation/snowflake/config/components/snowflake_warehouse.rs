use dbt_common::{AdapterError, AdapterResult};
use dbt_schemas::schemas::{DbtModel, InternalDbtNodeAttributes};
use minijinja::Value;

use crate::relation::config_v2::{
    ComponentConfig, ComponentConfigLoader, SimpleComponentConfigImpl, impl_loader,
};
use crate::relation::snowflake::config::{
    DescribeDynamicTableResults, get_string_by_name_from_record_batch,
};

pub(crate) const TYPE_NAME: &str = "snowflake_warehouse";

/// Component for Snowflake dynamic table warehouse.
pub(crate) type SnowflakeWarehouse = SimpleComponentConfigImpl<String>;

fn to_jinja(v: &String) -> Value {
    Value::from(v)
}

// Snowflake warehouse names are case-insensitive.
// The type restriction of `diff_fn` requires that this function take in exactly
// &String, &String, so the clippy warning is incorrect.
#[allow(clippy::ptr_arg)]
fn diff_warehouse(desired: &String, current: &String) -> Option<String> {
    if desired.eq_ignore_ascii_case(current) {
        None
    } else {
        Some(desired.clone())
    }
}

fn new_component(warehouse: String) -> SnowflakeWarehouse {
    SnowflakeWarehouse {
        type_name: TYPE_NAME,
        diff_fn: diff_warehouse,
        to_jinja_fn: to_jinja,
        value: warehouse,
    }
}

fn from_remote_state(results: &DescribeDynamicTableResults) -> AdapterResult<SnowflakeWarehouse> {
    let batch = &results.dynamic_table;
    let warehouse = get_string_by_name_from_record_batch(batch, "warehouse")
        .map_err(|e| AdapterError::new(dbt_common::AdapterErrorKind::UnexpectedResult, e))?;
    Ok(new_component(warehouse))
}

fn from_local_config(
    relation_config: &dyn InternalDbtNodeAttributes,
) -> AdapterResult<SnowflakeWarehouse> {
    let snowflake_config = relation_config
        .as_any()
        .downcast_ref::<DbtModel>()
        .ok_or_else(|| {
            AdapterError::new(
                dbt_common::AdapterErrorKind::UnexpectedResult,
                "relation config needs to be a model",
            )
        })?
        .__adapter_attr__
        .snowflake_attr
        .as_ref()
        .ok_or_else(|| {
            AdapterError::new(
                dbt_common::AdapterErrorKind::Configuration,
                "relation config needs to be Snowflake model",
            )
        })?;
    let warehouse = snowflake_config
        .snowflake_warehouse
        .clone()
        .ok_or_else(|| {
            AdapterError::new(
                dbt_common::AdapterErrorKind::Configuration,
                "Failed to get required field snowflake_warehouse from dynamic_table config.",
            )
        })?;
    Ok(new_component(warehouse))
}

impl_loader!(SnowflakeWarehouse, DescribeDynamicTableResults);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::snowflake::config::test_helpers;

    #[test]
    fn from_remote_state_with_warehouse() {
        let remote_state = test_helpers::make_remote_config(test_helpers::TestDynamicTableConfig {
            snowflake_warehouse: "warehouse",
            ..Default::default()
        });
        let loaded = from_remote_state(&remote_state).unwrap();
        assert_eq!(loaded.value, "warehouse");
    }

    #[test]
    fn from_local_state_with_warehouse() {
        let local_state = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            snowflake_warehouse: "warehouse",
            ..Default::default()
        });
        let loaded = from_local_config(&local_state).unwrap();
        assert_eq!(loaded.value, "warehouse");
    }
}
