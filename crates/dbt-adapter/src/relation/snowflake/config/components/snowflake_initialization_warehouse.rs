use dbt_common::{AdapterError, AdapterResult};
use dbt_schemas::schemas::{DbtModel, InternalDbtNodeAttributes};
use minijinja::Value;

use crate::relation::snowflake::config::{
    DescribeDynamicTableResults, get_string_by_name_from_record_batch,
};
use crate::{
    relation::config_v2::{
        ComponentConfig, ComponentConfigLoader, SimpleComponentConfigImpl, diff, impl_loader,
    },
    value::none_value,
};

pub(crate) const TYPE_NAME: &str = "snowflake_initialization_warehouse";

/// Component for Snowflake dynamic table initialization warehouse
pub(crate) type SnowflakeInitializationWarehouse = SimpleComponentConfigImpl<Option<String>>;

fn to_jinja(v: &Option<String>) -> Value {
    v.as_ref().map(Value::from).unwrap_or_else(none_value)
}

fn new_component(initialization_warehouse: Option<String>) -> SnowflakeInitializationWarehouse {
    SnowflakeInitializationWarehouse {
        type_name: TYPE_NAME,
        diff_fn: diff::desired_state,
        to_jinja_fn: to_jinja,
        value: initialization_warehouse,
    }
}

fn from_remote_state(
    results: &DescribeDynamicTableResults,
) -> AdapterResult<SnowflakeInitializationWarehouse> {
    let batch = &results.dynamic_table;
    // Some Snowflake accounts don't support the initialization_warehouse field, so this column might not be present.
    // Snowflake returns the string "NONE" for unset optional warehouse values; normalize to None.
    let initialization_warehouse =
        match get_string_by_name_from_record_batch(batch, "initialization_warehouse") {
            Ok(s) if !s.is_empty() && !s.eq_ignore_ascii_case("NONE") => Some(s),
            _ => None,
        };
    Ok(new_component(initialization_warehouse))
}

fn from_local_config(
    relation_config: &dyn InternalDbtNodeAttributes,
) -> AdapterResult<SnowflakeInitializationWarehouse> {
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
    let initialization_warehouse = snowflake_config.snowflake_initialization_warehouse.clone();
    Ok(new_component(initialization_warehouse))
}

impl_loader!(
    SnowflakeInitializationWarehouse,
    DescribeDynamicTableResults
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::snowflake::config::test_helpers;

    #[test]
    fn from_remote_state_some() {
        let remote_state = test_helpers::make_remote_config(test_helpers::TestDynamicTableConfig {
            snowflake_initialization_warehouse: Some("warehouse"),
            ..Default::default()
        });
        let loaded = from_remote_state(&remote_state).unwrap();
        assert!(loaded.value.is_some());
        assert_eq!(loaded.value.unwrap(), "warehouse");
    }

    #[test]
    fn from_remote_state_snowflake_none() {
        let remote_state = test_helpers::make_remote_config(test_helpers::TestDynamicTableConfig {
            snowflake_initialization_warehouse: Some("NONE"),
            ..Default::default()
        });
        let loaded = from_remote_state(&remote_state).unwrap();
        assert!(loaded.value.is_none());
    }

    #[test]
    fn from_remote_state_none() {
        let remote_state = test_helpers::make_remote_config(test_helpers::TestDynamicTableConfig {
            snowflake_initialization_warehouse: None,
            ..Default::default()
        });
        let loaded = from_remote_state(&remote_state).unwrap();
        assert!(loaded.value.is_none());
    }

    #[test]
    fn from_local_state_some() {
        let local_state = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            snowflake_initialization_warehouse: Some("warehouse"),
            ..Default::default()
        });
        let loaded = from_local_config(&local_state).unwrap();
        assert!(loaded.value.is_some());
        assert_eq!(loaded.value.unwrap(), "warehouse");
    }

    #[test]
    fn from_local_state_none() {
        let local_state = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            snowflake_initialization_warehouse: None,
            ..Default::default()
        });
        let loaded = from_local_config(&local_state).unwrap();
        assert!(loaded.value.is_none());
    }
}
