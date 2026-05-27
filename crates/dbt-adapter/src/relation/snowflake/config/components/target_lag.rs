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

pub(crate) const TYPE_NAME: &str = "target_lag";

/// Component for Snowflake dynamic table target lag
pub(crate) type TargetLag = SimpleComponentConfigImpl<Option<String>>;

fn to_jinja(v: &Option<String>) -> Value {
    v.as_ref().map(Value::from).unwrap_or_else(none_value)
}

fn new_component(target_lag: Option<String>) -> TargetLag {
    TargetLag {
        type_name: TYPE_NAME,
        // TODO: The old implementation of TargetLag in Fusion was more
        // permissive for diffs than this is. In particular, Snowflake
        // normalizes target lag configs (ex. '1 MINUTES' -> '1 minute').
        // The old version wouldn't detect a config change in this case, but
        // both Core and the new version do.
        diff_fn: diff::desired_state,
        to_jinja_fn: to_jinja,
        value: target_lag,
    }
}

fn from_remote_state(results: &DescribeDynamicTableResults) -> AdapterResult<TargetLag> {
    let batch = &results.dynamic_table;
    let target_lag = match get_string_by_name_from_record_batch(batch, "target_lag") {
        Ok(s) if !s.is_empty() => Some(s),
        _ => None,
    };
    Ok(new_component(target_lag))
}

fn from_local_config(relation_config: &dyn InternalDbtNodeAttributes) -> AdapterResult<TargetLag> {
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
    let target_lag = snowflake_config.target_lag.clone();

    // Reject configs entirely if scheduler is ENABLE
    // Reference: https://docs.getdbt.com/reference/resource-configs/snowflake-configs?version=2.0#how-target_lag-interacts-with-scheduler
    // https://github.com/igorbelianski-cyber/dbt-adapters/blob/8a84e6396cdddfa88d9775500dd6ee60f8212fc3/dbt-snowflake/src/dbt/adapters/snowflake/relation_configs/dynamic_table.py#L144-L145
    if let Some(scheduler) = snowflake_config.scheduler.clone() {
        if scheduler.eq_ignore_ascii_case("enable") && target_lag.is_none() {
            return Err(AdapterError::new(
                dbt_common::AdapterErrorKind::Configuration,
                "Invalid dynamic table config: `scheduler=ENABLE` requires `target_lag`.",
            ));
        } else if scheduler.eq_ignore_ascii_case("disable") && target_lag.is_some() {
            return Err(AdapterError::new(
                dbt_common::AdapterErrorKind::Configuration,
                "Invalid dynamic table config: `scheduler=DISABLE` requires `target_lag` to be omitted.",
            ));
        }
    }

    Ok(new_component(target_lag))
}

impl_loader!(TargetLag, DescribeDynamicTableResults);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::snowflake::config::test_helpers;

    #[test]
    fn from_remote_state_none() {
        let remote_state = test_helpers::make_remote_config(test_helpers::TestDynamicTableConfig {
            target_lag: None,
            ..Default::default()
        });
        let loaded = from_remote_state(&remote_state).unwrap();
        assert!(loaded.value.is_none());
    }

    #[test]
    fn from_remote_state_empty() {
        let remote_state = test_helpers::make_remote_config(test_helpers::TestDynamicTableConfig {
            target_lag: Some(""),
            ..Default::default()
        });
        let loaded = from_remote_state(&remote_state).unwrap();
        assert!(loaded.value.is_none());
    }

    #[test]
    fn from_remote_state_hours() {
        let remote_state = test_helpers::make_remote_config(test_helpers::TestDynamicTableConfig {
            target_lag: Some("5 hours"),
            ..Default::default()
        });
        let loaded = from_remote_state(&remote_state).unwrap();
        assert!(loaded.value.is_some());
        assert_eq!(loaded.value.unwrap(), "5 hours");
    }

    #[test]
    fn from_local_state_none_and_scheduler_omitted() {
        let local_state = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            scheduler: None,
            target_lag: None,
            ..Default::default()
        });
        let loaded = from_local_config(&local_state).unwrap();
        assert!(loaded.value.is_none());
    }

    #[test]
    fn from_local_state_none_and_scheduler_ok() {
        let local_state = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            scheduler: Some("DISABLE"),
            target_lag: None,
            ..Default::default()
        });
        let loaded = from_local_config(&local_state).unwrap();
        assert!(loaded.value.is_none());
    }

    #[test]
    fn from_local_state_none_and_scheduler_invalid() {
        let local_state = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            scheduler: Some("ENABLE"),
            target_lag: None,
            ..Default::default()
        });
        let err = from_local_config(&local_state);
        assert!(err.is_err_and(|e| {
            e.message()
                .contains("Invalid dynamic table config: `scheduler=ENABLE`")
        }))
    }

    #[test]
    fn from_local_state_some_and_scheduler_omitted() {
        let local_state = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            scheduler: None,
            target_lag: Some("1 hour"),
            ..Default::default()
        });
        let loaded = from_local_config(&local_state).unwrap();
        assert!(loaded.value.is_some());
        assert_eq!(loaded.value.unwrap(), "1 hour");
    }

    #[test]
    fn from_local_state_some_and_scheduler_ok() {
        let local_state = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            scheduler: Some("ENABLE"),
            target_lag: Some("1 hour"),
            ..Default::default()
        });
        let loaded = from_local_config(&local_state).unwrap();
        assert!(loaded.value.is_some());
        assert_eq!(loaded.value.unwrap(), "1 hour");
    }

    #[test]
    fn from_local_state_some_and_scheduler_invalid() {
        let local_state = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            scheduler: Some("DISABLE"),
            target_lag: Some("1 hour"),
            ..Default::default()
        });
        let err = from_local_config(&local_state);
        assert!(err.is_err_and(|e| {
            e.message()
                .contains("Invalid dynamic table config: `scheduler=DISABLE`")
        }))
    }
}
