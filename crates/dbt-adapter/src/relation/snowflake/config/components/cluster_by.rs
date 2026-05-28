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

pub(crate) const TYPE_NAME: &str = "cluster_by";

/// Component for Snowflake dynamic table cluster by
pub(crate) type ClusterBy = SimpleComponentConfigImpl<Option<String>>;

fn to_jinja(v: &Option<String>) -> Value {
    v.as_ref().map(Value::from).unwrap_or_else(none_value)
}

fn new_component(cluster_by: Option<String>) -> ClusterBy {
    ClusterBy {
        type_name: TYPE_NAME,
        diff_fn: diff::desired_state,
        to_jinja_fn: to_jinja,
        value: cluster_by,
    }
}

fn from_remote_state(results: &DescribeDynamicTableResults) -> AdapterResult<ClusterBy> {
    let batch = &results.dynamic_table;
    let cluster_by = match get_string_by_name_from_record_batch(batch, "cluster_by") {
        Ok(s) if !s.is_empty() && !s.eq_ignore_ascii_case("NONE") => Some(s),
        _ => None,
    };
    Ok(new_component(cluster_by))
}

fn from_local_config(relation_config: &dyn InternalDbtNodeAttributes) -> AdapterResult<ClusterBy> {
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
    let cluster_by = snowflake_config
        .cluster_by
        .as_ref()
        .map(|c| c.fields().join(", "));
    Ok(new_component(cluster_by))
}

impl_loader!(ClusterBy, DescribeDynamicTableResults);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::snowflake::config::test_helpers;

    #[test]
    fn from_remote_state_none() {
        let remote_state = test_helpers::make_remote_config(test_helpers::TestDynamicTableConfig {
            cluster_by: None,
            ..Default::default()
        });
        let loaded = from_remote_state(&remote_state).unwrap();
        assert!(loaded.value.is_none());
    }

    #[test]
    fn from_remote_state_some_string() {
        let remote_state = test_helpers::make_remote_config(test_helpers::TestDynamicTableConfig {
            cluster_by: Some(dbt_schemas::schemas::common::ClusterConfig::String(
                "id".to_owned(),
            )),
            ..Default::default()
        });
        let loaded = from_remote_state(&remote_state).unwrap();
        assert!(loaded.value.is_some());
        assert_eq!(loaded.value.unwrap(), "id")
    }

    #[test]
    fn from_remote_state_some_list() {
        let remote_state = test_helpers::make_remote_config(test_helpers::TestDynamicTableConfig {
            cluster_by: Some(dbt_schemas::schemas::common::ClusterConfig::List(vec![
                "id".to_owned(),
                "id2".to_owned(),
            ])),
            ..Default::default()
        });
        let loaded = from_remote_state(&remote_state).unwrap();
        assert!(loaded.value.is_some());
        assert_eq!(loaded.value.unwrap(), "id, id2")
    }

    #[test]
    fn from_local_state_none() {
        let local_state = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            cluster_by: None,
            ..Default::default()
        });
        let loaded = from_local_config(&local_state).unwrap();
        assert!(loaded.value.is_none());
    }

    #[test]
    fn from_local_state_some_list() {
        let local_state = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            cluster_by: Some(dbt_schemas::schemas::common::ClusterConfig::List(vec![
                "id".to_owned(),
                "id2".to_owned(),
            ])),
            ..Default::default()
        });
        let loaded = from_local_config(&local_state).unwrap();
        assert!(loaded.value.is_some());
        assert_eq!(loaded.value.unwrap(), "id, id2")
    }

    #[test]
    fn from_local_state_some_string() {
        let local_state = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            cluster_by: Some(dbt_schemas::schemas::common::ClusterConfig::String(
                "id".to_owned(),
            )),
            ..Default::default()
        });
        let loaded = from_local_config(&local_state).unwrap();
        assert!(loaded.value.is_some());
        assert_eq!(loaded.value.unwrap(), "id")
    }
}
