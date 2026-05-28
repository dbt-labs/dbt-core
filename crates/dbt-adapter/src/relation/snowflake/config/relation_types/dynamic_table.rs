//! https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-snowflake/src/dbt/adapters/snowflake/relation_configs/dynamic_table.py

use crate::AdapterType;
use crate::relation::config_v2::ComponentConfigChange;
use crate::relation::config_v2::{ComponentConfigLoader, RelationConfigLoader};
use crate::relation::snowflake::config::{DescribeDynamicTableResults, components};
use indexmap::IndexMap;

fn requires_full_refresh(components: &IndexMap<&'static str, ComponentConfigChange>) -> bool {
    const REFRESH_ON: [&str; 2] = [
        components::transient::TYPE_NAME,
        components::refresh_mode::TYPE_NAME,
    ];
    REFRESH_ON.iter().any(|k| components.contains_key(k))
}

/// Create a `RelationConfigLoader` for Snowflake dynamic tables.
pub(crate) fn new_loader() -> RelationConfigLoader<'static, DescribeDynamicTableResults> {
    let loaders: [Box<dyn ComponentConfigLoader<DescribeDynamicTableResults>>; 12] = [
        Box::new(components::ClusterByLoader),
        Box::new(components::ImmutableWhereLoader),
        Box::new(components::InitializeLoader),
        Box::new(components::RefreshModeLoader),
        Box::new(components::RefreshWarehouseLoader),
        Box::new(components::RowAccessPolicyLoader),
        Box::new(components::SchedulerLoader),
        Box::new(components::SnowflakeInitializationWarehouseLoader),
        Box::new(components::SnowflakeWarehouseLoader),
        Box::new(components::TableTagLoader),
        Box::new(components::TargetLagLoader),
        Box::new(components::TransientLoader),
    ];

    RelationConfigLoader::new(AdapterType::Snowflake, loaders, requires_full_refresh)
}

#[cfg(test)]
mod tests {
    use super::requires_full_refresh;
    use crate::relation::config_v2::{
        ComponentConfigChange, RelationConfig, SimpleComponentConfigImpl,
    };
    use crate::relation::snowflake::config::{components, test_helpers};
    use indexmap::IndexMap;

    #[test]
    fn transient_change_triggers_full_refresh() {
        let changes = IndexMap::from_iter([(
            components::transient::TYPE_NAME,
            ComponentConfigChange::Drop,
        )]);
        assert!(requires_full_refresh(&changes));
    }

    #[test]
    fn refresh_mode_change_triggers_full_refresh() {
        let changes = IndexMap::from_iter([(
            components::refresh_mode::TYPE_NAME,
            ComponentConfigChange::Drop,
        )]);
        assert!(requires_full_refresh(&changes));
    }

    #[test]
    fn alterable_changes_do_not_trigger_full_refresh() {
        let changes = IndexMap::from_iter([
            (
                components::target_lag::TYPE_NAME,
                ComponentConfigChange::Drop,
            ),
            (
                components::snowflake_warehouse::TYPE_NAME,
                ComponentConfigChange::Drop,
            ),
            (
                components::snowflake_initialization_warehouse::TYPE_NAME,
                ComponentConfigChange::Drop,
            ),
            (
                components::scheduler::TYPE_NAME,
                ComponentConfigChange::Drop,
            ),
            (
                components::immutable_where::TYPE_NAME,
                ComponentConfigChange::Drop,
            ),
            (
                components::cluster_by::TYPE_NAME,
                ComponentConfigChange::Drop,
            ),
            (
                components::refresh_warehouse::TYPE_NAME,
                ComponentConfigChange::Drop,
            ),
        ]);
        assert!(!requires_full_refresh(&changes));
    }

    #[test]
    fn refresh_warehouse_differs_from_existing_triggers_snowflake_warehouse_change() {
        let loader = super::new_loader();
        // local: split — DDL on EXEC_WH, self-refresh on REFRESH_WH
        let local = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            snowflake_warehouse: "EXEC_WH",
            refresh_warehouse: Some("REFRESH_WH"),
            target_lag: Some("1 hour"),
            initialize: "on_create",
            ..Default::default()
        });
        // remote: existing dynamic table still has WAREHOUSE = EXEC_WH
        let remote = test_helpers::make_remote_config(test_helpers::TestDynamicTableConfig {
            snowflake_warehouse: "EXEC_WH",
            target_lag: Some("1 hour"),
            ..Default::default()
        });

        let desired = loader.from_local_config(&local).unwrap();
        let existing = loader.from_remote_state(&remote).unwrap();
        let changes = RelationConfig::diff(&desired, &existing);

        // (1) snowflake_warehouse change carries the refresh warehouse as desired
        let change = changes.get(components::snowflake_warehouse::TYPE_NAME);
        let cfg = match change {
            ComponentConfigChange::Some(c) => c,
            other => panic!(
                "expected snowflake_warehouse component change, got {:?}",
                other
            ),
        };
        let desired_value: &SimpleComponentConfigImpl<String> = cfg
            .as_any()
            .downcast_ref()
            .expect("snowflake_warehouse component should be SimpleComponentConfigImpl<String>");
        assert_eq!(desired_value.value, "REFRESH_WH");

        // (2) refresh_warehouse component is absent from the changeset
        assert!(matches!(
            changes.get(components::refresh_warehouse::TYPE_NAME),
            ComponentConfigChange::None
        ));
    }

    #[test]
    fn refresh_warehouse_matches_existing_no_change() {
        let loader = super::new_loader();
        let local = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            snowflake_warehouse: "EXEC_WH",
            refresh_warehouse: Some("REFRESH_WH"),
            target_lag: Some("1 hour"),
            initialize: "on_create",
            ..Default::default()
        });
        // remote already on REFRESH_WH — matches desired effective WAREHOUSE
        let remote = test_helpers::make_remote_config(test_helpers::TestDynamicTableConfig {
            snowflake_warehouse: "REFRESH_WH",
            target_lag: Some("1 hour"),
            ..Default::default()
        });

        let desired = loader.from_local_config(&local).unwrap();
        let existing = loader.from_remote_state(&remote).unwrap();
        let changes = RelationConfig::diff(&desired, &existing);

        assert!(matches!(
            changes.get(components::snowflake_warehouse::TYPE_NAME),
            ComponentConfigChange::None
        ));
        assert!(matches!(
            changes.get(components::refresh_warehouse::TYPE_NAME),
            ComponentConfigChange::None
        ));
    }

    #[test]
    fn refresh_warehouse_case_insensitive_no_change() {
        let loader = super::new_loader();
        let local = test_helpers::make_local_config(test_helpers::TestDynamicTableConfig {
            snowflake_warehouse: "EXEC_WH",
            refresh_warehouse: Some("refresh_wh"), // lowercase desired
            target_lag: Some("1 hour"),
            initialize: "on_create",
            ..Default::default()
        });
        let remote = test_helpers::make_remote_config(test_helpers::TestDynamicTableConfig {
            snowflake_warehouse: "REFRESH_WH", // uppercase from Snowflake
            target_lag: Some("1 hour"),
            ..Default::default()
        });

        let desired = loader.from_local_config(&local).unwrap();
        let existing = loader.from_remote_state(&remote).unwrap();
        let changes = RelationConfig::diff(&desired, &existing);

        assert!(matches!(
            changes.get(components::snowflake_warehouse::TYPE_NAME),
            ComponentConfigChange::None
        ));
        assert!(matches!(
            changes.get(components::refresh_warehouse::TYPE_NAME),
            ComponentConfigChange::None
        ));
    }
}
