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
    let loaders: [Box<dyn ComponentConfigLoader<DescribeDynamicTableResults>>; 11] = [
        Box::new(components::ClusterByLoader),
        Box::new(components::ImmutableWhereLoader),
        Box::new(components::InitializeLoader),
        Box::new(components::RefreshModeLoader),
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
    use crate::relation::config_v2::ComponentConfigChange;
    use crate::relation::snowflake::config::components;
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
        ]);
        assert!(!requires_full_refresh(&changes));
    }
}
