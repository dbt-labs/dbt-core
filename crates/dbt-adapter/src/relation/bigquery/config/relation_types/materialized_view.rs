use crate::AdapterType;
use crate::relation::bigquery::config::components;
use crate::relation::config_v2::ComponentConfigChange;
use crate::relation::config_v2::{ComponentConfigLoader, RelationConfigLoader};
use arrow_schema::Schema;
use indexmap::IndexMap;

fn requires_full_refresh(components: &IndexMap<&'static str, ComponentConfigChange>) -> bool {
    const REFRESH_ON: [&str; 2] = [
        components::cluster_by::TYPE_NAME,
        components::partition_by::TYPE_NAME,
    ];

    for name in REFRESH_ON {
        let Some(change) = components.get(name) else {
            continue;
        };

        match change {
            ComponentConfigChange::Some(_) => return true,
            ComponentConfigChange::Drop => return true,
            ComponentConfigChange::None => continue,
        };
    }

    false
}

/// Create a `RelationConfigLoader` for BigQuery materialized views
pub(crate) fn new_loader() -> RelationConfigLoader<'static, Schema> {
    let loaders: [Box<dyn ComponentConfigLoader<Schema>>; 7] = [
        Box::new(components::ClusterByLoader),
        Box::new(components::DescriptionLoader),
        Box::new(components::KmsKeyLoader),
        Box::new(components::LabelsLoader),
        Box::new(components::PartitionByLoader),
        Box::new(components::RefreshLoader),
        Box::new(components::TagsLoader),
    ];

    RelationConfigLoader::new(AdapterType::Bigquery, loaders, requires_full_refresh)
}

#[cfg(test)]
mod tests {
    use super::requires_full_refresh;
    use crate::relation::bigquery::config::components;
    use crate::relation::config_v2::ComponentConfigChange;
    use indexmap::IndexMap;

    #[test]
    fn partition_by_change_triggers_full_refresh() {
        let changes = IndexMap::from_iter([(
            components::partition_by::TYPE_NAME,
            ComponentConfigChange::Drop,
        )]);
        assert!(requires_full_refresh(&changes));
    }

    #[test]
    fn cluster_by_change_triggers_full_refresh() {
        let changes = IndexMap::from_iter([(
            components::cluster_by::TYPE_NAME,
            ComponentConfigChange::Drop,
        )]);
        assert!(requires_full_refresh(&changes));
    }

    #[test]
    fn option_changes_do_not_trigger_full_refresh() {
        let changes = IndexMap::from_iter([
            (
                components::description::TYPE_NAME,
                ComponentConfigChange::Drop,
            ),
            (components::kms_key::TYPE_NAME, ComponentConfigChange::Drop),
            (components::labels::TYPE_NAME, ComponentConfigChange::Drop),
            (components::refresh::TYPE_NAME, ComponentConfigChange::Drop),
            (components::tags::TYPE_NAME, ComponentConfigChange::Drop),
        ]);
        assert!(!requires_full_refresh(&changes));
    }
}
