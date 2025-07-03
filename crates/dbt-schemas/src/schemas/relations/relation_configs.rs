use crate::schemas::{relations::base::BaseRelation, InternalDbtNodeAttributes};
use dbt_common::FsResult;
use minijinja::Value;
use std::{any::Any, collections::BTreeMap, fmt, sync::Arc};

/// Represents a changeset composed of [ComponentConfig] structs
pub trait RelationChangeSet: Send + Sync + fmt::Debug {
    /// Get all changes that need to be applied
    fn changes(&self) -> &BTreeMap<String, Arc<dyn ComponentConfig>>;

    /// Whether this change set requires a full refresh of the relation
    fn requires_full_refresh(&self) -> bool;

    /// Check if there are any changes to apply
    fn has_changes(&self) -> bool {
        !self.changes().is_empty()
    }

    /// Get a specific change by component name
    fn get_change(&self, component_name: &str) -> Option<&dyn ComponentConfig>;
}

/// A trait for components that can be part of a relation configuration
/// [BaseRelationConfig] follows an aggregate pattern with [ComponentConfig] objects
pub trait ComponentConfig: Send + Sync + fmt::Debug + Any {
    /// Get the difference between this component and another
    /// Returns None if no changes are needed, or Some with the changes if needed
    fn get_diff(&self, other: &dyn ComponentConfig) -> Option<Arc<dyn ComponentConfig>>;

    /// Convert the component to a value that can be used in templates
    fn as_value(&self) -> Value;

    /// Get a reference to the Any trait
    fn as_any(&self) -> &dyn Any;
}

/// Base implementation of [RelationChangeSet]
#[derive(Debug, Clone)]
pub struct BaseRelationChangeSet {
    changes: Arc<BTreeMap<String, Arc<dyn ComponentConfig>>>,
    requires_full_refresh: bool,
}

impl BaseRelationChangeSet {
    pub fn new(
        changes: BTreeMap<String, Arc<dyn ComponentConfig>>,
        requires_full_refresh: bool,
    ) -> Self {
        Self {
            changes: Arc::new(changes),
            requires_full_refresh,
        }
    }
}

impl RelationChangeSet for BaseRelationChangeSet {
    fn changes(&self) -> &BTreeMap<String, Arc<dyn ComponentConfig>> {
        &self.changes
    }

    fn requires_full_refresh(&self) -> bool {
        self.requires_full_refresh
    }

    fn get_change(&self, component_name: &str) -> Option<&dyn ComponentConfig> {
        self.changes.get(component_name).map(|inner| inner.as_ref())
    }
}

/// Static methods for creating relation configs
/// This trait allows adapters to implement their own config retrieval logic
pub trait RelationConfigFactory: Send + Sync {
    /// Get the relation config from a resolved [DbtModel]
    fn from_node(model: &dyn InternalDbtNodeAttributes) -> FsResult<Arc<dyn BaseRelationConfig>>;

    /// Get the relation config for a given relation
    fn from_relation(
        relation: &dyn BaseRelation,
        state: &minijinja::State,
    ) -> FsResult<Arc<dyn BaseRelationConfig>>;
}

/// Trait for diffable relation configs
/// Allows us to compute a [RelationChangeSet] between two [BaseRelationConfig]
pub trait BaseRelationConfig: Send + Sync + fmt::Debug + Any {
    fn get_changeset(
        &self,
        existing: Option<&dyn BaseRelationConfig>,
    ) -> Option<Arc<dyn RelationChangeSet>>;

    fn as_any(&self) -> &dyn Any;

    /// Convert to Minijinja Value for configs used in macros
    fn to_value(&self) -> Value;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    // Mock ComponentConfig for testing
    #[derive(Debug)]
    struct MockComponent {
        value: String,
    }

    impl ComponentConfig for MockComponent {
        fn get_diff(&self, other: &dyn ComponentConfig) -> Option<Arc<dyn ComponentConfig>> {
            if let Some(other_mock) = other.as_any().downcast_ref::<MockComponent>() {
                if self.value != other_mock.value {
                    Some(Arc::new(MockComponent {
                        value: self.value.clone(),
                    }))
                } else {
                    None
                }
            } else {
                None
            }
        }

        fn as_value(&self) -> Value {
            Value::from(self.value.clone())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_base_relation_change_set() {
        let mut changes = BTreeMap::new();
        changes.insert(
            "test1".to_string(),
            Arc::new(MockComponent {
                value: "value1".to_string(),
            }) as Arc<dyn ComponentConfig>,
        );
        changes.insert(
            "test2".to_string(),
            Arc::new(MockComponent {
                value: "value2".to_string(),
            }) as Arc<dyn ComponentConfig>,
        );

        let change_set = BaseRelationChangeSet::new(changes, false);

        assert!(change_set.has_changes());

        let test1 = change_set.get_change("test1").unwrap();
        assert_eq!(test1.as_value(), Value::from("value1"));

        assert!(change_set.get_change("nonexistent").is_none());

        assert!(!change_set.requires_full_refresh());

        let empty_change_set = BaseRelationChangeSet::new(BTreeMap::new(), true);
        assert!(!empty_change_set.has_changes());
        assert!(empty_change_set.requires_full_refresh());
    }
}
