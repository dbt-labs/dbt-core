//! NOTE: This module will replace config.rs, but for backwards compat reasons I kept both
//! until everything is migrated.
//!
//! This module contains the primitives for describing and diffing relation configurations.
//!
//! 1. There is a single `RelationConfig` which contains a set of `RelationConfigComponent`. All
//!    components are type erased to avoid lots of specialization for every component type of every
//!    warehouse platform.
//!
//! 2. Each component can be read either from a remote dataset (the current applied state) or from
//!    local configuration (the desired state).
//!
//! 3. Each component implements its own diffing logic to generate the diff between applied and
//!    desired states. This can be used by macros to generate ALTER statements.
//!
//! 4. A `RelationComponentConfigChangeSet` aggregates diffs from many components, and represents the
//!    changeset needed to take the current state to the desired state.
//!
//! 5. Each warehouse implements its own `<Warehouse>RelationConfigObject` that wraps a
//!    `RelationConfig` but captures historical differences in Jinja implementations across
//!    adapters.

use crate::funcs::none_value;

use dbt_schemas::schemas::InternalDbtNodeAttributes;
use indexmap::{IndexMap, map::Iter as IndexMapIter};
use minijinja::{
    arg_utils::ArgParser,
    listener::RenderingEventListener,
    value::{Enumerator, Object, Value, ValueMap},
};
use std::{any::Any, fmt, rc::Rc, sync::Arc};

pub trait ComponentConfig: fmt::Debug + Send + Sync + Any {
    /// Assuming self is the desired state, get the diff that takes the current state to the desired state.
    ///
    /// Returns None if no change was detected.
    fn diff_from(
        &self,
        current_state: Option<&dyn ComponentConfig>,
    ) -> Option<Box<dyn ComponentConfig>>;

    /// The unique name that identifies this component's type
    fn type_name(&self) -> &'static str;

    fn as_any(&self) -> &dyn Any;

    fn as_jinja(&self) -> Value;
}

/// Contains custom diffing functions that can be used by component implementations
pub(crate) mod diff {
    use super::*;
    use std::hash::Hash;

    /// The signature of a diff function
    pub(crate) type DiffFn<T> = fn(&T, &T) -> Option<T>;

    /// The resulting diff is simply a clone of the desired state
    pub(crate) fn desired_state<T: Sized + Eq + Clone>(
        desired_state: &T,
        current_state: &T,
    ) -> Option<T> {
        if desired_state != current_state {
            Some(desired_state.clone())
        } else {
            None
        }
    }

    /// The resulting diff contains only the keys:
    /// 1. that are present in the desired state but not the current state; or
    /// 2. whose value in the desired state does not match the current state.
    /// 3. whose value is not present in the desired state but it is in current state,
    ///    (new state assumes Default).
    pub(crate) fn changed_keys<K, V>(
        desired_state: &IndexMap<K, V>,
        current_state: &IndexMap<K, V>,
    ) -> Option<IndexMap<K, V>>
    where
        K: Clone + Eq + Hash,
        V: Clone + Eq + Default,
    {
        let mut diff: IndexMap<K, V> = desired_state
            .iter()
            .filter_map(|(k, v)| {
                if Some(v) != current_state.get(k) {
                    Some((k.clone(), v.clone()))
                } else {
                    None
                }
            })
            .collect();

        for current_k in current_state.keys() {
            if !desired_state.contains_key(current_k) {
                diff.insert(current_k.clone(), V::default());
            }
        }

        if diff.is_empty() { None } else { Some(diff) }
    }
}

/// A function that takes a config component value and turns it into a Jinja `Value`
pub type ToJinjaFn<T> = fn(&T) -> Value;

#[derive(Clone, Debug)]
pub(crate) struct SimpleComponentConfigImpl<T: fmt::Debug + Send + Sync + Any + Clone> {
    // TODO(serramatutu): maybe dynamic dispatch here with dyn T
    pub type_name: &'static str,
    pub diff_fn: diff::DiffFn<T>,
    pub to_jinja_fn: ToJinjaFn<T>,
    pub value: T,
}

impl<T: fmt::Debug + Send + Sync + Any + Clone> ComponentConfig for SimpleComponentConfigImpl<T> {
    fn diff_from(
        &self,
        current_state: Option<&dyn ComponentConfig>,
    ) -> Option<Box<dyn ComponentConfig>> {
        let current_state = match current_state {
            Some(current_state) => current_state.as_any().downcast_ref::<Self>(),
            None => return Some(Box::new(self.clone())),
        };

        let value = match current_state {
            Some(current_state) => &current_state.value,
            None => {
                debug_assert!(
                    false,
                    "type of value passed to SimpleComponentConfigImpl::diff() is incorrect"
                );
                return None;
            }
        };

        if let Some(diff) = (self.diff_fn)(&self.value, value) {
            let self_clone = Self {
                type_name: self.type_name,
                diff_fn: self.diff_fn,
                to_jinja_fn: self.to_jinja_fn,
                value: diff,
            };

            Some(Box::new(self_clone))
        } else {
            None
        }
    }

    fn type_name(&self) -> &'static str {
        self.type_name
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_jinja(&self) -> Value {
        (self.to_jinja_fn)(&self.value)
    }
}

/// Represents a change in a certain configuration by comparing the applied state with the desired state
#[derive(Debug)]
pub enum ComponentConfigChange {
    /// The config has changed
    Some(Box<dyn ComponentConfig>),
    /// The config used to exist but has been dropped
    Drop,
    /// There were no detected changes
    None,
}

impl ComponentConfigChange {
    fn as_jinja(&self) -> Value {
        match self {
            Self::Some(v) => v.as_jinja(),
            _ => none_value(),
        }
    }
}

/// A function that evaluates a set of components that have been changed and returns whether or not
/// those will require a full refresh
pub(crate) type RequiresFullRefreshFn = fn(&IndexMap<&'static str, ComponentConfigChange>) -> bool;

#[derive(Debug)]
pub struct RelationConfig(
    IndexMap<&'static str, Box<dyn ComponentConfig>>,
    RequiresFullRefreshFn,
);

impl RelationConfig {
    pub fn new(
        configs: impl IntoIterator<Item = Box<dyn ComponentConfig>>,
        requires_full_refresh: RequiresFullRefreshFn,
    ) -> Self {
        Self(
            configs
                .into_iter()
                .map(|cfg| (cfg.type_name(), cfg))
                .collect(),
            requires_full_refresh,
        )
    }
}

impl RelationConfig {
    /// Get a component by type name
    pub(crate) fn get<'a>(
        &'a self,
        component_type_name: &'static str,
    ) -> Option<&'a dyn ComponentConfig> {
        self.0.get(&component_type_name).map(|inner| inner.as_ref())
    }

    /// Get the diff that takes the current state to the desired state
    pub fn diff(
        desired_state: &RelationConfig,
        current_state: &RelationConfig,
    ) -> RelationComponentConfigChangeSet {
        let mut diffs = IndexMap::new();

        for (type_name, desired_component) in &desired_state.0 {
            let current_component = current_state.get(type_name);

            if let Some(diff) = desired_component.diff_from(current_component) {
                let change = ComponentConfigChange::Some(diff);
                diffs.insert(*type_name, change);
            }
        }

        for type_name in current_state.0.keys() {
            if desired_state.get(type_name).is_none() {
                diffs.insert(*type_name, ComponentConfigChange::Drop);
            }
        }

        RelationComponentConfigChangeSet(diffs, desired_state.1)
    }
}

impl Object for RelationConfig {
    fn call_method(
        self: &Arc<Self>,
        _state: &minijinja::State,
        name: &str,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, minijinja::Error> {
        // TODO: args iter
        let mut parser = ArgParser::new(args, None);
        match name {
            "get_changeset" => {
                let val = if let Some(existing) = parser
                    .get::<Value>("existing_relation")?
                    .downcast_object::<RelationConfig>()
                {
                    let change_set = RelationConfig::diff(self.as_ref(), existing.as_ref());
                    if !change_set.is_empty() {
                        let intermediate_map = Value::from(ValueMap::from([
                            (
                                Value::from("requires_full_refresh"),
                                Value::from(change_set.requires_full_refresh()),
                            ),
                            (Value::from("changes"), Value::from_object(change_set)),
                        ]));
                        Value::from_serialize(intermediate_map)
                    } else {
                        none_value()
                    }
                } else {
                    none_value()
                };

                Ok(val)
            }
            _ => unimplemented!("RelationConfigBaseObject does not support method: {}", name),
        }
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        let key = key.as_str()?;
        self.0.get(key).map(|v| v.as_jinja())
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Values(self.0.keys().map(|v| Value::from(*v)).collect())
    }
}

/// Loads a `ComponentConfig` from the remote data platform state (current state)
/// or from the local configs (desired state).
#[expect(dead_code)]
pub(crate) trait ComponentConfigLoader<R> {
    /// Load the current applied state for the component given the remote state
    #[expect(clippy::wrong_self_convention)]
    fn from_remote_state(&self, remote_state: &R) -> Box<dyn ComponentConfig>;

    /// Load the desired component state from local dbt configs
    #[expect(clippy::wrong_self_convention)]
    fn from_local_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> Box<dyn ComponentConfig>;

    /// The unique type name of the component loaded by this loader
    fn type_name(&self) -> &'static str;
}

/// Holds a collection of `ComponentConfigLoader` to populate a `RelationConfig`
/// by loading each of its components one by one
pub(crate) struct RelationConfigLoader<R>(
    Vec<Box<dyn ComponentConfigLoader<R>>>,
    RequiresFullRefreshFn,
);

impl<R> RelationConfigLoader<R> {
    pub(crate) fn new(
        component_loaders: impl IntoIterator<Item = Box<dyn ComponentConfigLoader<R>>>,
        requires_full_refresh: RequiresFullRefreshFn,
    ) -> Self {
        Self(
            component_loaders.into_iter().collect(),
            requires_full_refresh,
        )
    }

    /// Load the current applied state for the relation and all its components given the remote state
    #[expect(clippy::wrong_self_convention)]
    pub(crate) fn from_remote_state(&self, remote_state: &R) -> RelationConfig {
        RelationConfig::new(
            self.0
                .iter()
                .map(|l| l.from_remote_state(remote_state))
                .collect::<Vec<_>>(),
            self.1,
        )
    }

    /// Load the desired relation state from local dbt configs
    #[expect(clippy::wrong_self_convention)]
    pub(crate) fn from_local_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> RelationConfig {
        RelationConfig::new(
            self.0
                .iter()
                .map(|l| l.from_local_config(relation_config))
                .collect::<Vec<_>>(),
            self.1,
        )
    }
}

#[derive(Debug)]
pub struct RelationComponentConfigChangeSet(
    IndexMap<&'static str, ComponentConfigChange>,
    RequiresFullRefreshFn,
);

impl RelationComponentConfigChangeSet {
    pub fn new(
        changes: impl Into<IndexMap<&'static str, ComponentConfigChange>>,
        requires_full_refresh: RequiresFullRefreshFn,
    ) -> Self {
        Self(changes.into(), requires_full_refresh)
    }

    /// Get the count of changes in this changeset
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> IndexMapIter<'_, &'static str, ComponentConfigChange> {
        self.0.iter()
    }

    /// Get a change by TypeId
    pub fn get<'a>(&'a self, component_type_name: &'static str) -> &'a ComponentConfigChange {
        self.0
            .get(&component_type_name)
            .unwrap_or(&ComponentConfigChange::None)
    }

    /// Whether applying this config to an existing table requires a full refresh
    pub fn requires_full_refresh(&self) -> bool {
        self.1(&self.0)
    }
}

impl Object for RelationComponentConfigChangeSet {
    fn call_method(
        self: &Arc<Self>,
        _state: &minijinja::State,
        name: &str,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, minijinja::Error> {
        // TODO: ArgsIter
        let mut parser = ArgParser::new(args, None);
        match name {
            // support example `_configuration_changes.changes.get("tags", None)`
            "get" => {
                let key = parser.get::<Value>("key")?;
                let key = key.as_str().ok_or_else(|| {
                    minijinja::Error::new(
                        minijinja::ErrorKind::InvalidArgument,
                        "key must be a string",
                    )
                })?;

                Ok(self
                    .0
                    .get(key)
                    .map(|v| v.as_jinja())
                    .unwrap_or_else(none_value))
            }
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::UnknownMethod,
                format!("RelationComponentConfigChangeSet has no method named '{name}'"),
            )),
        }
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        let key = key.as_str()?;
        self.0.get(key).map(|v| v.as_jinja())
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Iter(Box::new(
            self.0
                .keys()
                .map(|v| Value::from(*v))
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl<T: PartialEq + fmt::Debug + Send + Sync + Any + Clone> PartialEq
        for SimpleComponentConfigImpl<T>
    {
        fn eq(&self, other: &Self) -> bool {
            self.value == other.value
        }
    }

    impl<T: Eq + fmt::Debug + Send + Sync + Any + Clone> Eq for SimpleComponentConfigImpl<T> {}

    fn custom_diff(desired_state: &u8, current_state: &u8) -> Option<u8> {
        let diff = desired_state - current_state;
        if diff != 0 { Some(diff) } else { None }
    }

    fn assert_dyn_eq<T: Clone + Eq + fmt::Debug + 'static>(a: T, b: Box<dyn ComponentConfig>) {
        assert_eq!(
            a,
            b.as_ref()
                .as_any()
                .downcast_ref::<T>()
                .expect("Downcast failed")
                .clone()
        )
    }

    fn assert_component_config_change_eq<T: fmt::Debug + PartialEq + 'static>(
        a: &ComponentConfigChange,
        b: &ComponentConfigChange,
    ) {
        assert_eq!(std::mem::discriminant(a), std::mem::discriminant(b));
        if let (ComponentConfigChange::Some(a_diff), ComponentConfigChange::Some(b_diff)) = (a, b) {
            assert_eq!(
                a_diff.as_any().downcast_ref::<T>(),
                b_diff.as_any().downcast_ref::<T>()
            );
        }
    }

    fn return_true(_: &IndexMap<&'static str, ComponentConfigChange>) -> bool {
        true
    }

    const TYPE_NAME: &str = "mock";

    type MockComponent = SimpleComponentConfigImpl<u8>;

    fn to_jinja(v: &u8) -> Value {
        Value::from(*v)
    }

    #[test]
    fn test_simple_diff_config_created() {
        let next = MockComponent {
            type_name: TYPE_NAME,
            diff_fn: diff::desired_state,
            to_jinja_fn: to_jinja,
            value: 1,
        };
        let diff = ComponentConfig::diff_from(&next, None).unwrap();
        assert_dyn_eq(next, diff);
    }

    #[test]
    fn test_simple_diff_no_change() {
        let prev = MockComponent {
            diff_fn: diff::desired_state,
            to_jinja_fn: to_jinja,
            type_name: TYPE_NAME,
            value: 1,
        };
        let next = MockComponent {
            type_name: TYPE_NAME,
            diff_fn: diff::desired_state,
            to_jinja_fn: to_jinja,
            value: 1,
        };
        let diff = ComponentConfig::diff_from(&next, Some(&prev));
        assert!(diff.is_none());
    }

    #[test]
    fn test_simple_diff_with_change() {
        let prev = MockComponent {
            diff_fn: diff::desired_state,
            to_jinja_fn: to_jinja,
            type_name: TYPE_NAME,
            value: 1,
        };
        let next = MockComponent {
            type_name: TYPE_NAME,
            diff_fn: diff::desired_state,
            to_jinja_fn: to_jinja,
            value: 10,
        };
        let diff = ComponentConfig::diff_from(&next, Some(&prev)).unwrap();
        assert_dyn_eq(next, diff);
    }

    #[test]
    fn test_custom_diff_with_change() {
        let prev = MockComponent {
            type_name: TYPE_NAME,
            diff_fn: custom_diff,
            to_jinja_fn: to_jinja,
            value: 1,
        };
        let next = MockComponent {
            type_name: TYPE_NAME,
            diff_fn: custom_diff,
            to_jinja_fn: to_jinja,
            value: 10,
        };
        let diff = ComponentConfig::diff_from(&next, Some(&prev)).unwrap();

        let expected = MockComponent {
            type_name: TYPE_NAME,
            diff_fn: custom_diff,
            to_jinja_fn: to_jinja,
            // 10 - 1, per our custom diff
            value: 9,
        };

        assert_dyn_eq(expected, diff);
    }

    #[test]
    fn test_relation_config_diff_created() {
        let next_component = MockComponent {
            type_name: TYPE_NAME,
            diff_fn: diff::desired_state,
            to_jinja_fn: to_jinja,
            value: 10,
        };
        let prev = RelationConfig::new([], return_true);
        let next = RelationConfig::new(
            [Box::new(next_component.clone()) as Box<dyn ComponentConfig>],
            return_true,
        );
        let changeset = RelationConfig::diff(&next, &prev);
        assert!(changeset.requires_full_refresh());
        assert_eq!(changeset.0.len(), 1);
        let change = changeset.get(TYPE_NAME);
        assert_component_config_change_eq::<MockComponent>(
            change,
            &ComponentConfigChange::Some(Box::new(next_component) as Box<dyn ComponentConfig>),
        );
    }

    #[test]
    fn test_relation_config_diff_no_changes() {
        let component = MockComponent {
            type_name: TYPE_NAME,
            diff_fn: diff::desired_state,
            to_jinja_fn: to_jinja,
            value: 10,
        };
        let relation_config = RelationConfig::new(
            [Box::new(component) as Box<dyn ComponentConfig>],
            return_true,
        );
        let changeset = RelationConfig::diff(&relation_config, &relation_config);
        assert!(changeset.requires_full_refresh());
        assert_eq!(changeset.0.len(), 0);
        let change = changeset.get(TYPE_NAME);
        assert_component_config_change_eq::<MockComponent>(change, &ComponentConfigChange::None);
    }

    #[test]
    fn test_relation_config_diff_with_changes() {
        let prev_component = MockComponent {
            type_name: TYPE_NAME,
            diff_fn: diff::desired_state,
            to_jinja_fn: to_jinja,
            value: 1,
        };
        let next_component = MockComponent {
            type_name: TYPE_NAME,
            diff_fn: diff::desired_state,
            to_jinja_fn: to_jinja,
            value: 10,
        };
        let prev = RelationConfig::new(
            [Box::new(prev_component) as Box<dyn ComponentConfig>],
            return_true,
        );
        let next = RelationConfig::new(
            [Box::new(next_component.clone()) as Box<dyn ComponentConfig>],
            return_true,
        );
        let changeset = RelationConfig::diff(&next, &prev);
        assert!(changeset.requires_full_refresh());
        assert_eq!(changeset.0.len(), 1);
        let change = changeset.get(TYPE_NAME);
        assert_component_config_change_eq::<MockComponent>(
            change,
            &ComponentConfigChange::Some(Box::new(next_component) as Box<dyn ComponentConfig>),
        );
    }

    #[test]
    fn test_relation_config_diff_drop() {
        let prev_component = MockComponent {
            type_name: TYPE_NAME,
            diff_fn: diff::desired_state,
            to_jinja_fn: to_jinja,
            value: 1,
        };
        let prev = RelationConfig::new(
            [Box::new(prev_component) as Box<dyn ComponentConfig>],
            return_true,
        );
        let next = RelationConfig::new([], return_true);
        let changeset = RelationConfig::diff(&next, &prev);
        assert!(changeset.requires_full_refresh());
        assert_eq!(changeset.0.len(), 1);
        let change = changeset.get(TYPE_NAME);
        assert_component_config_change_eq::<MockComponent>(change, &ComponentConfigChange::Drop);
    }

    #[test]
    fn test_diff_changed_keys_no_changes() {
        let hashmap = IndexMap::from([("a", 1), ("b", 2)]);
        let diff = diff::changed_keys(&hashmap, &hashmap);
        assert!(diff.is_none());
    }

    #[test]
    fn test_diff_changed_keys_with_changes() {
        let prev = IndexMap::from([("a", 1), ("b", 2)]);
        let next = IndexMap::from([("a", 1), ("b", 3)]);
        let diff = diff::changed_keys(&next, &prev);
        let expected = Some(IndexMap::from([("b", 3)]));
        assert_eq!(diff, expected);
    }

    #[test]
    fn test_diff_changed_keys_dropped_key() {
        let prev = IndexMap::from([("a", 1), ("b", 2)]);
        let next = IndexMap::from([("a", 1)]);
        let diff = diff::changed_keys(&next, &prev);
        // Dropping key resets the value to the default
        let expected = Some(IndexMap::from([("b", 0)]));
        assert_eq!(diff, expected);
    }
}
