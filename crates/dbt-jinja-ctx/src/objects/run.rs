//! Run-phase `Object` impls — std-only types moved here from
//! `dbt-jinja-utils` so the typed run-phase ctx structs can hold them via
//! `JinjaObject<T>` rather than opaque `MinijinjaValue`.
//!
//! Currently moved: `HookConfig`, `LazyModelWrapper`.
//!
//! Pending later PRs (need handle traits or `dbt-common` decoupling to move):
//! * `RunConfig` → uses `dbt_schemas::schemas::project::ConfigKeys`.
//! * `WriteConfig` → uses `dbt_common::path::get_target_write_path` +
//!   `dbt_common::constants::DBT_RUN_DIR_NAME`.

use std::path::PathBuf;
use std::sync::Arc;

use indexmap::IndexMap;
use minijinja::value::{Enumerator, Object, Value as MinijinjaValue};

/// `{{ pre_hooks[i] }}` / `{{ post_hooks[i] }}` — single hook entry from
/// the node's YAML config. Wraps the SQL string and the per-hook
/// `transaction` flag.
#[derive(Clone)]
pub struct HookConfig {
    /// SQL template the hook executes.
    pub sql: String,
    /// Whether this hook participates in the surrounding transaction.
    pub transaction: bool,
}

impl Object for HookConfig {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        match key.as_str() {
            Some("sql") => Some(MinijinjaValue::from(self.sql.clone())),
            Some("transaction") => Some(MinijinjaValue::from(self.transaction)),
            _ => None,
        }
    }
    fn render(self: &Arc<Self>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.sql)
    }
}

impl std::fmt::Debug for HookConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HookConfig {{ sql: {} }}", self.sql)
    }
}

/// `{{ model.* }}` / `{{ node.* }}` at run scope — wraps the serialized
/// node map and lazy-loads `compiled_code` / `compiled_sql` from the
/// on-disk compiled SQL file when accessed.
///
/// Files are read fresh on every attribute access (no caching) to keep
/// memory pressure low; users that read the field repeatedly in a single
/// render typically see identical bytes anyway.
#[derive(Debug)]
pub struct LazyModelWrapper {
    /// The original model data as a map.
    model_map: IndexMap<String, MinijinjaValue>,
    /// Path to the compiled SQL file.
    compiled_path: PathBuf,
}

impl LazyModelWrapper {
    /// Create a new lazy model wrapper.
    pub fn new(model_map: IndexMap<String, MinijinjaValue>, compiled_path: PathBuf) -> Self {
        Self {
            model_map,
            compiled_path,
        }
    }

    /// Load the compiled SQL content (no caching - read fresh each time).
    fn load_compiled_sql(&self) -> Option<String> {
        std::fs::read_to_string(&self.compiled_path).ok()
    }
}

impl Object for LazyModelWrapper {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        let key_str = key.as_str()?;

        match key_str {
            "compiled_code" | "compiled_sql" => {
                // Both fields return the same compiled SQL content
                self.load_compiled_sql().map(MinijinjaValue::from)
            }
            _ => self.model_map.get(key_str).cloned(),
        }
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        // Only enumerate fields from model_map (not lazy-loaded fields) so
        // serialization includes all stable fields (e.g. `resource_type`)
        // but doesn't trigger a disk read at enumerate time.
        let keys: Vec<MinijinjaValue> = self
            .model_map
            .keys()
            .map(|k| MinijinjaValue::from(k.as_str()))
            .collect();

        Enumerator::Iter(Box::new(keys.into_iter()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lazy_model_wrapper_basic() {
        let mut model_map = IndexMap::new();
        model_map.insert("name".to_string(), MinijinjaValue::from("test_model"));
        model_map.insert("version".to_string(), MinijinjaValue::from(2));

        let wrapper = Arc::new(LazyModelWrapper::new(
            model_map,
            PathBuf::from("/non/existent/path.sql"),
        ));

        assert_eq!(
            wrapper.get_value(&MinijinjaValue::from("name")),
            Some(MinijinjaValue::from("test_model"))
        );
        assert_eq!(
            wrapper.get_value(&MinijinjaValue::from("version")),
            Some(MinijinjaValue::from(2))
        );

        // Missing file → compiled_code/compiled_sql resolve to None.
        assert!(
            wrapper
                .get_value(&MinijinjaValue::from("compiled_code"))
                .is_none()
        );
        assert!(
            wrapper
                .get_value(&MinijinjaValue::from("compiled_sql"))
                .is_none()
        );
    }

    #[test]
    fn test_lazy_compiled_fields() {
        let mut model_map = IndexMap::new();
        model_map.insert("name".to_string(), MinijinjaValue::from("test_model"));

        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_compiled_pr9.sql");
        std::fs::write(&test_file, "SELECT * FROM table").unwrap();

        let wrapper = Arc::new(LazyModelWrapper::new(model_map, test_file.clone()));

        let compiled_code = wrapper.get_value(&MinijinjaValue::from("compiled_code"));
        assert_eq!(
            compiled_code.and_then(|v| v.as_str().map(String::from)),
            Some("SELECT * FROM table".to_string())
        );

        let compiled_sql = wrapper.get_value(&MinijinjaValue::from("compiled_sql"));
        assert_eq!(
            compiled_sql.and_then(|v| v.as_str().map(String::from)),
            Some("SELECT * FROM table".to_string())
        );

        std::fs::remove_file(&test_file).ok();
    }
}
