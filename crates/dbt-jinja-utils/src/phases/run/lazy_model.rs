//! Lazy loading wrapper for model objects in Jinja context
//!
//! This provides lazy loading for compiled_code and compiled_sql fields to reduce memory pressure
//! by only reading the compiled files when accessed. Files are read fresh each access with no caching
//! to minimize memory usage.

use std::path::PathBuf;
use std::sync::Arc;

use indexmap::IndexMap;
use minijinja::value::{Object, Value as MinijinjaValue};

/// Wrapper for model objects that provides lazy loading for compiled fields
#[derive(Debug)]
pub struct LazyModelWrapper {
    /// The original model data as a map
    model_map: IndexMap<String, MinijinjaValue>,
    /// Path to the compiled SQL file
    compiled_path: PathBuf,
}

impl LazyModelWrapper {
    /// Create a new lazy model wrapper
    pub fn new(model_map: IndexMap<String, MinijinjaValue>, compiled_path: PathBuf) -> Self {
        Self {
            model_map,
            compiled_path,
        }
    }

    /// Load the compiled SQL content (no caching - read fresh each time)
    fn load_compiled_sql(&self) -> Option<String> {
        // Try to read the compiled SQL file
        std::fs::read_to_string(&self.compiled_path).ok()
    }
}

impl Object for LazyModelWrapper {
    fn get_value(self: &Arc<Self>, key: &MinijinjaValue) -> Option<MinijinjaValue> {
        let key_str = key.as_str()?;

        // Handle lazy-loaded fields
        match key_str {
            "compiled_code" | "compiled_sql" => {
                // Both fields return the same compiled SQL content
                self.load_compiled_sql().map(MinijinjaValue::from)
            }
            _ => {
                // For all other fields, get from the model map
                self.model_map.get(key_str).cloned()
            }
        }
    }

    fn enumerate(self: &Arc<Self>) -> minijinja::value::Enumerator {
        // Only enumerate fields from model_map (not lazy-loaded fields)
        // This ensures serialization includes all fields like "resource_type"
        let keys: Vec<MinijinjaValue> = self
            .model_map
            .keys()
            .map(|k| MinijinjaValue::from(k.as_str()))
            .collect();

        minijinja::value::Enumerator::Iter(Box::new(keys.into_iter()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lazy_model_wrapper_basic() {
        // Create a model map
        let mut model_map = IndexMap::new();
        model_map.insert("name".to_string(), MinijinjaValue::from("test_model"));
        model_map.insert("version".to_string(), MinijinjaValue::from(2));

        // Create the lazy wrapper with a non-existent file path (will test missing file scenario)
        let wrapper = Arc::new(LazyModelWrapper::new(
            model_map,
            PathBuf::from("/non/existent/path.sql"),
        ));

        // Test accessing regular fields
        assert_eq!(
            wrapper.get_value(&MinijinjaValue::from("name")),
            Some(MinijinjaValue::from("test_model"))
        );
        assert_eq!(
            wrapper.get_value(&MinijinjaValue::from("version")),
            Some(MinijinjaValue::from(2))
        );

        // Test that accessing compiled_code returns None when file doesn't exist
        let compiled_code = wrapper.get_value(&MinijinjaValue::from("compiled_code"));
        assert!(compiled_code.is_none());

        // Test that accessing compiled_sql returns None when file doesn't exist
        let compiled_sql = wrapper.get_value(&MinijinjaValue::from("compiled_sql"));
        assert!(compiled_sql.is_none());
    }

    #[test]
    fn test_lazy_compiled_fields() {
        // Test that lazy-loaded fields are accessible
        let mut model_map = IndexMap::new();
        model_map.insert("name".to_string(), MinijinjaValue::from("test_model"));

        // Create a temp file with compiled SQL for testing
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("test_compiled.sql");
        std::fs::write(&test_file, "SELECT * FROM table").unwrap();

        let wrapper = Arc::new(LazyModelWrapper::new(model_map, test_file.clone()));

        // Test that compiled_code loads the SQL
        let compiled_code = wrapper.get_value(&MinijinjaValue::from("compiled_code"));
        assert_eq!(
            compiled_code.and_then(|v| v.as_str().map(String::from)),
            Some("SELECT * FROM table".to_string())
        );

        // Test that compiled_sql loads the same SQL
        let compiled_sql = wrapper.get_value(&MinijinjaValue::from("compiled_sql"));
        assert_eq!(
            compiled_sql.and_then(|v| v.as_str().map(String::from)),
            Some("SELECT * FROM table".to_string())
        );

        // Clean up
        std::fs::remove_file(&test_file).ok();
    }
}
