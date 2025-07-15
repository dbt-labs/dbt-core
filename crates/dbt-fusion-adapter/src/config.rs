use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use serde_json::Value;
use std::collections::HashMap;

/// Configuration for adapters
#[derive(Debug, Default)]
pub struct AdapterConfig {
    db_config: HashMap<String, Value>,
}

impl AdapterConfig {
    /// Get all the top level keys in the config
    pub fn keys(&self) -> Vec<String> {
        self.db_config.keys().cloned().collect()
    }

    /// Make new config
    pub fn new(db_config: HashMap<String, Value>) -> Self {
        Self { db_config }
    }

    /// Get a value from a map or return an error.
    pub fn maybe_get_str(&self, key: &str) -> AdapterResult<Option<String>> {
        if let Some(value) = self.db_config.get(key) {
            let s = value.as_str();
            if let Some(s) = s {
                Ok(Some(s.to_string()))
            } else if let Some(n) = value.as_u64() {
                Ok(Some(n.to_string()))
            } else if let Some(i) = value.as_i64() {
                Ok(Some(i.to_string()))
            } else if let Some(f) = value.as_f64() {
                Ok(Some(f.to_string()))
            } else if let Some(b) = value.as_bool() {
                Ok(Some(b.to_string()))
            } else if value.is_null() {
                Ok(None)
            } else {
                Err(AdapterError::new(
                    AdapterErrorKind::Configuration,
                    format!("{key} value: {value} is not a string, integer, float, or boolean"),
                ))
            }
        } else {
            Ok(None)
        }
    }

    /// Get a value from a map or return an error.
    pub fn get_str(&self, key: &str) -> AdapterResult<String> {
        if let Some(s) = self.maybe_get_str(key)? {
            Ok(s)
        } else {
            Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                format!("{key} missing"),
            ))
        }
    }

    /// Get the raw config as a HashMap
    pub fn raw_config(&self) -> HashMap<String, Value> {
        self.db_config.clone()
    }
}
