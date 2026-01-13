//! Agate data type objects
//!
//! In Python agate, column types are objects with methods like `jsonify()` and `cast()`.
//! This module provides the Rust equivalent.

use std::rc::Rc;
use std::sync::Arc;

use minijinja::listener::RenderingEventListener;
use minijinja::value::Object;
use minijinja::{Error, ErrorKind, State, Value};

/// Represents an agate data type with methods for type conversion
#[derive(Debug, Clone)]
pub struct DataType {
    /// The name of the data type ("Text", "Number", "Boolean", "Date", "DateTime", "TimeDelta")
    type_name: String,
}

impl DataType {
    /// Create a new DataType from a type name string
    pub fn new(type_name: String) -> Self {
        Self { type_name }
    }

    /// Get the type name
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// The `jsonify` method converts a value to a JSON-serializable format
    ///
    /// This is used by elementary's `agate_to_dicts` macro to serialize values.
    fn jsonify(&self, value: &Value) -> Result<Value, Error> {
        // Handle None/null values
        if value.is_none() || value.is_undefined() {
            return Ok(Value::from(()));
        }

        // For most types, the value is already JSON-serializable
        // minijinja::Value already handles numbers, strings, bools, etc.
        // We just need to pass through the value or convert it to string for dates
        match self.type_name.as_str() {
            "Text" => {
                // Text values should be converted to strings
                if let Some(s) = value.as_str() {
                    Ok(Value::from(s))
                } else {
                    Ok(Value::from(value.to_string()))
                }
            }
            "Number" | "Boolean" => {
                // Numbers and booleans are already JSON-serializable in minijinja
                Ok(value.clone())
            }
            "Date" | "DateTime" | "TimeDelta" => {
                // Date/time values should be converted to ISO strings
                // For now, just convert to string representation
                Ok(Value::from(value.to_string()))
            }
            _ => {
                // Unknown types, just pass through
                Ok(value.clone())
            }
        }
    }
}

impl Object for DataType {
    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        name: &str,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        match name {
            "jsonify" => {
                if args.len() != 1 {
                    return Err(Error::new(
                        ErrorKind::InvalidOperation,
                        format!("jsonify() takes exactly 1 argument ({} given)", args.len()),
                    ));
                }
                self.jsonify(&args[0])
            }
            _ => Err(Error::new(
                ErrorKind::UnknownMethod,
                format!("DataType has no method named '{}'", name),
            )),
        }
    }

    fn render(self: &Arc<Self>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<class 'agate.data_types.{}'>", self.type_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use minijinja::Environment;

    #[test]
    fn test_data_type_jsonify_text() {
        let env = Environment::new();
        let state = env.empty_state();
        let dt = Arc::new(DataType::new("Text".to_string()));

        let result = dt
            .call_method(&state, "jsonify", &[Value::from("hello")], &[])
            .unwrap();
        assert_eq!(result, Value::from("hello"));
    }

    #[test]
    fn test_data_type_jsonify_number() {
        let env = Environment::new();
        let state = env.empty_state();
        let dt = Arc::new(DataType::new("Number".to_string()));

        let result = dt
            .call_method(&state, "jsonify", &[Value::from(42)], &[])
            .unwrap();
        assert_eq!(result, Value::from(42));
    }

    #[test]
    fn test_data_type_jsonify_boolean() {
        let env = Environment::new();
        let state = env.empty_state();
        let dt = Arc::new(DataType::new("Boolean".to_string()));

        let result = dt
            .call_method(&state, "jsonify", &[Value::from(true)], &[])
            .unwrap();
        assert_eq!(result, Value::from(true));
    }

    #[test]
    fn test_data_type_jsonify_null() {
        let env = Environment::new();
        let state = env.empty_state();
        let dt = Arc::new(DataType::new("Text".to_string()));

        let result = dt
            .call_method(&state, "jsonify", &[Value::from(())], &[])
            .unwrap();
        assert_eq!(result, Value::from(()));
    }
}
