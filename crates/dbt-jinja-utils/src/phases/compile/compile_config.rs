//! Module for the parse config object to be used during parsing

use std::{collections::BTreeMap, rc::Rc, sync::Arc};

use dashmap::DashMap;
use minijinja::{
    Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State, Value,
    arg_utils::ArgParser,
    listener::RenderingEventListener,
    value::{Enumerator, Object},
};

/// A struct that represents a compile time config object to be used during compile time
#[derive(Debug, Clone)]
pub struct CompileConfig {
    /// A map of config values to be used during compile time
    pub config: Arc<DashMap<String, Value>>,
}

impl Object for CompileConfig {
    fn call(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        _args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        Ok(Value::from(""))
    }

    /// Get the value of a key from the config
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        self.config
            .get(key.as_str().unwrap())
            .map(|v| v.value().clone())
    }

    /// Flatten the wrapper struct so that this Object can be treated like a Map of the inner `config`
    /// This is critical when a Value of this is properly deserialized to the underlying type
    /// Without this, the deserialized instance has its fields all set to default value
    /// See an example of the ManifestModelConfig::deserialize usage in adapters crate
    fn enumerate(self: &Arc<Self>) -> Enumerator {
        let keys = self
            .config
            .iter()
            .map(|v| Value::from(v.key().clone()))
            .collect::<Vec<_>>();
        Enumerator::Iter(Box::new(keys.into_iter()))
    }

    fn call_method(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        name: &str,
        args: &[Value],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        match name {
            // At compile time, this will return the value of the config variable if it exists
            // Here, we just return an empty string
            "get" => {
                let mut args = ArgParser::new(args, None);
                let name: String = args.get("name")?;
                let default = args
                    .get_optional::<Value>("default")
                    .unwrap_or(Value::from(None::<Option<String>>));

                // Get the value first
                let result = match self.config.get(&name) {
                    Some(val) => val.clone(),
                    _ => default,
                };

                // Then handle validator if provided
                let validator = args.get_optional::<Value>("validator");
                if let Some(validator) = validator {
                    // Pass the actual value to the validator
                    let result = validator.call(state, &[result.clone()], listeners);
                    result?;
                }

                Ok(result)
            }
            // At compile time, this just returns an empty string
            "set" => {
                let mut args = ArgParser::new(args, None);
                let key: String = args.get("name")?;
                let value: String = args.get("value")?;
                self.config.insert(key, Value::from(value));
                Ok(Value::from(""))
            }
            // At compile time, this will throw an error if the config required does not exist
            "require" => {
                let mut args = ArgParser::new(args, None);
                let _: String = args.get("name")?;
                Ok(Value::from(""))
            }
            "persist_relation_docs" => {
                let default_value = Value::from(BTreeMap::<String, Value>::new());
                let persist_docs = match self.config.get("persist_docs") {
                    Some(val) if !val.is_none() => val.value().clone(),
                    _ => default_value,
                };
                let persist_docs_map = match persist_docs.as_object() {
                    Some(obj) => obj,
                    None => {
                        return Err(MinijinjaError::new(
                            MinijinjaErrorKind::InvalidOperation,
                            "persist_docs must be a dictionary".to_string(),
                        ));
                    }
                };

                Ok(persist_docs_map
                    .get_value(&Value::from("relation"))
                    .unwrap_or(Value::from(false)))
            }
            "persist_column_docs" => {
                let default_value = Value::from(BTreeMap::<String, Value>::new());
                let persist_docs = match self.config.get("persist_docs") {
                    Some(val) if !val.is_none() => val.value().clone(),
                    _ => default_value,
                };
                let persist_docs_map = match persist_docs.as_object() {
                    Some(obj) => obj,
                    None => {
                        return Err(MinijinjaError::new(
                            MinijinjaErrorKind::InvalidOperation,
                            "persist_docs must be a dictionary".to_string(),
                        ));
                    }
                };

                Ok(persist_docs_map
                    .get_value(&Value::from("columns"))
                    .unwrap_or(Value::from(false)))
            }
            _ => Err(MinijinjaError::new(
                MinijinjaErrorKind::UnknownMethod("CompileConfig".to_string(), name.to_string()),
                format!("Unknown method on compile config: {name}"),
            )),
        }
    }
}
