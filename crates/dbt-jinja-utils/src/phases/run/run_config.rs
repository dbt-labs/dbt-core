//! Module for the parse config object to be used during parsing

use std::{collections::BTreeMap, rc::Rc, sync::Arc};

use minijinja::{
    Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State, Value,
    arg_utils::ArgParser,
    listener::RenderingEventListener,
    value::{Enumerator, Object},
};

/// A struct that represents a runtime config object to be used during runtime
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// The `config` entry from `model` (converted from a ManifestModelConfig value)
    pub model_config: BTreeMap<String, Value>,
    /// A model's attributes/config values (converted from a DbtModel value)
    pub model: BTreeMap<String, Value>,
}

impl Object for RunConfig {
    /// Get the value of a key from the config
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        if key.as_str().unwrap() == "model" {
            return Some(Value::from_serialize(self.model.clone()));
        }
        self.model_config.get(key.as_str().unwrap()).cloned()
    }

    fn call_method(
        self: &Arc<Self>,
        _state: &State<'_, '_>,
        name: &str,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
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

                match self.model_config.get(&name) {
                    Some(val) => {
                        if val.is_none() {
                            Ok(default)
                        } else {
                            Ok(val.clone())
                        }
                    }
                    _ => Ok(default),
                }
            }
            // At compile time, this just returns an empty string
            "set" => {
                let mut args = ArgParser::new(args, None);
                let _: String = args.get("name")?;
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
                let persist_docs = match self.model_config.get("persist_docs") {
                    Some(val) if !val.is_none() => val,
                    _ => &default_value,
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
                let persist_docs = match self.model_config.get("persist_docs") {
                    Some(val) if !val.is_none() => val,
                    _ => &default_value,
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
                MinijinjaErrorKind::UnknownMethod,
                format!("Unknown method on parse: {name}"),
            )),
        }
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        let keys = self
            .model_config
            .keys()
            .map(|k| Value::from(k.to_string()))
            .collect::<Vec<_>>();
        Enumerator::Iter(Box::new(keys.into_iter()))
    }
}
