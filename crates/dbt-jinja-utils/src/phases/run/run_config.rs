//! Module for the parse config object to be used during parsing

use indexmap::IndexMap;
use std::{collections::BTreeMap, rc::Rc, sync::Arc};

use dbt_common::{ErrorCode, tracing::emit::emit_warn_log_message};
use minijinja::{
    Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State, Value,
    arg_utils::ArgParser,
    listener::RenderingEventListener,
    value::{Enumerator, Object},
};

/// A struct that represents a runtime config object to be used during runtime
// TODO(anna): I would like this to be an IndexMap<String, Value>, which requires a change to dbt-jinja
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// The `config` entry from `model` (converted from a ManifestModelConfig value)
    pub model_config: IndexMap<String, Value>,
    /// A model's attributes/config values (converted from a DbtModel value)
    pub model: IndexMap<String, Value>,
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
            // At runtime, this will return the value of the config variable if it exists
            // If not found in config, checks config.meta for the key
            "get" => {
                let mut args = ArgParser::new(args, None);
                let name: String = args.get("name")?;
                let default = args
                    .get_optional::<Value>("default")
                    .unwrap_or_else(|| Value::from(None::<Option<String>>));

                match self.model_config.get(&name) {
                    Some(val) if !val.is_none() => Ok(val.clone()),
                    _ => {
                        // If not found or None in model_config, check model_config.meta.<key>
                        if let Some(meta_value) = self.model_config.get("meta") {
                            if let Some(meta_obj) = meta_value.as_object() {
                                if let Some(value) = meta_obj.get_value(&Value::from(name.clone()))
                                {
                                    if !value.is_none() {
                                        // Emit deprecation warning
                                        emit_warn_log_message(
                                            ErrorCode::Generic,
                                            format!(
                                                "DeprecationWarning: Custom config found under \"meta\" using config.get(\"{}\"). \
                                                Please replace this with config.meta_get(\"{}\") to avoid collisions with \
                                                configs introduced by dbt.",
                                                name, name
                                            ),
                                            None,
                                        );
                                        return Ok(value);
                                    }
                                }
                            }
                        }
                        Ok(default)
                    }
                }
            }
            // At runtime, this just returns an empty string
            "set" => {
                let mut args = ArgParser::new(args, None);
                let _: String = args.get("name")?;
                Ok(Value::from(""))
            }
            // At runtime, this will throw an error if the config required does not exist
            // If not found in config, checks config.meta for the key
            "require" => {
                let mut args = ArgParser::new(args, None);
                let name: String = args.get("name")?;

                // First, try to get the value from model_config.<key>
                match self.model_config.get(&name) {
                    Some(val) if !val.is_none() => Ok(val.clone()),
                    _ => {
                        // If not found or None in model_config, check model_config.meta.<key>
                        if let Some(meta_value) = self.model_config.get("meta") {
                            if let Some(meta_obj) = meta_value.as_object() {
                                if let Some(value) = meta_obj.get_value(&Value::from(name.clone()))
                                {
                                    if !value.is_none() {
                                        // Emit deprecation warning
                                        emit_warn_log_message(
                                            ErrorCode::Generic,
                                            format!(
                                                "DeprecationWarning: Custom config found under \"meta\" using config.require(\"{}\"). \
                                                Please replace this with config.meta_require(\"{}\") to avoid collisions with \
                                                configs introduced by dbt.",
                                                name, name
                                            ),
                                            None,
                                        );
                                        return Ok(value);
                                    }
                                }
                            }
                        }
                        // If not found in either model_config or meta, throw an error
                        Err(MinijinjaError::new(
                            MinijinjaErrorKind::InvalidOperation,
                            format!(
                                "Required config key '{}' not found in config or config.meta",
                                name
                            ),
                        ))
                    }
                }
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
                    .unwrap_or_else(|| Value::from(false)))
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
                    .unwrap_or_else(|| Value::from(false)))
            }
            // New method that only checks config.meta
            "meta_get" => {
                let mut args = ArgParser::new(args, None);
                let name: String = args.get("name")?;
                let default = args
                    .get_optional::<Value>("default")
                    .unwrap_or_else(|| Value::from(None::<Option<String>>));

                // Only check model_config.meta.<key>
                if let Some(meta_value) = self.model_config.get("meta") {
                    if let Some(meta_obj) = meta_value.as_object() {
                        if let Some(value) = meta_obj.get_value(&Value::from(name)) {
                            if !value.is_none() {
                                return Ok(value);
                            }
                        }
                    }
                }
                Ok(default)
            }
            // New method that only checks config.meta and requires the key
            "meta_require" => {
                let mut args = ArgParser::new(args, None);
                let name: String = args.get("name")?;

                // Only check model_config.meta.<key>
                if let Some(meta_value) = self.model_config.get("meta") {
                    if let Some(meta_obj) = meta_value.as_object() {
                        if let Some(value) = meta_obj.get_value(&Value::from(name.clone())) {
                            if !value.is_none() {
                                return Ok(value);
                            }
                        }
                    }
                }
                // If not found in meta, throw an error
                Err(MinijinjaError::new(
                    MinijinjaErrorKind::InvalidOperation,
                    format!("Required config key '{}' not found in config.meta", name),
                ))
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
