use std::{collections::BTreeMap, rc::Rc, sync::Arc};

use dbt_common::current_function_name;
use minijinja::{
    arg_utils::{check_num_args, ArgParser},
    listener::RenderingEventListener,
    value::Object,
    Error as MinijinjaError, ErrorKind as MinijinjaErrorKind, State, Value,
};
use serde::{Deserialize, Serialize};

/// Databricks relation config
#[derive(Debug, Clone, Default)]
pub struct DatabricksRelationConfig {}

impl Object for DatabricksRelationConfig {
    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        method: &str,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        match method {
            "get_changeset" => {
                let mut parser = ArgParser::new(args, None);
                check_num_args(current_function_name!(), &parser, 1, 1)?;

                let existing_config = parser
                    .get::<Value>("existing_config")?
                    .downcast_object::<DatabricksRelationConfig>()
                    .ok_or(MinijinjaError::new(
                        MinijinjaErrorKind::InvalidArgument,
                        "existing_config must be a DatabricksRelationConfig",
                    ))?;

                let result = self.get_changeset(existing_config).map(Value::from_object);
                Ok(Value::from(result))
            }
            _ => Err(MinijinjaError::new(
                MinijinjaErrorKind::InvalidOperation,
                format!("Method {} not found ", method),
            )),
        }
    }
}

impl DatabricksRelationConfig {
    fn get_changeset(&self, _existing: Arc<Self>) -> Option<DatabricksRelationChangeSet> {
        // TODO: implement
        None
    }
}

/// Databricks relation change set
#[derive(Debug, Clone)]
pub struct DatabricksRelationChangeSet {
    changes: Arc<BTreeMap<String, DatabricksComponentConfig>>,
    requires_full_refresh: bool,
}

// TODO: add fields
/// Databricks component config
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatabricksComponentConfig;

impl Object for DatabricksRelationChangeSet {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("changes") => Some(Value::from_serialize(self.changes.clone())),
            Some("requires_full_refresh") => Some(Value::from(self.requires_full_refresh)),
            _ => None,
        }
    }
}
