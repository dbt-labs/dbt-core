use crate::error::Error;
use crate::value::{Object, Value};
use crate::vm::types::builtin::Type;
use crate::vm::types::function::FunctionType;
use std::hash::Hash;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct ApiType {
    // pub relation: Arc<dyn BaseRelation>,
}

impl Object for ApiType {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str().unwrap_or("default") {
            "Column" => Some(Value::from(Type::ApiColumn(ApiColumnType::default()))),

            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct ApiColumnType {
    // pub relation: Arc<dyn BaseRelation>,
}

impl Object for ApiColumnType {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str().unwrap_or("default") {
            "from_description" => Some(Value::from(ApiColumnFromDescriptionFunction::default())),
            _ => None,
        }
    }
}
#[derive(Debug, Default)]
struct ApiColumnFromDescriptionFunction {}

impl From<ApiColumnFromDescriptionFunction> for Value {
    fn from(func: ApiColumnFromDescriptionFunction) -> Self {
        Value::from_object(func)
    }
}

impl FunctionType for ApiColumnFromDescriptionFunction {
    fn _resolve_arguments(self: &Arc<Self>, args: &[Type]) -> Result<Type, crate::Error> {
        for arg in args {
            if arg.coerce(&Type::String).is_none() {
                return Err(Error::new(
                    crate::error::ErrorKind::TypeError,
                    format!("args type mismatch: expected String, got {arg:?}"),
                ));
            }
        }
        Ok(Type::StdColumn)
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["name".to_string(), "raw_data_type".to_string()]
    }
}
