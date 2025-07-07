use crate::value::{Object, Value};
use crate::vm::types::builtin::Type;
use crate::vm::types::function::FunctionType;
use crate::vm::types::relation::RelationType;
use std::hash::Hash;
use std::sync::Arc;

/// Metadata for relation objects, including valid attributes and their return types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct AdapterType {
    // pub relation: Arc<dyn BaseRelation>,
}

impl Object for AdapterType {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str().unwrap_or("default") {
            "get_relation" => Some(Value::from(AdapterGetRelationFunction::default())),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
struct AdapterGetRelationFunction {}

impl From<AdapterGetRelationFunction> for Value {
    fn from(func: AdapterGetRelationFunction) -> Self {
        Value::from_object(func)
    }
}

impl FunctionType for AdapterGetRelationFunction {
    fn _resolve_arguments(self: &Arc<Self>, _args: &[Type]) -> Result<Type, crate::Error> {
        Ok(Type::Relation(RelationType::default()))
    }

    fn arg_names(&self) -> Vec<String> {
        vec![
            "database".to_string(),
            "schema".to_string(),
            "identifier".to_string(),
        ]
    }
}
