use crate::error::Error;
use crate::value::{Object, Value};
use crate::vm::types::builtin::Type;
use crate::vm::types::function::FunctionType;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::sync::Arc;

/// Metadata for relation objects, including valid attributes and their return types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct RelationType {
    pub valid_attr: Vec<String>,
    pub attr_ret_types: BTreeMap<String, Type>,
    // pub relation: Arc<dyn BaseRelation>,
}

impl Object for RelationType {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str().unwrap_or("default") {
            "database" => Some(Value::from(Type::String)),
            "schema" => Some(Value::from(Type::String)),
            "identifier" => Some(Value::from(Type::String)),
            "type" => Some(Value::from(Type::String)),
            "is_table" => Some(Value::from(Type::Bool)),
            "is_view" => Some(Value::from(Type::Bool)),
            "is_materialized_view" => Some(Value::from(Type::Bool)),
            "is_cte" => Some(Value::from(Type::Bool)),
            "is_pointer" => Some(Value::from(Type::Bool)),
            "can_be_renamed" => Some(Value::from(Type::Bool)),
            "can_be_replaced" => Some(Value::from(Type::Bool)),
            "include" => Some(Value::from(RelationIncludeFunction::default())),
            _ => None,
        }
    }
}
#[derive(Debug, Default)]
struct RelationIncludeFunction {}

impl From<RelationIncludeFunction> for Value {
    fn from(func: RelationIncludeFunction) -> Self {
        Value::from_object(func)
    }
}

impl FunctionType for RelationIncludeFunction {
    fn _resolve_arguments(self: &Arc<Self>, args: &[Type]) -> Result<Type, crate::Error> {
        for arg in args {
            if arg.coerce(&Type::Bool).is_none() {
                return Err(Error::new(
                    crate::error::ErrorKind::TypeError,
                    format!("Expected bool for relation include function arguments, found {arg}"),
                ));
            }
        }

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
