use crate::types::builtin::Type;
use crate::types::class::{ClassType, DynClassType};
use crate::types::function::{DynFunctionType, FunctionType};
use crate::types::relation::RelationType;
use std::hash::Hash;
use std::sync::Arc;

/// Metadata for relation objects, including valid attributes and their return types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct AdapterType {
    // pub relation: Arc<dyn BaseRelation>,
}

impl ClassType for AdapterType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "get_relation" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                AdapterGetRelationFunction::default(),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                "Type does not support attribute access",
            )),
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct AdapterGetRelationFunction {}

impl FunctionType for AdapterGetRelationFunction {
    fn _resolve_arguments(&self, _args: &[Type]) -> Result<Type, crate::Error> {
        Ok(Type::Class(DynClassType::new(Arc::new(
            RelationType::default(),
        ))))
    }

    fn arg_names(&self) -> Vec<String> {
        vec![
            "database".to_string(),
            "schema".to_string(),
            "identifier".to_string(),
        ]
    }
}
