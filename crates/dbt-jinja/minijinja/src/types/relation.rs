use crate::error::Error;
use crate::types::builtin::Type;
use crate::types::class::{ClassType, DynClassType};
use crate::types::function::{DynFunctionType, FunctionType};
use std::hash::Hash;
use std::sync::Arc;

/// Metadata for relation objects, including valid attributes and their return types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct RelationType {}

impl ClassType for RelationType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "database" => Ok(Type::String(None)),
            "schema" => Ok(Type::String(None)),
            "identifier" => Ok(Type::String(None)),
            "type" => Ok(Type::String(None)),
            "is_table" => Ok(Type::Bool),
            "is_view" => Ok(Type::Bool),
            "is_materialized_view" => Ok(Type::Bool),
            "is_cte" => Ok(Type::Bool),
            "is_pointer" => Ok(Type::Bool),
            "can_be_renamed" => Ok(Type::Bool),
            "can_be_replaced" => Ok(Type::Bool),
            "include" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                RelationIncludeFunction::default(),
            )))),
            "render" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                RelationRenderFunction::default(),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct RelationIncludeFunction {}

impl FunctionType for RelationIncludeFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        for arg in args {
            if !arg.is_subtype_of(&Type::Bool) {
                return Err(Error::new(
                    crate::error::ErrorKind::TypeError,
                    format!("Expected bool for relation include function arguments, found {arg}"),
                ));
            }
        }

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

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct RelationRenderFunction {}

impl FunctionType for RelationRenderFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if !args.is_empty() {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected no arguments for relation render function",
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec![]
    }
}
