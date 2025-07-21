use crate::error::Error;
use crate::types::builtin::Type;
use crate::types::class::{ClassType, DynClassType};
use crate::types::function::{DynFunctionType, FunctionType};
use crate::types::relation::RelationType;
use std::hash::Hash;
use std::sync::Arc;

#[derive(Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct ApiType {}

impl std::fmt::Debug for ApiType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("api")
    }
}

impl ClassType for ApiType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "Column" => Ok(Type::Class(DynClassType::new(Arc::new(
                ApiColumnType::default(),
            )))),
            "Relation" => Ok(Type::Class(DynClassType::new(Arc::new(
                RelationType::default(),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct ApiColumnType {
    // pub relation: Arc<dyn BaseRelation>,
}

impl std::fmt::Debug for ApiColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("api.Column")
    }
}

impl ClassType for ApiColumnType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "from_description" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                ApiColumnFromDescriptionFunction::default(),
            )))),
            "get" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                ApiColumnGetFunction::default(),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }

    fn constructor(&self, _args: &[Type]) -> Result<Type, crate::Error> {
        // TODO: args
        Ok(Type::Class(DynClassType::new(Arc::new(
            ApiColumnType::default(),
        ))))
    }

    fn subscript(&self, index: &Type) -> Result<Type, crate::Error> {
        match index {
            Type::String(Some(index)) => match index.as_str() {
                "name" => Ok(Type::String(None)),
                "data_type" => Ok(Type::String(None)),
                _ => Err(crate::Error::new(
                    crate::error::ErrorKind::InvalidOperation,
                    format!("{self:?} does not support subscript"),
                )),
            },
            Type::String(None) => Ok(Type::Any { hard: true }),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?} does not support subscript"),
            )),
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct ApiColumnFromDescriptionFunction {}

impl FunctionType for ApiColumnFromDescriptionFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        for arg in args {
            if !arg.is_subtype_of(&Type::String(None)) {
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

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct ApiColumnGetFunction {}

impl FunctionType for ApiColumnGetFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "args type mismatch: expected 1 argument, got {}",
                    args.len()
                ),
            ));
        }
        match &args[0] {
            Type::String(Some(name)) => match name.as_str() {
                "name" => Ok(Type::String(None)),
                "data_type" => Ok(Type::String(None)),
                "quote" => Ok(Type::String(None)),
                _ => Err(Error::new(
                    crate::error::ErrorKind::InvalidOperation,
                    format!("{self:?}.get({name}) is not supported"),
                )),
            },
            Type::String(None) => Ok(Type::Any { hard: true }),
            _ => Err(Error::new(
                crate::error::ErrorKind::TypeError,
                format!("args type mismatch: expected String, got {:?}", args[0]),
            )),
        }
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["name".to_string()]
    }
}
