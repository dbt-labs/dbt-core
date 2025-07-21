use std::sync::Arc;

use crate::types::{
    builtin::Type,
    class::ClassType,
    dict::DictType,
    function::{DynFunctionType, FunctionType},
    list::ListType,
};

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct ConfigType {}

impl ClassType for ConfigType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "get" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                ConfigGetFunction::default(),
            )))),
            "set" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                ConfigSetFunction::default(),
            )))),
            "persist_relation_docs" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                ConfigPersistRelationDocsFunction::default(),
            )))),
            "persist_column_docs" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                ConfigPersistColumnDocsFunction::default(),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct ConfigPersistRelationDocsFunction {}

impl FunctionType for ConfigPersistRelationDocsFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if !args.is_empty() {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "args type mismatch: expected 0 arguments, got {}",
                    args.len()
                ),
            ));
        }
        Ok(Type::Bool)
    }

    fn arg_names(&self) -> Vec<String> {
        vec![]
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct ConfigGetFunction {}

impl FunctionType for ConfigGetFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "args type mismatch: expected 1 argument, got {}",
                    args.len()
                ),
            ));
        }
        match &args[0] {
            Type::String(Some(key)) => match key.as_str() {
                "indexes" => Ok(Type::List(ListType::new(Type::Dict(DictType::new(
                    Type::String(None),
                    Type::String(None),
                ))))),
                "full_refresh" => Ok(Type::Bool),
                "store_failures" => Ok(Type::Bool),
                _ => Err(crate::Error::new(
                    crate::error::ErrorKind::TypeError,
                    format!("invalid key: {key}"),
                )),
            },
            Type::String(None) => Ok(Type::Any { hard: true }),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!("invalid key: {:?}", args[0]),
            )),
        }
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["key".to_string()]
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct ConfigPersistColumnDocsFunction {}

impl FunctionType for ConfigPersistColumnDocsFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if !args.is_empty() {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "args type mismatch: expected 0 arguments, got {}",
                    args.len()
                ),
            ));
        }
        Ok(Type::Bool)
    }

    fn arg_names(&self) -> Vec<String> {
        vec![]
    }
}

#[derive(Default, Eq, PartialEq, Clone)]
pub struct ConfigSetFunction {}

impl std::fmt::Debug for ConfigSetFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("config.set")
    }
}

impl FunctionType for ConfigSetFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 2 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "args type mismatch: expected 2 arguments, got {}",
                    args.len()
                ),
            ));
        }
        if !args[0].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!("expected string as key, got {:?}", args[0]),
            ));
        }
        if !args[1].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!("expected string as value, got {:?}", args[1]),
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["key".to_string(), "value".to_string()]
    }
}
