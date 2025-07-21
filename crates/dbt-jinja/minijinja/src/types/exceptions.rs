use std::fmt;
use std::sync::Arc;

use crate::types::builtin::Type;
use crate::types::class::{ClassType, DynClassType};
use crate::types::function::{DynFunctionType, FunctionType};
use crate::types::model::ModelType;

#[derive(Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExceptionsType;

impl fmt::Debug for ExceptionsType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExceptionsType")
    }
}

impl ClassType for ExceptionsType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "raise_not_implemented" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                RaiseNotImplementedFunctionType::default(),
            )))),
            "raise_compiler_error" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                RaiseCompilerErrorFunctionType::default(),
            )))),
            "column_type_missing" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                ColumnTypeMissingFunctionType::default(),
            )))),
            "warn" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                WarnFunctionType::default(),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Unknown attribute: {self:?}.{key}"),
            )),
        }
    }
}

#[derive(Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RaiseNotImplementedFunctionType;

impl fmt::Debug for RaiseNotImplementedFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "exceptions.raise_not_implemented")
    }
}

impl FunctionType for RaiseNotImplementedFunctionType {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Expected 1 argument, got {}", args.len()),
            ));
        }
        if !matches!(args[0], Type::String(_)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Expected string, got {:?}", args[0]),
            ));
        }
        Ok(Type::None)
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["message".to_string()]
    }
}

#[derive(Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RaiseCompilerErrorFunctionType;

impl fmt::Debug for RaiseCompilerErrorFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "exceptions.raise_compiler_error")
    }
}

impl FunctionType for RaiseCompilerErrorFunctionType {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.is_empty() || args.len() > 2 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Expected 1 or 2 arguments, got {}", args.len()),
            ));
        }
        if !args[0].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Expected string, got {:?}", args[0]),
            ));
        }
        if args.len() == 2
            && !args[1].is_subtype_of(&Type::Class(DynClassType::new(Arc::new(
                ModelType::default(),
            ))))
        {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Expected model, got {:?}", args[1]),
            ));
        }
        Ok(Type::None)
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["message".to_string(), "model".to_string()]
    }
}

#[derive(Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnTypeMissingFunctionType;

impl fmt::Debug for ColumnTypeMissingFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "exceptions.column_type_missing")
    }
}

impl FunctionType for ColumnTypeMissingFunctionType {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Expected 1 argument, got {}", args.len()),
            ));
        }
        if !matches!(
            args[0],
            Type::List(_) | Type::Iterable(_) | Type::Any { hard: true }
        ) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Expected list, got {:?}", args[0]),
            ));
        }
        Ok(Type::None)
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["column_names".to_string()]
    }
}

#[derive(Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WarnFunctionType;

impl fmt::Debug for WarnFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "exceptions.warning")
    }
}

impl FunctionType for WarnFunctionType {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Expected 1 argument, got {}", args.len()),
            ));
        }
        if !matches!(args[0], Type::String(_)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Expected string, got {:?}", args[0]),
            ));
        }
        Ok(Type::None)
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["message".to_string()]
    }
}
