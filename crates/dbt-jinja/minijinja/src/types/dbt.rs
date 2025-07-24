use crate::error::Error;
use crate::types::builtin::Type;
use crate::types::class::ClassType;
use crate::types::function::{ArgSpec, DynFunctionType, FunctionType};
use std::hash::Hash;
use std::sync::Arc;

/// Metadata for relation objects, including valid attributes and their return types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct DbtType {}

impl ClassType for DbtType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "string_literal" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                DbtStringLiteralFunction::default(),
            )))),
            "escape_single_quotes" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                DbtEscapeSingleQuotesFunction::default(),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct DbtStringLiteralFunction {}

impl FunctionType for DbtStringLiteralFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        // accepts a string, returns a string
        if args.len() != 1 {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "Expected 1 argument for dbt string literal function, found {}",
                    args.len()
                ),
            ));
        }
        if !matches!(args[0], Type::String(_)) && !matches!(args[0], Type::Any { hard: true }) {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "Expected a string type for dbt string literal function argument, found {}",
                    args[0]
                ),
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::new("value", false)]
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct DbtEscapeSingleQuotesFunction {}

impl FunctionType for DbtEscapeSingleQuotesFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        // accepts a string, returns a string
        if args.len() != 1 {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "Expected 1 argument for dbt escape single quotes function, found {}",
                    args.len()
                ),
            ));
        }
        if !matches!(args[0], Type::String(_)) && !matches!(args[0], Type::Any { hard: true }) {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "Expected a string type for dbt string literal function argument, found {}",
                    args[0]
                ),
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::new("value", false)]
    }
}
