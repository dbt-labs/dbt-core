use crate::types::function::{ArgSpec, FunctionType};
use crate::types::{DynObject, Object, Type};
use crate::TypecheckingEventListener;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::Arc;

/// Metadata for relation objects, including valid attributes and their return types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct DbtType {}

impl Object for DbtType {
    fn get_attribute(
        &self,
        key: &str,
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        match key {
            "string_literal" => Ok(Type::Object(DynObject::new(Arc::new(
                DbtStringLiteralFunction::default(),
            )))),
            "escape_single_quotes" => Ok(Type::Object(DynObject::new(Arc::new(
                DbtEscapeSingleQuotesFunction::default(),
            )))),
            _ => {
                listener.warn(&format!("{self:?}.{key} is not supported"));
                Ok(Type::Any { hard: false })
            }
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct DbtStringLiteralFunction {}

impl FunctionType for DbtStringLiteralFunction {
    fn _resolve_arguments(
        &self,
        args: &[Type],
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        // accepts a string, returns a string
        if !matches!(args[0], Type::String(_)) && !matches!(args[0], Type::Any { hard: true }) {
            listener.warn(&format!(
                "Expected a string type for dbt string literal function argument, found {}",
                args[0]
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
    fn _resolve_arguments(
        &self,
        args: &[Type],
        listener: Rc<dyn TypecheckingEventListener>,
    ) -> Result<Type, crate::Error> {
        // accepts a string, returns a string
        if !matches!(args[0], Type::String(_)) && !matches!(args[0], Type::Any { hard: true }) {
            listener.warn(&format!(
                "Expected a string type for dbt string literal function argument, found {}",
                args[0]
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::new("value", false)]
    }
}
