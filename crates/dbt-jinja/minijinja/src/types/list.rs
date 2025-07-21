use std::sync::Arc;

use crate::types::{
    builtin::Type,
    class::ClassType,
    function::{DynFunctionType, FunctionType},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ListType {
    pub element: Box<Type>,
}

impl ListType {
    pub fn new(element: Type) -> Self {
        Self {
            element: Box::new(element),
        }
    }
}

impl ClassType for ListType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "append" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                ListAppendFunctionType::default(),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }

    fn subscript(&self, index: &Type) -> Result<Type, crate::Error> {
        match index {
            Type::Integer(_) => Ok(*self.element.clone()),
            Type::Any { hard: true } => Ok(Type::Any { hard: true }),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Failed to subscript {self:?} with {index:?}"),
            )),
        }
    }
}

#[derive(Debug, Default)]
pub struct ListAppendFunctionType;

impl FunctionType for ListAppendFunctionType {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                "append takes exactly one argument",
            ));
        }
        Ok(Type::None)
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["item".to_string()]
    }
}
