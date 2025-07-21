use std::sync::Arc;

use crate::types::{
    builtin::Type,
    class::ClassType,
    function::{DynFunctionType, FunctionType},
    iterable::IterableType,
    tuple::TupleType,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DictType {
    pub key: Box<Type>,
    pub value: Box<Type>,
}

impl DictType {
    pub fn new(key: Type, value: Type) -> Self {
        Self {
            key: Box::new(key),
            value: Box::new(value),
        }
    }
}

impl ClassType for DictType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "items" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                DictItemsFunction::new(*self.key.clone(), *self.value.clone()),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Dict.{key} is not supported"),
            )),
        }
    }

    fn subscript(&self, index: &Type) -> Result<Type, crate::Error> {
        match index {
            Type::String(_) | Type::Integer(_) => Ok(*self.value.clone()),
            Type::Any { hard: true } => Ok(Type::Any { hard: true }),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Failed to subscript {self:?} with {index:?}"),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DictItemsFunction {
    pub key: Box<Type>,
    pub value: Box<Type>,
}

impl DictItemsFunction {
    pub fn new(key: Type, value: Type) -> Self {
        Self {
            key: Box::new(key),
            value: Box::new(value),
        }
    }
}

impl FunctionType for DictItemsFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if !args.is_empty() {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                "Dict.items() takes 0 arguments",
            ));
        }
        let element_type =
            Type::Tuple(TupleType::new(vec![*self.key.clone(), *self.value.clone()]));
        Ok(Type::Iterable(IterableType::new(element_type)))
    }

    fn arg_names(&self) -> Vec<String> {
        vec![]
    }
}
