use std::{collections::BTreeMap, sync::Arc};

use crate::types::{
    builtin::Type,
    class::ClassType,
    function::{DynFunctionType, FunctionType},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StructType {
    pub fields: BTreeMap<String, Type>,
}

impl StructType {
    pub fn new(fields: BTreeMap<String, Type>) -> Self {
        Self { fields }
    }
}

impl ClassType for StructType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "get" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                StructGetFunctionType {
                    fields: self.fields.clone(),
                },
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Struct does not have field {key}"),
            )),
        }
    }

    fn subscript(&self, index: &Type) -> Result<Type, crate::Error> {
        match index {
            Type::String(Some(index)) => self.get_attribute(index),
            Type::String(None) => Ok(Type::Any { hard: true }),
            Type::Any { hard: true } => Ok(Type::Any { hard: true }),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Failed to subscript {self:?} with {index:?}"),
            )),
        }
    }
}

#[derive(Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StructGetFunctionType {
    pub fields: BTreeMap<String, Type>,
}

impl std::fmt::Debug for StructGetFunctionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StructGetFunctionType")
    }
}

impl FunctionType for StructGetFunctionType {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Expected 1 argument, got {}", args.len()),
            ));
        }
        match &args[0] {
            Type::String(Some(field_name)) => {
                if let Some(field_type) = self.fields.get(field_name) {
                    Ok(field_type.clone())
                } else {
                    Err(crate::Error::new(
                        crate::error::ErrorKind::InvalidOperation,
                        format!("Struct does not have field {field_name}"),
                    ))
                }
            }
            Type::String(None) => Ok(Type::Any { hard: true }),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Expected string, got {:?}", args[0]),
            )),
        }
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["field_name".to_string()]
    }
}
