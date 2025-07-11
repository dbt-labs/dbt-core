use crate::error::Error;
use crate::types::builtin::Type;
use crate::types::class::{ClassType, DynClassType};
use crate::types::function::{DynFunctionType, FunctionType};
use std::hash::Hash;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct ApiType {
    // pub relation: Arc<dyn BaseRelation>,
}

impl ClassType for ApiType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "Column" => Ok(Type::Class(DynClassType::new(Arc::new(
                ApiColumnType::default(),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                "Type does not support attribute access",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct ApiColumnType {
    // pub relation: Arc<dyn BaseRelation>,
}

impl ClassType for ApiColumnType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "from_description" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                ApiColumnFromDescriptionFunction::default(),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                "Type does not support attribute access",
            )),
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct ApiColumnFromDescriptionFunction {}

impl FunctionType for ApiColumnFromDescriptionFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        for arg in args {
            if arg.coerce(&Type::String).is_none() {
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
