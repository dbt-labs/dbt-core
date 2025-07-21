use crate::types::batch::BatchType;
use crate::types::builtin::Type;
use crate::types::class::{ClassType, DynClassType};
use crate::types::dict::DictType;
use crate::types::list::ListType;
use crate::types::union::UnionType;
use std::hash::Hash;
use std::sync::Arc;

/// Metadata for model objects, including valid attributes and their return types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct ModelType {}

impl ClassType for ModelType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "description" => Ok(Type::String(None)),
            "columns" => Ok(Type::Dict(DictType::new(
                Type::String(None),
                Type::List(ListType::new(Type::String(None))),
            ))),
            "batch" => Ok(Type::Union(UnionType::new(vec![
                Type::Class(DynClassType::new(Arc::new(BatchType::default()))),
                Type::None,
            ]))),
            "unique_id" => Ok(Type::String(None)),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }

    fn subscript(&self, index: &Type) -> Result<Type, crate::Error> {
        match index {
            Type::String(Some(key)) => self.get_attribute(key),
            Type::String(None) => Ok(Type::Any { hard: true }),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?} does not support subscript"),
            )),
        }
    }
}
