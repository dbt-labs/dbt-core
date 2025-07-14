use std::sync::Arc;

use crate::types::{
    api::ApiColumnType,
    builtin::Type,
    class::{ClassType, DynClassType},
    list::ListType,
};

#[derive(Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct AgateTableType {}

impl std::fmt::Debug for AgateTableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("agate_table")
    }
}

impl ClassType for AgateTableType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "columns" => Ok(Type::List(ListType::new(Type::Class(DynClassType::new(
                Arc::new(ApiColumnType::default()),
            ))))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }
}
