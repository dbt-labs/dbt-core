use crate::types::{builtin::Type, class::ClassType};

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct BatchType {}

impl ClassType for BatchType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "id" => Ok(Type::String(None)),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }
}
