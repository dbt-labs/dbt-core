use crate::types::{builtin::Type, class::ClassType};

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct LoopType;

impl ClassType for LoopType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "last" => Ok(Type::Bool),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Unknown attribute: {self:?}.{key}"),
            )),
        }
    }
}
