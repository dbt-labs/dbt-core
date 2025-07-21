use crate::types::{builtin::Type, class::ClassType};

#[derive(Default, Clone, Eq, PartialEq)]
pub struct FlagsType;

impl std::fmt::Debug for FlagsType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("flags")
    }
}

impl ClassType for FlagsType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "FULL_REFRESH" => Ok(Type::Bool),
            "STORE_FAILURES" => Ok(Type::Bool),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!("invalid key: {self:?}.{key}"),
            )),
        }
    }
}
