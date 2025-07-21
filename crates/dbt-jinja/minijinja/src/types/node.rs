use crate::types::{builtin::Type, class::ClassType};

#[derive(Default, Clone, Eq, PartialEq)]
pub struct NodeType;

impl std::fmt::Debug for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("node")
    }
}

impl ClassType for NodeType {
    fn get_attribute(&self, name: &str) -> Result<Type, crate::Error> {
        match name {
            "name" => Ok(Type::String(None)),
            "version" => Ok(Type::String(None)),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?} does not have attribute {name}"),
            )),
        }
    }
}
