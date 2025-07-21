use crate::types::{builtin::Type, class::ClassType};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IterableType {
    pub element: Box<Type>,
}

impl IterableType {
    pub fn new(element: Type) -> Self {
        Self {
            element: Box::new(element),
        }
    }
}

impl ClassType for IterableType {
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
