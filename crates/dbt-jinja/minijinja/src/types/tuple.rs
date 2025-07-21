use crate::types::{builtin::Type, class::ClassType};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TupleType {
    pub fields: Vec<Type>,
}

impl TupleType {
    pub fn new(fields: Vec<Type>) -> Self {
        Self { fields }
    }
}

impl ClassType for TupleType {
    fn get_attribute(&self, _key: &str) -> Result<Type, crate::Error> {
        Err(crate::Error::new(
            crate::error::ErrorKind::InvalidOperation,
            "Tuple does not support attribute access",
        ))
    }

    fn subscript(&self, index: &Type) -> Result<Type, crate::Error> {
        match index {
            Type::Integer(Some(index)) => {
                if *index < 0 || *index >= self.fields.len() as i64 {
                    return Err(crate::Error::new(
                        crate::error::ErrorKind::InvalidOperation,
                        format!("Index out of range: {index}"),
                    ));
                }
                Ok(self.fields[*index as usize].clone())
            }
            Type::Integer(None) => Ok(Type::Any { hard: true }),
            Type::Any { hard: true } => Ok(Type::Any { hard: true }),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("Failed to subscript {self:?} with {index:?}"),
            )),
        }
    }
}
