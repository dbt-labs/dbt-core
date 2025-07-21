use crate::types::{builtin::Type, class::ClassType, union::UnionType};

#[derive(Default, Clone, Eq, PartialEq)]
pub struct ColumnSchemaType {}

impl std::fmt::Debug for ColumnSchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("column_schema")
    }
}

impl ClassType for ColumnSchemaType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "column" => Ok(Type::String(None)),
            "dtype" => Ok(Type::String(None)),
            "char_size" => Ok(Type::Union(UnionType::new(vec![
                Type::Integer(None),
                Type::None,
            ]))),
            "numeric_precision" => Ok(Type::Union(UnionType::new(vec![
                Type::Integer(None),
                Type::None,
            ]))),
            "numeric_scale" => Ok(Type::Union(UnionType::new(vec![
                Type::Integer(None),
                Type::None,
            ]))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }
}
