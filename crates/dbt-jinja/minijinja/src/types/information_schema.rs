use std::sync::Arc;

use crate::types::{
    builtin::Type,
    class::{ClassType, DynClassType},
    function::{ArgSpec, DynFunctionType, FunctionType},
};

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct InformationSchemaType {}

impl ClassType for InformationSchemaType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "database" => Ok(Type::String(None)),
            "schema" => Ok(Type::String(None)),
            "replace" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                InformationSchemaReplaceFunction::default(),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct InformationSchemaReplaceFunction {}

impl FunctionType for InformationSchemaReplaceFunction {
    fn _resolve_arguments(&self, _args: &[Type]) -> Result<Type, crate::Error> {
        Ok(Type::Class(DynClassType::new(Arc::new(
            InformationSchemaType::default(),
        ))))
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::new("information_schema_view", false)]
    }
}
