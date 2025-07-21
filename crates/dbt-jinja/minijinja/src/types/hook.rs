use std::sync::Arc;

use crate::types::{
    builtin::Type,
    class::ClassType,
    function::{DynFunctionType, FunctionType},
};

#[derive(Default, Eq, PartialEq, Clone)]
pub struct HookType {}

impl std::fmt::Debug for HookType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("hook")
    }
}

impl ClassType for HookType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "sql" => Ok(Type::String(None)),
            "transaction" => Ok(Type::Bool),
            "get" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                HookGetFunction::default(),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct HookGetFunction {}

impl std::fmt::Debug for HookGetFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("hook.get")
    }
}

impl FunctionType for HookGetFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected 1 argument for hook.get",
            ));
        }
        match &args[0] {
            Type::String(Some(key)) => HookType::default().get_attribute(key),
            Type::String(None) => Ok(Type::Any { hard: true }),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!("Expected a string argument for hook.get, got {:?}", args[0]),
            )),
        }
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["key".to_string()]
    }
}
