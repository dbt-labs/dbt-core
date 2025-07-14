use crate::types::{builtin::Type, function::FunctionType};

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct StringStripFunction {}

impl FunctionType for StringStripFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if !args.is_empty() {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "args type mismatch: expected 0 argument, got {}",
                    args.len()
                ),
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["str".to_string()]
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct StringLowerFunction {}

impl FunctionType for StringLowerFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if !args.is_empty() {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "args type mismatch: expected 0 argument, got {}",
                    args.len()
                ),
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["str".to_string()]
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct StringUpperFunction {}

impl FunctionType for StringUpperFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if !args.is_empty() {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "args type mismatch: expected 0 argument, got {}",
                    args.len()
                ),
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["str".to_string()]
    }
}
