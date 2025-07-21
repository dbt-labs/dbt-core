use crate::types::{builtin::Type, function::FunctionType, list::ListType};

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

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct StringReplaceFunction {}

impl FunctionType for StringReplaceFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 3 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "args type mismatch: expected 3 arguments, got {}",
                    args.len()
                ),
            ));
        }
        let str = args[0].clone();
        let old = args[1].clone();
        let new = args[2].clone();
        if matches!(str, Type::String(_))
            || matches!(old, Type::String(_))
            || matches!(new, Type::String(_))
        {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "args type mismatch: expected 3 arguments, got {}",
                    args.len()
                ),
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["str".to_string(), "old".to_string(), "new".to_string()]
    }
}

#[derive(Default, Eq, PartialEq, Clone)]
pub struct StringSplitFunction {}

impl std::fmt::Debug for StringSplitFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("string.split")
    }
}

impl FunctionType for StringSplitFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "args type mismatch: expected 1 argument, got {}",
                    args.len()
                ),
            ));
        }
        if !args[0].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a string argument for split function",
            ));
        }
        Ok(Type::List(ListType::new(Type::String(None))))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["sep".to_string()]
    }
}

#[derive(Default, Eq, PartialEq, Clone)]
pub struct StringFormatFunction {}

impl std::fmt::Debug for StringFormatFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("string.format")
    }
}

impl FunctionType for StringFormatFunction {
    fn _resolve_arguments(&self, _args: &[Type]) -> Result<Type, crate::Error> {
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["format_string".to_string()]
    }
}
