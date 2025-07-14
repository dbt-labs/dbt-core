use crate::types::builtin::Type;
use crate::types::iterable::IterableType;
use crate::types::list::ListType;
use crate::types::utils::CodeLocation;
use std::fmt;
use std::hash::{Hash, Hasher};

// Import the type_erase macro
use super::type_erase::type_erase;

pub trait FunctionType: Send + Sync + std::fmt::Debug {
    fn resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        let mut args = Vec::new();
        let mut kwargs_map = std::collections::BTreeMap::new();
        let mut has_kwargs = false;

        for arg in actual_arguments {
            if let Type::Kwargs(kwargs) = arg {
                has_kwargs = true;
                for (k, v) in kwargs.iter() {
                    kwargs_map.insert(k.clone(), v.as_ref().clone());
                }
            } else {
                //positional arguments are always before kwargs
                args.push(arg.clone());
            }
        }

        if has_kwargs {
            // Fill in positional arguments first, then fill from kwargs by arg_names order
            let arg_names = self.arg_names();
            let mut sorted_args = Vec::new();
            let mut positional_index = 0;

            for name in arg_names {
                if positional_index < args.len() {
                    // Use positional argument if available
                    sorted_args.push(args[positional_index].clone());
                    positional_index += 1;
                } else if let Some(val) = kwargs_map.get(&name) {
                    // Use kwarg if available
                    sorted_args.push(val.clone());
                } else {
                    // Missing argument, use Type::Any as fallback
                    // This is for the case where the previous arguments have default values and users do not provide them
                    sorted_args.push(Type::Any { hard: true });
                }
            }
            self._resolve_arguments(&sorted_args)
        } else {
            self._resolve_arguments(&args)
        }
    }

    fn arg_names(&self) -> Vec<String>;

    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error>;
}

// Type-erased version of FunctionType
type_erase! {
    pub trait FunctionType => DynFunctionType {
        fn resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error>;
        fn arg_names(&self) -> Vec<String>;
        fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error>;
    }
}

impl std::fmt::Debug for DynFunctionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.type_debug())
    }
}

impl PartialEq for DynFunctionType {
    fn eq(&self, other: &Self) -> bool {
        // Compare by pointer equality for type-erased objects
        self.ptr == other.ptr && self.vtable == other.vtable
    }
}

impl Eq for DynFunctionType {}

impl Hash for DynFunctionType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the pointer and vtable
        self.ptr.hash(state);
        self.vtable.hash(state);
    }
}

impl PartialOrd for DynFunctionType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DynFunctionType {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by pointer values
        match self.ptr.cmp(&other.ptr) {
            std::cmp::Ordering::Equal => self.vtable.cmp(&other.vtable),
            other => other,
        }
    }
}

unsafe impl Send for DynFunctionType {}
unsafe impl Sync for DynFunctionType {}

#[derive(Clone, Eq, PartialEq)]
pub struct UserDefinedFunctionType {
    pub name: String,
    pub args: Vec<Type>,
    pub ret_type: Type,
    pub location: CodeLocation,
}

impl fmt::Debug for UserDefinedFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl UserDefinedFunctionType {
    pub fn new(name: &str, args: Vec<Type>, ret_type: Type, location: CodeLocation) -> Self {
        Self {
            name: name.to_string(),
            args,
            ret_type,
            location,
        }
    }
}

impl FunctionType for UserDefinedFunctionType {
    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        // match the actual arguments with the expected arguments, if matches return Ok else Err
        if self.args.len() != actual_arguments.len() {
            Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "Argument number mismatch: expected {}, got {}",
                    self.args.len(),
                    actual_arguments.len()
                ),
            ))
        } else {
            for (i, (expected, actual)) in self.args.iter().zip(actual_arguments).enumerate() {
                if !actual.is_subtype_of(expected) {
                    return Err(crate::Error::new(
                        crate::error::ErrorKind::TypeError,
                        format!(
                            "Argument type mismatch: expected {expected:?}, got {actual:?}, at index {i}"
                        ),
                    ));
                }
            }
            Ok(self.ret_type.clone())
        }
    }

    fn arg_names(&self) -> Vec<String> {
        self.args
            .iter()
            .enumerate()
            .map(|(i, _)| format!("arg{i}"))
            .collect()
    }
}

pub struct UndefinedFunctionType {
    pub name: String,
    pub location: CodeLocation,
}

impl fmt::Debug for UndefinedFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name.to_string())
    }
}

impl UndefinedFunctionType {
    pub fn new(name: &str, location: CodeLocation) -> Self {
        Self {
            name: name.to_string(),
            location,
        }
    }
}

impl FunctionType for UndefinedFunctionType {
    fn _resolve_arguments(&self, _actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        Err(crate::Error::new(
            crate::error::ErrorKind::TypeError,
            format!("Function {} @ {} is not defined", self.name, self.location),
        ))
    }

    fn arg_names(&self) -> Vec<String> {
        vec![]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct StoreResultFunctionType;

impl FunctionType for StoreResultFunctionType {
    fn _resolve_arguments(&self, _actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        // TODO: check args
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec![
            "name".to_string(),
            "response".to_string(),
            "agate_table".to_string(),
        ]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct LoadResultFunctionType;

impl FunctionType for LoadResultFunctionType {
    fn _resolve_arguments(&self, _actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        // TODO: check args and return the result type
        Ok(Type::Any { hard: true })
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["name".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct StoreRawResultFunctionType;

impl FunctionType for StoreRawResultFunctionType {
    fn _resolve_arguments(&self, _actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        // TODO: check args
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec![
            "name".to_string(),
            "message".to_string(),
            "code".to_string(),
            "rows_affected".to_string(),
            "agate_table".to_string(),
        ]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct RefFunctionType;

impl FunctionType for RefFunctionType {
    fn _resolve_arguments(&self, _actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["name".to_string(), "namespace".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct SourceFunctionType;

impl FunctionType for SourceFunctionType {
    fn _resolve_arguments(&self, _actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["name".to_string(), "namespace".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct DiffOfTwoDictsFunctionType;

impl FunctionType for DiffOfTwoDictsFunctionType {
    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        // TODO: check args
        Ok(actual_arguments[0].clone())
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["dict_a".to_string(), "dict_b".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct LogFunctionType;

impl FunctionType for LogFunctionType {
    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        if actual_arguments.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "log requires exactly 1 argument",
            ));
        }
        if !matches!(
            actual_arguments[0],
            Type::String(_) | Type::Integer(_) | Type::Float | Type::Bool
        ) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "log requires a string argument",
            ));
        }
        Ok(Type::None)
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["message".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct LengthFunctionType;

impl FunctionType for LengthFunctionType {
    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        if actual_arguments.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "length requires exactly 1 argument",
            ));
        }
        if !matches!(
            actual_arguments[0],
            Type::List(_)
                | Type::String(_)
                | Type::Dict(_)
                | Type::Iterable(_)
                | Type::Any { hard: true }
        ) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "length requires a list argument, got {:?}",
                    actual_arguments[0]
                ),
            ));
        }
        Ok(Type::Integer(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["value".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct JoinFunctionType;

impl FunctionType for JoinFunctionType {
    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        if actual_arguments.len() != 2 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "join requires exactly 2 arguments",
            ));
        }
        let iterable = actual_arguments[0].clone();
        let separator = actual_arguments[1].clone();
        if !matches!(iterable, Type::List(_) | Type::Iterable(_)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "join requires a list or iterable argument as the first argument, got {iterable:?}"
                ),
            ));
        }
        if !matches!(separator, Type::String(_)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "join requires a string argument as the second argument, got {separator:?}"
                ),
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["iterable".to_string(), "separator".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct MapFunctionType {}

impl FunctionType for MapFunctionType {
    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        if actual_arguments.len() != 2 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "map requires exactly 2 arguments",
            ));
        }
        if let Type::String(Some(key)) = &actual_arguments[1] {
            let element = match &actual_arguments[0] {
                Type::List(ListType { element }) | Type::Iterable(IterableType { element }) => {
                    element.get_attribute(key.as_str())
                }
                Type::Any { hard: true } => Ok(Type::Any { hard: true }),
                _ => {
                    return Err(crate::Error::new(
                        crate::error::ErrorKind::TypeError,
                        format!(
                            "map requires a list or iterable argument as the first argument, got {:?}",
                            actual_arguments[0]
                        ),
                    ));
                }
            }?;
            Ok(Type::Iterable(IterableType::new(
                element.get_attribute(key.as_str())?,
            )))
        } else {
            Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "map requires a literal string argument as the second argument, got {:?}",
                    actual_arguments[1]
                ),
            ))
        }
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["iterable".to_string(), "attribute".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct ListFunctionType;

impl FunctionType for ListFunctionType {
    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        if actual_arguments.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "list requires exactly 1 argument",
            ));
        }
        let element = match &actual_arguments[0] {
            Type::List(ListType { element }) | Type::Iterable(IterableType { element }) => element,
            _ => {
                return Err(crate::Error::new(
                    crate::error::ErrorKind::TypeError,
                    format!(
                        "list requires a list or iterable argument, got {:?}",
                        actual_arguments[0]
                    ),
                ));
            }
        };
        Ok(Type::List(ListType::new(*element.clone())))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["iterable".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct CastFunctionType;

impl FunctionType for CastFunctionType {
    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        if actual_arguments.len() != 2 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "cast requires exactly 1 argument",
            ));
        }
        if !matches!(actual_arguments[0], Type::String(_)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "cast requires a string argument as the first argument",
            ));
        }
        if !matches!(actual_arguments[1], Type::String(_)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "cast requires a string argument as the second argument",
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["type".to_string(), "value".to_string()]
    }
}
