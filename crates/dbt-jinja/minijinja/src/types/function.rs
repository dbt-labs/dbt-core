use crate::types::builtin::Type;
use crate::types::utils::parse_type;
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
                    sorted_args.push(Type::None);
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
        write!(f, "DynFunctionType({})", self.type_name())
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
pub struct BasicFunctionType {
    pub name: String,
    pub args: Vec<Type>,
    pub ret_type: Type,
    pub location: CodeLocation,
    pub has_signature: bool,
}

impl BasicFunctionType {
    pub fn new(
        name: &str,
        args: Vec<Type>,
        ret_type: Type,
        location: CodeLocation,
        has_signature: bool,
    ) -> Self {
        Self {
            name: name.to_string(),
            args,
            ret_type,
            location,
            has_signature,
        }
    }
}

impl FunctionType for BasicFunctionType {
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
            for (expected, actual) in self.args.iter().zip(actual_arguments) {
                if expected.coerce(actual).is_none() {
                    return Err(crate::Error::new(
                        crate::error::ErrorKind::TypeError,
                        format!("Argument type mismatch: expected {expected:?}, got {actual:?}"),
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

pub fn parse_macro_signature(funcsign_str: String) -> BasicFunctionType {
    // parse the function signature string
    let parts: Vec<&str> = funcsign_str.split("->").collect();
    if parts.len() != 2 {
        panic!("Invalid function signature format");
    }
    let sig = parts[0].trim();
    let ret_type_str = parts[1].trim();

    // Find function name and parameter list
    if let Some(paren_start) = sig.find('(') {
        let params_str = &sig[paren_start + 1..sig.len() - 1]; // remove parentheses
        let args = if params_str.trim().is_empty() {
            Vec::new()
        } else {
            params_str
                .split(',')
                .map(|s| parse_type(s.trim()))
                .collect()
        };
        let ret_type = parse_type(ret_type_str);
        BasicFunctionType {
            name: String::new(),
            args,
            ret_type,
            location: CodeLocation::default(),
            has_signature: true,
        }
    } else {
        panic!("Invalid function signature format: missing '('");
    }
}

impl std::fmt::Debug for BasicFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({}) -> {}",
            self.name,
            self.args
                .iter()
                .map(|t| format!("{t}"))
                .collect::<Vec<_>>()
                .join(", "),
            self.ret_type
        )
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct StoreResultFunctionType;

impl FunctionType for StoreResultFunctionType {
    fn _resolve_arguments(&self, _actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        // TODO: check args
        Ok(Type::String)
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
        Ok(Type::Any)
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
        Ok(Type::String)
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
