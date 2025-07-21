use crate::types::builtin::Type;
use crate::types::class::DynClassType;
use crate::types::column_schema::ColumnSchemaType;
use crate::types::iterable::IterableType;
use crate::types::list::ListType;
use crate::types::model::ModelType;
use crate::types::utils::CodeLocation;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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
                } else if let Some(val) = kwargs_map.get(&format!("String({name})")) {
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
pub struct EnvVarFunctionType;

impl FunctionType for EnvVarFunctionType {
    fn _resolve_arguments(&self, _actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        // TODO: check args and return the result type
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["value".to_string()]
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
        } else if matches!(actual_arguments[1], Type::Any { hard: true }) {
            Ok(Type::Any { hard: true })
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
            Type::Any { hard: true } => {
                return Ok(Type::List(ListType::new(Type::Any { hard: true })))
            }
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

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct TrimFunctionType;

impl FunctionType for TrimFunctionType {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        // args[0] is necessary, args[1] is optional
        if args.is_empty() || args.len() > 2 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "trim requires 1 or 2 arguments",
            ));
        }
        if !args[0].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "trim requires a string argument as the first argument, got {:?}",
                    args[0]
                ),
            ));
        }
        if args.len() == 2 && !args[1].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "trim requires a string argument as the second argument, got {:?}",
                    args[1]
                ),
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["s".to_string(), "chars".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct UpperFunctionType;

impl FunctionType for UpperFunctionType {
    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        if actual_arguments.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "upper requires exactly 1 argument",
            ));
        }
        if !matches!(actual_arguments[0], Type::String(_))
            && !matches!(actual_arguments[0], Type::Any { hard: true })
        {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "upper requires a string argument as the first argument",
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["v".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct LowerFunctionType;

impl FunctionType for LowerFunctionType {
    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        if actual_arguments.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "lower requires exactly 1 argument",
            ));
        }
        if !matches!(actual_arguments[0], Type::String(_))
            && !matches!(actual_arguments[0], Type::Any { hard: true })
        {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "lower requires a string argument as the first argument",
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["v".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct RangeFunctionType;

impl FunctionType for RangeFunctionType {
    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        // accepts one or two integer arguments, returns a list of integers
        if actual_arguments.is_empty() || actual_arguments.len() > 2 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "range requires 1 or 2 arguments",
            ));
        }
        for arg in actual_arguments {
            if !matches!(arg, Type::Integer(_) | Type::Any { hard: true }) {
                return Err(crate::Error::new(
                    crate::error::ErrorKind::TypeError,
                    "range requires integer arguments",
                ));
            }
        }
        Ok(Type::List(ListType::new(Type::Integer(None))))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["arg1".to_string(), "arg2".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct StringFunctionType;

impl FunctionType for StringFunctionType {
    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        // accepts one arguments, returns a string
        if actual_arguments.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "string requires exactly 1 argument",
            ));
        }

        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["value".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct ReplaceFunctionType;

impl FunctionType for ReplaceFunctionType {
    fn _resolve_arguments(&self, actual_arguments: &[Type]) -> Result<Type, crate::Error> {
        // accepts three or four arguments, returns a string
        // actual_arguments[0,1,2] is necessary, actual_arguments[3] is optional
        if actual_arguments.len() < 3 || actual_arguments.len() > 4 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "replace requires 3 or 4 arguments",
            ));
        }
        if actual_arguments.len() == 4 {
            if !actual_arguments[3].is_subtype_of(&Type::Integer(None))
                || !actual_arguments[2].is_subtype_of(&Type::String(None))
                || !actual_arguments[1].is_subtype_of(&Type::String(None))
                || !actual_arguments[0].is_subtype_of(&Type::String(None))
            {
                return Err(crate::Error::new(
                    crate::error::ErrorKind::TypeError,
                    "replace requires a integer argument as the fourth argument",
                ));
            }
        } else if actual_arguments.len() == 3
            && (!actual_arguments[2].is_subtype_of(&Type::String(None))
                || !actual_arguments[1].is_subtype_of(&Type::String(None))
                || !actual_arguments[0].is_subtype_of(&Type::String(None)))
        {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "replace requires a string arguments as the first three arguments",
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["value".to_string()]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct GetColumnSchemaFromQueryFunction;

impl FunctionType for GetColumnSchemaFromQueryFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected 1 argument for get_column_schema_from_query function",
            ));
        }
        if !args[0].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a string argument for get_column_schema_from_query function",
            ));
        }
        Ok(Type::List(ListType::new(Type::Class(DynClassType::new(
            Arc::new(ColumnSchemaType::default()),
        )))))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["sql".to_string()]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct TryOrCompilerErrorFunctionType;

impl fmt::Debug for TryOrCompilerErrorFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "try_or_compiler_error")
    }
}

impl FunctionType for TryOrCompilerErrorFunctionType {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() <= 2 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected at least 2 arguments for try_or_compiler_error function",
            ));
        }
        if !args[0].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a string argument for try_or_compiler_error function",
            ));
        }
        if let Type::Function(_func) = &args[1] {
            // It is not possible to resolve the arguments of the function,
            // because the function args are not known.
            // let rest_args = args[2..].to_vec();
            // func.resolve_arguments(&rest_args)
        } else {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "Expected a function argument for try_or_compiler_error function, got {:?}",
                    args[1]
                ),
            ));
        }
        Ok(Type::Any { hard: true })
    }

    fn arg_names(&self) -> Vec<String> {
        vec![
            "message_if_exception".to_string(),
            "func".to_string(),
            "args".to_string(),
            // TODO: arg number depends on the function
        ]
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct CallerFunctionType;

impl FunctionType for CallerFunctionType {
    fn _resolve_arguments(&self, _args: &[Type]) -> Result<Type, crate::Error> {
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec![]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct WriteFunctionType;

impl fmt::Debug for WriteFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("write")
    }
}

impl FunctionType for WriteFunctionType {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected 1 argument for write function",
            ));
        }
        if !args[0].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a string argument for write function",
            ));
        }
        Ok(Type::None)
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["value".to_string()]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct SubmitPythonJobFunctionType;

impl fmt::Debug for SubmitPythonJobFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("submit_python_job")
    }
}

impl FunctionType for SubmitPythonJobFunctionType {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 2 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected 2 arguments for submit_python_job function",
            ));
        }
        if !args[0].is_subtype_of(&Type::Class(DynClassType::new(Arc::new(
            ModelType::default(),
        )))) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a model argument for submit_python_job function",
            ));
        }
        if !args[1].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a string argument for submit_python_job function",
            ));
        }
        // TODO: the response type
        Ok(Type::Any { hard: false })
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["model".to_string(), "compiled_code".to_string()]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct SelectAttrFunctionType;

impl fmt::Debug for SelectAttrFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("selectattr")
    }
}

impl FunctionType for SelectAttrFunctionType {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 4 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected 4 arguments for selectattr function",
            ));
        }
        if !args[0].is_subtype_of(&Type::List(ListType::new(Type::Any { hard: true }))) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "Expected a list argument for selectattr function, got {:?}",
                    args[0]
                ),
            ));
        }
        if !args[1].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "Expected a string argument for selectattr function, got {:?}",
                    args[1]
                ),
            ));
        }
        if !args[2].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "Expected a string argument for selectattr function, got {:?}",
                    args[2]
                ),
            ));
        }
        if !args[3].is_subtype_of(&Type::Bool) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "Expected a boolean argument for selectattr function, got {:?}",
                    args[3]
                ),
            ));
        }
        Ok(args[0].clone())
    }

    fn arg_names(&self) -> Vec<String> {
        vec![
            "list".to_string(),
            "name".to_string(),
            "value".to_string(),
            "inside_transaction".to_string(),
        ]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct ToJsonFunctionType;

impl fmt::Debug for ToJsonFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tojson")
    }
}

impl FunctionType for ToJsonFunctionType {
    fn _resolve_arguments(&self, _args: &[Type]) -> Result<Type, crate::Error> {
        // TODO: check the arguments
        Ok(Type::Any { hard: true })
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["value".to_string()]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct RenderFunctionType;

impl fmt::Debug for RenderFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("render")
    }
}

impl FunctionType for RenderFunctionType {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected 1 argument for render function",
            ));
        }
        if !args[0].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a string argument for render function",
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["value".to_string()]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct PrintFunctionType;

impl fmt::Debug for PrintFunctionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("print")
    }
}

impl FunctionType for PrintFunctionType {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        println!("print: {args:?}");
        Ok(Type::None)
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["value".to_string()]
    }
}
