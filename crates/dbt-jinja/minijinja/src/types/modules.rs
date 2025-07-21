use crate::error::Error;
use crate::types::builtin::Type;
use crate::types::class::{ClassType, DynClassType};
use crate::types::function::{DynFunctionType, FunctionType};
use std::hash::Hash;
use std::sync::Arc;

/// Metadata for relation objects, including valid attributes and their return types.
#[derive(Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct ModulesType {}

impl std::fmt::Debug for ModulesType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("modules")
    }
}

impl ClassType for ModulesType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "datetime" => Ok(Type::Class(DynClassType::new(Arc::new(
                ModulesDateTimeType::default(),
            )))),
            _ => Err(Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct ModulesDateTimeType {}

impl std::fmt::Debug for ModulesDateTimeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("modules.datetime")
    }
}

impl ClassType for ModulesDateTimeType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "datetime" => Ok(Type::Class(DynClassType::new(Arc::new(
                PyDateTimeType::default(),
            )))),
            "date" => Ok(Type::Class(DynClassType::new(Arc::new(
                PyDateType::default(),
            )))),
            "time" => Ok(Type::Class(DynClassType::new(Arc::new(
                PyTimeType::default(),
            )))),
            "timedelta" => Ok(Type::Class(DynClassType::new(Arc::new(
                PyTimeDeltaType::default(),
            )))),
            "tzinfo" => Ok(Type::Class(DynClassType::new(Arc::new(
                PyTzInfoType::default(),
            )))),
            _ => Err(Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct PyDateTimeType {}

impl std::fmt::Debug for PyDateTimeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("modules.datetime.datetime")
    }
}

impl ClassType for PyDateTimeType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "strptime" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                PyDateTimeStrptimeFunction::default(),
            )))),
            "now" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                PyDateTimeNowFunction::default(),
            )))),
            _ => Err(Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct PyDateType {}

impl std::fmt::Debug for PyDateType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("modules.datetime.date")
    }
}

impl ClassType for PyDateType {
    fn get_attribute(&self, _key: &str) -> Result<Type, crate::Error> {
        // TODO: enrich with actual pydate methods
        Ok(Type::Any { hard: false })
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct PyTimeType {}

impl std::fmt::Debug for PyTimeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("modules.datetime.time")
    }
}

impl ClassType for PyTimeType {
    fn get_attribute(&self, _key: &str) -> Result<Type, crate::Error> {
        // TODO: enrich with actual pytime methods
        Ok(Type::Any { hard: false })
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct PyTimeDeltaType {}

impl std::fmt::Debug for PyTimeDeltaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("modules.datetime.timedelta")
    }
}

impl ClassType for PyTimeDeltaType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "days" => Ok(Type::Integer(None)),
            _ => Err(Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }

    fn constructor(&self, _args: &[Type]) -> Result<Type, crate::Error> {
        // TODO: check _args
        Ok(Type::Class(DynClassType::new(Arc::new(
            PyTimeDeltaType::default(),
        ))))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct PyTzInfoType {}

impl ClassType for PyTzInfoType {
    fn get_attribute(&self, _key: &str) -> Result<Type, crate::Error> {
        // TODO: enrich with actual pytzinfo methods
        Ok(Type::Any { hard: false })
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct PyDateTimeStrptimeFunction;

impl std::fmt::Debug for PyDateTimeStrptimeFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("modules.datetime.datetime.strptime")
    }
}

impl FunctionType for PyDateTimeStrptimeFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 2 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected 2 arguments for strptime function",
            ));
        }
        if !args[0].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a string argument for strptime function",
            ));
        }
        if !args[1].is_subtype_of(&Type::String(None)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a string argument for strptime function",
            ));
        }
        Ok(Type::TimeStamp)
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["date_str".to_string(), "date_fmt".to_string()]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct PyDateTimeNowFunction;

impl std::fmt::Debug for PyDateTimeNowFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("modules.datetime.datetime.now")
    }
}

impl FunctionType for PyDateTimeNowFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if !args.is_empty() {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected 0 arguments for now function",
            ));
        }
        Ok(Type::TimeStamp)
    }

    fn arg_names(&self) -> Vec<String> {
        vec![]
    }
}
