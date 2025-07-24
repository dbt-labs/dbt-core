use crate::error::Error;
use crate::types::builtin::Type;
use crate::types::class::{ClassType, DynClassType};
use crate::types::function::{ArgSpec, DynFunctionType, FunctionType};
use std::hash::Hash;
use std::sync::Arc;

/// Metadata for relation objects, including valid attributes and their return types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct RelationType {}

impl ClassType for RelationType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "name" => Ok(Type::String(None)),
            "database" => Ok(Type::String(None)),
            "schema" => Ok(Type::String(None)),
            "identifier" => Ok(Type::String(None)),
            "type" => Ok(Type::String(None)),
            "is_table" => Ok(Type::Bool),
            "is_view" => Ok(Type::Bool),
            "is_materialized_view" => Ok(Type::Bool),
            "is_cte" => Ok(Type::Bool),
            "is_pointer" => Ok(Type::Bool),
            "can_be_renamed" => Ok(Type::Bool),
            "can_be_replaced" => Ok(Type::Bool),
            "MaterializedView" => Ok(Type::String(None)),
            "DynamicTable" => Ok(Type::String(None)),
            "include" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                RelationIncludeFunction::default(),
            )))),
            "incorporate" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                RelationIncorporateFunction::default(),
            )))),
            "render" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                RelationRenderFunction::default(),
            )))),
            "create" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                RelationCreateFunction::default(),
            )))),
            "without_identifier" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                RelationWithoutIdentifierFunction::default(),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct RelationIncludeFunction {}

impl std::fmt::Debug for RelationIncludeFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("relation.include").finish()
    }
}

impl FunctionType for RelationIncludeFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        for arg in args {
            if !arg.is_subtype_of(&Type::Bool) {
                return Err(Error::new(
                    crate::error::ErrorKind::TypeError,
                    format!("Expected bool for relation include function arguments, found {arg}"),
                ));
            }
        }

        Ok(Type::Class(DynClassType::new(Arc::new(
            RelationType::default(),
        ))))
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::new("database", true),
            ArgSpec::new("schema", true),
            ArgSpec::new("identifier", true),
        ]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct RelationRenderFunction {}

impl std::fmt::Debug for RelationRenderFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("relation.render").finish()
    }
}

impl FunctionType for RelationRenderFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if !args.is_empty() {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected no arguments for relation render function",
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct RelationIncorporateFunction {}

impl std::fmt::Debug for RelationIncorporateFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("relation.incorporate")
    }
}

impl FunctionType for RelationIncorporateFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        // args[0] and args[1] are optional
        if !args.is_empty()
            && !matches!(args[0], Type::Struct(_))
            && !matches!(args[0], Type::Kwargs(_))
            && !matches!(args[0], Type::Any { hard: true })
        {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a dict or kwargs type for relation incorporate function arguments 'path'",
            ));
        }
        if args.len() > 1
            && !matches!(args[1], Type::String(_))
            && !matches!(args[1], Type::Any { hard: true })
        {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a string type for relation incorporate function arguments 'type'",
            ));
        }

        Ok(Type::Class(DynClassType::new(Arc::new(
            RelationType::default(),
        ))))
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::new("path", true), ArgSpec::new("type", true)]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct RelationCreateFunction {}

impl std::fmt::Debug for RelationCreateFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("relation.create").finish()
    }
}

impl FunctionType for RelationCreateFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 4 {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected 4 arguments for relation create function",
            ));
        }
        if !args[0].is_subtype_of(&Type::String(None)) {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a string type for relation create function arguments 'database'",
            ));
        }
        if !args[1].is_subtype_of(&Type::String(None)) {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a string type for relation create function arguments 'schema'",
            ));
        }
        if !args[2].is_subtype_of(&Type::String(None)) {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a string type for relation create function arguments 'identifier'",
            ));
        }
        if !args[3].is_subtype_of(&Type::String(None)) {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected a string type for relation create function arguments 'type'",
            ));
        }
        Ok(Type::Class(DynClassType::new(Arc::new(
            RelationType::default(),
        ))))
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::new("database", true),
            ArgSpec::new("schema", true),
            ArgSpec::new("identifier", true),
            ArgSpec::new("type", true),
        ]
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct RelationWithoutIdentifierFunction {}

impl std::fmt::Debug for RelationWithoutIdentifierFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("relation.without_identifier").finish()
    }
}

impl FunctionType for RelationWithoutIdentifierFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if !args.is_empty() {
            return Err(Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected no arguments for relation without identifier function",
            ));
        }
        Ok(Type::Class(DynClassType::new(Arc::new(
            RelationType::default(),
        ))))
    }

    fn arg_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
}
