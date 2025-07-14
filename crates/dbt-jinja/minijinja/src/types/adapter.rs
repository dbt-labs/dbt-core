use crate::compiler::typecheck::FunctionRegistry;
use crate::types::agate_table::AgateTableType;
use crate::types::builtin::Type;
use crate::types::class::{ClassType, DynClassType};
use crate::types::function::{DynFunctionType, FunctionType};
use crate::types::relation::RelationType;
use crate::types::struct_::StructType;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, OnceLock};

/// Metadata for relation objects, including valid attributes and their return types.
#[derive(Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AdapterType {}

impl std::fmt::Debug for AdapterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("adapter")
    }
}

impl ClassType for AdapterType {
    fn get_attribute(&self, key: &str) -> Result<Type, crate::Error> {
        match key {
            "get_relation" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                AdapterGetRelationFunction::default(),
            )))),
            "dispatch" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                AdapterDispatchFunction::instance(),
            )))),
            "standardize_grants_dict" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                AdapterStandardizeGrantsDictFunction::default(),
            )))),
            "type" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                AdapterTypeFunction::default(),
            )))),
            "get_column_schema_from_query" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                AdapterGetColumnSchemaFromQueryFunction::default(),
            )))),
            "quote" => Ok(Type::Function(DynFunctionType::new(Arc::new(
                AdapterQuoteFunction::default(),
            )))),
            _ => Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!("{self:?}.{key} is not supported"),
            )),
        }
    }
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct AdapterGetRelationFunction {}

impl std::fmt::Debug for AdapterGetRelationFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("adapter.get_relation")
    }
}

impl FunctionType for AdapterGetRelationFunction {
    fn _resolve_arguments(&self, _args: &[Type]) -> Result<Type, crate::Error> {
        Ok(Type::Class(DynClassType::new(Arc::new(
            RelationType::default(),
        ))))
    }

    fn arg_names(&self) -> Vec<String> {
        vec![
            "database".to_string(),
            "schema".to_string(),
            "identifier".to_string(),
        ]
    }
}

#[derive(Clone)]
/// AdapterDispatchFunction is a singleton that type check adapter.dispatch
pub struct AdapterDispatchFunction {
    function_registry: Arc<Mutex<Option<Arc<FunctionRegistry>>>>,
}

// Singleton instance storage
static ADAPTER_DISPATCH_INSTANCE: OnceLock<AdapterDispatchFunction> = OnceLock::new();

impl AdapterDispatchFunction {
    fn new(function_registry: Arc<Mutex<Option<Arc<FunctionRegistry>>>>) -> Self {
        Self { function_registry }
    }

    /// Get the singleton instance of AdapterDispatchFunction
    pub fn instance() -> Self {
        ADAPTER_DISPATCH_INSTANCE
            .get_or_init(|| AdapterDispatchFunction::new(Arc::new(Mutex::new(None))))
            .clone()
    }

    /// Get a reference to the function registry
    pub fn function_registry(&self) -> &Arc<Mutex<Option<Arc<FunctionRegistry>>>> {
        &self.function_registry
    }

    /// Set the function registry
    pub fn set_function_registry(&self, new_registry: Arc<FunctionRegistry>) {
        let mut registry = self.function_registry.lock().unwrap();
        *registry = Some(new_registry);
    }
}

impl std::fmt::Debug for AdapterDispatchFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("adapter.dispatch")
    }
}

impl FunctionType for AdapterDispatchFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() > 2 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected at most 2 arguments for adapter.dispatch",
            ));
        }
        if let Some(Type::String(Some(name))) = args.get(0) {
            if let Ok(registry_opt) = self.function_registry.lock() {
                if let Some(ref registry) = *registry_opt {
                    if let Some(func) = registry.get(name) {
                        Ok(Type::Function(func.clone()))
                    } else {
                        Err(crate::Error::new(
                            crate::error::ErrorKind::InvalidOperation,
                            format!("Function {name} not found"),
                        ))
                    }
                } else {
                    Err(crate::Error::new(
                        crate::error::ErrorKind::InvalidOperation,
                        "Function registry not initialized",
                    ))
                }
            } else {
                Err(crate::Error::new(
                    crate::error::ErrorKind::InvalidOperation,
                    "Failed to lock function registry",
                ))
            }
        } else {
            Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected literal string for first argument of adapter.dispatch",
            ))
        }
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["name".to_string()]
    }
}

#[derive(Default, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct AdapterStandardizeGrantsDictFunction {}

impl std::fmt::Debug for AdapterStandardizeGrantsDictFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("adapter.standardize_grants_dict")
    }
}

impl FunctionType for AdapterStandardizeGrantsDictFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        // The first arg must be a agate_table
        if let Some(Type::Class(class)) = args.get(0) {
            if !class.is::<AgateTableType>() {
                return Err(crate::Error::new(
                    crate::error::ErrorKind::TypeError,
                    format!(
                        "Expected agate_table type for first argument of adapter.standardize_grants_dict, got {class:?}"
                    ),
                ));
            }
        } else {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                format!(
                    "Expected agate_table type for first argument of adapter.standardize_grants_dict, got {:?}",
                    args.get(0)
                ),
            ));
        }
        if args.len() > 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::TypeError,
                "Expected at most 1 argument for adapter.standardize_grants_dict",
            ));
        }
        Ok(Type::Struct(StructType::new(BTreeMap::from([(
            "return_val".to_string(),
            Type::Any { hard: true }, // TODO
        )]))))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["grants_dict".to_string()]
    }
}

#[derive(Default, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct AdapterTypeFunction {}

impl std::fmt::Debug for AdapterTypeFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("adapter.type")
    }
}

impl FunctionType for AdapterTypeFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if !args.is_empty() {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                "Expected 0 arguments for adapter.type",
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec![]
    }
}

#[derive(Default, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct AdapterGetColumnSchemaFromQueryFunction {}

impl std::fmt::Debug for AdapterGetColumnSchemaFromQueryFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("adapter.get_column_schema_from_query")
    }
}

impl FunctionType for AdapterGetColumnSchemaFromQueryFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                "Expected 1 argument for adapter.get_column_schema_from_query",
            ));
        }
        if !matches!(args.get(0), Some(Type::String(_))) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                "Expected string for first argument of adapter.get_column_schema_from_query",
            ));
        }
        Ok(Type::String(None))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["select_sql".to_string(), "select_sql_header".to_string()]
    }
}

#[derive(Default, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct AdapterQuoteFunction {}

impl std::fmt::Debug for AdapterQuoteFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("adapter.quote")
    }
}

impl FunctionType for AdapterQuoteFunction {
    fn _resolve_arguments(&self, args: &[Type]) -> Result<Type, crate::Error> {
        if args.len() != 1 {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                "Expected 1 argument for adapter.quote",
            ));
        }
        if !matches!(args[0], Type::String(_)) {
            return Err(crate::Error::new(
                crate::error::ErrorKind::InvalidOperation,
                format!(
                    "Expected string for first argument of adapter.quote, got {:?}",
                    args[0]
                ),
            ));
        }
        Ok(Type::String(Some(args[0].to_string())))
    }

    fn arg_names(&self) -> Vec<String> {
        vec!["name".to_string()]
    }
}
