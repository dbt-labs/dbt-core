use crate::types::function::DynFunctionType;
use std::collections::BTreeMap;

/// macro signatures
pub type FunctionRegistry = BTreeMap<String, DynFunctionType>;
