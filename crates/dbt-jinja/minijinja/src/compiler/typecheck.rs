use std::collections::BTreeMap;

use crate::vm::types::function::BasicFunctionType;

/// macro signatures
pub type FunctionRegistry = BTreeMap<String, BasicFunctionType>;
