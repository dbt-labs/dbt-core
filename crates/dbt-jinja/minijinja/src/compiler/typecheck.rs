use std::collections::BTreeMap;

use crate::types::function::BasicFunctionType;

/// macro signatures
pub type FunctionRegistry = BTreeMap<String, BasicFunctionType>;
