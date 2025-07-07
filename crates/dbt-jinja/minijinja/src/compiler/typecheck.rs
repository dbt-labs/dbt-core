use std::{collections::BTreeMap, sync::Arc};

use crate::vm::types::function::BasicFunctionType;

/// macro signatures
pub type FunctionRegistry = BTreeMap<String, Arc<BasicFunctionType>>;
