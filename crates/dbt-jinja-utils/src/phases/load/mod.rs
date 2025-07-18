//! This module contains the rendering functionality for the load phase.

use std::collections::BTreeMap;

use minijinja::Value;
use serde::Serialize;

use crate::{phases::load::secret_renderer::secret_context_env_var_fn, var_fn};

pub mod init;
pub mod secret_renderer;

/// A struct that contains the context for the deps phase.
#[derive(Serialize)]
pub struct LoadContext {
    env_var: Value,
    var: Value,
}

impl LoadContext {
    /// Create a new DepsContext.
    pub fn new(vars: BTreeMap<String, dbt_serde_yaml::Value>) -> Self {
        Self {
            env_var: Value::from_function(secret_context_env_var_fn()),
            var: Value::from_function(var_fn(vars)),
        }
    }
}
