//! This module contains the scope guard for rendering profile.
use std::collections::BTreeMap;

use std::marker::PhantomData;

use crate::{
    functions::{env_var_fn, var_fn},
    jinja_environment::JinjaEnvironment,
};

use super::secret_renderer::secret_context_env_var_fn;

/// A scope guard that configures render properties on creation and cleans them up on drop
pub struct RenderSecretScope<'a> {
    /// The Jinja environment to configure
    pub jinja_env: &'a mut JinjaEnvironment<'static>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> RenderSecretScope<'a> {
    /// Create a new guard that configures render properties for the given project
    pub fn new(
        jinja_env: &'a mut JinjaEnvironment<'static>,
        cli_vars: BTreeMap<String, dbt_serde_yaml::Value>,
    ) -> Self {
        // Configure the environment when creating the guard
        jinja_env.add_function("env_var".to_owned(), secret_context_env_var_fn());
        jinja_env.add_function("var".to_owned(), var_fn(cli_vars));

        Self {
            jinja_env,
            _phantom: PhantomData,
        }
    }
}

impl Drop for RenderSecretScope<'_> {
    fn drop(&mut self) {
        // Clean up when the guard goes out of scope
        // TODO (alex): This is a bit of a hack. We need to refactor this to use contexts instead!
        self.jinja_env.add_function("env_var", env_var_fn());
        self.jinja_env.remove_global("var");
    }
}
