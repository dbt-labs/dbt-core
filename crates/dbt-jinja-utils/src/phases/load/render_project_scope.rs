//! This module contains the scope guard for rendering properties.

use std::collections::BTreeMap;

use std::marker::PhantomData;

use crate::{functions::var_fn, jinja_environment::JinjaEnvironment};

/// A scope guard that configures render properties on creation and cleans them up on drop
pub struct RenderProjectScope<'a> {
    /// The Jinja environment to configure
    pub jinja_env: &'a mut JinjaEnvironment<'static>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> RenderProjectScope<'a> {
    /// Create a new guard that configures render properties for the given project
    pub fn new(
        jinja_env: &'a mut JinjaEnvironment<'static>,
        cli_vars: BTreeMap<String, dbt_serde_yaml::Value>,
    ) -> Self {
        // Configure the environment when creating the guard
        jinja_env.add_function("var".to_owned(), var_fn(cli_vars));

        Self {
            jinja_env,
            _phantom: PhantomData,
        }
    }
}

impl Drop for RenderProjectScope<'_> {
    fn drop(&mut self) {
        // Clean up when the guard goes out of scope
        self.jinja_env.remove_global("var");
    }
}
