//! Minimal Jinja environment for profile rendering.
//!
//! Implements `env_var` and `var` functions plus base filters via `dbt-jinja-filters`.
//! No dependency on `dbt-jinja-utils` or `dbt-common`.

use std::collections::BTreeMap;

use dbt_jinja_vars::Var;
use minijinja::{Environment, Value};
use serde::Serialize;

/// Build a bare minijinja [`Environment`] with the filters profiles.yml needs.
pub fn profile_environment() -> Environment<'static> {
    let mut env = Environment::new();
    dbt_jinja_filters::register_filters(&mut env);
    env
}

// ── Context ──────────────────────────────────────────────────────────

/// The rendering context passed to `env.render_str(...)`.
#[derive(Serialize)]
pub struct ProfileContext {
    pub env_var: Value,
    pub var: Value,
    pub context: Value,
}

impl ProfileContext {
    pub fn new(vars: BTreeMap<String, dbt_yaml::Value>) -> Self {
        Self {
            env_var: Value::from_func_func("env_var", |state, args| {
                dbt_jinja_vars::env_var(false, None, None, state, args)
            }),
            var: Value::from_object(Var::new(vars)),
            context: Value::from_serialize(BTreeMap::<String, Value>::new()),
        }
    }
}
