//! Configured Var is a function that resolves a var in the current package's namespace

use std::{collections::BTreeMap, rc::Rc, sync::Arc};

use dbt_schemas::state::DbtVars;
use indexmap::IndexMap;

use minijinja::{
    Error, ErrorKind, State, Value, constants::TARGET_PACKAGE_NAME,
    listener::RenderingEventListener, value::Object,
};

use super::VarFunction;

/// A struct that represent a var object to be used in configuration contexts
#[derive(Debug)]
pub struct ConfiguredVar {
    vars: BTreeMap<String, IndexMap<String, DbtVars>>,
    cli_vars: BTreeMap<String, dbt_yaml::Value>,
}

// Similar to Var, but handling package specific vars
// see https://github.com/dbt-labs/dbt-core/blob/9f2b9b6d9cc5c93b5178eb209179b2f3a81cdb8e/core/dbt/context/configured.py#L34
impl ConfiguredVar {
    pub fn new(
        vars: BTreeMap<String, IndexMap<String, DbtVars>>,
        cli_vars: BTreeMap<String, dbt_yaml::Value>,
    ) -> Self {
        Self { vars, cli_vars }
    }
}

impl VarFunction for ConfiguredVar {
    fn contains_var(&self, state: &State<'_, '_>, var_name: &str) -> Result<bool, Error> {
        let Some(package_name) = state
            .lookup(TARGET_PACKAGE_NAME)
            .and_then(|v| v.as_str().map(|s| s.to_string()))
        else {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                format!(
                    "'TARGET_PACKAGE_NAME' should be set. Missing in configured var context while looking up var: {var_name}"
                ),
            ));
        };
        let vars_lookup = self.vars.get(&package_name).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidOperation,
                format!("Package vars should be initialized for package: {package_name}"),
            )
        })?;
        Ok(vars_lookup.contains_key(var_name))
    }

    fn call_as_function(
        &self,
        state: &State<'_, '_>,
        var_name: String,
        default_value: Option<Value>,
    ) -> Result<Value, Error> {
        // 1. CLI vars
        if let Some(value) = self.cli_vars.get(&var_name) {
            return Ok(Value::from_serialize(value));
        }
        // 2. Check if this is dbt_project.yml parsing
        if Some("dbt_project.yml".to_string())
            == state
                .lookup(TARGET_PACKAGE_NAME)
                .and_then(|v| v.as_str().map(|s| s.to_string()))
        {
            if let Some(default_value) = default_value {
                return Ok(default_value);
            } else {
                return Err(Error::new(
                    ErrorKind::InvalidOperation,
                    format!("Missing default value for var in dbt_project.yml: {var_name}"),
                ));
            }
        }

        // 3. Package vars
        let Some(package_name) = state
            .lookup(TARGET_PACKAGE_NAME)
            .and_then(|v| v.as_str().map(|s| s.to_string()))
        else {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                format!(
                    "'TARGET_PACKAGE_NAME' should be set. Missing in configured var context while looking up var: {var_name}"
                ),
            ));
        };
        let vars_lookup = self.vars.get(&package_name).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidOperation,
                format!("Package vars should be initialized for package: {package_name}"),
            )
        })?;
        if let Some(var) = vars_lookup.get(&var_name) {
            Ok(Value::from_serialize(var))
        } else if let Some(default_value) = default_value {
            Ok(default_value)
        } else if state.lookup("this").is_none() {
            // if parsing config and var is missing, throw an error
            Err(Self::missing_var_error(
                &package_name,
                &var_name,
                vars_lookup,
            ))
        } else if let Some(execute) = state.lookup("execute").map(|v| v.is_true()) {
            if !execute {
                // if parsing a model and var is missing, return none
                Ok(Value::from(()))
            } else {
                // if compiling a model and var is missing, throw an error
                Err(Self::missing_var_error(
                    &package_name,
                    &var_name,
                    vars_lookup,
                ))
            }
        } else {
            Err(Error::new(
                // TODO: make another error type for this
                ErrorKind::InvalidOperation,
                "No execute var found in state",
            ))
        }
    }
}

impl Object for ConfiguredVar {
    fn call(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        args: &[Value],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        self.call_impl(state, args, listeners)
    }

    fn call_method(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        method: &str,
        args: &[Value],
        listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        self.call_method_impl(state, method, args, listeners)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use minijinja::constants::TARGET_PACKAGE_NAME;
    use minijinja::value::Value as MinijinjaValue;

    fn make_env_with_var() -> minijinja::Environment<'static> {
        let mut env = minijinja::Environment::new();

        // Mirror Fusion's global Jinja context: dbt projects frequently use
        // Python-ish constants (capitalized) like `None`.
        env.add_global("None", MinijinjaValue::from(()));
        env.add_global("True", MinijinjaValue::from(true));
        env.add_global("False", MinijinjaValue::from(false));

        // Provide the current package name so ConfiguredVar can look up the vars namespace.
        env.add_global(TARGET_PACKAGE_NAME, MinijinjaValue::from("my_new_project"));

        // Provide an empty vars namespace for this package (required by ConfiguredVar).
        let mut vars = BTreeMap::new();
        vars.insert("my_new_project".to_string(), IndexMap::new());

        let cli_vars = BTreeMap::new();
        env.add_global(
            "var",
            MinijinjaValue::from_object(ConfiguredVar::new(vars, cli_vars)),
        );
        env
    }

    #[test]
    fn var_default_keyword_none_is_treated_as_none() {
        let env = make_env_with_var();
        let template = env
            .template_from_str(
                "{% set x = var('x', default=None) %}{% if x is not none %}NOT{% else %}NONE{% endif %}",
            )
            .unwrap();

        let rendered = template.render(minijinja::context!(), &[]).unwrap();
        assert_eq!(rendered, "NONE");
    }

    #[test]
    fn var_default_keyword_string_is_used_when_var_missing() {
        let env = make_env_with_var();
        let template = env
            .template_from_str("{{ var('x', default='abc') }}")
            .unwrap();

        let rendered = template.render(minijinja::context!(), &[]).unwrap();
        assert_eq!(rendered, "abc");
    }

    #[test]
    fn var_default_positional_string_is_used_when_var_missing() {
        let env = make_env_with_var();
        let template = env.template_from_str("{{ var('x', 'abc') }}").unwrap();

        let rendered = template.render(minijinja::context!(), &[]).unwrap();
        assert_eq!(rendered, "abc");
    }

    #[test]
    fn var_default_positional_map_is_used_when_var_missing() {
        let env = make_env_with_var();
        let template = env
            .template_from_str("{{ var('config', {'materialized': 'table'}).materialized }}")
            .unwrap();

        let rendered = template.render(minijinja::context!(), &[]).unwrap();
        assert_eq!(rendered, "table");
    }

    #[test]
    fn var_has_var() {
        let env = make_env_with_var();
        let template = env
            .template_from_str("{{ var.has_var('somevar') }}")
            .unwrap();

        let rendered = template.render(minijinja::context!(), &[]).unwrap();
        assert_eq!(rendered, "False");
    }
}
