//! Configured Var is a function that resolves a var in the current package's namespace

use std::{collections::BTreeMap, rc::Rc, sync::Arc};

use dbt_schemas::state::DbtVars;
use indexmap::IndexMap;
use minijinja::value::ValueKind;
use minijinja::{
    Error, ErrorKind, State, Value, arg_utils::ArgsIter, constants::TARGET_PACKAGE_NAME,
    listener::RenderingEventListener, value::Object,
};

/// A struct that represent a var object to be used in configuration contexts
#[derive(Debug)]
pub struct ConfiguredVar {
    vars: BTreeMap<String, IndexMap<String, DbtVars>>,
    cli_vars: BTreeMap<String, dbt_serde_yaml::Value>,
}

impl ConfiguredVar {
    pub fn new(
        vars: BTreeMap<String, IndexMap<String, DbtVars>>,
        cli_vars: BTreeMap<String, dbt_serde_yaml::Value>,
    ) -> Self {
        Self { vars, cli_vars }
    }
}

impl Object for ConfiguredVar {
    /// Implement the call method on the var object
    fn call(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        // NOTE: Minijinja encodes keyword arguments into `args` as a final map value.
        // Using `ArgsIter` ensures we correctly support both:
        // - var("name", "default")
        // - var("name", default="default")
        // and we don't accidentally treat the kwargs map itself as the default value.
        let iter = ArgsIter::new("var", &["name", "default"], args);
        // Jinja will happily pass "undefined" into functions if the caller writes `var(x)`
        // and `x` is not defined. Jinja2's Undefined is string-coercible, so dbt-core will
        // end up treating it like an empty string key and still honor the provided default.
        //
        // For compatibility (and to keep tests/fixtures stable), we explicitly coerce
        // undefined/none to an empty string here instead of raising a type error.
        let var_name_value = iter.next_arg::<Value>()?;
        let var_name = if let Some(s) = var_name_value.as_str() {
            s.to_string()
        } else if var_name_value.is_undefined() || var_name_value.is_none() {
            "".to_string()
        } else {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "argument 'name' to var() has incompatible type; value is not a string",
            ));
        };

        // IMPORTANT: ArgsIter's `next_kwarg::<Option<&Value>>` cannot distinguish between:
        // - kwarg missing
        // - kwarg present with value `none`
        //
        // dbt projects commonly do `var("x", default=None)`, so we need to preserve an
        // explicit `none` default.
        //
        // We therefore parse the kwargs map ourselves (if present), and fall back to
        // a true positional default only when the 2nd argument is not that kwargs map.
        let mut default_value: Option<Value> = None;

        // there are two ways for the second argument to be a map:
        // 1. var("x", {"a": 1})
        // 2. var("x", {"default": 1})
        // The first use provides a default map-typed value for x
        // The second use provides a default int-typed value for x

        // Handling the default value
        if args.len() == 2 {
            let second = args.get(1).expect("second must be present");
            let default_key = Value::from("default");
            let keyword_default_value = if second.kind() == ValueKind::Map {
                // NOTE: `get_item` can succeed and still return `undefined` when the key
                // does not exist. Treat that as "not present".
                match second.get_item(&default_key) {
                    Ok(v) if !v.is_undefined() => Some(v),
                    _ => None,
                }
            } else {
                None
            };

            if let Some(v) = keyword_default_value {
                // Keyword default: var("x", default=...)
                default_value = Some(v);
            } else {
                // Positional default: var("x", "abc")
                default_value = Some(second.clone());
            }
        }

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
        // TODO (alex): this check only works for parse. At compile time, this needs to be phase dependent
        // "this" is only present when resolving models, which is when we need to
        } else if state.lookup("this").is_none() {
            Err(Error::new(
                ErrorKind::InvalidOperation,
                format!(
                    "Missing context variable 'this'. Var should be initialized for package: {package_name}"
                ),
            ))
        // if the var isn't found, if parse, return none, if compile, return error
        } else if let Some(execute) = state.lookup("execute").map(|v| v.is_true()) {
            if !execute {
                Ok(Value::from(()))
            } else {
                Err(Error::new(
                    ErrorKind::InvalidOperation,
                    format!(
                        "Required var '{}' not found in config:\nVars supplied to {} = {}",
                        var_name,
                        package_name,
                        serde_json::to_string_pretty(&vars_lookup).unwrap()
                    ),
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

    fn call_method(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        method: &str,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        // implement the has_var method
        if method == "has_var" {
            let var_name = args[0].as_str().unwrap().to_string();
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
            if vars_lookup.contains_key(&var_name) {
                Ok(Value::from(true))
            } else {
                Ok(Value::from(false))
            }
        } else {
            Err(Error::new(
                ErrorKind::InvalidOperation,
                format!("Method {method} not found"),
            ))
        }
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
}
