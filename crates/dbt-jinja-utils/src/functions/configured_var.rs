//! Configured Var is a function that resolves a var in the current package's namespace

use std::{collections::BTreeMap, rc::Rc, sync::Arc};

use dbt_schemas::state::DbtVars;
use minijinja::{
    constants::TARGET_PACKAGE_NAME, listener::RenderingEventListener, value::Object, Error,
    ErrorKind, State, Value,
};

/// A struct that represent a var object to be used in configuration contexts
#[derive(Debug)]
pub struct ConfiguredVar {
    vars: BTreeMap<String, BTreeMap<String, DbtVars>>,
    cli_vars: BTreeMap<String, dbt_serde_yaml::Value>,
}

impl ConfiguredVar {
    pub fn new(
        vars: BTreeMap<String, BTreeMap<String, DbtVars>>,
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
        // Safely get var_name, defaulting to empty string if args is empty or not a string
        let var_name = args
            .first()
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let default_value = args.get(1);
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
                return Ok(Value::from_serialize(default_value));
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
        let vars_lookup = self.vars.get(&package_name).ok_or(Error::new(
            ErrorKind::InvalidOperation,
            format!("Package vars should be initialized for package: {package_name}"),
        ))?;
        if let Some(var) = vars_lookup.get(&var_name) {
            Ok(Value::from_serialize(var))
        } else if let Some(default_value) = default_value {
            Ok(Value::from_serialize(default_value))
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
            let vars_lookup = self.vars.get(&package_name).ok_or(Error::new(
                ErrorKind::InvalidOperation,
                format!("Package vars should be initialized for package: {package_name}"),
            ))?;
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
