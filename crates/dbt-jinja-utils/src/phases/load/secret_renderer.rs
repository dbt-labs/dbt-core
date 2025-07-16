//! This module contains the logic for rendering secrets in the dbt project.

use dbt_common::{ErrorCode, FsResult, fs_err};
use minijinja::{arg_utils::ArgParser, value::Kwargs};
use regex::Regex;

use crate::utils::{DBT_INTERNAL_ENV_VAR_PREFIX, ENV_VARS, SECRET_ENV_VAR_PREFIX};

const SECRET_PLACEHOLDER: &str = "$$$DBT_SECRET_START$$${}$$$DBT_SECRET_END$$$";

/// Prefix which identifies environment variables which contains secrets.
/// A function that returns an environment variable from the environment, with special handling for secrets
pub fn secret_context_env_var_fn()
-> impl Fn(&[minijinja::Value], Kwargs) -> Result<minijinja::Value, minijinja::Error> {
    move |args: &[minijinja::Value], kwargs: Kwargs| -> Result<minijinja::Value, minijinja::Error> {
        let mut env_vars_guard = ENV_VARS.lock().unwrap();

        let mut arg_parser = ArgParser::new(args, Some(kwargs));
        let var_name = arg_parser
            .get::<String>("value")
            .or_else(|_| arg_parser.get::<String>("var"))
            .map_err(|_| {
                minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    "env_var requires a 'value' or 'var' argument",
                )
            })?;
        let default_value = arg_parser.get_optional::<minijinja::Value>("default");

        // First check if the variable exists
        match std::env::var(&var_name) {
            Ok(value) => {
                // If it exists and is a secret, return placeholder
                if var_name.starts_with(SECRET_ENV_VAR_PREFIX) {
                    return Ok(minijinja::Value::from(
                        SECRET_PLACEHOLDER.replace("{}", &var_name),
                    ));
                } else if var_name.starts_with(DBT_INTERNAL_ENV_VAR_PREFIX) {
                    return Err(minijinja::Error::new(
                        minijinja::ErrorKind::InvalidOperation,
                        format!("'env_var': environment variable '{var_name}' is reserved"),
                    ));
                }
                // Otherwise store and return the actual value
                env_vars_guard.insert(var_name, value.clone());
                Ok(value.into())
            }
            Err(_) => {
                // Variable doesn't exist, use default if provided
                if let Some(default) = default_value {
                    Ok(default)
                } else {
                    Err(minijinja::Error::new(
                        minijinja::ErrorKind::InvalidOperation,
                        format!("'env_var': environment variable '{var_name}' not found"),
                    ))
                }
            }
        }
    }
}

/// Renders actual secrets that have been rendered with placeholders
pub fn render_secrets(rendered_str: String) -> FsResult<String> {
    if rendered_str.contains(SECRET_ENV_VAR_PREFIX) {
        // Create a regex that matches the entire placeholder pattern
        let pattern = SECRET_PLACEHOLDER
            .replace("{}", &format!("({SECRET_ENV_VAR_PREFIX}(.*))"))
            .replace("$", r"\$");
        let re = Regex::new(&pattern).unwrap();
        let mut result = rendered_str.clone();
        // Find all matches
        for caps in re.captures_iter(&rendered_str) {
            let var_name = &caps[1]; // This captures the full env var name
            let full_match = &caps[0]; // This is the entire placeholder

            // Check if the secret exists
            match std::env::var(var_name) {
                Ok(value) => {
                    // Replace the entire placeholder with the value
                    result = result.replace(full_match, &value);
                }
                Err(_) => {
                    return Err(fs_err!(
                        ErrorCode::InvalidConfig,
                        "Environment variable '{}' not found",
                        var_name
                    ));
                }
            }
        }

        return Ok(result);
    }
    Ok(rendered_str)
}
