use minijinja::arg_utils::ArgsIter;
use minijinja::{Error, ErrorKind, State, Value};

/// The prefix for environment variables that contain secrets
pub const SECRET_ENV_VAR_PREFIX: &str = "DBT_ENV_SECRET";

/// The prefix for environment variables that are reserved for dbt
pub const DBT_INTERNAL_ENV_VAR_PREFIX: &str = "_DBT";

/// The default placeholder for environment variables when the default value is used
pub const DEFAULT_ENV_PLACEHOLDER: &str = "__dbt_placeholder__";

/// The placeholder for secret environment variables
pub const SECRET_PLACEHOLDER: &str = "$$$DBT_SECRET_START$$${}$$$DBT_SECRET_END$$$";

/// Type alias for an optional env-var override lookup function.
pub type LookupFn = dyn Fn(&str) -> Option<Value>;

/// A function that returns an environment variable from the environment
///
/// `placeholder_on_secret_access` is used to control whether to return a placeholder
/// or produce a hard error when an attempt is made to access a secret environment variable.
///
/// `overrides_fn` is an optional function that can be checked before accessing the real
/// environment variable. Effectively, this allows for mocking or overriding environment
/// variables.
///
/// `tracker` is an optional callback invoked on each successful env-var access (or default
/// fallback) so the caller can record the resolved name/value pair.
///
/// ```python
/// def env_var(self, var: str, default: Optional[str] = None) -> str
/// ```
/// https://github.com/dbt-labs/dbt-core/blob/303c63ccc836a357505f241dbe90c3abb7b73d57/core/dbt/context/base.py#L305
#[allow(clippy::type_complexity)]
pub fn env_var(
    placeholder_on_secret_access: bool,
    overrides_fn: Option<&LookupFn>,
    tracker: Option<&dyn Fn(&str, &str)>,
    _state: &State,
    args: &[Value],
) -> Result<Value, Error> {
    let iter = ArgsIter::new("env_var", &["var"], args);
    let var = iter.next_arg::<&str>()?;
    let default = iter.next_kwarg::<Option<&Value>>("default")?;

    if let Some(value) = overrides_fn.and_then(|f| f(var)) {
        return Ok(value);
    }

    let is_secret = var.starts_with(SECRET_ENV_VAR_PREFIX);
    if is_secret && !placeholder_on_secret_access {
        let err = Error::new(
            ErrorKind::InvalidOperation,
            format!(
                "Secret environment variables (starting with {SECRET_ENV_VAR_PREFIX}) \
                cannot be accessed here"
            ),
        );
        return Err(err);
    }
    let is_internal = var.starts_with(DBT_INTERNAL_ENV_VAR_PREFIX);
    if is_internal {
        let err = Error::new(
            ErrorKind::InvalidOperation,
            format!(
                "Environment variables (starting with {DBT_INTERNAL_ENV_VAR_PREFIX}) \
                cannot be accessed here"
            ),
        );
        return Err(err);
    }

    match (std::env::var(var), default) {
        (Ok(value), _) => {
            if is_secret {
                debug_assert!(placeholder_on_secret_access);
                let value = Value::from(SECRET_PLACEHOLDER.replace("{}", var));
                Ok(value)
            } else {
                if let Some(tracker) = tracker {
                    tracker(var, &value);
                }
                Ok(Value::from(value))
            }
        }
        (Err(_), Some(default)) => {
            if let Some(tracker) = tracker {
                tracker(var, DEFAULT_ENV_PLACEHOLDER);
            }
            Ok(default.clone())
        }
        _ => {
            let err = Error::new(
                ErrorKind::InvalidOperation,
                format!("'env_var': environment variable '{var}' not found"),
            );
            Err(err)
        }
    }
}
