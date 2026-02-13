//! Core functions that are shared across all contexts

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    rc::Rc,
    sync::Arc,
};

use dbt_agate::AgateTable;
use dbt_common::{
    CodeLocationWithFile, ErrorCode, fs_err,
    io_args::IoArgs,
    io_utils::StatusReporter,
    tracing::emit::{
        emit_debug_event, emit_info_event, emit_warn_log_from_fs_error, emit_warn_log_message,
    },
};
use dbt_schemas::schemas::{InternalDbtNode, Nodes};
use dbt_telemetry::UserLogMessage;
use minijinja::{
    arg_utils::ArgsIter,
    constants::TARGET_PACKAGE_NAME,
    value::{ValueKind, ValueMap, mutable_map::MutableMap, mutable_set::MutableSet},
};

use minijinja::{
    Environment, Error, ErrorKind, State, Value,
    arg_utils::ArgParser,
    listener::RenderingEventListener,
    value::{Kwargs, Object},
};
type YmlValue = dbt_yaml::Value;
use crate::utils::{
    DBT_INTERNAL_ENV_VAR_PREFIX, ENV_VARS, SECRET_ENV_VAR_PREFIX, get_status_reporter,
    node_metadata_from_state,
};

use crate::functions::contract_error::get_contract_mismatches;
use serde::Serialize;

/// A JSON formatter that matches Python's json.dumps() default style:
/// - Compact (no newlines or indentation)
/// - Space after colons (e.g., `{"key": "value"}` instead of `{"key":"value"}`)
/// - Space after commas (e.g., `{"a": 1, "b": 2}` instead of `{"a": 1,"b": 2}`)
struct PythonStyleFormatter;

impl serde_json::ser::Formatter for PythonStyleFormatter {
    fn begin_object_key<W>(&mut self, writer: &mut W, first: bool) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_value<W>(&mut self, writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        writer.write_all(b": ")
    }

    fn begin_array_value<W>(&mut self, writer: &mut W, first: bool) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }
}

/// Serialize a value to JSON string with Python-style formatting (space after colon)
fn to_json_string_python_style<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let mut buf = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut buf, PythonStyleFormatter);
    value.serialize(&mut ser)?;
    // Safe because serde_json only produces valid UTF-8
    Ok(unsafe { String::from_utf8_unchecked(buf) })
}

/// The default placeholder for environment variables when the default value is used
pub const DEFAULT_ENV_PLACEHOLDER: &str = "__dbt_placeholder__";
pub const SECRET_PLACEHOLDER: &str = "$$$DBT_SECRET_START$$${}$$$DBT_SECRET_END$$$";

/// Registers all the functions shared across all contexts
pub fn register_base_functions(env: &mut Environment, io_args: IoArgs) {
    env.add_global("dbt_version", Value::from(crate::utils::DBT_VERSION));
    env.add_global(
        "exceptions".to_owned(),
        Value::from_object(Exceptions { io_args }),
    );
    // dbt-core templates commonly use Python-ish constants (capitalized).
    // In Jinja2 the canonical values are `none/true/false`, but many dbt projects
    // (and dbt-core's Python context) also make `None/True/False` available.
    // Missing these can cause `default=None` to be treated as an undefined variable,
    // which in turn can make `var(..., default=None)` behave like a required var.
    env.add_global("None", Value::from(()));
    env.add_global("True", Value::from(true));
    env.add_global("False", Value::from(false));

    env.add_func_func("fromjson", fromjson);
    env.add_func_func("tojson", tojson);
    env.add_func_func("fromyaml", fromyaml);
    env.add_func_func("toyaml", toyaml);
    env.add_function("set", set_fn());
    env.add_function("render", render_fn());
    env.add_function("set_strict", set_strict_fn());
    env.add_function("zip", zip_fn());
    env.add_function("zip_strict", zip_strict_fn());
    // TODO: log
    // env.add_global("invocation_id", panic!("TODO_INVOCATION_ID"));
    // TODO: modules
    // TODO: flags
    env.add_function("print", print_fn());
    env.add_function("log", log_fn());
    env.add_function("diff_of_two_dicts", diff_of_two_dicts_fn());
    env.add_function("local_md5", local_md5_fn());
    env.add_func_func("env_var", |state, args| env_var(false, None, state, args));
    env.add_function("try_or_compiler_error", try_or_compiler_error_fn());
    // var and env_Var are slightly different depending on the context
}

/// Silences the base context by overriding the print and log functions
pub fn silence_base_context(base_ctx: &mut BTreeMap<String, Value>) {
    base_ctx.insert(
        "print".to_string(),
        Value::from_function(|_args: &[Value], _kwargs: Kwargs| Ok(())),
    );
    base_ctx.insert(
        "log".to_string(),
        Value::from_function(|_args: &[Value], _kwargs: Kwargs| Ok(())),
    );
}

/// A struct that represents a reusable doc object to be used in configuration contexts
#[derive(Debug)]
pub struct DocMacro {
    /// The name of the current package being rendered
    package_name: String,
    /// The actual doc strings stored once to avoid duplication
    docs_content: Vec<String>,
    /// Maps (package_name, doc_name) to index in docs_content
    package_doc_map: HashMap<(String, String), usize>,
    /// Maps doc_name to a list of (package_name, content_index) pairs
    doc_name_map: HashMap<String, Vec<(String, usize)>>,
}

impl DocMacro {
    /// Initializes the doc macro
    pub fn new(package_name: String, docs: BTreeMap<(String, String), String>) -> Self {
        let mut docs_content = Vec::new();
        let mut package_doc_map = HashMap::new();
        let mut doc_name_map: HashMap<String, Vec<(String, usize)>> = HashMap::new();

        // Convert the BTreeMap into our optimized structure
        for ((package, doc_name), content) in docs {
            let content_idx = docs_content.len();
            docs_content.push(content);

            // Update both lookup maps
            package_doc_map.insert((package.clone(), doc_name.clone()), content_idx);
            doc_name_map
                .entry(doc_name)
                .or_default()
                .push((package, content_idx));
        }

        Self {
            package_name,
            docs_content,
            package_doc_map,
            doc_name_map,
        }
    }

    /// Lookup a doc by package name and doc name
    fn lookup_doc(&self, package_name: &str, doc_name: &str) -> Option<&str> {
        self.package_doc_map
            .get(&(package_name.to_string(), doc_name.to_string()))
            .map(|&idx| self.docs_content[idx].as_str())
    }

    /// Lookup a doc by doc name
    fn lookup_doc_in_packages(&self, doc_name: &str) -> Option<&str> {
        self.doc_name_map.get(doc_name).and_then(|package_indices| {
            // Return the first doc found in the list of packages
            package_indices
                .first()
                .map(|(_, idx)| self.docs_content[*idx].as_str())
        })
    }
}

impl Object for DocMacro {
    /// Implements the call method on the var object
    fn call(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        let mut args = ArgParser::new(args, None);
        let arg1 = args.get::<String>("");
        let arg2 = args.get_optional::<String>("");

        let (doc, target_package, doc_name) = match (&arg1, &arg2) {
            // Two arguments: explicit package and doc name
            (Ok(package_name), Some(doc_name)) => (
                self.lookup_doc(package_name, doc_name),
                package_name.clone(),
                doc_name.clone(),
            ),
            // One argument: search in current package first, then others
            (Ok(doc_name), None) => {
                if let Some(doc) = self.lookup_doc(&self.package_name, doc_name) {
                    (Some(doc), self.package_name.clone(), doc_name.clone())
                } else {
                    (
                        self.lookup_doc_in_packages(doc_name),
                        self.package_name.clone(),
                        doc_name.clone(),
                    )
                }
            }

            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidOperation,
                    "Invalid arguments to doc macro",
                ));
            }
        };

        match doc {
            Some(content) => Ok(Value::from_serialize(content)),
            None => {
                let status_reporter = get_status_reporter(state.env());
                let current_span = state.current_span_of_context();
                let current_file_path = state.current_path().clone();
                let location = CodeLocationWithFile::new(
                    current_span.start_line,
                    current_span.start_col,
                    current_span.start_offset,
                    current_file_path,
                );
                self.warn_missing_doc(&target_package, &doc_name, location, status_reporter);
                Ok(Value::from(Self::missing_doc_placeholder(
                    &target_package,
                    &doc_name,
                )))
            }
        }
    }
}

impl DocMacro {
    fn warn_missing_doc(
        &self,
        package_name: &str,
        doc_name: &str,
        location: CodeLocationWithFile,
        status_reporter: Option<&Arc<dyn StatusReporter>>,
    ) {
        let code = ErrorCode::InvalidConfig;
        let message = format!(
            "doc macro reference '{}' not found for package '{}'",
            doc_name, package_name
        );
        let warning = fs_err!(code, "{}", message).with_location(location);
        emit_warn_log_from_fs_error(&warning, status_reporter)
    }

    fn missing_doc_placeholder(package_name: &str, doc_name: &str) -> String {
        format!("<missing doc('{}', package='{}')>", doc_name, package_name)
    }
}

// Shared behavior for dbt var-like functions
pub trait VarFunction: Object {
    // Type-specific presence lookup
    fn contains_var(&self, state: &State<'_, '_>, var_name: &str) -> Result<bool, Error>;

    // Type-specific call as function
    fn call_as_function(
        &self,
        state: &State<'_, '_>,
        var_name: String,
        default_value: Option<Value>,
    ) -> Result<Value, Error>;

    // Parse var() arguments into (name, default)
    fn parse_args(args: &[Value]) -> Result<(String, Option<Value>), Error> {
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

        Ok((var_name, default_value))
    }

    // Consistent missing-var error shape
    fn missing_var_error<M>(package_name: &str, var_name: &str, vars_lookup: &M) -> Error
    where
        M: Serialize + ?Sized,
    {
        Error::new(
            ErrorKind::InvalidOperation,
            format!(
                "Required var '{}' not found in config:\nVars supplied to {} = {}",
                var_name,
                package_name,
                serde_json::to_string_pretty(vars_lookup).unwrap()
            ),
        )
    }

    // Common handler for `has_var`
    fn call_has_var(&self, state: &State<'_, '_>, args: &[Value]) -> Result<Value, Error> {
        if args.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "has_var requires 1 argument",
            ));
        }
        let var_name = args[0].as_str().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidOperation,
                "argument 'name' to has_var() has incompatible type; value is not a string",
            )
        })?;
        let present = self.contains_var(state, var_name)?;
        Ok(Value::from(present))
    }

    // Common implementation for `Object::call`
    fn call_impl(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        let (var_name, default_value) = Self::parse_args(args)?;
        self.call_as_function(state, var_name, default_value)
    }

    // Common implementation for `Object::call_method`
    fn call_method_impl(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        method: &str,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        if method == "has_var" {
            self.call_has_var(state, args)
        } else {
            Err(Error::new(
                ErrorKind::InvalidOperation,
                format!("Method {method} not found"),
            ))
        }
    }
}

/// A struct that returns a variable from a map of variables (with optional overrides)
/// https://github.com/dbt-labs/dbt-core/blob/31881d2a3bea030e700e9df126a3445298385698/core/dbt/context/base.py#L139
#[derive(Debug)]
pub struct Var {
    vars: BTreeMap<String, dbt_yaml::Value>,
    overrides: Option<BTreeMap<String, dbt_yaml::Value>>,
}

impl Var {
    /// Make a new Var struct
    pub fn new(vars: BTreeMap<String, dbt_yaml::Value>) -> Self {
        Self {
            vars,
            overrides: None,
        }
    }

    /// Make a new Var struct with override values
    pub fn with_overrides(
        vars: BTreeMap<String, dbt_yaml::Value>,
        overrides: Option<BTreeMap<String, dbt_yaml::Value>>,
    ) -> Self {
        Self { vars, overrides }
    }
}

impl VarFunction for Var {
    fn contains_var(&self, _state: &State<'_, '_>, var_name: &str) -> Result<bool, Error> {
        // Preserve existing behavior: check only self.vars
        Ok(self.vars.contains_key(var_name))
    }

    fn call_as_function(
        &self,
        _state: &State<'_, '_>,
        var_name: String,
        default_value: Option<Value>,
    ) -> Result<Value, Error> {
        if let Some(overrides) = &self.overrides
            && let Some(value) = overrides.get(&var_name)
        {
            Ok(Value::from_serialize(value.clone()))
        } else if let Some(value) = self.vars.get(&var_name) {
            Ok(Value::from_serialize(value.clone()))
        } else if let Some(default) = default_value {
            Ok(default)
        } else {
            Err(Self::missing_var_error(
                "<Configuration>",
                &var_name,
                &self.vars,
            ))
        }
    }
}

impl Object for Var {
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

type LookupFn = dyn Fn(&str) -> Option<Value>;

/// A function that returns an environment variable from the environment
///
/// `placeholder_on_secret_access` is used to control whether to return a placeholder
/// or produce a hard error when an attempt is made to access a secret environment variable.
///
/// `overrides_fn` is an optional function that can be checked before accessing the real
/// environment variable. Effectively, this allows for mocking or overriding environment
/// variables.
///
/// ```python
/// def env_var(self, var: str, default: Optional[str] = None) -> str
/// ```
/// https://github.com/dbt-labs/dbt-core/blob/303c63ccc836a357505f241dbe90c3abb7b73d57/core/dbt/context/base.py#L305
pub fn env_var(
    placeholder_on_secret_access: bool,
    overrides_fn: Option<&LookupFn>,
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

    let mut env_vars_guard = ENV_VARS.lock().unwrap();
    match (std::env::var(var), default) {
        (Ok(value), _) => {
            if is_secret {
                debug_assert!(placeholder_on_secret_access);
                let value = Value::from(SECRET_PLACEHOLDER.replace("{}", var));
                Ok(value)
            } else {
                env_vars_guard.insert(var.to_string(), value.clone());
                Ok(Value::from(value))
            }
        }
        (Err(_), Some(default)) => {
            env_vars_guard.insert(var.to_string(), DEFAULT_ENV_PLACEHOLDER.to_string());
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

/// Deserialize a JSON string into a Python object primitive (e.g., a dict or list).
///
/// ```python
/// def fromjson(string: str, default: Any = None) -> Any:
///     """The `fromjson` context method can be used to deserialize a json
///     string into a Python object primitive, eg. a `dict` or `list`.
///
///     :param value: The json string to deserialize
///     :param default: A default value to return if the `string` argument
///         cannot be deserialized (optional)
///
///     Usage:
///
///         {% set my_json_str = '{"abc": 123}' %}
///         {% set my_dict = fromjson(my_json_str) %}
///         {% do log(my_dict['abc']) %}
///     """
/// ```
pub fn fromjson(_state: &State, args: &[Value]) -> Result<Value, Error> {
    let iter = ArgsIter::new("fromjson", &["string"], args);
    let string = iter.next_arg::<&str>()?;
    let default = iter.next_kwarg::<Option<Value>>("default")?;
    iter.finish()?;

    // Try strict JSON first
    match serde_json::from_str::<serde_json::Value>(string) {
        Ok(value) => Ok(Value::from_serialize(value)),
        Err(json_err) => {
            // Fall back to YAML to support unquoted scalars or simple mappings
            match dbt_yaml::from_str::<dbt_yaml::Value>(string) {
                Ok(yaml_value) => Ok(Value::from_serialize(yaml_value)),
                Err(_) => match default {
                    Some(default_value) => Ok(default_value),
                    None => Err(Error::new(
                        ErrorKind::InvalidOperation,
                        format!("Failed to parse JSON: {json_err}"),
                    )),
                },
            }
        }
    }
}

/// Serialize a Python object primitive (e.g., a dict or list) to a JSON string.
///
/// NOTE: Not all dbt kwargs like separators/indent are fully implemented here.
///
/// ```python
/// def tojson(value: Any, default: Any = None, sort_keys: bool = False) -> Any:
///     """The `tojson` context method can be used to serialize a Python
///     object primitive, eg. a `dict` or `list` to a json string.
///
///     :param value: The value serialize to json
///     :param default: A default value to return if the `value` argument
///         cannot be serialized
///     :param sort_keys: If True, sort the keys.
///
///
///     Usage:
///
///         {% set my_dict = {"abc": 123} %}
///         {% set my_json_string = tojson(my_dict) %}
///         {% do log(my_json_string) %}
///     """
/// ```
pub fn tojson(_state: &State, args: &[Value]) -> Result<Value, Error> {
    let iter = ArgsIter::new("tojson", &["value"], args);
    let value = iter.next_arg::<&Value>()?;
    let default = iter.next_kwarg::<Option<&Value>>("default")?;
    let sort_keys = iter
        .next_kwarg::<Option<bool>>("sort_keys")?
        .unwrap_or(false);
    iter.finish()?;

    // Return default if value is undefined
    if value.is_undefined() {
        return match default {
            Some(default_value) => Ok(default_value.clone()),
            None => Ok(Value::from(())),
        };
    }

    // First convert to serde_json::Value for consistent serialization
    match serde_json::to_value(value) {
        Ok(mut json_value) => {
            if sort_keys && let Some(obj) = json_value.as_object_mut() {
                // Sort the keys using BTreeMap
                let sorted: serde_json::Map<String, serde_json::Value> = obj
                    .iter()
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                json_value = serde_json::Value::Object(sorted);
            }
            // Use Python-style formatting (space after colon)
            let json_str =
                to_json_string_python_style(&json_value).unwrap_or_else(|_| "{}".to_string());
            Ok(Value::from_safe_string(json_str))
        }
        Err(err) => match default {
            Some(default_value) => Ok(default_value.clone()),
            None => Err(Error::new(
                ErrorKind::InvalidOperation,
                format!("Failed to convert value to JSON: {err}"),
            )),
        },
    }
}

/// Deserialize a YAML string into a Python object primitive.
///
/// ```python
/// def fromyaml(value: str, default: Any = None) -> Any:
///     """The fromyaml context method can be used to deserialize a yaml string
///     into a Python object primitive, eg. a `dict` or `list`.
///
///     :param value: The yaml string to deserialize
///     :param default: A default value to return if the `string` argument
///         cannot be deserialized (optional)
///
///     Usage:
///
///         {% set my_yml_str -%}
///         dogs:
///          - good
///          - bad
///         {%- endset %}
///         {% set my_dict = fromyaml(my_yml_str) %}
///         {% do log(my_dict['dogs'], info=true) %}
///         -- ["good", "bad"]
///         {% do my_dict['dogs'].pop() }
///         {% do log(my_dict['dogs'], info=true) %}
///         -- ["good"]
///     """
/// ```
pub fn fromyaml(_state: &State, args: &[Value]) -> Result<Value, Error> {
    let iter = ArgsIter::new("fromyaml", &["value"], args);
    let value = iter.next_arg::<&str>()?;
    let default = iter.next_kwarg::<Option<Value>>("default")?;
    iter.finish()?;

    match dbt_yaml::from_str::<dbt_yaml::Value>(value) {
        Ok(serde_value) => Ok(Value::from_serialize(serde_value)),
        Err(err) => match default {
            Some(default_value) => Ok(default_value),
            None => Err(Error::new(
                ErrorKind::InvalidOperation,
                format!("Failed to parse YAML: {err}"),
            )),
        },
    }
}

/// Serialize a Python object primitive to a YAML string.
///
/// ```python
/// def toyaml(
///     value: Any, default: Optional[str] = None, sort_keys: bool = False
/// ) -> Optional[str]:
///     """The `tojson` context method can be used to serialize a Python
///     object primitive, eg. a `dict` or `list` to a yaml string.
///
///     :param value: The value serialize to yaml
///     :param default: A default value to return if the `value` argument
///         cannot be serialized
///     :param sort_keys: If True, sort the keys.
///
///
///     Usage:
///
///         {% set my_dict = {"abc": 123} %}
///         {% set my_yaml_string = toyaml(my_dict) %}
///         {% do log(my_yaml_string) %}
///     """
/// ```
pub fn toyaml(_state: &State, args: &[Value]) -> Result<Value, Error> {
    let iter = ArgsIter::new("toyaml", &["value"], args);
    let value = iter.next_arg::<&Value>()?;
    let default = iter.next_kwarg::<Option<&Value>>("default")?;
    let sort_keys = iter
        .next_kwarg::<Option<bool>>("sort_keys")?
        .unwrap_or(false);

    // If the value is undefined or none and there's a default, return it
    if value.is_undefined() || value.is_none() {
        return match default {
            Some(def) => Ok(def.clone()),
            // Return none for undefined/none values when no default is provided
            None => Ok(Value::from(())),
        };
    }

    // Convert the Minijinja Value to a serde_json::Value
    // Should this say YAML or JSON cause this is toyaml function, not sure
    let mut json_value = match serde_json::to_value(value) {
        Ok(val) => val,
        Err(err) => {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                format!("Failed to convert value to YAML: {err}"),
            ));
        }
    };

    // Sort keys if requested
    if sort_keys && let Some(obj) = json_value.as_object_mut() {
        let sorted_map: BTreeMap<_, _> = obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        *obj = sorted_map.into_iter().collect();
    }

    match dbt_yaml::to_string(&json_value) {
        Ok(yaml_str) => Ok(Value::from(yaml_str)),
        Err(err) => Err(Error::new(
            ErrorKind::InvalidOperation,
            format!("Failed to convert value to YAML: {err}"),
        )),
    }
}

/// A function that returns the difference between two dictionaries
/// Not documented in dbt Jinja docs, but included in base.py
pub fn diff_of_two_dicts_fn() -> impl Fn(&[Value], Kwargs) -> Result<Value, Error> {
    move |args: &[Value], kwargs: Kwargs| -> Result<Value, Error> {
        if args.len() != 2 {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "diff_of_two_dicts requires exactly 2 arguments",
            ));
        }

        let dict_a_arg = match (kwargs.get("dict_a"), args.first()) {
            (Ok(value), _) => value,
            (_, Some(value)) => value,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidOperation,
                    "diff_of_two_dicts requires a dict_a argument",
                ));
            }
        }
        .clone();

        let dict_b_arg = match (kwargs.get("dict_b"), args.get(1)) {
            (Ok(value), _) => value,
            (_, Some(value)) => value,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidOperation,
                    "diff_of_two_dicts requires a dict_b argument",
                ));
            }
        }
        .clone();

        let dict_a = parse_dict_of_lists(&dict_a_arg)?;
        let dict_b = parse_dict_of_lists(&dict_b_arg)?;

        // Convert dict_b to lowercase for case-insensitive comparison
        let mut dict_b_lowered: HashMap<String, Vec<String>> = HashMap::new();
        for (key, value_list) in dict_b {
            dict_b_lowered.insert(
                key.to_lowercase(),
                value_list.into_iter().map(|v| v.to_lowercase()).collect(),
            );
        }

        // Perform the difference
        let mut dict_diff: HashMap<String, Vec<String>> = HashMap::new();
        for (key, value_list) in dict_a {
            if let Some(lowered_b_vals) = dict_b_lowered.get(&key.to_lowercase()) {
                // Filter out values that appear in dict_b, ignoring case
                let diff: Vec<String> = value_list
                    .into_iter()
                    .filter(|v| !lowered_b_vals.contains(&v.to_lowercase()))
                    .collect();
                if !diff.is_empty() {
                    dict_diff.insert(key, diff);
                }
            } else {
                // Key doesn't exist in dict_b ignoring case, so keep all
                dict_diff.insert(key, value_list);
            }
        }
        Ok(Value::from_serialize(&dict_diff))
    }
}

/// Convert any iterable to a set with unique elements.
///
/// Args:
///     value: An iterable value to convert to a set (required)
///     default: (optional) Value to return if conversion fails (can be passed
///              as second positional argument or kwarg: default="...")
///
/// Example:
/// ```jinja
/// {% set my_list = [1, 2, 2, 3] %}
/// {% set unique_values = set(my_list) %}
/// -- Returns set with {1, 2, 3}
/// {% set empty = set([]) %}
/// -- Returns empty set
/// ```
pub fn set_fn() -> impl Fn(&[Value], Kwargs) -> Result<Value, Error> {
    move |args: &[Value], kwargs: Kwargs| -> Result<Value, Error> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "set() requires 1 argument",
            ));
        }

        let mut arg_parser = ArgParser::new(args, Some(kwargs));
        let value = arg_parser.get::<Value>("value")?;
        let default = arg_parser.get_optional::<Value>("default");

        match value.try_iter() {
            Ok(iter) => Ok(Value::from_object(iter.collect::<MutableSet>())),
            Err(_) => match default {
                Some(def) => Ok(def),
                None => Ok(Value::from(())),
            },
        }
    }
}
/// Renders a string as a Jinja template using the current context.
///
/// Args:
///     sql: The string to render as a template.
///
/// Example:
/// ```jinja
/// {% set rendered = render("Hello {{ this.name }}") %}
/// ```
///
/// Returns:
///     The rendered string with all template expressions evaluated in the current context.
///
/// Errors:
///     Raises an error if the argument is not a string or if rendering fails.
pub fn render_fn() -> impl Fn(&State, &[Value], Kwargs) -> Result<Value, Error> {
    move |state: &State, args: &[Value], _kwargs: Kwargs| -> Result<Value, Error> {
        if args.len() != 1 {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "render requires exactly one argument (the string to render)",
            ));
        }
        // dbt-core (Jinja2/Python) effectively accepts any value here and stringifies it.
        // In practice, many dbt projects call `render(...)` on values that can legitimately be
        // `none` (e.g. optional metadata-driven SQL snippets from `run_query`), expecting
        // `"None"` and handling that downstream.
        //
        // Fusion uses minijinja which is stricter by default; align behavior by accepting
        // `none` and treating it like Python's `str(None)` => `"None"`.
        let sql = if args[0].is_none() {
            "None"
        } else {
            args[0].as_str().ok_or_else(|| {
                Error::new(ErrorKind::InvalidOperation, "Argument must be a string")
            })?
        };

        let env = state.env();

        let template = env.template_from_str(sql)?;
        let rendered = template.render(state.get_base_context(), &[])?;
        Ok(Value::from(rendered))
    }
}

/// Strict version of set() that fails if the input is not iterable.
///
/// Args:
///     value: An iterable value to convert to a set
///
/// Example:
/// ```jinja
/// {% set my_list = [1, 2, 2, 3] %}
/// {% set unique_values = set_strict(my_list) %}
/// -- Returns [1, 2, 3] or fails if my_list is not iterable
/// ```
pub fn set_strict_fn() -> impl Fn(&[Value], Kwargs) -> Result<Value, Error> {
    move |args: &[Value], _kwargs: Kwargs| -> Result<Value, Error> {
        if args.len() != 1 {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "set_strict requires exactly 1 argument",
            ));
        }

        let value = &args[0];
        match value.try_iter() {
            Ok(iter) => {
                let set: BTreeSet<_> = iter.map(|v| v.to_string()).collect();
                Ok(Value::from_iter(set))
            }
            Err(_) => Err(Error::new(
                ErrorKind::InvalidOperation,
                "set_strict requires an iterable value",
            )),
        }
    }
}

/// Try to call a function and raise a CompilationError if it raises an exception.
///
/// Args:
///     message_if_exception: The message to raise if the function raises an exception
///     func: The function to call
///     *args: The arguments to pass to the function
///     **kwargs: The keyword arguments to pass to the function
///
/// Example:
/// ```jinja
/// {% set result = try_or_compiler_error("Error", my_function, arg1, arg2, kwarg1="value1", kwarg2="value2") %}
/// ```
pub fn try_or_compiler_error_fn()
-> impl Fn(&State<'_, '_>, &[Value], Kwargs) -> Result<Value, Error> {
    move |state: &State<'_, '_>, args: &[Value], kwargs: Kwargs| -> Result<Value, Error> {
        let mut args = ArgParser::new(args, Some(kwargs));
        let message_if_exception = args.get::<String>("message_if_exception")?;
        let location_of_func = args.get::<Value>("loc")?;
        let func_name = args.get::<String>("func")?;
        // Get remaining args and kwargs
        let mut remaining_args = args.get_args_as_vec_of_values();
        let drained_kwargs = args.drain_kwargs();
        let remaining_kwargs = Kwargs::from_iter(drained_kwargs);
        remaining_args.push(remaining_kwargs.into());

        let result = if location_of_func.is_none() {
            let func = state.lookup(&func_name).ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidOperation,
                    format!("Unknown function {func_name}"),
                )
            })?;
            func.call(state, &remaining_args, &[])
        } else {
            location_of_func.call_method(state, &func_name, &remaining_args, &[])
        };
        match result {
            Ok(result) => Ok(result),
            // TODO: we need to raise CompilationError(message_if_exception, self.model)
            Err(_) => Err(Error::new(
                ErrorKind::InvalidOperation,
                message_if_exception,
            )),
        }
    }
}

/// Return an iterator of tuples where each tuple contains the i-th element from each of the input iterables.
/// dbt's zip also supports a custom kwarg `fillvalue` (default=None) to match the longest iterable.
///
/// Args:
///     *iterables: Two or more iterables
///     fillvalue: (optional) Value to fill in shorter iterables, default=None
///
/// Example:
/// ```jinja
/// {% set list1 = [1, 2] %}
/// {% set list2 = ['a', 'b', 'c'] %}
/// {% set pairs = zip(list1, list2, fillvalue='N/A') %}
/// -- Returns [(1, 'a'), (2, 'b'), ('N/A', 'c')]
/// ```
pub fn zip_fn() -> impl Fn(&[Value], Kwargs) -> Result<Value, Error> {
    move |args: &[Value], kwargs: Kwargs| -> Result<Value, Error> {
        if args.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "zip requires at least 1 argument",
            ));
        }

        let default = match (kwargs.get::<Value>("default"), args.get(1)) {
            (Ok(value), _) => Some(value),
            (_, Some(value)) => Some(value.clone()),
            _ => None,
        };

        // Try to convert each argument to an internal Vec<Value>
        let mut iterators: Vec<Vec<Value>> = Vec::new();
        for arg in args {
            match arg.try_iter() {
                Ok(iter) => iterators.push(iter.collect()),
                Err(_) => return Ok(default.unwrap_or_else(|| Value::from(()))),
            }
        }

        // Find shortest length (Python's zip stops at shortest iterator)
        let min_len = iterators.iter().map(|v| v.len()).min().unwrap_or(0);

        let mut zipped = Vec::new();
        for i in 0..min_len {
            let tuple: Vec<Value> = iterators.iter().map(|iter| iter[i].clone()).collect();
            zipped.push(Value::from(tuple));
        }

        Ok(Value::from_iter(zipped))
    }
}

/// Strict version of zip() that fails if any input is not iterable or the iterables differ in length.
///
/// Args:
///     *iterables: Two or more iterables
///
/// Example:
/// ```jinja
/// {% set list1 = [1, 2, 3] %}
/// {% set list2 = ['a', 'b', 'c'] %}
/// {% set pairs = zip_strict(list1, list2) %}
/// -- Returns [(1, 'a'), (2, 'b'), (3, 'c')] or fails if inputs aren't iterable or lengths differ
/// ```
pub fn zip_strict_fn() -> impl Fn(&[Value], Kwargs) -> Result<Value, Error> {
    move |args: &[Value], _kwargs: Kwargs| -> Result<Value, Error> {
        if args.len() < 2 {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "zip_strict requires two or more iterable arguments",
            ));
        }

        let mut iterators: Vec<Vec<Value>> = Vec::new();
        for arg in args {
            match arg.try_iter() {
                Ok(iter) => iterators.push(iter.collect()),
                Err(_) => {
                    return Err(Error::new(
                        ErrorKind::InvalidOperation,
                        "zip_strict requires all arguments to be iterable",
                    ));
                }
            }
        }

        // Find shortest length (Python's zip behavior)
        let min_len = iterators.iter().map(|v| v.len()).min().unwrap_or(0);

        let mut zipped = Vec::new();
        for i in 0..min_len {
            let tuple: Vec<Value> = iterators.iter().map(|iter| iter[i].clone()).collect();
            zipped.push(Value::from(tuple));
        }

        Ok(Value::from_iter(zipped))
    }
}

/// Print a message to the log file and stdout.
///
/// Args:
///     msg: Message to print
///
/// Example:
/// ```jinja
/// {{ print("Hello world!") }}
/// ```
pub fn print_fn() -> impl Fn(&State<'_, '_>, &[Value], Kwargs) -> Result<Value, Error> {
    move |state: &State<'_, '_>, args: &[Value], _kwargs: Kwargs| -> Result<Value, Error> {
        if args.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "print requires at least one argument (a message to print)",
            ));
        }
        if args.len() > 1 {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "print accepts only one argument",
            ));
        }

        // Format the message using Display formatting (not Debug) to match dbt's behavior
        // This ensures strings aren't wrapped in quotes (e.g., "string" instead of "'string'")
        let msg = format!("{}", args[0]);

        // Get metadata for the event
        let current_package_name = state
            .lookup(TARGET_PACKAGE_NAME)
            .and_then(|v| v.as_str().map(|s| s.to_string()));
        let line = state.current_span().start_line;
        let column = state.current_span().start_col;
        let cur_file_path = state.current_path().to_str().map(str::to_string);

        // Emit UserLogMessage event for print
        emit_info_event(
            UserLogMessage::print(
                current_package_name,
                Some(line),
                Some(column),
                cur_file_path,
            ),
            Some(&msg),
        );

        Ok(Value::from(""))
    }
}

/// Print a message to the log file and stdout.
///
/// Args:
///     msg: Message to print
///     info: If true, log at info level. If False emit at debug level.
///
/// Example:
/// ```jinja
/// {{ log("Hello world!", info=true) }}
/// ```
pub fn log_fn() -> impl Fn(&State<'_, '_>, &[Value], Kwargs) -> Result<Value, Error> {
    move |state: &State<'_, '_>, args: &[Value], kwargs: Kwargs| -> Result<Value, Error> {
        let mut args = ArgParser::new(args, Some(kwargs));
        let msg = args.get::<Value>("msg")?.to_string();
        let info = args.get::<Value>("info").ok();

        // Get metadata for the event
        let current_package_name = state
            .lookup(TARGET_PACKAGE_NAME)
            .and_then(|v| v.as_str().map(|s| s.to_string()));
        let line = state.current_span().start_line;
        let column = state.current_span().start_col;
        let cur_file_path = state.current_path().to_str().map(str::to_string);

        if info.is_some() && info.unwrap().is_true() {
            // Emit UserLogMessage event for log
            emit_info_event(
                UserLogMessage::log_info(
                    current_package_name,
                    Some(line),
                    Some(column),
                    cur_file_path,
                ),
                Some(&msg),
            );
        } else {
            // Emit UserLogMessage event for debug log
            emit_debug_event(
                UserLogMessage::log_debug(
                    current_package_name,
                    Some(line),
                    Some(column),
                    cur_file_path,
                ),
                Some(&msg),
            );
        }

        Ok(Value::from(""))
    }
}

/// Calculate an MD5 hash of the given string.
///
/// Args:
///     value: String to hash
///
/// Example:
/// ```jinja
/// {% set hash = local_md5("hello") %}
/// -- Returns "5d41402abc4b2a76b9719d911017c592"
/// ```
pub fn local_md5_fn() -> impl Fn(&[Value], Kwargs) -> Result<Value, Error> {
    move |args: &[Value], _kwargs: Kwargs| -> Result<Value, Error> {
        if args.len() != 1 {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "local_md5 requires exactly 1 argument",
            ));
        }

        let value = args[0].as_str().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidOperation,
                "local_md5's argument must be a string",
            )
        })?;
        // Create MD5 hasher
        let result = format!("{:x}", md5::compute(value.as_bytes()));
        Ok(Value::from(result))
    }
}

/// Parse a dictionary of lists into a BTreeMap<String, Vec<String>>
fn parse_dict_of_lists(dict: &Value) -> Result<BTreeMap<String, Vec<String>>, Error> {
    let mut result = BTreeMap::new();

    // Iterate over the keys in the dictionary
    for key in dict.try_iter()? {
        // Get the value associated with the key
        let value = dict.get_item(&key)?;

        // Try to iterate over the value as a list
        let mut value_list = Vec::new();
        for item in value.try_iter()? {
            value_list.push(item.to_string());
        }
        // Insert the key-value pair into the result
        result.insert(key.to_string(), value_list);
    }

    Ok(result)
}

/// A struct that represents the 'exceptions' object, which makes exceptions.warn() and...
#[derive(Debug)]
pub struct Exceptions {
    io_args: IoArgs,
}

impl Object for Exceptions {
    // todo: create a shared enum for call_method and get_value to work off
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        // exceptions evaluate to 'true' in Python for truthiness checks
        // usage: https://github.com/dbt-labs/dbt-adapters/blob/be0ab62ae2ad3504a37287f7a4ac12d30e7e94d9/dbt-adapters/src/dbt/include/global_project/macros/materializations/snapshots/helpers.sql#L271
        match key.as_str()? {
            "warn_snapshot_timestamp_data_types" => Some(Value::from(true)),
            _ => None,
        }
    }

    fn call_method(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        method: &str,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        // reference: core/dbt/context/exceptions_jinja.py
        // We are only implementing methods actually used in the supported adapters.
        match method {
            "warn" => {
                let mut args = ArgParser::new(args, None);
                let warn_string = args.get::<String>("").unwrap_or_else(|_| "".to_string());

                emit_warn_log_message(
                    ErrorCode::DependencyWarning,
                    warn_string,
                    self.io_args.status_reporter.as_ref(),
                );

                Ok(Value::UNDEFINED)
            }
            // (msg, node=None)
            "raise_compiler_error" => {
                let mut args = ArgParser::new(args, None);
                let message = args.get::<String>("msg")?;
                if let Some((node_id, file_path)) = node_metadata_from_state(state) {
                    Err(Error::new(
                        ErrorKind::InvalidOperation,
                        format!(
                            "Compilation Error for {} from {}: {}",
                            node_id,
                            file_path.display(),
                            message
                        ),
                    ))
                } else {
                    Err(Error::new(
                        ErrorKind::InvalidOperation,
                        format!("Compilation Error: {message}"),
                    ))
                }
            }
            // (msg) String
            "raise_not_implemented" => {
                let mut args = ArgParser::new(args, None);
                let message = args.get::<String>("msg")?;
                Err(Error::new(
                    ErrorKind::InvalidOperation,
                    format!("Not implemented: {message}"),
                ))
            }
            // (relation, expected_type, model=None)
            //   Relation,  String
            "relation_wrong_type" => {
                let mut args = ArgParser::new(args, None);
                let relation = args.get::<Value>("relation").unwrap_or(Value::UNDEFINED);
                let expected_type = args.get::<String>("expected_type").unwrap_or_default();

                // Get the relation type from the relation value
                let relation_type = if relation.is_undefined() {
                    "unknown".to_string()
                } else {
                    match relation.get_item(&Value::from("type")) {
                        Ok(type_value) => type_value.to_string(),
                        Err(_) => "unknown".to_string(),
                    }
                };

                let message = format!(
                    "Trying to create {expected_type} {relation}, but it currently exists as a {relation_type}. Either drop {relation} manually, or run dbt with `--full-refresh` and dbt will drop it for you."
                );
                if let Some((node_id, file_path)) = node_metadata_from_state(state) {
                    Err(Error::new(
                        ErrorKind::InvalidOperation,
                        format!(
                            "Compilation Error for {} from {}: {}",
                            node_id,
                            file_path.display(),
                            message
                        ),
                    ))
                } else {
                    Err(Error::new(
                        ErrorKind::InvalidOperation,
                        format!("Compilation Error: {message}"),
                    ))
                }
            }
            // (yaml_columns, sql_columns)
            // [{"name": ..., "data_type": ..., "formatted": ...},...]
            "raise_contract_error" => {
                let mut args = ArgParser::new(args, None);
                let yaml_columns = args
                    .get::<Value>("yaml_columns")
                    .unwrap_or(Value::UNDEFINED);
                let sql_columns = args.get::<Value>("sql_columns").unwrap_or(Value::UNDEFINED);
                let column_diff_table: &Arc<AgateTable> =
                    get_contract_mismatches(yaml_columns, sql_columns)?;
                let column_diff_display = column_diff_table
                    .display()
                    .with_max_rows(50)
                    .with_max_columns(50)
                    .with_max_column_width(50);
                let message = format_args!(
                    "This model has an enforced contract that failed.\n Please ensure the name, data_type, and number of columns in your contract match the columns in your model's definition.\n\n{column_diff_display}"
                );
                if let Some((node_id, file_path)) = node_metadata_from_state(state) {
                    Err(Error::new(
                        ErrorKind::InvalidOperation,
                        format!(
                            "Compilation Error for {} from {}: {}",
                            node_id,
                            file_path.display(),
                            message
                        ),
                    ))
                } else {
                    Err(Error::new(
                        ErrorKind::InvalidOperation,
                        format!("Compilation Error: {message}"),
                    ))
                }
            }
            // (column_names)
            // ["column1", "column2"]
            "column_type_missing" => {
                let mut args = ArgParser::new(args, None);
                // column_names should be a list of strings
                let column_names = args
                    .get::<Value>("column_names")
                    .unwrap_or(Value::UNDEFINED);

                // Convert column_names to a vector of strings
                let column_names_string = if column_names.is_undefined() {
                    "".to_string()
                } else {
                    match column_names.try_iter() {
                        Ok(iter) => {
                            let strings: Vec<String> = iter.map(|v| v.to_string()).collect();
                            strings.join(", ")
                        }
                        Err(_) => "".to_string(),
                    }
                };

                let message = format!(
                    "Contracted models require data_type to be defined for each column.  Please ensure that the column name and data_type are defined within the YAML configuration for the {column_names_string} column(s)."
                );
                if let Some((node_id, file_path)) = node_metadata_from_state(state) {
                    Err(Error::new(
                        ErrorKind::InvalidOperation,
                        format!(
                            "Contract Error for {} from {}: {}",
                            node_id,
                            file_path.display(),
                            message
                        ),
                    ))
                } else {
                    Err(Error::new(
                        ErrorKind::InvalidOperation,
                        format!("Contract Error: {message}"),
                    ))
                }
            }
            // (msg, node=None)
            "raise_fail_fast_error" => {
                let mut args = ArgParser::new(args, None);
                let message = args.get::<String>("msg")?;
                if let Some((node_id, file_path)) = node_metadata_from_state(state) {
                    Err(Error::new(
                        ErrorKind::InvalidOperation,
                        format!(
                            "FailFast Error for {} from {}: {}",
                            node_id,
                            file_path.display(),
                            message
                        ),
                    ))
                } else {
                    Err(Error::new(
                        ErrorKind::InvalidOperation,
                        format!("FailFast Error: {message}"),
                    ))
                }
            }
            // (snapshot_time_data_type: str, updated_at_data_type: str)
            "warn_snapshot_timestamp_data_types" => {
                let mut args = ArgParser::new(args, None);
                let snapshot_time_data_type = args
                    .get::<String>("snapshot_time_data_type")
                    .unwrap_or_else(|_| "".to_string());
                let updated_at_data_type = args
                    .get::<String>("updated_at_data_type")
                    .unwrap_or_else(|_| "".to_string());
                let warning = format!(
                    "Data type of snapshot table timestamp columns ({snapshot_time_data_type}) doesn't match derived column 'updated_at' ({updated_at_data_type}). Please update snapshot config 'updated_at'."
                );

                emit_warn_log_message(
                    ErrorCode::InvalidConfig,
                    warning,
                    self.io_args.status_reporter.as_ref(),
                );

                Ok(Value::UNDEFINED)
            }
            _ => Err(Error::new(
                ErrorKind::UnknownMethod,
                format!("Unknown method on Exceptions: {method}"),
            )),
        }
    }
}

/// Builds a flat graph for use in a compile context, using
/// a serialized manifest and restricting to particular keys
pub fn build_flat_graph(nodes: &Nodes) -> MutableMap {
    let mut graph = ValueMap::new();
    let nodes_insert: BTreeMap<String, Value> = nodes
        .models
        .iter()
        .map(|(unique_id, model)| {
            let mut serialized = (Arc::as_ref(model) as &dyn InternalDbtNode).serialize_keep_none();
            if let YmlValue::Mapping(ref mut map, _) = serialized {
                // Set description to empty string if null
                let desc_key = YmlValue::string("description".to_string());
                if let Some(desc_value) = map.get_mut(&desc_key) {
                    if desc_value.as_str().is_none() {
                        *desc_value = YmlValue::string("".to_string());
                    }
                }
                // Strip "models/" prefix from path if present.
                // dbt-core's manifest stores paths relative to the models folder (e.g., "3-data_vault/...")
                // not including "models/" prefix. Customer macros like bfs_find_all_downstream_nodes
                // rely on path.startswith() checks that assume this format.
                let path_key = YmlValue::string("path".to_string());
                if let Some(path_value) = map.get(&path_key) {
                    if let Some(path_str) = path_value.as_str() {
                        let stripped = path_str.strip_prefix("models/").unwrap_or(path_str);
                        map.insert(path_key, YmlValue::string(stripped.to_string()));
                    }
                }
            }
            (unique_id.clone(), Value::from_serialize(serialized))
        })
        .chain(nodes.snapshots.iter().map(|(unique_id, snapshot)| {
            let mut serialized =
                (Arc::as_ref(snapshot) as &dyn InternalDbtNode).serialize_keep_none();
            // Strip resource folder prefix from path for consistency with dbt-core manifest format
            if let YmlValue::Mapping(ref mut map, _) = serialized {
                let path_key = YmlValue::string("path".to_string());
                if let Some(path_value) = map.get(&path_key) {
                    if let Some(path_str) = path_value.as_str() {
                        let stripped = path_str.strip_prefix("snapshots/").unwrap_or(path_str);
                        map.insert(path_key, YmlValue::string(stripped.to_string()));
                    }
                }
            }
            (unique_id.clone(), Value::from_serialize(serialized))
        }))
        .chain(nodes.tests.iter().map(|(unique_id, test)| {
            // For tests, update original_file_path to use patch_path if it exists
            // we also do this this when we write the test to the manifest
            // (indicates test was defined in a YAML file)
            let mut serialized = (Arc::as_ref(test) as &dyn InternalDbtNode).serialize_keep_none();
            if let YmlValue::Mapping(ref mut map, _) = serialized {
                if let Some(patch_path) = &test.__common_attr__.patch_path {
                    map.insert(
                        YmlValue::string("original_file_path".to_string()),
                        YmlValue::string(patch_path.display().to_string()),
                    );
                }
                // For tests, use just the file name (not the full path) for consistency with dbt-core manifest format
                // Generic tests have paths like "tests/generic_tests/not_null_foo_id.sql" but manifest expects "not_null_foo_id.sql"
                let path_key = YmlValue::string("path".to_string());
                if let Some(path_value) = map.get(&path_key) {
                    if let Some(path_str) = path_value.as_str() {
                        let file_name = std::path::Path::new(path_str)
                            .file_name()
                            .and_then(|s| s.to_str())
                            .unwrap_or(path_str);
                        map.insert(path_key.clone(), YmlValue::string(file_name.to_string()));
                    }
                }
                // Set severity to ERROR in config if not already set
                let config_key = YmlValue::string("config".to_string());
                if let Some(YmlValue::Mapping(config_map, _)) = map.get_mut(&config_key) {
                    let severity_key = YmlValue::string("severity".to_string());
                    let needs_default = match config_map.get(&severity_key) {
                        None => true,
                        Some(v) => v.as_str().is_none(),
                    };
                    if needs_default {
                        config_map.insert(severity_key, YmlValue::string("ERROR".to_string()));
                    }
                }
            }
            (unique_id.clone(), Value::from_serialize(serialized))
        }))
        .chain(nodes.seeds.iter().map(|(unique_id, seed)| {
            let mut serialized = (Arc::as_ref(seed) as &dyn InternalDbtNode).serialize_keep_none();
            // Strip resource folder prefix from path for consistency with dbt-core manifest format
            if let YmlValue::Mapping(ref mut map, _) = serialized {
                let path_key = YmlValue::string("path".to_string());
                if let Some(path_value) = map.get(&path_key) {
                    if let Some(path_str) = path_value.as_str() {
                        let stripped = path_str.strip_prefix("seeds/").unwrap_or(path_str);
                        map.insert(path_key, YmlValue::string(stripped.to_string()));
                    }
                }
            }
            (unique_id.clone(), Value::from_serialize(serialized))
        }))
        .collect();
    graph.insert(Value::from("nodes"), Value::from_serialize(nodes_insert));

    let sources_insert: BTreeMap<String, Value> = nodes
        .sources
        .iter()
        .map(|(unique_id, source)| {
            (
                unique_id.clone(),
                Value::from_serialize(
                    (Arc::as_ref(source) as &dyn InternalDbtNode).serialize_keep_none(),
                ),
            )
        })
        .collect();

    graph.insert(
        Value::from("sources"),
        Value::from_serialize(sources_insert),
    );
    let exposures_insert: BTreeMap<String, Value> = nodes
        .exposures
        .iter()
        .map(|(unique_id, exposure)| {
            (
                unique_id.clone(),
                Value::from_serialize((Arc::as_ref(exposure) as &dyn InternalDbtNode).serialize()),
            )
        })
        .collect();
    graph.insert(
        Value::from("exposures"),
        Value::from_serialize(exposures_insert),
    );
    let groups_insert: BTreeMap<String, Value> = nodes
        .groups
        .iter()
        .map(|(unique_id, group)| (unique_id.clone(), Value::from_serialize(Arc::as_ref(group))))
        .collect();
    graph.insert(Value::from("groups"), Value::from_serialize(groups_insert));
    let metrics_insert: BTreeMap<String, Value> = nodes
        .metrics
        .iter()
        .map(|(unique_id, metric)| {
            (
                unique_id.clone(),
                Value::from_serialize((Arc::as_ref(metric) as &dyn InternalDbtNode).serialize()),
            )
        })
        .collect();
    graph.insert(
        Value::from("metrics"),
        Value::from_serialize(metrics_insert),
    );
    let semantic_models_insert: BTreeMap<String, Value> = nodes
        .semantic_models
        .iter()
        .map(|(unique_id, semantic_model)| {
            (
                unique_id.clone(),
                Value::from_serialize(
                    (Arc::as_ref(semantic_model) as &dyn InternalDbtNode).serialize(),
                ),
            )
        })
        .collect();
    graph.insert(
        Value::from("semantic_models"),
        Value::from_serialize(semantic_models_insert),
    );
    let saved_queries_insert: BTreeMap<String, Value> = nodes
        .saved_queries
        .iter()
        .map(|(unique_id, saved_query)| {
            (
                unique_id.clone(),
                Value::from_serialize(
                    (Arc::as_ref(saved_query) as &dyn InternalDbtNode).serialize(),
                ),
            )
        })
        .collect();
    graph.insert(
        Value::from("saved_queries"),
        Value::from_serialize(saved_queries_insert),
    );
    MutableMap::from(graph)
}

#[cfg(test)]
mod tests {
    use super::*;
    use minijinja::{Environment, Value};
    use minijinja_contrib::pycompat::unknown_method_callback;

    #[test]
    fn test_set_union_integration() {
        let mut env = Environment::new();

        // Register the set function from base.rs
        env.add_function("set", set_fn());

        // Enable pycompat for union() method
        env.set_unknown_method_callback(unknown_method_callback);

        // Test the exact DBT use case: {% set res = set([1, 2]).union(set([3, 4])) %}
        let template_source = r#"
        {%- set set1 = set([1, 2, 2]) -%}
        {%- set set2 = set([3, 4, 4]) -%}
        {%- set result = set1.union(set2) -%}
        {{ result | sort | join(',') }}
        "#;

        let tmpl = env.template_from_str(template_source).unwrap();
        let output = tmpl.render(Value::UNDEFINED, &[]).unwrap();

        // Should contain all unique elements from both sets: 1,2,3,4
        let result = output.trim();
        assert_eq!(result, "1,2,3,4");
    }

    #[test]
    fn test_set_union_multiple_args() {
        let mut env = Environment::new();
        env.add_function("set", set_fn());
        env.set_unknown_method_callback(unknown_method_callback);

        let template_source = r#"
        {%- set set1 = set([1, 2]) -%}
        {%- set set2 = set([3, 4]) -%}
        {%- set set3 = set([5, 6]) -%}
        {%- set result = set1.union(set2, set3) -%}
        {{ result | length }}
        "#;

        let tmpl = env.template_from_str(template_source).unwrap();
        let output = tmpl.render(Value::UNDEFINED, &[]).unwrap();

        // Should have 6 unique elements
        assert_eq!(output.trim(), "6");
    }

    #[test]
    fn test_set_union_with_duplicates() {
        let mut env = Environment::new();
        env.add_function("set", set_fn());
        env.set_unknown_method_callback(unknown_method_callback);

        let template_source = r#"
        {%- set original = [1, 1, 2, 2, 3] -%}
        {%- set other = [3, 4, 4, 5] -%}
        {%- set result = set(original).union(set(other)) -%}
        {{ result | sort | join(',') }}
        "#;

        let tmpl = env.template_from_str(template_source).unwrap();
        let output = tmpl.render(Value::UNDEFINED, &[]).unwrap();

        // Should remove duplicates: 1,2,3,4,5
        assert_eq!(output.trim(), "1,2,3,4,5");
    }

    #[test]
    fn test_render_accepts_none() {
        let mut env = Environment::new();
        env.add_function("render", render_fn());

        // dbt projects frequently pass `none` into `render(...)` (often after `run_query`)
        // and expect it to stringify to "None" (Python/Jinja2 behavior).
        let template_source = r#"{{ render(None) }}"#;
        let tmpl = env.template_from_str(template_source).unwrap();
        let output = tmpl.render(Value::UNDEFINED, &[]).unwrap();

        assert_eq!(output.trim(), "None");
    }

    #[test]
    fn test_tojson_simple_dict() {
        let mut env = Environment::new();
        env.add_function("tojson", tojson);

        let template_source = r#"{{ tojson({'a': 1, 'b': 'hello'}) }}"#;
        let tmpl = env.template_from_str(template_source).unwrap();
        let output = tmpl.render(Value::UNDEFINED, &[]).unwrap();
        assert_eq!(output.trim(), r#"{"a": 1, "b": "hello"}"#);
    }

    #[test]
    fn test_tojson_nested_dict() {
        let mut env = Environment::new();
        env.add_function("tojson", tojson);

        let template_source = r#"{{ tojson({'a': 1, 'b': {'c': 3}}) }}"#;
        let tmpl = env.template_from_str(template_source).unwrap();
        let output = tmpl.render(Value::UNDEFINED, &[]).unwrap();
        assert_eq!(output.trim(), r#"{"a": 1, "b": {"c": 3}}"#);
    }

    #[test]
    fn test_tojson_with_sort_keys() {
        let mut env = Environment::new();
        env.add_function("tojson", tojson);

        let template_source = r#"{{ tojson({'b': 2, 'a': 1}, sort_keys=True) }}"#;
        let tmpl = env.template_from_str(template_source).unwrap();
        let output = tmpl.render(Value::UNDEFINED, &[]).unwrap();
        assert_eq!(output.trim(), r#"{"a": 1, "b": 2}"#);
    }

    #[test]
    fn test_toyaml_simple_dict() {
        let mut env = Environment::new();
        env.add_function("toyaml", toyaml);

        let template_source = r#"{{ toyaml({'a': 1, 'b': 'hello'}) }}"#;
        let tmpl = env.template_from_str(template_source).unwrap();
        let output = tmpl.render(Value::UNDEFINED, &[]).unwrap();
        assert_eq!(output.trim(), "a: 1\nb: hello");
    }

    #[test]
    fn test_toyaml_nested_dict() {
        let mut env = Environment::new();
        env.add_function("toyaml", toyaml);

        let template_source = r#"{{ toyaml({'a': 1, 'b': {'c': 3}}) }}"#;
        let tmpl = env.template_from_str(template_source).unwrap();
        let output = tmpl.render(Value::UNDEFINED, &[]).unwrap();
        assert_eq!(output.trim(), "a: 1\nb:\n  c: 3");
    }

    #[test]
    fn test_toyaml_with_sort_keys() {
        let mut env = Environment::new();
        env.add_function("toyaml", toyaml);

        let template_source = r#"{{ toyaml({'b': 2, 'a': 1}, sort_keys=True) }}"#;
        let tmpl = env.template_from_str(template_source).unwrap();
        let output = tmpl.render(Value::UNDEFINED, &[]).unwrap();
        assert_eq!(output.trim(), "a: 1\nb: 2");
    }

    #[test]
    fn doc_macro_missing_doc_returns_placeholder() {
        let mut env = Environment::new();
        let docs = BTreeMap::<(String, String), String>::new();
        env.add_global(
            "doc",
            Value::from_object(DocMacro::new("pkg".to_string(), docs)),
        );

        let template_source = "{{ doc('unknown') }}";
        let tmpl = env.template_from_str(template_source).unwrap();
        let output = tmpl.render(Value::UNDEFINED, &[]).unwrap();
        assert!(
            output.contains("<missing doc('unknown', package='pkg')>"),
            "expected placeholder, got {output}"
        );
    }

    #[test]
    fn test_fromjson_parses_plain_string_via_yaml_fallback() {
        let mut env = Environment::new();
        env.add_func_func("fromjson", fromjson);

        // Should parse as a string via YAML fallback when JSON parsing fails
        let tmpl = env
            .template_from_str("{{ fromjson('i_am_string') }}")
            .unwrap();
        let output = tmpl.render(Value::UNDEFINED, &[]).unwrap();
        assert_eq!(output.trim(), "i_am_string");
    }

    fn make_env_with_var() -> minijinja::Environment<'static> {
        let mut env = minijinja::Environment::new();

        // Mirror Fusion's global Jinja context: dbt projects frequently use
        // Python-ish constants (capitalized) like `None`.
        env.add_global("None", Value::from(()));
        env.add_global("True", Value::from(true));
        env.add_global("False", Value::from(false));

        env.add_global("var", Value::from_object(Var::new(BTreeMap::new())));
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

    #[test]
    fn build_flat_graph_populates_semantic_models_metrics_saved_queries_and_groups() {
        use dbt_schemas::schemas::manifest::{DbtMetric, DbtSavedQuery, DbtSemanticModel};
        use dbt_schemas::schemas::nodes::DbtGroup;

        let mut nodes = Nodes::default();

        // Insert one entry per collection
        nodes.semantic_models.insert(
            "semantic_model.pkg.sm1".to_string(),
            Arc::new(DbtSemanticModel::default()),
        );
        nodes.metrics.insert(
            "metric.pkg.m1".to_string(),
            Arc::new(DbtMetric {
                __common_attr__: Default::default(),
                __base_attr__: Default::default(),
                __metric_attr__: Default::default(),
                deprecated_config: Default::default(),
                __other__: Default::default(),
            }),
        );
        nodes.saved_queries.insert(
            "saved_query.pkg.sq1".to_string(),
            Arc::new(DbtSavedQuery {
                __common_attr__: Default::default(),
                __base_attr__: Default::default(),
                __saved_query_attr__: Default::default(),
                deprecated_config: Default::default(),
                __other__: Default::default(),
            }),
        );
        nodes
            .groups
            .insert("group.pkg.g1".to_string(), Arc::new(DbtGroup::default()));

        let graph = build_flat_graph(&nodes);
        let graph_val = Value::from_object(graph);

        // Each key should contain exactly one entry
        for key in &["semantic_models", "metrics", "saved_queries", "groups"] {
            let collection = graph_val.get_attr(key).unwrap();
            assert_ne!(
                collection.len(),
                Some(0),
                "expected graph.{key} to be non-empty"
            );
        }
    }
}
