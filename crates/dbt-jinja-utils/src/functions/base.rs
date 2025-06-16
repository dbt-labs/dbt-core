//! Core functions that are shared across all contexts

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    rc::Rc,
    sync::Arc,
};

use dbt_schemas::schemas::manifest::{DbtNode, InternalDbtNode, Nodes};
use minijinja::value::{mutable_map::MutableMap, ValueMap};

use minijinja::{
    arg_utils::ArgParser,
    listener::RenderingEventListener,
    value::{Kwargs, Object},
    Environment, Error, ErrorKind, State, Value,
};

use crate::utils::{
    node_metadata_from_state, DBT_INTERNAL_ENV_VAR_PREFIX, ENV_VARS, SECRET_ENV_VAR_PREFIX,
};

/// The default placeholder for environment variables when the default value is used
pub const DEFAULT_ENV_PLACEHOLDER: &str = "__dbt_placeholder__";

/// Registers all the functions shared across all contexts
pub fn register_base_functions(env: &mut Environment) {
    env.add_global("dbt_version", Value::from(crate::utils::DBT_VERSION));
    env.add_global("exceptions".to_owned(), Value::from_object(Exceptions {}));

    env.add_function("return", return_macro);
    env.add_function("fromjson", fromjson_fn());
    env.add_function("tojson", tojson_fn());
    env.add_function("fromyaml", fromyaml_fn());
    env.add_function("toyaml", toyaml_fn());
    env.add_function("set", set_fn());
    env.add_function("render", render_fn());
    env.add_function("set_strict", set_strict_fn());
    env.add_function("zip", zip_fn());
    env.add_function("zip_strict", zip_strict_fn());
    // TODO: log
    // env.add_global("invocation_id", panic!("TODO_INVOCATION_ID"));
    env.add_function("thread_id", thread_id_fn());
    // TODO: modules
    // TODO: flags
    env.add_function("print", print_fn());
    env.add_function("log", log_fn());
    env.add_function("diff_of_two_dicts", diff_of_two_dicts_fn());
    env.add_function("local_md5", local_md5_fn());
    env.add_function("env_var", env_var_fn());
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
        _state: &State<'_, '_>,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        let mut args = ArgParser::new(args, None);
        let arg1 = args.get::<String>("");
        let arg2 = args.get_optional::<String>("");

        let doc = match (&arg1, &arg2) {
            // Two arguments: explicit package and doc name
            (Ok(package_name), Some(doc_name)) => self.lookup_doc(package_name, doc_name),
            // One argument: search in current package first, then others
            (Ok(doc_name), None) => {
                self.lookup_doc(&self.package_name, doc_name).or_else(|| {
                    // If not found in current package, look in other packages
                    self.lookup_doc_in_packages(doc_name)
                })
            }

            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidOperation,
                    "Invalid arguments to doc macro",
                ))
            }
        };

        match doc {
            Some(content) => Ok(Value::from_serialize(content)),
            None => Err(Error::new(
                ErrorKind::InvalidOperation,
                match arg2 {
                    Some(_) => format!(
                        "doc: doc '{}' not found for package '{}'",
                        arg1.unwrap_or_default(),
                        arg2.unwrap_or_default()
                    ),
                    None => format!(
                        "doc: doc '{}' not found for package '{}'",
                        arg1.unwrap_or_default(),
                        self.package_name
                    ),
                },
            )),
        }
    }
}

// TODO: implement var class
// https://github.com/dbt-labs/dbt-core/blob/31881d2a3bea030e700e9df126a3445298385698/core/dbt/context/base.py#L139
// Vars have functions that are availble in the jinja context that need to be implemented
/// A function that returns a variable from a map of variables
pub fn var_fn(
    vars: BTreeMap<String, dbt_serde_yaml::Value>,
) -> impl Fn(String, Option<Value>) -> Result<Value, Error> {
    move |var_name: String, default_value: Option<Value>| -> Result<Value, Error> {
        let value = if let Some(value) = vars.get(&var_name) {
            Value::from_serialize(value.clone())
        } else if let Some(default) = default_value {
            default
        } else {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                format!("'var': variable '{}' not found", var_name),
            ));
        };
        Ok(value)
    }
}

/// A function that returns an environment variable from the environment
pub fn env_var_fn() -> impl Fn(&[Value], Kwargs) -> Result<Value, Error> {
    move |args: &[Value], kwargs: Kwargs| -> Result<Value, Error> {
        let mut env_vars_guard = ENV_VARS.lock().unwrap();

        let mut arg_parser = ArgParser::new(args, Some(kwargs));
        let var_name = arg_parser
            .get::<String>("value")
            .or_else(|_| arg_parser.get::<String>("var"))
            .map_err(|_| {
                Error::new(
                    ErrorKind::InvalidOperation,
                    "env_var requires a 'value' or 'var' argument",
                )
            })?;
        let default_value = arg_parser.get_optional::<Value>("default");

        if var_name.starts_with(SECRET_ENV_VAR_PREFIX) {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                format!(
                    "Secret environment variables (starting with {}) cannot be accessed here",
                    SECRET_ENV_VAR_PREFIX
                ),
            ));
        } else if var_name.starts_with(DBT_INTERNAL_ENV_VAR_PREFIX) {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                format!(
                    "Environment variables (starting with {}) cannot be accessed here",
                    DBT_INTERNAL_ENV_VAR_PREFIX
                ),
            ));
        }

        let return_value = match (std::env::var(&var_name), default_value) {
            (Ok(value), _) => Some(value),
            (_, Some(default)) => Some(default.to_string()),
            _ => None,
        };

        match return_value {
            Some(value) => {
                env_vars_guard.insert(
                    var_name.clone(),
                    if std::env::var(&var_name).is_ok() {
                        value.clone()
                    } else {
                        DEFAULT_ENV_PLACEHOLDER.to_string()
                    },
                );
                Ok(value.into())
            }
            None => Err(Error::new(
                ErrorKind::InvalidOperation,
                format!("'env_var': environment variable '{}' not found", var_name),
            )),
        }
    }
}

/// The return function can be used in macros to return data to the caller.
/// The type of the data (dict, list, int, etc) will be preserved through the return call.
///
/// Example:
/// ```jinja
/// {% macro my_macro() %}
///   {{ return([1,2,3]) }}
/// {% endmacro %}
/// ```
pub fn return_macro(arg: Value) -> Result<Value, Error> {
    Ok(arg)
}

/// Deserialize a JSON string into a Python object primitive (e.g., a dict or list).
///
/// Args:
///     string: A string containing JSON data
///     default: (optional) Value to return if the JSON is invalid
///
/// Example:
/// ```jinja
/// {% set my_json_str = '{"abc": 123}' %}
/// {% set my_dict = fromjson(my_json_str) %}
/// ```
pub fn fromjson_fn() -> impl Fn(&[Value], Kwargs) -> Result<Value, Error> {
    move |args: &[Value], kwargs: Kwargs| -> Result<Value, Error> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "fromjson takes 1 or 2 arguments: string and optional default",
            ));
        }

        let mut arg_parser = ArgParser::new(args, Some(kwargs));
        let string = arg_parser.get::<String>("value")?;
        let default = arg_parser.get_optional::<Value>("default");

        match serde_json::from_str::<serde_json::Value>(&string) {
            Ok(value) => Ok(Value::from_serialize(&value)),
            Err(err) => match default {
                Some(default_value) => Ok(default_value),
                None => Err(Error::new(
                    ErrorKind::InvalidOperation,
                    format!("Failed to parse JSON: {}", err),
                )),
            },
        }
    }
}

/// Serialize a Python object primitive (e.g., a dict or list) to a JSON string.
///
/// Args:
///     value: Object to serialize to JSON
///     default: (optional) Value to return if serialization fails.
///     sort_keys: (optional, default: false) Whether to sort dictionary keys.
///     (Not all dbt kwargs like separators/indent are fully implemented here.)
///
/// Example:
/// ```jinja
/// {% set my_dict = {"abc": 123} %}
/// {% set my_json_str = tojson(my_dict) %}
/// ```
pub fn tojson_fn() -> impl Fn(&[Value], Kwargs) -> Result<Value, Error> {
    move |args: &[Value], kwargs: Kwargs| -> Result<Value, Error> {
        if args.is_empty() || args.len() > 3 {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "tojson requires at least 1 argument (the value to serialize)",
            ));
        }

        let mut arg_parser = ArgParser::new(args, Some(kwargs));
        let value = arg_parser.get::<Value>("value")?;
        let default = arg_parser.get_optional::<Value>("default");
        let sort_keys = arg_parser
            .get_optional::<bool>("sort_keys")
            .unwrap_or(false);

        // Return default if value is undefined
        if value.is_undefined() {
            return match default {
                Some(default_value) => Ok(default_value),
                None => Ok(Value::from(())),
            };
        }

        match serde_json::to_string(&value) {
            Ok(mut json_str) => {
                if sort_keys {
                    // Parse the JSON string back to a Value to sort keys
                    if let Ok(mut json_value) = serde_json::from_str::<serde_json::Value>(&json_str)
                    {
                        if let Some(obj) = json_value.as_object_mut() {
                            let sorted: serde_json::Map<String, serde_json::Value> = obj
                                .iter()
                                .collect::<BTreeMap<_, _>>()
                                .into_iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect();
                            json_value = serde_json::Value::Object(sorted);
                            json_str = serde_json::to_string(&json_value)
                                .unwrap_or_else(|_| "{}".to_string());
                        }
                    }
                }
                Ok(Value::from(json_str))
            }
            Err(err) => match default {
                Some(default_value) => Ok(default_value),
                None => Err(Error::new(
                    ErrorKind::InvalidOperation,
                    format!("Failed to convert value to JSON: {}", err),
                )),
            },
        }
    }
}

/// Deserialize a YAML string into a Python object primitive.
///
/// Args:
///     string: A string containing YAML data
///     default: (optional) Value to return if the YAML is invalid
///
/// Example:
/// ```jinja
/// {% set my_yaml_str = 'abc: 123' %}
/// {% set my_dict = fromyaml(my_yaml_str) %}
/// ```
pub fn fromyaml_fn() -> impl Fn(&[Value], Kwargs) -> Result<Value, Error> {
    move |args: &[Value], kwargs: Kwargs| -> Result<Value, Error> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "fromyaml takes 1 or 2 arguments: string and optional default",
            ));
        }

        let mut arg_parser = ArgParser::new(args, Some(kwargs));
        let string = arg_parser.get::<String>("value")?;
        let default = arg_parser.get_optional::<Value>("default");

        match dbt_serde_yaml::from_str::<dbt_serde_yaml::Value>(&string) {
            Ok(value) => Ok(Value::from_serialize(&value)),
            Err(err) => match default {
                Some(default_value) => Ok(default_value),
                None => Err(Error::new(
                    ErrorKind::InvalidOperation,
                    format!("Failed to parse YAML: {}", err),
                )),
            },
        }
    }
}

/// Serialize a Python object primitive to a YAML string.
///
/// Args:
///     value: Object to serialize to YAML
///     default: (optional) Value to return if serialization fails
///     sort_keys: (optional, default: false) Whether to sort dictionary keys
///
/// Example:
/// ```jinja
/// {% set my_dict = {"abc": 123} %}
/// {% set my_yaml_str = toyaml(my_dict) %}
/// ```
pub fn toyaml_fn() -> impl Fn(&[Value], Kwargs) -> Result<Value, Error> {
    move |args: &[Value], kwargs: Kwargs| -> Result<Value, Error> {
        if args.is_empty() || args.len() > 3 {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "toyaml requires at least 1 argument (the value to serialize)",
            ));
        }

        let mut arg_parser = ArgParser::new(args, Some(kwargs));
        let value = arg_parser.get::<Value>("value")?;
        let default = arg_parser.get_optional::<Value>("default");
        let sort_keys = arg_parser
            .get_optional::<bool>("sort_keys")
            .unwrap_or(false);

        // If the value is undefined or none and there's a default, return it
        if value.is_undefined() || value.is_none() {
            return match default {
                Some(def) => Ok(def),
                // Return none for undefined/none values when no default is provided
                None => Ok(Value::from(())),
            };
        }

        // Convert the Minijinja Value to a serde_json::Value
        // Should this say YAML or JSON cause this is toyaml function, not sure
        let mut json_value = match serde_json::to_value(&value) {
            Ok(val) => val,
            Err(err) => {
                return Err(Error::new(
                    ErrorKind::InvalidOperation,
                    format!("Failed to convert value to YAML: {}", err),
                ));
            }
        };

        // Sort keys if requested
        if sort_keys {
            if let Some(obj) = json_value.as_object_mut() {
                let sorted_map: BTreeMap<_, _> =
                    obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                *obj = sorted_map.into_iter().collect();
            }
        }

        match dbt_serde_yaml::to_string(&json_value) {
            Ok(yaml_str) => Ok(Value::from(yaml_str)),
            Err(err) => Err(Error::new(
                ErrorKind::InvalidOperation,
                format!("Failed to convert value to YAML: {}", err),
            )),
        }
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
                ))
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
                ))
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

/// Convert any iterable to a sequence of unique elements.
///
/// Args:
///     value: An iterable value to convert to a set
///     default: (optional) Value to return if conversion fails (can be passed
///              as second positional argument or kwarg: default="...")
///
/// Example:
/// ```jinja
/// {% set my_list = [1, 2, 2, 3] %}
/// {% set unique_values = set(my_list) %}
/// -- Returns [1, 2, 3]
/// ```
pub fn set_fn() -> impl Fn(&[Value], Kwargs) -> Result<Value, Error> {
    move |args: &[Value], kwargs: Kwargs| -> Result<Value, Error> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::new(
                ErrorKind::InvalidOperation,
                "set requires at least 1 argument",
            ));
        }

        let mut arg_parser = ArgParser::new(args, Some(kwargs));
        let value = arg_parser.get::<Value>("value")?;
        let default = arg_parser.get_optional::<Value>("default");

        match value.try_iter() {
            Ok(iter) => {
                let set: HashSet<_> = iter.map(|v| v.to_string()).collect();
                Ok(Value::from_iter(set))
            }
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
        let sql = args[0]
            .as_str()
            .ok_or_else(|| Error::new(ErrorKind::InvalidOperation, "Argument must be a string"))?;

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
                let set: HashSet<_> = iter.map(|v| v.to_string()).collect();
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
pub fn try_or_compiler_error_fn(
) -> impl Fn(&State<'_, '_>, &[Value], Kwargs) -> Result<Value, Error> {
    move |state: &State<'_, '_>, args: &[Value], kwargs: Kwargs| -> Result<Value, Error> {
        let mut args = ArgParser::new(args, Some(kwargs));
        let message_if_exception = args.get::<String>("message_if_exception")?;
        let func = args.get::<Value>("func")?;
        // Get remaining args and kwargs
        let mut remaining_args = args.get_args_as_vec_of_values();
        let drained_kwargs = args.drain_kwargs();
        let remaining_kwargs = Kwargs::from_iter(drained_kwargs);
        remaining_args.push(remaining_kwargs.into());
        // Call the function
        match func.call(state, &remaining_args, &[]) {
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
                Err(_) => return Ok(default.unwrap_or(Value::from(()))),
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

/// Returns an identifier for the current Python thread.
/// Useful for debugging concurrent operations.
///
/// Example:
/// ```jinja
/// {% set tid = thread_id() %}
/// ```
pub fn thread_id_fn() -> impl Fn() -> Result<Value, Error> {
    move || -> Result<Value, Error> {
        let thread_id = std::thread::current().id();
        Ok(Value::from(format!("{:?}", thread_id)))
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
pub fn print_fn() -> impl Fn(&[Value], Kwargs) -> Result<Value, Error> {
    move |args: &[Value], _kwargs: Kwargs| -> Result<Value, Error> {
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

        // TODO: fusion print is different from dbt print due to this is printing Debug
        // for example print('string')
        // fusion: 'string' // things are always wrapped in single quotes
        // dbt: string
        // changed to log::info!("{}", args[0]); if we have to make them consistent
        log::info!("{:?}", args[0]);
        Ok(Value::from(""))
    }
}

/// Print a message to the log file and stdout.
///
/// Args:
///     msg: Message to print
///
/// Example:
/// ```jinja
/// {{ log("Hello world!") }}
/// ```
pub fn log_fn() -> impl Fn(&[Value], Kwargs) -> Result<Value, Error> {
    move |args: &[Value], kwargs: Kwargs| -> Result<Value, Error> {
        let mut args = ArgParser::new(args, Some(kwargs));
        let msg = args.get::<Value>("msg")?;
        let info = args.get::<Value>("info").ok();
        // todo: print should go to log, or not?
        if info.is_some() && info.unwrap().is_true() {
            log::info!("{}", msg);
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
pub struct Exceptions {}

impl Object for Exceptions {
    fn call_method(
        self: &Arc<Self>,
        state: &State<'_, '_>,
        method: &str,
        args: &[Value],
        _listeners: &[Rc<dyn RenderingEventListener>],
    ) -> Result<Value, Error> {
        // TODO: Implement below
        // reference: https://github.com/dbt-labs/dbt-core/blob/c28cb92af51d7f2cb27618aeb43705ba951aa3ef/core/dbt/context/exceptions_jinja.py#L130
        // so far, stubs are only provided for methods seen used from dbt_macro_assets
        match method {
            "warn" => Ok(Value::UNDEFINED),
            "raise_compiler_error" => {
                let mut args = ArgParser::new(args, None);
                let message = args.get::<String>("message")?;
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
                    // TODO: error on None?
                    Err(Error::new(
                        ErrorKind::InvalidOperation,
                        format!("Compilation Error: {}", message),
                    ))
                }
            }
            "raise_not_implemented" => Ok(Value::UNDEFINED),
            "relation_wrong_type" => Ok(Value::UNDEFINED),
            "raise_contract_error" => Ok(Value::UNDEFINED),
            "column_type_missing" => Ok(Value::UNDEFINED),
            "raise_fail_fast_error" => Ok(Value::UNDEFINED),
            "warn_snapshot_timestamp_data_types" => Ok(Value::UNDEFINED),
            _ => Err(Error::new(
                ErrorKind::UnknownMethod("Exceptions".to_string(), method.to_string()),
                format!("Unknown method on Exceptions: {}", method),
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
            (
                unique_id.clone(),
                Value::from_serialize(DbtNode::Model((**model).clone())),
            )
        })
        .chain(nodes.snapshots.iter().map(|(unique_id, snapshot)| {
            (
                unique_id.clone(),
                Value::from_serialize(DbtNode::Snapshot((**snapshot).clone())),
            )
        }))
        .chain(nodes.tests.iter().map(|(unique_id, test)| {
            (
                unique_id.clone(),
                Value::from_serialize(DbtNode::Test((**test).clone())),
            )
        }))
        .chain(nodes.seeds.iter().map(|(unique_id, seed)| {
            (
                unique_id.clone(),
                Value::from_serialize(DbtNode::Seed((**seed).clone())),
            )
        }))
        .collect();
    graph.insert(Value::from("nodes"), Value::from_serialize(nodes_insert));

    let sources_insert: BTreeMap<String, Value> = nodes
        .sources
        .iter()
        .map(|(unique_id, source)| {
            (
                unique_id.clone(),
                Value::from_serialize((Arc::as_ref(source) as &dyn InternalDbtNode).serialize()),
            )
        })
        .collect();

    graph.insert(
        Value::from("sources"),
        Value::from_serialize(sources_insert),
    );
    graph.insert(
        Value::from("exposures"),
        Value::from_serialize(BTreeMap::<String, Value>::new()),
    );
    graph.insert(
        Value::from("groups"),
        Value::from_serialize(BTreeMap::<String, Value>::new()),
    );
    graph.insert(
        Value::from("metrics"),
        Value::from_serialize(BTreeMap::<String, Value>::new()),
    );
    graph.insert(
        Value::from("semantic_models"),
        Value::from_serialize(BTreeMap::<String, Value>::new()),
    );
    graph.insert(
        Value::from("saved_queries"),
        Value::from_serialize(BTreeMap::<String, Value>::new()),
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
}
