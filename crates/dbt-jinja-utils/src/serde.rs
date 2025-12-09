//! Provides the machineries to deserialize and process dbt YAML files.
//!
//! # Basic primitives
//!
//! * [`value_from_str()`]: this function creates a `yaml::Value` from a Yaml
//!   string, with proper warnings for duplicate keys
//! * [`into_typed_with_jinja<T>`]: this function consumes a `yaml::Value` to
//!   construct a `Deserialize` type `T`, while applying Jinja according to the
//!   rules encoded in `T`.
//!
//! There's also a shorthand, [`into_typed_raw<T>`], which is basically syntactic
//! sugar for `into_typed_with_jinja<Verbatim<T>>`.
//!
//! # Types
//!
//! * [`Omissible<T>`]: this is a wrapper type for use in
//!   `#[derive(Deserialize)]` structs, which allows you to distinguish between
//!   "omitted" and "explicit null" values.
//!
//! # General usage guidelines
//!
//! * `yaml::Value` objects (and recursively all child `Value` objects)
//!   constructed by `value_from_str` are *fully self-contained with regards to
//!   source location*. This means that you can take a `Value`, pass it around,
//!   mix them up, then `into_typed` whenever you have the right Jinja context,
//!   and it's guaranteed to always raise errors with the correct location info.
//!   (i.e. you can use `yaml::Value` as ASTs for Yaml sources)
//!
//! * In `#[derive(Deserialize)]` schemas, use `Verbatim<Value>` for fields that
//!   require "deferred Jinja" processing; on the other hand, if the field
//!   should never be Jinja'd, you can directly `Verbatim` into the primitive
//!   type, e.g. `pub git: Verbatim<String>`
//!
//! * Avoid re-reading yaml files from disk for deferred-jinja -- you can now
//!   easily read to a `yaml::Value` for "raw" processing, and only apply Jinja
//!   when you have the full Jinja context
//!
//! * Avoid `json::Value` in Yaml structs -- we now have proper support for
//!   duplicate fields so there's no need to resort to `json::Value` to silently
//!   eat up duplicate fields.
//!
//! * Use the `dbt_serde_yaml::Spanned` wrapper type to capture the source
//!   location of any Yaml field.
//!
//! * `Option<Verbatim<T>>` does not work as expected due to implementation
//!   limitation -- always use `Verbatim<Option<T>>` instead.
//!
//! * Avoid using `#[serde(flatten)]` -- `Verbatim<T>` does not work with
//!   `#[serde(flatten)]`. Instead, use field names that starts and ends with
//!   `__` (e.g. `__additional_properties__`) -- all such named fields are
//!   flattened by `dbt_serde_yaml`, just as if they were annotated with
//!   `#[serde(flatten)]`. **NOTE** structs containing such fields will not
//!   serialize correctly with default serde serializers -- if you ever need to
//!   (re)serialize structs containing such fields, say into a
//!   `minijinja::Value`, serialize them to a `yaml::Value` *first*, then
//!   serialize the `yaml::Value` to the target format.
//!
//! * Untagged enums (`#[serde(untagged)]`) containing "magic" dbt-serde_yaml
//!   facilities, such as `Verbatim<T>` or `flatten_dunder` fields, does
//!   *not* work with the default `#[derive(Deserialize)]` decorator -- use
//!   `#[derive(UntaggedEnumDeserialize)]` instead (Note:
//!   `UntaggedEnumDeserialize` works on untagged enums *only* -- for all other
//!   types, use the default `#[derive(Deserialize)]` decorator).
//!
//! * For the specific use case of error recovery during deserialization, the
//!   `dbt_serde_yaml::ShouldBe<T>` wrapper type should be preferred -- unlike
//!   general `#[serde(untagged)]` enums which requires backtracking during
//!   deserialization, `ShouldBe<T>` does not backtrack and is zero overhead on
//!   the happy path (see type documentation for more details).

use std::{
    path::{Path, PathBuf},
    rc::Rc,
    sync::LazyLock,
};

use dbt_common::{
    CodeLocation, ErrorCode, FsError, FsResult, fs_err, io_args::IoArgs,
    io_utils::try_read_yml_to_str, tracing::emit::emit_strict_parse_error,
};
use dbt_schemas::schemas::serde::yaml_to_fs_error;
use dbt_serde_yaml::Value;
use minijinja::listener::RenderingEventListener;
use regex::Regex;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    jinja_environment::JinjaEnv, phases::load::secret_renderer::render_secrets,
    typecheck_listener::YamlTypecheckingEventListener,
};

pub use dbt_common::serde_utils::Omissible;

/// Deserializes a YAML file into a `Value`, using the file's absolute path for error reporting.
///
/// `dependency_package_name` is used to determine if the file is part of a dependency package,
/// which affects how errors are reported.
pub fn value_from_file(
    io_args: &IoArgs,
    path: &Path,
    show_errors_or_warnings: bool,
    dependency_package_name: Option<&str>,
) -> FsResult<Value> {
    let input = try_read_yml_to_str(path)?;
    value_from_str(
        io_args,
        dbt_sanitize_yml(&input),
        Some(path),
        show_errors_or_warnings,
        dependency_package_name,
    )
}

/// Renders a Yaml `Value` containing Jinja expressions into a target
/// `Deserialize` type T.
///
/// `dependency_package_name` is used to determine if the file is part of a dependency package,
/// which affects how errors are reported.
#[allow(clippy::too_many_arguments)]
pub fn into_typed_with_jinja<T, S>(
    io_args: &IoArgs,
    value: Value,
    should_render_secrets: bool,
    env: &JinjaEnv,
    ctx: &S,
    listeners: &[Rc<dyn RenderingEventListener>],
    dependency_package_name: Option<&str>,
    show_errors_or_warnings: bool,
) -> FsResult<T>
where
    T: DeserializeOwned,
    S: Serialize,
{
    let (res, errors) = into_typed_with_jinja_error(
        Some(io_args),
        value,
        should_render_secrets,
        env,
        ctx,
        listeners,
    )?;

    if show_errors_or_warnings {
        for error in errors {
            emit_strict_parse_error(&error, dependency_package_name, io_args);
        }
    }

    Ok(res)
}

/// Renders a Yaml `Value` containing Jinja expressions into a target
/// `Deserialize` type T.
///
/// `dependency_package_name` is used to determine if the file is part of a dependency package,
/// which affects how errors are reported.
#[allow(clippy::too_many_arguments)]
pub fn into_typed_with_jinja_error_context<T, S>(
    io_args: Option<&IoArgs>,
    value: Value,
    should_render_secrets: bool,
    env: &JinjaEnv,
    ctx: &S,
    listeners: &[Rc<dyn RenderingEventListener>],
    // A function that takes FsError and returns a string to be used as the error context
    error_context: impl Fn(&FsError) -> String,
    dependency_package_name: Option<&str>,
) -> FsResult<T>
where
    T: DeserializeOwned,
    S: Serialize,
{
    let (res, errors) =
        into_typed_with_jinja_error(io_args, value, should_render_secrets, env, ctx, listeners)?;

    if let Some(io_args) = io_args {
        for error in errors {
            let context = error_context(&error);
            let error = error.with_context(context);
            emit_strict_parse_error(&error, dependency_package_name, io_args);
        }
    }

    Ok(res)
}

/// Deserializes a Yaml `Value` into a target `Deserialize` type T.
pub fn into_typed_with_error<T>(
    io_args: &IoArgs,
    value: Value,
    show_errors_or_warnings: bool,
    dependency_package_name: Option<&str>,
    error_path: Option<PathBuf>,
) -> FsResult<T>
where
    T: DeserializeOwned,
{
    let (res, errors) = into_typed_internal(value, |_value| Ok(None))?;

    if show_errors_or_warnings {
        for error in errors {
            let error =
                error.with_location(CodeLocation::from(error_path.clone().unwrap_or_default()));
            emit_strict_parse_error(&error, dependency_package_name, io_args);
        }
    }

    Ok(res)
}

/// Deserializes a Yaml string into a Rust type T.
///
/// `dependency_package_name` is used to determine if the file is part of a dependency package,
/// which affects how errors are reported.
pub fn from_yaml_raw<T>(
    io_args: &IoArgs,
    input: &str,
    error_display_path: Option<&Path>,
    show_errors_or_warnings: bool,
    dependency_package_name: Option<&str>,
) -> FsResult<T>
where
    T: DeserializeOwned,
{
    let value = value_from_str(
        io_args,
        dbt_sanitize_yml(input),
        error_display_path,
        show_errors_or_warnings,
        dependency_package_name,
    )?;
    // Use the identity transform for the 'raw' version of this function.
    let expand_jinja = |_: &Value| Ok(None);

    let (res, errors) = into_typed_internal(value, expand_jinja)?;

    if show_errors_or_warnings {
        for error in errors {
            emit_strict_parse_error(&error, dependency_package_name, io_args);
        }
    }

    Ok(res)
}

/// Apply pre-processing to the Yaml input string to compat with dbt's YAML
/// behavior.
fn dbt_sanitize_yml(input: &str) -> &str {
    // Remove any UTF-8 BOM from the beginning of the input string -- the YAML
    // parser confuses it with a document separator:
    let input = input.strip_prefix('\u{feff}').unwrap_or(input);

    // Trim trailing whitespace
    let input = input.trim_end();

    // If the fist non-empty line has leading whitespaces, then trim leading
    // whitespaces as well
    if let Some(first_non_empty_line) = input.lines().find(|line| !line.trim().is_empty())
        && first_non_empty_line
            .chars()
            .next()
            .map(|c| c.is_whitespace())
            .unwrap_or(false)
    {
        // Note: if the input contains leading blank lines, this would cause all
        // line numbers to be off by the number of leading blank lines. The
        // better way is to retain the leading blank lines while trimming only
        // the first line. However since these inputs are invalid YAML anyway,
        // we won't bother for now.
        input.trim_start()
    } else {
        input
    }
}

/// Internal function that deserializes a YAML string into a `Value`.
/// The error_display_path should be an absolute, canonicalized path.
///
/// `dependency_package_name` is used to determine if the file is part of a dependency package,
/// which affects how errors are reported.
fn value_from_str(
    io_args: &IoArgs,
    input: &str,
    error_display_path: Option<&Path>,
    show_errors_or_warnings: bool,
    dependency_package_name: Option<&str>,
) -> FsResult<Value> {
    let _f = dbt_serde_yaml::with_filename(error_display_path.map(PathBuf::from));

    let mut value = Value::from_str(input, |path, key, existing_key| {
        let key_repr = dbt_serde_yaml::to_string(&key).unwrap_or_else(|_| "<opaque>".to_string());
        let path = strip_dunder_fields_from_path(&path.to_string());
        let duplicate_key_error = fs_err!(
            code => ErrorCode::DuplicateConfigKey,
            loc => key.span().clone(),
            "Duplicate key `{}`. This key overwrites a previous definition of the same key \
                at line {} column {}. YAML path: `{}`.",
            key_repr.trim(),
            existing_key.span().start.line,
            existing_key.span().start.column,
            path
        );

        if show_errors_or_warnings {
            emit_strict_parse_error(&duplicate_key_error, dependency_package_name, io_args);
        }
        // last key wins:
        dbt_serde_yaml::mapping::DuplicateKey::Overwrite
    })
    .map_err(|e| yaml_to_fs_error(e, error_display_path))?;
    value
        .apply_merge()
        .map_err(|e| yaml_to_fs_error(e, error_display_path))?;

    Ok(value)
}

/// Variant of into_typed_with_jinja which returns a Vec of warnings rather
/// than firing them.
fn into_typed_with_jinja_error<T, S>(
    io_args: Option<&IoArgs>,
    value: Value,
    should_render_secrets: bool,
    env: &JinjaEnv,
    ctx: &S,
    listeners: &[Rc<dyn RenderingEventListener>],
) -> FsResult<(T, Vec<FsError>)>
where
    T: DeserializeOwned,
    S: Serialize,
{
    let jinja_renderer = |value: &Value| match value {
        Value::String(s, yaml_span) => {
            let expanded = render_jinja_str(
                io_args,
                s,
                should_render_secrets,
                env,
                ctx,
                listeners,
                yaml_span,
            )
            .map_err(|e| e.with_location(yaml_span.clone()))?;
            Ok(Some(expanded.with_span(yaml_span.clone())))
        }
        _ => Ok(None),
    };

    into_typed_internal(value, jinja_renderer)
}

fn into_typed_internal<T, F>(value: Value, transform: F) -> FsResult<(T, Vec<FsError>)>
where
    T: DeserializeOwned,
    F: FnMut(&Value) -> Result<Option<Value>, Box<dyn std::error::Error + 'static + Send + Sync>>,
{
    let mut warnings: Vec<FsError> = Vec::new();
    let warn_unused_keys = |path: dbt_serde_yaml::path::Path, key: &Value, _: &Value| {
        let key_repr = dbt_serde_yaml::to_string(key).unwrap_or_else(|_| "<opaque>".to_string());
        let path = strip_dunder_fields_from_path(&path.to_string());
        warnings.push(*fs_err!(
            code => ErrorCode::UnusedConfigKey,
            loc => key.span().clone(),
            "Ignored unexpected key `{:?}`. YAML path: `{}`.", key_repr.trim(), path
        ))
    };

    let res = value
        .into_typed(warn_unused_keys, transform)
        .map_err(|e| yaml_to_fs_error(e, None))?;
    Ok((res, warnings))
}

/// Strips any dunder fields (fields of the form `__<something>__`) from a dot-separated path string.
/// For example, "foo.__bar__.baz" becomes "foo.baz".
pub fn strip_dunder_fields_from_path(path: &str) -> String {
    path.split('.')
        .filter(|segment| {
            // Check if the segment is a dunder field: starts and ends with double underscores
            !(segment.starts_with("__") && segment.ends_with("__") && segment.len() > 4)
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Render a Jinja expression to a Value
fn render_jinja_str<S: Serialize>(
    io_args: Option<&IoArgs>,
    s: &str,
    should_render_secrets: bool,
    env: &JinjaEnv,
    ctx: &S,
    listeners: &[Rc<dyn RenderingEventListener>],
    yaml_span: &dbt_serde_yaml::Span,
) -> FsResult<Value> {
    if check_single_expression_without_whitepsace_control(s) {
        let compiled = env.compile_expression(&s[2..s.len() - 2])?;

        // Perform static type checking if we have io_args and span information
        perform_typecheck(
            io_args,
            env,
            yaml_span,
            |funcsigns, builtins, listener, ctx| {
                compiled.typecheck(funcsigns, builtins, listener, ctx)
            },
        );

        let eval = compiled.eval(ctx, listeners)?;
        let val = dbt_serde_yaml::to_value(eval).map_err(|e| {
            yaml_to_fs_error(
                e,
                // The caller will attach the error location using the span in the
                // `Value` object, if available:
                None,
            )
        })?;
        let val = match val {
            Value::String(s, span) if should_render_secrets => {
                Value::string(render_secrets(s)?).with_span(span)
            }
            _ => val,
        };
        Ok(val)
    // Otherwise, process the entire string through Jinja
    } else {
        // Compile template and perform typechecking
        let template = env.template_from_str(s)?;

        // Perform static type checking if we have io_args and span information
        perform_typecheck(
            io_args,
            env,
            yaml_span,
            |funcsigns, builtins, listener, ctx| {
                template.typecheck(funcsigns, builtins, listener, ctx)
            },
        );

        let compiled = env.render_str(s, ctx, listeners)?;
        let compiled = if should_render_secrets {
            render_secrets(compiled)?
        } else {
            compiled
        };
        Ok(Value::string(compiled))
    }
}

/// Helper function to perform typechecking on Jinja expressions/templates
fn perform_typecheck<F>(
    io_args: Option<&IoArgs>,
    env: &JinjaEnv,
    yaml_span: &dbt_serde_yaml::Span,
    typecheck_fn: F,
) where
    F: FnOnce(
        std::sync::Arc<minijinja::compiler::typecheck::FunctionRegistry>,
        std::sync::Arc<dashmap::DashMap<String, minijinja::Type>>,
        Rc<dyn minijinja::TypecheckingEventListener>,
        std::collections::BTreeMap<String, minijinja::Value>,
    ) -> FsResult<()>,
{
    if let Some(io_args) = io_args {
        // Get status reporter from io_args
        let status_reporter = io_args.status_reporter.clone();

        // Get file path from yaml_span
        let path = yaml_span
            .filename
            .as_ref()
            .map(|p| p.to_path_buf())
            .unwrap_or_default();

        // Create minijinja span from yaml span
        let minijinja_span = minijinja::machinery::Span {
            start_line: yaml_span.start.line as u32,
            start_col: yaml_span.start.column as u32,
            start_offset: yaml_span.start.index as u32,
            end_line: yaml_span.end.line as u32,
            end_col: yaml_span.end.column as u32,
            end_offset: yaml_span.end.index as u32,
        };

        let typecheck_listener = Rc::new(YamlTypecheckingEventListener::new(
            status_reporter,
            path,
            minijinja_span,
        ));

        // Load builtins from the macro namespace registry
        let macro_namespace_registry = env.env.get_macro_namespace_registry();
        if let Ok(builtins) = minijinja::load_builtins_with_namespace(macro_namespace_registry) {
            // Build typecheck context with required values
            let mut typecheck_resolved_context = std::collections::BTreeMap::new();
            typecheck_resolved_context.insert(
                "ROOT_PACKAGE_NAME".to_string(),
                minijinja::Value::from("dbt"),
            );

            // Get DBT_AND_ADAPTERS_NAMESPACE directly from globals as a Value
            let dbt_and_adapters = env
                .env
                .get_global("DBT_AND_ADAPTERS_NAMESPACE")
                .unwrap_or_else(|| {
                    minijinja::Value::from_object(indexmap::IndexMap::<
                        minijinja::Value,
                        minijinja::Value,
                    >::new())
                });
            typecheck_resolved_context
                .insert("DBT_AND_ADAPTERS_NAMESPACE".to_string(), dbt_and_adapters);

            // Perform typecheck (ignore errors as they're already emitted as warnings)
            let _ = typecheck_fn(
                env.jinja_function_registry.clone(),
                builtins,
                typecheck_listener,
                typecheck_resolved_context,
            );
        }
    }
}

static RE_SIMPLE_EXPR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*\{\{\s*[^{}]+\s*\}\}\s*$").expect("valid regex"));

/// Check if the input is a single Jinja expression without whitespace control
pub fn check_single_expression_without_whitepsace_control(input: &str) -> bool {
    // The regex matches:
    //   ^\s*      -> optional whitespace at the beginning
    //   \{\{      -> the literal '{{'
    //   \s*       -> optional whitespace
    //   [^{}]+   -> one or more characters that are not '{', '}', or '-'
    //   \s*       -> optional whitespace
    //   \}\}      -> the literal '}}'
    //   \s*$      -> optional whitespace at the end
    !input.starts_with("{{-")
        && !input.ends_with("-}}")
        && input.starts_with("{{")
        && input.ends_with("}}")
        && { RE_SIMPLE_EXPR.is_match(input) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_common::io_args::IoArgs;
    use dbt_serde_yaml::Value;

    #[test]
    fn test_check_single_expression_without_whitepsace_control() {
        assert!(check_single_expression_without_whitepsace_control(
            "{{ config(enabled=true) }}"
        ));
        assert!(!check_single_expression_without_whitepsace_control(
            "{{- config(enabled=true) -}}"
        ));
    }

    #[test]
    fn test_from_yaml_raw_strips_utf8_bom_and_parses_ok() {
        // \u{feff} is the UTF-8 BOM. BOM at start should be ignored and parsing should succeed.
        let io = IoArgs::default();
        let input = "\u{feff}version: 2\nmodels:\n  - name: dim_bom_test\n";
        let res = from_yaml_raw(&io, input, None, false, None);
        assert!(
            res.is_ok(),
            "Expected BOM-prefixed YAML to parse successfully, got: {:?}",
            res.err()
        );
        let val: Value = res.unwrap();
        match val {
            Value::Mapping(_, _) => {} // minimal structural check
            other => panic!("Expected top-level mapping, got: {:?}", other),
        }
    }
}
