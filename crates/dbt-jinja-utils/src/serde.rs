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
//!  serialize correctly with default serde serializers -- if you ever need to
//!  (re)serialize structs containing such fields, say into a
//!  `minijinja::Value`, serialize them to a `yaml::Value` *first*, then
//!  serialize the `yaml::Value` to the target format.

use std::{
    path::{Path, PathBuf},
    rc::Rc,
    sync::LazyLock,
};

use dbt_common::{
    fs_err, io_args::IoArgs, io_utils::try_read_yml_to_str, show_warning,
    show_warning_soon_to_be_error, CodeLocation, ErrorCode, FsError, FsResult,
};
use dbt_serde_yaml::Value;
use minijinja::listener::RenderingEventListener;
use regex::Regex;
use serde::{de::DeserializeOwned, Serialize};

use crate::{jinja_environment::JinjaEnvironment, phases::load::secret_renderer::render_secrets};

pub use dbt_common::serde_utils::Omissible;

fn detect_yaml_indentation(input: &str) -> Option<usize> {
    for line in input.lines() {
        if let Some((indentation, _)) = line.char_indices().find(|&(_, c)| !c.is_whitespace()) {
            if indentation == 2 || indentation == 4 {
                return Some(indentation);
            }
        }
    }

    None
}
fn replace_tabs_with_spaces(input: &str) -> String {
    // check if we have "\t"
    if input.contains("\t") {
        // detect the indentation spaces
        let indentation = detect_yaml_indentation(input).unwrap_or(2);
        input.replace("\t", &" ".repeat(indentation))
    } else {
        input.to_string()
    }
}

fn trim_beginning_whitespace_for_first_line_with_content(input: &str) -> String {
    let mut lines = input.lines();

    // Find the first line with content
    while let Some(line) = lines.next() {
        if line.trim().is_empty() {
            continue;
        }

        // Found a line with content, trim its beginning whitespace
        if let Some((whitespace_len, _)) = line.char_indices().find(|&(_, c)| !c.is_whitespace()) {
            // Return the first line with leading whitespace removed, followed by the rest of the input
            let rest_of_input = lines.collect::<Vec<&str>>().join("\n");
            if rest_of_input.is_empty() {
                return line[whitespace_len..].to_string();
            } else {
                return format!("{}\n{}", &line[whitespace_len..], rest_of_input);
            }
        }

        // If we get here, the line has content but no leading whitespace
        return input.to_string();
    }

    // If we get here, the input is empty or only contains empty lines
    input.to_string()
}

/// Deserializes a YAML file into a `Value`, using the file's absolute path for error reporting.
pub fn value_from_file(io_args: Option<&IoArgs>, path: &Path) -> FsResult<Value> {
    let input = try_read_yml_to_str(path)?;
    value_from_str(io_args, &input, Some(path))
}

/// Internal function that deserializes a YAML string into a `Value`.
/// The error_display_path should be an absolute, canonicalized path.
fn value_from_str(
    io_args: Option<&IoArgs>,
    input: &str,
    error_display_path: Option<&Path>,
) -> FsResult<Value> {
    let _f = dbt_serde_yaml::with_filename(error_display_path.map(PathBuf::from));

    // replace tabs with spaces
    // trim beginning whitespace for the first line with content
    let input = replace_tabs_with_spaces(input);
    let input = trim_beginning_whitespace_for_first_line_with_content(&input);
    let mut value = Value::from_str(&input, |path, key, existing_key| {
        let key_repr = dbt_serde_yaml::to_string(&key).unwrap_or_else(|_| "<opaque>".to_string());
        if let Some(io_args) = io_args {
            show_warning!(
                io_args,
                fs_err!(
                    code => ErrorCode::DuplicateConfigKey,
                    loc => key.span(),
                    "Duplicate key `{}`. This key overwrites a previous definition of the same key \
                     at line {} column {}. YAML path: `{}`.",
                    key_repr.trim(),
                    existing_key.span().start.line,
                    existing_key.span().start.column,
                    path
                )
            );
        }
        // last key wins:
        dbt_serde_yaml::mapping::DuplicateKey::Overwrite
    })
    .map_err(|e| from_yaml_error(e, error_display_path))?;
    value
        .apply_merge()
        .map_err(|e| from_yaml_error(e, error_display_path))?;

    Ok(value)
}

/// Renders a Yaml `Value` containing Jinja expressions into a target
/// `Deserialize` type T.
pub fn into_typed_with_jinja<T, S>(
    io_args: Option<&IoArgs>,
    value: Value,
    should_render_secrets: bool,
    env: &JinjaEnvironment<'static>,
    ctx: &S,
    listeners: &[Rc<dyn RenderingEventListener>],
) -> FsResult<T>
where
    T: DeserializeOwned,
    S: Serialize,
{
    let (res, errors) =
        into_typed_with_jinja_error(value, should_render_secrets, env, ctx, listeners)?;

    if let Some(io_args) = io_args {
        for error in errors {
            show_warning_soon_to_be_error!(io_args, error);
        }
    }

    Ok(res)
}

/// Variant of into_typed_with_jinja which returns a Vec of warnings rather
/// than firing them.
pub fn into_typed_with_jinja_error<T, S>(
    value: Value,
    should_render_secrets: bool,
    env: &JinjaEnvironment<'static>,
    ctx: &S,
    listeners: &[Rc<dyn RenderingEventListener>],
) -> FsResult<(T, Vec<FsError>)>
where
    T: DeserializeOwned,
    S: Serialize,
{
    let jinja_renderer = |value: Value| match value {
        Value::String(s, span) => {
            let expanded = render_jinja_str(&s, should_render_secrets, env, ctx, listeners)
                .map_err(|e| e.with_location(span.clone()))?;
            Ok(expanded.with_span(span))
        }
        _ => Ok(value),
    };

    into_typed_internal(value, jinja_renderer)
}

/// Deserializes a Yaml `Value` into a target `Deserialize` type T.
pub fn into_typed_raw<T>(io_args: Option<&IoArgs>, value: Value) -> FsResult<T>
where
    T: DeserializeOwned,
{
    // Use the identity transform for the 'raw' version of this function.
    let expand_jinja = |value: Value| Ok(value);

    let (res, errors) = into_typed_internal(value, expand_jinja)?;

    if let Some(io_args) = io_args {
        for error in errors {
            show_warning_soon_to_be_error!(io_args, error);
        }
    }

    Ok(res)
}

fn into_typed_internal<T, F>(value: Value, transform: F) -> FsResult<(T, Vec<FsError>)>
where
    T: DeserializeOwned,
    F: FnMut(Value) -> Result<Value, Box<dyn std::error::Error + 'static + Send + Sync>>,
{
    let mut warnings: Vec<FsError> = Vec::new();
    let warn_unused_keys = |path: dbt_serde_yaml::path::Path, key: Value, _| {
        let key_repr = dbt_serde_yaml::to_string(&key).unwrap_or_else(|_| "<opaque>".to_string());
        warnings.push(*fs_err!(
            code => ErrorCode::UnusedConfigKey,
            loc => key.span(),
            "Ignored unexpected key `{:?}`. YAML path: `{}`.", key_repr.trim(), path
        ))
    };

    let res = value
        .into_typed(warn_unused_keys, transform)
        .map_err(|e| from_yaml_error(e, None))?;
    Ok((res, warnings))
}

/// Render a Jinja expression to a Value
fn render_jinja_str<S: Serialize>(
    s: &str,
    should_render_secrets: bool,
    env: &JinjaEnvironment,
    ctx: &S,
    listeners: &[Rc<dyn RenderingEventListener>],
) -> FsResult<Value> {
    if check_single_expression_without_whitepsace_control(s) {
        let compiled = env.compile_expression(&s[2..s.len() - 2])?;
        let eval = compiled.eval(ctx, listeners)?;
        let val = dbt_serde_yaml::to_value(eval).map_err(|e| {
            from_yaml_error(
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
        let compiled = env.render_str(s, ctx, listeners)?;
        let compiled = if should_render_secrets {
            render_secrets(compiled)?
        } else {
            compiled
        };
        Ok(Value::string(compiled))
    }
}

/// Deserializes a Yaml string containing Jinja expressions into a Rust type T.
#[allow(clippy::too_many_arguments)]
pub fn from_yaml_jinja<T, S: Serialize>(
    io_args: Option<&IoArgs>,
    input: &str,
    should_render_secrets: bool,
    env: &JinjaEnvironment<'static>,
    ctx: &S,
    listeners: &[Rc<dyn RenderingEventListener>],
    error_display_path: Option<&Path>,
) -> FsResult<T>
where
    T: DeserializeOwned,
{
    into_typed_with_jinja(
        io_args,
        value_from_str(io_args, input, error_display_path)?,
        should_render_secrets,
        env,
        ctx,
        listeners,
    )
}

/// Deserializes a Yaml string into a Rust type T.
pub fn from_yaml_raw<T>(
    io_args: Option<&IoArgs>,
    input: &str,
    error_display_path: Option<&Path>,
) -> FsResult<T>
where
    T: DeserializeOwned,
{
    into_typed_raw(io_args, value_from_str(io_args, input, error_display_path)?)
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

/// Converts a `dbt_serde_yaml::Error` into a `FsError`, attaching the error location
pub fn from_yaml_error(err: dbt_serde_yaml::Error, filename: Option<&Path>) -> Box<FsError> {
    let msg = err.display_no_mark().to_string();
    let location = err
        .span()
        .map_or_else(CodeLocation::default, CodeLocation::from);
    let location = if let Some(filename) = filename {
        location.with_file(filename)
    } else {
        location
    };

    if let Some(err) = err.into_external() {
        if let Ok(err) = err.downcast::<FsError>() {
            // These are errors raised from our own callbacks:
            return err;
        }
    }
    FsError::new(ErrorCode::SerializationError, format!("YAML error: {msg}"))
        .with_location(location)
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_single_expression_without_whitepsace_control() {
        assert!(check_single_expression_without_whitepsace_control(
            "{{ config(enabled=true) }}"
        ));
        assert!(!check_single_expression_without_whitepsace_control(
            "{{- config(enabled=true) -}}"
        ));
    }
}
