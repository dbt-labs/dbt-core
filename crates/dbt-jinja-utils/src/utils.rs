use dbt_common::{constants::DBT_CTE_PREFIX, error::MacroSpan, tokiofs, FsResult};
use dbt_common::{fs_err, stdfs, ErrorCode, FsError};
use dbt_frontend_common::{error::CodeLocation, span::Span};
use dbt_fusion_adapter::relation_object::create_relation_internal;
use dbt_fusion_adapter::{AdapterTyping, ParseAdapter};
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::{DbtModel, DbtSeed, DbtSnapshot, DbtTest, DbtUnitTest, InternalDbtNode};
use minijinja::arg_utils::ArgParser;
use minijinja::{functions::debug, value::Rest, Error, ErrorKind, MacroSpans, State, Value};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Mutex,
};

use crate::{jinja_environment::JinjaEnvironment, listener::ListenerFactory};

/// The prefix for environment variables that contain secrets
pub const SECRET_ENV_VAR_PREFIX: &str = "DBT_ENV_SECRET";

/// The prefix for environment variables that are reserved for dbt
pub const DBT_INTERNAL_ENV_VAR_PREFIX: &str = "_DBT";

/// The version of dbt used in this crate
pub const DBT_VERSION: &str = "2.0.0"; // easter egg jokes for now

/// A lazy initialized mutex-protected hash map for storing environment variables
pub static ENV_VARS: LazyLock<Mutex<HashMap<String, String>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Cache for template lookups per (current_project, root_project, component)
static TEMPLATE_CACHE: LazyLock<Mutex<HashMap<(String, String), String>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Converts a value to a boolean
pub fn as_bool(args: Value) -> Result<Value, Error> {
    let input = args.to_string();
    match input.parse::<i64>() {
        Ok(int_value) => Ok(Value::from(int_value != 0)),
        Err(_) => match input.parse::<f64>() {
            Ok(float_value) => Ok(Value::from(!float_value.is_nan() && float_value != 0.0)),
            Err(_) => match input.to_ascii_lowercase().as_str() {
                "true" => Ok(Value::from(true)),
                "false" => Ok(Value::from(false)),
                _ => Ok(Value::from(!input.is_empty())),
            },
        },
    }
}

/// Asserts a condition using Jinja
pub fn assert_minijinja(_state: &State, args: Rest<Value>) -> Result<Value, Error> {
    if args.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidOperation,
            "Expected at least one argument",
        ));
    }
    let expr = args[0].clone();
    let message = args.get(1).map_or_else(String::new, |v| v.to_string());
    let condition = as_bool(expr)?;
    if condition == Value::from(false) {
        eprintln!("error: {} assertion failed", &message);
    }
    Ok(Value::from(""))
}

/// Logs a message using Jinja
pub fn log_minijinja(state: &State, args: Rest<Value>) -> String {
    let debug_str = debug(state, args);
    eprintln!("log: {}", &debug_str);
    "".to_owned()
}

/// escape a string with ascii <,>, &, ", ', /, ' with html substitutions
pub fn escape(s: &str) -> String {
    let mut output = String::with_capacity(s.len() * 2); // Reserve capacity for worst case

    for c in s.chars() {
        match c {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&apos;"),
            '\\' => output.push_str("&#92;"),
            _ => output.push(c),
        }
    }
    output
}

/// unescape a string with html substitutions for  <,>, &, ", ', /, with their ascii equivalents
pub fn unescape(html: &str) -> String {
    let mut output = Vec::with_capacity(html.len());
    let html = html.as_bytes();
    let mut i = 0;
    while i < html.len() {
        let c = html[i];
        if c == b'&' {
            if let Some(end) = html[i..].iter().position(|&x| x == b';') {
                let entity = &html[i..i + end + 1];
                match entity {
                    b"&amp;" => output.push(b'&'),
                    b"&lt;" => output.push(b'<'),
                    b"&gt;" => output.push(b'>'),
                    b"&quot;" => output.push(b'"'),
                    b"&apos;" => output.push(b'\''),
                    b"&#91;" => output.push(b'['),
                    b"&#92;" => output.push(b'\\'),
                    b"&#93;" => output.push(b']'),
                    _ => {
                        output.extend_from_slice(entity);
                    }
                }
                i += end + 1;
            } else {
                output.push(c);
                i += 1;
            }
        } else {
            output.push(c);
            i += 1;
        }
    }
    String::from_utf8_lossy(&output).into_owned()
}

/// Handles ephemeral model CTEs in SQL
///
/// This function processes SQL that contains DBT CTE prefixes, extracts model names,
/// reads the corresponding SQL files from the ephemeral directory, and incorporates them
/// as CTEs in the final SQL. It also adjusts macro spans to account for the added lines.
pub async fn inject_and_persist_ephemeral_models(
    sql: String,
    macro_spans: &mut MacroSpans,
    model_name: &str,
    is_current_model_ephemeral: bool,
    ephemeral_dir: &Path,
) -> FsResult<String> {
    if !sql.contains(DBT_CTE_PREFIX) {
        // Write the ephemeral model to the ephemeral directory
        if is_current_model_ephemeral {
            let ephemeral_path = ephemeral_dir.join(format!("{}.sql", model_name));
            tokiofs::create_dir_all(ephemeral_path.parent().unwrap()).await?;
            tokiofs::write(
                ephemeral_path,
                format!("{}{} as (\n{}\n)", DBT_CTE_PREFIX, model_name, sql),
            )
            .await?;
        }
        return Ok(sql);
    }
    // Extract all model names from __dbt__cte__ references
    let mut ephemeral_model_names = Vec::new();
    let mut pos = 0;
    let mut final_sql = sql;

    while let Some(start) = final_sql[pos..].find(DBT_CTE_PREFIX) {
        pos += start + DBT_CTE_PREFIX.len();
        let name_end = final_sql[pos..]
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(final_sql[pos..].len());
        let model_name = &final_sql[pos..pos + name_end];
        // Only add if not already seen
        if !ephemeral_model_names.contains(&model_name.to_string()) {
            ephemeral_model_names.push(model_name.to_string());
        }
        pos += name_end;
    }

    // Read ephemeral SQL from ephemeral dir and build cumulative CTEs
    let sep = "\x00";
    let mut seen_ctes = HashSet::new();
    let mut all_ctes = Vec::new();

    for model_name in ephemeral_model_names {
        let path = format!("{}/{}.sql", ephemeral_dir.to_str().unwrap(), model_name);
        let ephemeral_sql = stdfs::read_to_string(&path)?;

        // Split existing CTEs and add any new ones
        let existing_ctes: Vec<String> = ephemeral_sql.split(sep).map(|s| s.to_string()).collect();
        for cte in existing_ctes {
            if !seen_ctes.contains(&cte) {
                seen_ctes.insert(cte.clone());
                all_ctes.push(cte);
            }
        }
    }
    // Write all CTEs up to this point for this model for next use.
    // this avoid graph walk for ephemeral models
    if is_current_model_ephemeral {
        let ephemeral_path = ephemeral_dir.join(format!("{}.sql", model_name));
        tokiofs::create_dir_all(ephemeral_path.parent().unwrap()).await?;
        let cte_line = format!("{}{} as (\n{}\n)", DBT_CTE_PREFIX, model_name, final_sql);
        all_ctes.push(cte_line.clone());
        tokiofs::write(ephemeral_path, &all_ctes.join(sep)).await?;
        all_ctes.pop();
    }

    // Find first non-whitespace/comment content
    let sql_lower = final_sql.to_lowercase();
    let mut content_start = 0;
    let mut in_comment = false;
    let mut in_multiline = false;
    let mut chars = sql_lower.chars().enumerate().peekable();
    while let Some((i, c)) = chars.next() {
        if in_multiline {
            if c == '*' && chars.next_if(|(_, c)| *c == '/').is_some() {
                in_multiline = false;
            }
            continue;
        }
        if in_comment {
            if c == '\n' {
                in_comment = false;
            }
            continue;
        }
        if c == '/' {
            if let Some((_, _)) = chars.next_if(|(_, c)| *c == '/') {
                in_comment = true;
                continue;
            }
            if let Some((_, _)) = chars.next_if(|(_, c)| *c == '*') {
                in_multiline = true;
                continue;
            }
        }
        if !c.is_whitespace() {
            content_start = i;
            break;
        }
    }

    // Check if SQL already has a WITH statement
    let with_prefix = "with";
    let mut insert_pos = content_start;
    let has_with = sql_lower[content_start..].starts_with(with_prefix);

    // handle recursive CTEs
    if has_with {
        insert_pos += with_prefix.len();
        let after_with = &sql_lower[insert_pos..];
        let has_recursive = after_with.trim_start().starts_with("recursive");
        if has_recursive {
            insert_pos += "recursive".len();
        }
    }
    let ctes = all_ctes.join(", ");
    if has_with {
        // SQL already has WITH - insert CTEs after WITH keyword
        final_sql.insert_str(insert_pos, &format!(" {}, ", ctes));
    } else {
        // No WITH - add one at start with the CTEs
        final_sql.insert_str(0, &format!("with {} ", ctes));
    }
    // Shift expanded macro spans down by number of added lines
    let added_lines = ctes.lines().count();
    for span in macro_spans.items.iter_mut() {
        span.1.start_line += added_lines as u32;
        span.1.end_line += added_lines as u32;
    }
    Ok(final_sql)
}

/// Renders SQL with Jinja macros
#[allow(clippy::too_many_arguments)]
pub fn render_sql<'a, E>(
    sql: &str,
    env: E,
    ctx: &BTreeMap<String, Value>,
    listener_factory: &dyn ListenerFactory,
    filename: &Path,
) -> FsResult<String>
where
    E: AsRef<JinjaEnvironment<'a>>,
{
    let listeners = listener_factory.create_listeners(filename);
    let result = env
        .as_ref()
        .render_named_str(filename.to_str().unwrap(), sql, ctx, &listeners)
        .map_err(|e| FsError::from_jinja_err(e, "Failed to render SQL"))?;
    for listener in listeners {
        listener_factory.destroy_listener(filename, listener);
    }

    Ok(result)
}

/// Converts a MacroSpans object to a vector of MacroSpan objects
pub fn macro_spans_to_macro_span_vec(macro_spans: &MacroSpans) -> Vec<MacroSpan> {
    macro_spans
        .items
        .iter()
        .map(|(source, expanded)| MacroSpan {
            macro_span: Span {
                start: CodeLocation {
                    line: source.start_line as usize,
                    col: source.start_col as usize,
                    index: source.start_offset as usize,
                },
                stop: CodeLocation {
                    line: source.end_line as usize,
                    col: source.end_col as usize,
                    index: source.end_offset as usize,
                },
            },
            expanded_span: Span {
                start: CodeLocation {
                    line: expanded.start_line as usize,
                    col: expanded.start_col as usize,
                    index: expanded.start_offset as usize,
                },
                stop: CodeLocation {
                    line: expanded.end_line as usize,
                    col: expanded.end_col as usize,
                    index: expanded.end_offset as usize,
                },
            },
        })
        .collect::<Vec<_>>()
}

/// This provides a 'get' method for object supports access via obj.get('key', 'default)
/// `map` is the inner data structure of this object
pub fn get_method(args: &[Value], map: &BTreeMap<String, Value>) -> Result<Value, Error> {
    let mut args = ArgParser::new(args, None);
    let name: String = args.get("name")?;
    let default = args
        .get_optional::<Value>("default")
        .unwrap_or(Value::from(""));

    Ok(match map.get(&name) {
        Some(val) if !val.is_none() => val.clone(),
        _ => default,
    })
}

/// Generate a component name using the specified macro
pub fn generate_component_name(
    env: &JinjaEnvironment,
    component: &str,
    root_project_name: &str,
    current_project_name: &str,
    base_ctx: &BTreeMap<String, Value>,
    custom_name: Option<String>,
    node: Option<&dyn InternalDbtNode>,
) -> FsResult<String> {
    let macro_name = format!("generate_{}_name", component);
    // find the macro template - this is now cached for performance
    let template_name =
        find_generate_macro_template(env, component, root_project_name, current_project_name)?;

    // Optimization: If the template starts with "default__", use the native Rust implementation
    if template_name == format!("dbt.generate_{}_name", component) {
        // technically this would call adapter.dispatch and the user could overwrite the default
        // but this isn't a a behavior we should support cause the user can already overrride without the default prefix
        // Determine which default implementation to use based on component type
        return match component {
            "database" => default_generate_database_name(env, custom_name, node),
            "schema" => default_generate_schema_name(env, custom_name, node),
            "alias" => default_generate_alias_name(custom_name, node),
            _ => Err(fs_err!(
                ErrorCode::JinjaError,
                "No default implementation for component: {}",
                component
            )),
        };
    }

    // Create a state object for rendering
    let template = env.get_template(&template_name)?;

    // Create a new state
    let new_state = template.eval_to_state(base_ctx, &[])?;

    // Build the args
    let mut args = custom_name
        .map(|name| vec![Value::from(name)])
        .unwrap_or(vec![Value::from(())]); // If no custom name, pass in none so the macro reads from the target context
    if let Some(node) = node {
        args.push(Value::from_serialize(node.serialize()));
    }
    // Call the macro
    let result = match new_state.call_macro(macro_name.as_str(), &args, &[]) {
        Ok(value) => value,
        Err(e) => {
            // These macros can call do return which returns an abrupt return error
            // with the value of the macro response in the error value
            if let Some(value) = e.try_abrupt_return() {
                value.to_string()
            } else {
                return Err(fs_err!(ErrorCode::JinjaError, "Failed to call macro"));
            }
        }
    }
    .trim()
    .to_string();
    // Return the result
    Ok(result)
}

/// Rust implementation of default__generate_database_name
fn default_generate_database_name(
    env: &JinjaEnvironment,
    custom_database_name: Option<String>,
    _node: Option<&dyn InternalDbtNode>,
) -> FsResult<String> {
    // Get target.database from context

    let target = env
        .get_global("target")
        .ok_or_else(|| fs_err!(ErrorCode::JinjaError, "Target not found in context"))?;

    let default_database = target
        .get_attr("database")
        .map_err(|_| fs_err!(ErrorCode::JinjaError, "database not found in target"))?
        .to_string();

    // Return either the custom name or default database
    Ok(match custom_database_name {
        None => default_database,
        Some(name) if name.is_empty() => default_database,
        Some(name) => name,
    })
}

/// Rust implementation of default__generate_schema_name
fn default_generate_schema_name(
    env: &JinjaEnvironment,
    custom_schema_name: Option<String>,
    _node: Option<&dyn InternalDbtNode>,
) -> FsResult<String> {
    // Get target.schema from context

    let target = env
        .get_global("target")
        .ok_or_else(|| fs_err!(ErrorCode::JinjaError, "Target not found in context"))?;

    let default_schema = target
        .get_attr("schema")
        .map_err(|_| fs_err!(ErrorCode::JinjaError, "schema not found in target"))?
        .to_string();

    // Return either the default schema or a combination with custom schema
    Ok(match custom_schema_name {
        None => default_schema,
        Some(name) if name.is_empty() => default_schema,
        Some(name) => format!("{}_{}", default_schema, name.trim()),
    })
}

/// Rust implementation of default__generate_alias_name
fn default_generate_alias_name(
    custom_alias_name: Option<String>,
    node: Option<&dyn InternalDbtNode>,
) -> FsResult<String> {
    if let Some(name) = custom_alias_name {
        if !name.is_empty() {
            return Ok(name.trim().to_string());
        }
    }

    // Get the node name and version if available
    if let Some(node) = node {
        // Get node common attributes for name
        let common = node.common();

        // For the version, we need to check the specific type
        match node.resource_type() {
            "model" => {
                if let Some(version) = &node.version() {
                    // Replace dots with underscores in version
                    let formatted_version = version.to_string().replace('.', "_");
                    return Ok(format!("{}_v{}", common.name, formatted_version));
                }
            }
            // Other node types don't have versions
            _ => {}
        }

        return Ok(common.name.clone());
    }

    Err(fs_err!(
        ErrorCode::JinjaError,
        "Cannot generate alias: no custom name or node provided"
    ))
}

/// Clear template cache (primarily for testing purposes)
pub fn clear_template_cache() {
    if let Ok(mut cache) = TEMPLATE_CACHE.lock() {
        cache.clear();
    }
}

/// Find a generate macro by name (database, schema, or alias)
pub fn find_generate_macro_template(
    env: &JinjaEnvironment,
    component: &str,
    root_project_name: &str,
    current_project_name: &str,
) -> FsResult<String> {
    let macro_name = format!("generate_{}_name", component);
    let cache_key = (current_project_name.to_string(), component.to_string());

    // Check cache first - return early if found
    if let Ok(cache) = TEMPLATE_CACHE.lock() {
        if let Some(template) = cache.get(&cache_key) {
            return Ok(template.clone());
        }
    }
    // First try - check the current project
    let template_name = format!("{}.{}", current_project_name, macro_name);
    if env.has_template(&template_name) {
        // Cache and return
        if let Ok(mut cache) = TEMPLATE_CACHE.lock() {
            cache.insert(cache_key, template_name.clone());
        }
        return Ok(template_name);
    }

    // Second try - check the root project
    let template_name = format!("{}.{}", root_project_name, macro_name);
    if env.has_template(&template_name) {
        // Cache and return
        if let Ok(mut cache) = TEMPLATE_CACHE.lock() {
            cache.insert(cache_key, template_name.clone());
        }
        return Ok(template_name);
    }

    // Last attempt - check dbt internal package
    let dbt_and_adapters = env.get_dbt_and_adapters_namespace();
    if let Some(package) = dbt_and_adapters.get(&Value::from(macro_name.as_str())) {
        let template_name = format!("{}.{}", package, macro_name);
        if env.has_template(&template_name) {
            // Cache and return
            if let Ok(mut cache) = TEMPLATE_CACHE.lock() {
                cache.insert(cache_key, template_name.clone());
            }
            return Ok(template_name);
        }
    }

    // Template not found in any location
    Err(fs_err!(
        ErrorCode::JinjaError,
        "Could not find template for {}",
        macro_name
    ))
}

/// Generate a relation name from database, schema, alias
pub fn generate_relation_name(
    parse_adapter: Arc<ParseAdapter>,
    database: &str,
    schema: &str,
    identifier: &str,
    quote_config: ResolvedQuoting,
) -> FsResult<String> {
    let adapter_type = parse_adapter.adapter_type().to_string();
    // Create relation using the adapter
    match create_relation_internal(
        adapter_type,
        database.to_owned(),
        schema.to_owned(),
        Some(identifier.to_owned()),
        None, // relation_type
        quote_config,
    ) {
        Ok(relation) => Ok(relation.render_self_as_str()),
        Err(e) => Err(e),
    }
}

type NodeId = String;
/// Returns the metadata of the current model from the given Jinja execution state
pub fn node_metadata_from_state(state: &State) -> Option<(NodeId, PathBuf)> {
    match state.lookup("model") {
        Some(node) => {
            if let Ok(model) = DbtModel::deserialize(&node) {
                Some((
                    model.common_attr.unique_id,
                    model.common_attr.original_file_path,
                ))
            } else if let Ok(test) = DbtTest::deserialize(&node) {
                Some((
                    test.common_attr.unique_id,
                    test.common_attr.original_file_path,
                ))
            } else if let Ok(snapshot) = DbtSnapshot::deserialize(&node) {
                Some((
                    snapshot.common_attr.unique_id,
                    snapshot.common_attr.original_file_path,
                ))
            } else if let Ok(seed) = DbtSeed::deserialize(&node) {
                Some((
                    seed.common_attr.unique_id,
                    seed.common_attr.original_file_path,
                ))
            } else if let Ok(unit_test) = DbtUnitTest::deserialize(&node) {
                Some((
                    unit_test.common_attr.unique_id,
                    unit_test.common_attr.original_file_path,
                ))
            } else {
                None
            }
        }
        None => None,
    }
}
