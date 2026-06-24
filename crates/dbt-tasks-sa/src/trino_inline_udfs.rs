use std::collections::BTreeSet;
use std::sync::Arc;

use dbt_common::{ErrorCode, FsResult, err, fs_err};
use dbt_schemas::schemas::properties::{
    FUNCTION_LANGUAGE_SQL, FunctionArgument, FunctionKind, Volatility,
};
use dbt_schemas::schemas::serde::StringOrArrayOfStrings;
use dbt_schemas::schemas::{DbtFunction, DbtModel, InternalDbtNodeAttributes};
use dbt_schemas::state::ResolverState;
use minijinja::MacroSpans;
use minijinja::machinery::Span;

pub fn inject_inline_trino_udfs(
    sql: String,
    macro_spans: &mut MacroSpans,
    node: &dyn InternalDbtNodeAttributes,
    resolver_state: &ResolverState,
) -> FsResult<String> {
    if !resolver_state
        .dbt_profile
        .db_config
        .trino_inline_udfs_enabled()
    {
        return Ok(sql);
    }

    let functions = collect_function_dependencies(node, resolver_state)?;
    if functions.is_empty() {
        return Ok(sql);
    }

    let mut names = BTreeSet::new();
    let mut declarations = Vec::new();
    for function in functions {
        let name = function.__common_attr__.name.as_str();
        if !names.insert(name.to_string()) {
            return err!(
                ErrorCode::InvalidConfig,
                "Trino inline UDF names must be unique within a query; found duplicate function name '{}'",
                name
            );
        }

        let (body, _) = resolver_state
            .render_results
            .rendering_results
            .get(&function.__common_attr__.unique_id)
            .ok_or_else(|| {
                fs_err!(
                    ErrorCode::InvalidConfig,
                    "Cannot inline Trino UDF '{}' because its rendered SQL body was not found",
                    function.__common_attr__.unique_id
                )
            })?;

        declarations.push(build_inline_declaration(&function, body)?);
    }

    insert_inline_declarations(sql, &declarations, macro_spans)
}

fn collect_function_dependencies(
    node: &dyn InternalDbtNodeAttributes,
    resolver_state: &ResolverState,
) -> FsResult<Vec<Arc<DbtFunction>>> {
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut functions = Vec::new();

    for dep_id in &node.base().depends_on.nodes {
        visit_function(
            dep_id,
            resolver_state,
            &mut visiting,
            &mut visited,
            &mut functions,
        )?;
    }

    for configured_name in configured_inline_udf_names(node) {
        let function = resolve_configured_function(&configured_name, node, resolver_state)?;
        visit_function(
            &function.__common_attr__.unique_id,
            resolver_state,
            &mut visiting,
            &mut visited,
            &mut functions,
        )?;
    }

    Ok(functions)
}

fn configured_inline_udf_names(node: &dyn InternalDbtNodeAttributes) -> Vec<String> {
    let Some(model) = node.as_any().downcast_ref::<DbtModel>() else {
        return Vec::new();
    };

    match &model.deprecated_config.inline_udfs {
        Some(StringOrArrayOfStrings::String(name)) => vec![name.clone()],
        Some(StringOrArrayOfStrings::ArrayOfStrings(names)) => names.clone(),
        None => Vec::new(),
    }
}

struct ConfiguredFunctionRef<'a> {
    raw: &'a str,
    package_name: Option<&'a str>,
    function_name: &'a str,
}

fn resolve_configured_function(
    configured_name: &str,
    node: &dyn InternalDbtNodeAttributes,
    resolver_state: &ResolverState,
) -> FsResult<Arc<DbtFunction>> {
    let reference = parse_configured_function_ref(configured_name, node)?;

    if let Some(package_name) = reference.package_name {
        return require_function_in_package(&reference, node, resolver_state, package_name);
    }

    resolve_unqualified_function(&reference, node, resolver_state)
}

fn parse_configured_function_ref<'a>(
    configured_name: &'a str,
    node: &dyn InternalDbtNodeAttributes,
) -> FsResult<ConfiguredFunctionRef<'a>> {
    let configured_name = configured_name.trim();
    if configured_name.is_empty() {
        return err!(
            ErrorCode::InvalidConfig,
            "Trino inline UDF config on '{}' contains an empty function name",
            node.unique_id()
        );
    }

    let (package_name, function_name) = configured_name
        .rsplit_once('.')
        .map_or((None, configured_name), |(package_name, function_name)| {
            (Some(package_name), function_name)
        });

    if function_name.is_empty() || package_name.is_some_and(str::is_empty) {
        return err!(
            ErrorCode::InvalidConfig,
            "Trino inline UDF config value '{}' on '{}' is not a valid function reference",
            configured_name,
            node.unique_id()
        );
    }

    Ok(ConfiguredFunctionRef {
        raw: configured_name,
        package_name,
        function_name,
    })
}

fn require_function_in_package(
    reference: &ConfiguredFunctionRef<'_>,
    node: &dyn InternalDbtNodeAttributes,
    resolver_state: &ResolverState,
    package_name: &str,
) -> FsResult<Arc<DbtFunction>> {
    find_function_in_package(resolver_state, package_name, reference.function_name).ok_or_else(|| {
        fs_err!(
            ErrorCode::InvalidConfig,
            "Cannot inline configured Trino UDF '{}' on '{}' because no dbt function resource named '{}' exists in package '{}'",
            reference.raw,
            node.unique_id(),
            reference.function_name,
            package_name
        )
    })
}

fn resolve_unqualified_function(
    reference: &ConfiguredFunctionRef<'_>,
    node: &dyn InternalDbtNodeAttributes,
    resolver_state: &ResolverState,
) -> FsResult<Arc<DbtFunction>> {
    let package_name = node.package_name();
    if let Some(function) =
        find_function_in_package(resolver_state, &package_name, reference.function_name)
    {
        return Ok(function);
    }

    require_unique_function_match(reference, node, resolver_state)
}

fn find_function_in_package(
    resolver_state: &ResolverState,
    package_name: &str,
    function_name: &str,
) -> Option<Arc<DbtFunction>> {
    resolver_state
        .nodes
        .functions
        .values()
        .find(|function| {
            function.__common_attr__.package_name == package_name
                && function.__common_attr__.name == function_name
        })
        .cloned()
}

fn require_unique_function_match(
    reference: &ConfiguredFunctionRef<'_>,
    node: &dyn InternalDbtNodeAttributes,
    resolver_state: &ResolverState,
) -> FsResult<Arc<DbtFunction>> {
    let matches = resolver_state
        .nodes
        .functions
        .values()
        .filter(|function| function.__common_attr__.name == reference.function_name)
        .cloned()
        .collect::<Vec<_>>();

    match matches.as_slice() {
        [function] => Ok(function.clone()),
        [] => err!(
            ErrorCode::InvalidConfig,
            "Cannot inline configured Trino UDF '{}' on '{}' because no dbt function resource named '{}' exists",
            reference.raw,
            node.unique_id(),
            reference.function_name
        ),
        _ => err!(
            ErrorCode::InvalidConfig,
            "Cannot inline configured Trino UDF '{}' on '{}' because the name is ambiguous; use 'package.{}'",
            reference.raw,
            node.unique_id(),
            reference.function_name
        ),
    }
}

fn visit_function(
    unique_id: &str,
    resolver_state: &ResolverState,
    visiting: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
    functions: &mut Vec<Arc<DbtFunction>>,
) -> FsResult<()> {
    if visited.contains(unique_id) {
        return Ok(());
    }

    let Some(function) = resolver_state.nodes.functions.get(unique_id).cloned() else {
        return Ok(());
    };

    if !visiting.insert(unique_id.to_string()) {
        return err!(
            ErrorCode::CyclicDependency,
            "Cycle detected while collecting Trino inline UDF dependencies at '{}'",
            unique_id
        );
    }

    for dep_id in &function.__base_attr__.depends_on.nodes {
        visit_function(dep_id, resolver_state, visiting, visited, functions)?;
    }

    visiting.remove(unique_id);
    visited.insert(unique_id.to_string());
    functions.push(function);
    Ok(())
}

fn build_inline_declaration(function: &DbtFunction, body: &str) -> FsResult<String> {
    let name = function.__common_attr__.name.as_str();
    validate_identifier("function", name)?;

    if function.__base_attr__.alias.as_str() != name {
        return err!(
            ErrorCode::InvalidConfig,
            "Trino inline UDF '{}' cannot use alias '{}'; inline UDF mode uses the dbt function name",
            name,
            function.__base_attr__.alias.as_str()
        );
    }

    if function
        .__function_attr__
        .language
        .as_deref()
        .unwrap_or(FUNCTION_LANGUAGE_SQL)
        != FUNCTION_LANGUAGE_SQL
    {
        return err!(
            ErrorCode::InvalidConfig,
            "Trino inline UDF '{}' must be a SQL function",
            name
        );
    }

    if !matches!(
        function.deprecated_config.function_kind.as_ref(),
        None | Some(FunctionKind::Scalar)
    ) {
        return err!(
            ErrorCode::InvalidConfig,
            "Trino inline UDF '{}' must be a scalar function",
            name
        );
    }

    let args = format_arguments(
        name,
        function
            .__function_attr__
            .arguments
            .as_deref()
            .unwrap_or(&[]),
    )?;
    let returns = function
        .__function_attr__
        .returns
        .as_ref()
        .and_then(|returns| returns.data_type.as_deref())
        .ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "Trino inline UDF '{}' must declare a return data_type",
                name
            )
        })?;
    let volatility = format_volatility(name, function.deprecated_config.volatility.as_ref())?;
    let body = normalize_body(body);

    if volatility.is_empty() {
        Ok(format!(
            "FUNCTION {name}({args})\n    RETURNS {returns}\n    {body}"
        ))
    } else {
        Ok(format!(
            "FUNCTION {name}({args})\n    RETURNS {returns}\n    {volatility}\n    {body}"
        ))
    }
}

fn format_arguments(function_name: &str, args: &[FunctionArgument]) -> FsResult<String> {
    let mut formatted = Vec::new();
    for arg in args {
        let name = arg.name.as_deref().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "Trino inline UDF '{}' has an argument without a name",
                function_name
            )
        })?;
        validate_identifier("argument", name)?;

        if arg.default_value.is_some() {
            return err!(
                ErrorCode::InvalidConfig,
                "Trino inline UDF '{}' argument '{}' cannot use a default value",
                function_name,
                name
            );
        }

        let data_type = arg.data_type.as_deref().ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "Trino inline UDF '{}' argument '{}' must declare a data_type",
                function_name,
                name
            )
        })?;
        formatted.push(format!("{name} {data_type}"));
    }
    Ok(formatted.join(", "))
}

fn format_volatility(
    function_name: &str,
    volatility: Option<&Volatility>,
) -> FsResult<&'static str> {
    match volatility {
        None | Some(Volatility::Deterministic) => Ok(""),
        Some(Volatility::NonDeterministic) => Ok("NOT DETERMINISTIC"),
        Some(Volatility::Stable) => err!(
            ErrorCode::InvalidConfig,
            "Trino inline UDF '{}' cannot use stable volatility",
            function_name
        ),
    }
}

fn normalize_body(body: &str) -> String {
    let body = body.trim().trim_end_matches(';').trim();
    let upper = body.to_ascii_uppercase();
    if upper.starts_with("RETURN ") || upper.starts_with("BEGIN") {
        body.to_string()
    } else {
        format!("RETURN {body}")
    }
}

fn validate_identifier(kind: &str, value: &str) -> FsResult<()> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return err!(
            ErrorCode::InvalidConfig,
            "Trino inline UDF {kind} name is empty"
        );
    };

    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
    {
        return err!(
            ErrorCode::InvalidConfig,
            "Trino inline UDF {kind} '{}' must be an unquoted SQL identifier",
            value
        );
    }
    Ok(())
}

fn insert_inline_declarations(
    sql: String,
    declarations: &[String],
    macro_spans: &mut MacroSpans,
) -> FsResult<String> {
    let declarations = declarations.join(",\n  ");
    let first_token_offset = first_sql_token_offset(&sql, 0);

    if starts_with_keyword(&sql[first_token_offset..], "with") {
        let after_with_offset = first_token_offset + "with".len();
        let next_token_offset = first_sql_token_offset(&sql, after_with_offset);
        if starts_with_keyword(&sql[next_token_offset..], "recursive") {
            return err!(
                ErrorCode::InvalidConfig,
                "Trino inline UDF insertion does not support SQL starting with WITH RECURSIVE"
            );
        }

        let insertion = format!("\n  {declarations},");
        let mut result = sql;
        result.insert_str(after_with_offset, &insertion);
        shift_macro_spans(macro_spans, after_with_offset as u32, &insertion);
        Ok(result)
    } else {
        let prefix = format!("WITH\n  {declarations}\n");
        shift_macro_spans(macro_spans, 0, &prefix);
        Ok(format!("{prefix}{sql}"))
    }
}

fn first_sql_token_offset(sql: &str, mut offset: usize) -> usize {
    let bytes = sql.as_bytes();
    offset = skip_whitespace(bytes, offset);

    while let Some(comment_end) = sql_comment_end(sql, offset) {
        offset = skip_whitespace(bytes, comment_end);
    }

    offset
}

fn skip_whitespace(bytes: &[u8], mut offset: usize) -> usize {
    while bytes
        .get(offset)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        offset += 1;
    }
    offset
}

fn sql_comment_end(sql: &str, offset: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    if bytes
        .get(offset..)
        .is_some_and(|rest| rest.starts_with(b"--"))
    {
        return Some(line_comment_end(bytes, offset + 2));
    }

    if bytes
        .get(offset..)
        .is_some_and(|rest| rest.starts_with(b"/*"))
    {
        return block_comment_end(sql, offset);
    }

    None
}

fn line_comment_end(bytes: &[u8], mut offset: usize) -> usize {
    while bytes.get(offset).is_some_and(|byte| *byte != b'\n') {
        offset += 1;
    }
    offset
}

fn block_comment_end(sql: &str, offset: usize) -> Option<usize> {
    sql[offset + 2..].find("*/").map(|end| offset + 2 + end + 2)
}

fn starts_with_keyword(sql: &str, keyword: &str) -> bool {
    sql.get(..keyword.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
        && sql[keyword.len()..]
            .chars()
            .next()
            .is_none_or(|c| !is_identifier_char(c))
}

fn is_identifier_char(c: char) -> bool {
    c == '_' || c.is_ascii_alphanumeric()
}

fn shift_macro_spans(macro_spans: &mut MacroSpans, insertion_offset: u32, inserted: &str) {
    let added_lines = inserted.matches('\n').count() as u32;
    let added_offset = inserted.len() as u32;
    for (_, expanded) in macro_spans
        .items
        .iter_mut()
        .chain(macro_spans.raw_source_spans.iter_mut())
    {
        shift_span(expanded, insertion_offset, added_lines, added_offset);
    }
}

fn shift_span(span: &mut Span, insertion_offset: u32, added_lines: u32, added_offset: u32) {
    if span.start_offset >= insertion_offset {
        span.start_line += added_lines;
        span.end_line += added_lines;
        span.start_offset += added_offset;
        span.end_offset += added_offset;
    } else if span.end_offset >= insertion_offset {
        span.end_line += added_lines;
        span.end_offset += added_offset;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserts_declarations_before_select() {
        let sql = insert_inline_declarations(
            "select doubleup(21)".to_string(),
            &["FUNCTION doubleup(x integer)\n    RETURNS integer\n    RETURN x * 2".to_string()],
            &mut MacroSpans::default(),
        )
        .unwrap();

        assert_eq!(
            sql,
            "WITH\n  FUNCTION doubleup(x integer)\n    RETURNS integer\n    RETURN x * 2\nselect doubleup(21)"
        );
    }

    #[test]
    fn merges_declarations_into_existing_with() {
        let sql = insert_inline_declarations(
            "with cte as (select 1) select * from cte".to_string(),
            &["FUNCTION meaning_of_life()\n    RETURNS bigint\n    RETURN 42".to_string()],
            &mut MacroSpans::default(),
        )
        .unwrap();

        assert_eq!(
            sql,
            "with\n  FUNCTION meaning_of_life()\n    RETURNS bigint\n    RETURN 42, cte as (select 1) select * from cte"
        );
    }

    #[test]
    fn merges_declarations_after_leading_comments() {
        let sql = insert_inline_declarations(
            "-- model comment\nwith cte as (select 1) select * from cte".to_string(),
            &["FUNCTION f()\n    RETURNS bigint\n    RETURN 1".to_string()],
            &mut MacroSpans::default(),
        )
        .unwrap();

        assert_eq!(
            sql,
            "-- model comment\nwith\n  FUNCTION f()\n    RETURNS bigint\n    RETURN 1, cte as (select 1) select * from cte"
        );
    }

    #[test]
    fn merges_declarations_when_comment_follows_with() {
        let sql = insert_inline_declarations(
            "with /* existing ctes */ cte as (select 1) select * from cte".to_string(),
            &["FUNCTION f()\n    RETURNS bigint\n    RETURN 1".to_string()],
            &mut MacroSpans::default(),
        )
        .unwrap();

        assert_eq!(
            sql,
            "with\n  FUNCTION f()\n    RETURNS bigint\n    RETURN 1, /* existing ctes */ cte as (select 1) select * from cte"
        );
    }

    #[test]
    fn rejects_with_recursive() {
        let err = insert_inline_declarations(
            "with recursive t(n) as (select 1) select * from t".to_string(),
            &["FUNCTION f()\n    RETURNS bigint\n    RETURN 1".to_string()],
            &mut MacroSpans::default(),
        )
        .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidConfig);
    }
}
