//! `GET /api/v1/macros/:id` — typed macro detail.
//! `GET /api/v1/macros` — cursor-paginated macro list.
//! `GET /api/v1/macros/facets` — filter facet metadata (packages).
//!
//! Definition-only resource: no `execution_info`, no `catalog`, no
//! `columns`, no `materialized`. Macros live in `dbt.macros` (not
//! `dbt.nodes`) and do not appear in `dbt.edges`; `depends_on` is
//! synthesised directly from the `depends_on_macros` list column on the
//! row itself, with `edge_type: "macro"` for every entry.
//!
//! `meta` and `arguments` are JSON-string parquet columns; both are
//! deserialized handler-side via
//! [`crate::handlers::json::json_parse_or_null`] so the response carries
//! real JSON values, not escaped strings.
//!
//! Data sources:
//! - `dbt.macros` — the macro row (everything on the response)

use std::fmt::Write as _;

use arrow_array::{Array, Float64Array, RecordBatch, StringArray};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::handlers::json::{bad_request, internal_error, json_parse_or_null, not_found};
use crate::handlers::node_base::{EdgeRef, NodeBase, extract_str_list, opt_str, str_col};
use crate::handlers::pagination::{Cursor, PageInfo, SortDir, clamp_first, cursor_where_fragment};
use crate::handlers::sql::escape_str;
use crate::state::SharedState;

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Response body for `GET /api/v1/macros/:id`.
#[derive(Serialize)]
pub struct MacroDetail {
    #[serde(flatten)]
    pub base: NodeBase,
    pub file_path: Option<String>,
    pub patch_path: Option<String>,
    /// Jinja template source (analog of `raw_code` on model-shaped resources).
    pub macro_sql: Option<String>,
    /// Parsed JSON array of `{name, type, description}`, or `null` when
    /// absent / unparseable.
    pub arguments: serde_json::Value,
    /// Upstream macros this macro calls. Synthesised from the row's own
    /// `depends_on_macros` list column; never sourced from `dbt.edges`.
    pub depends_on: Vec<EdgeRef>,
    pub docs_show: Option<bool>,
    pub supported_languages: Vec<String>,
    /// Parsed JSON object, or `null` when absent / unparseable.
    pub meta: serde_json::Value,
    pub created_at: Option<f64>,
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

const MACRO_DETAIL_SQL: &str = "\
SELECT unique_id, name, package_name, description, \
       original_file_path, file_path, patch_path, \
       macro_sql, arguments, meta, \
       docs_show, supported_languages, depends_on_macros, \
       created_at \
FROM dbt.macros \
WHERE unique_id = '{id}' \
LIMIT 1";

// ---------------------------------------------------------------------------
// Extractors
// ---------------------------------------------------------------------------

fn extract_macro_detail(batches: &[RecordBatch]) -> Option<MacroDetail> {
    use arrow_array::BooleanArray;
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;

    let s = |name: &'static str| -> Option<String> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<StringArray>()?;
        opt_str(col, 0)
    };
    let b = |name: &'static str| -> Option<bool> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<BooleanArray>()?;
        if col.is_null(0) {
            None
        } else {
            Some(col.value(0))
        }
    };
    let f = |name: &'static str| -> Option<f64> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<Float64Array>()?;
        if col.is_null(0) {
            None
        } else {
            Some(col.value(0))
        }
    };

    let meta = json_parse_or_null(s("meta").as_deref());
    let arguments = json_parse_or_null(s("arguments").as_deref());

    let depends_on = extract_str_list(batch, "depends_on_macros")
        .into_iter()
        .map(|unique_id| EdgeRef {
            unique_id,
            edge_type: "macro".to_owned(),
        })
        .collect();

    Some(MacroDetail {
        base: NodeBase {
            unique_id: s("unique_id").unwrap_or_default(),
            name: s("name").unwrap_or_default(),
            resource_type: "macro".to_owned(),
            package_name: s("package_name"),
            description: s("description"),
            original_file_path: s("original_file_path"),
        },
        file_path: s("file_path"),
        patch_path: s("patch_path"),
        macro_sql: s("macro_sql"),
        arguments,
        depends_on,
        docs_show: b("docs_show"),
        supported_languages: extract_str_list(batch, "supported_languages"),
        meta,
        created_at: f("created_at"),
    })
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `GET /api/v1/macros/:id` — full macro detail.
///
/// Macros are definition-only: no `execution_info`, `catalog`, `columns`,
/// or `materialized`. `depends_on` is synthesised from the row's
/// `depends_on_macros` list column, not from `dbt.edges`.
pub async fn get_macro(
    State(state): State<SharedState>,
    Path(unique_id): Path<String>,
) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }
    let id = escape_str(&unique_id);

    let sql = MACRO_DETAIL_SQL.replace("{id}", &id);

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        backend.query_arrow(&sql).map_err(|e| e.to_string())
    })
    .await;

    let batches = match result {
        Ok(Ok(b)) => b,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let Some(detail) = extract_macro_detail(&batches) else {
        return not_found(format!("macro {unique_id} not found"));
    };

    Json(detail).into_response()
}

// ---------------------------------------------------------------------------
// Response types: GET /api/v1/macros and GET /api/v1/macros/facets
// ---------------------------------------------------------------------------

/// One row in `GET /api/v1/macros`.
#[derive(Serialize)]
pub struct MacroSummary {
    pub unique_id: String,
    pub name: String,
    pub package_name: Option<String>,
    pub description: Option<String>,
    /// Parsed JSON array of `{name, type, description}`, or `null` when
    /// absent / unparseable.
    pub arguments: serde_json::Value,
}

/// Cursor-paginated response for `GET /api/v1/macros`.
#[derive(Serialize)]
pub struct MacroListResponse {
    pub data: Vec<MacroSummary>,
    pub page_info: PageInfo,
}

/// One package facet option; `count` is always `null` today (reserved).
#[derive(Serialize)]
pub struct MacroFacetValue {
    pub value: String,
    pub count: Option<u64>,
}

impl MacroFacetValue {
    fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            count: None,
        }
    }
}

/// Response body for `GET /api/v1/macros/facets`.
#[derive(Serialize)]
pub struct MacroFacetsResponse {
    pub packages: Vec<MacroFacetValue>,
}

/// Query parameters for `GET /api/v1/macros`.
#[derive(Debug, Default, Deserialize)]
pub struct MacroListParams {
    /// Exact-match package name filter. Empty string treated as absent.
    pub package: Option<String>,
    /// Sort: `name:asc` (default) or `name:desc`. Any other key returns 400.
    pub sort: Option<String>,
    pub first: Option<u32>,
    pub after: Option<String>,
}

// ---------------------------------------------------------------------------
// SQL: GET /api/v1/macros
// ---------------------------------------------------------------------------

const PACKAGES_FACET_SQL: &str = "\
SELECT DISTINCT package_name AS value \
FROM dbt.macros \
WHERE package_name IS NOT NULL \
ORDER BY value";

/// Build `(count_sql, rows_sql)` for `GET /api/v1/macros`.
///
/// Supports sort on `name` only; any other key returns `Err`.
/// The count query excludes the cursor predicate so `total_count` reflects
/// the full filter-matching set.
pub(crate) fn build_macro_list_sql(
    params: &MacroListParams,
    first: u32,
    cursor: Option<&Cursor>,
) -> Result<(String, String), &'static str> {
    let (sort_col, dir) = parse_macro_sort(params.sort.as_deref())?;

    let mut filter_where = String::from("WHERE 1=1");
    if let Some(pkg) = params.package.as_deref().filter(|s| !s.is_empty()) {
        let _ = write!(filter_where, " AND package_name = '{}'", escape_str(pkg));
    }

    let mut page_where = filter_where.clone();
    if let Some(c) = cursor {
        let frag = cursor_where_fragment(
            &format!("m.{sort_col}"),
            "m.unique_id",
            dir,
            c.sort_value.as_deref(),
            &c.unique_id,
        );
        let _ = write!(page_where, " AND {frag}");
    }

    let dir_sql = dir.as_sql();
    let count_sql = format!("SELECT count(*) FROM dbt.macros m {filter_where}");
    let peek = first + 1;
    let rows_sql = format!(
        "SELECT m.unique_id, m.name, m.package_name, m.description, m.arguments \
         FROM dbt.macros m {page_where} \
         ORDER BY m.{sort_col} {dir_sql} NULLS LAST, m.unique_id ASC \
         LIMIT {peek}"
    );

    Ok((count_sql, rows_sql))
}

/// Parse the `?sort=` parameter.
///
/// Returns `("name", Asc)` by default. Returns `Err` on unknown column or
/// unknown direction.
fn parse_macro_sort(sort: Option<&str>) -> Result<(&'static str, SortDir), &'static str> {
    let Some(raw) = sort.filter(|s| !s.is_empty()) else {
        return Ok(("name", SortDir::Asc));
    };
    let (col, dir_str) = raw
        .split_once(':')
        .ok_or("sort must be <column>:<asc|desc>")?;
    let dir = match dir_str {
        "asc" => SortDir::Asc,
        "desc" => SortDir::Desc,
        _ => return Err("sort direction must be asc or desc"),
    };
    match col {
        "name" => Ok(("name", dir)),
        _ => Err("unknown sort column; only 'name' is supported"),
    }
}

// ---------------------------------------------------------------------------
// Extraction helpers: GET /api/v1/macros
// ---------------------------------------------------------------------------

fn batches_to_macro_summary_rows(batches: &[RecordBatch]) -> Vec<MacroSummary> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let unique_id = str_col(batch, "unique_id");
        let name = str_col(batch, "name");
        let package_name = str_col(batch, "package_name");
        let description = str_col(batch, "description");
        let arguments_col = str_col(batch, "arguments");

        for i in 0..batch.num_rows() {
            let arguments = json_parse_or_null(if arguments_col.is_null(i) {
                None
            } else {
                Some(arguments_col.value(i))
            });
            rows.push(MacroSummary {
                unique_id: unique_id.value(i).to_owned(),
                name: name.value(i).to_owned(),
                package_name: opt_str(package_name, i),
                description: opt_str(description, i),
                arguments,
            });
        }
    }
    rows
}

fn batches_to_macro_facet_values(batches: &[RecordBatch]) -> Vec<MacroFacetValue> {
    let mut values = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = str_col(batch, "value");
        for i in 0..batch.num_rows() {
            if !col.is_null(i) {
                values.push(MacroFacetValue::new(col.value(i)));
            }
        }
    }
    values
}

// ---------------------------------------------------------------------------
// Handlers: GET /api/v1/macros and GET /api/v1/macros/facets
// ---------------------------------------------------------------------------

/// `GET /api/v1/macros` — cursor-paginated, filterable list of macro definitions.
///
/// Sort defaults to `name:asc`; only `name` is sortable. `?sort` with any
/// other column returns 400.
/// Filter: `package` (exact match on `package_name`).
/// `arguments` per row is parsed from the JSON-string parquet column; `null`
/// when absent or unparseable.
pub async fn list_macros(
    State(state): State<SharedState>,
    Query(params): Query<MacroListParams>,
) -> Response {
    let first = clamp_first(params.first);

    let cursor = match params.after.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => match Cursor::decode(s) {
            Ok(c) => Some(c),
            Err(msg) => return bad_request(msg),
        },
        None => None,
    };

    let (count_sql, rows_sql) = match build_macro_list_sql(&params, first, cursor.as_ref()) {
        Ok(pair) => pair,
        Err(msg) => return bad_request(msg),
    };

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let total = backend
            .query_scalar(&count_sql)
            .ok_or_else(|| "count query returned no rows".to_string())?
            .parse::<u64>()
            .map_err(|e| format!("could not parse macro count: {e}"))?;
        let batches = backend.query_arrow(&rows_sql).map_err(|e| e.to_string())?;
        Ok((total, batches))
    })
    .await;

    let (total_count, batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let mut rows = batches_to_macro_summary_rows(&batches);

    let has_next_page = rows.len() as u32 > first;
    if has_next_page {
        rows.truncate(first as usize);
    }

    let start_cursor = rows.first().map(|row| {
        Cursor {
            sort_value: Some(row.name.clone()),
            unique_id: row.unique_id.clone(),
        }
        .encode()
    });
    let end_cursor = if has_next_page {
        rows.last().map(|row| {
            Cursor {
                sort_value: Some(row.name.clone()),
                unique_id: row.unique_id.clone(),
            }
            .encode()
        })
    } else {
        None
    };

    Json(MacroListResponse {
        data: rows,
        page_info: PageInfo {
            total_count,
            start_cursor,
            end_cursor,
            has_next_page,
        },
    })
    .into_response()
}

/// `GET /api/v1/macros/facets` — filter facet values for the macros list.
///
/// Returns `packages`: distinct `package_name` values from `dbt.macros`,
/// sorted ascending. `count` is always `null` today (reserved). Empty array
/// when no macros exist.
pub async fn list_macro_facets(State(state): State<SharedState>) -> Response {
    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        backend
            .query_arrow(PACKAGES_FACET_SQL)
            .map_err(|e| e.to_string())
    })
    .await;

    let batches = match result {
        Ok(Ok(b)) => b,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    Json(MacroFacetsResponse {
        packages: batches_to_macro_facet_values(&batches),
    })
    .into_response()
}

#[cfg(test)]
#[path = "macros_tests.rs"]
mod tests;
