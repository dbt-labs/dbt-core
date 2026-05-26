//! `GET /api/v1/saved_queries/:id` — typed saved query detail.
//! `GET /api/v1/saved_queries` — cursor-paginated saved query list.
//! `GET /api/v1/saved_queries/facets` — filter facet metadata (empty in v0).
//!
//! Saved queries are declarative SL definitions; they have no run-time
//! execution status. If a saved query's exports are materialized, run
//! status lives on the resulting model nodes, queryable via
//! `GET /api/v1/models/:id`. This endpoint therefore omits
//! `execution_info`, `catalog`, `freshness`, `columns`, `compiled_code`,
//! and `raw_code`.
//!
//! `query_params` and `exports` are JSON-string parquet columns;
//! they're deserialized handler-side via
//! [`crate::handlers::json::json_parse_or_null`] so the response carries
//! real JSON values, not escaped strings. Malformed blobs become `null`.
//!
//! `depends_on` is synthesized from `dbt.saved_queries.depends_on_nodes`
//! — `dbt.edges` typically has no rows for `saved_query.*` unique_ids
//! in current parquet output. Each edge's `edge_type` is derived from
//! the dotted unique_id prefix (`metric.*` → `"metric"`,
//! `semantic_model.*` → `"semantic_model"`, etc.).
//!
//! Data sources:
//! - `dbt.saved_queries` — row + JSON-string columns
//! - `dbt.edges` — `referenced_by` (downstream only)

use std::fmt::Write as _;

use arrow_array::{Array, Float64Array, ListArray, RecordBatch, StringArray};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::handlers::json::{bad_request, internal_error, json_parse_or_null, not_found};
use crate::handlers::node_base::{
    EdgeRef, NodeBase, extract_edge_refs, extract_str_list, opt_str, str_col,
};
use crate::handlers::pagination::{Cursor, PageInfo, SortDir, clamp_first, cursor_where_fragment};
use crate::handlers::sql::escape_str;
use crate::state::SharedState;

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Response body for `GET /api/v1/saved_queries/:id`.
#[derive(Serialize)]
pub struct SavedQueryDetail {
    #[serde(flatten)]
    pub base: NodeBase,
    /// Display label from YAML.
    pub label: Option<String>,
    /// Project-relative path of the `.yml` containing the saved query
    /// block — equal to `original_file_path` for most projects.
    pub file_path: Option<String>,
    pub fqn: Vec<String>,
    pub tags: Vec<String>,
    pub group_name: Option<String>,
    /// Epoch seconds (float) when the saved query definition was
    /// generated; sourced from `dbt.saved_queries.created_at`.
    pub created_at: Option<f64>,
    /// Parsed JSON object, or `null` when absent / unparseable.
    pub query_params: serde_json::Value,
    /// Parsed JSON array of `Export` objects, or `null` when absent /
    /// unparseable.
    pub exports: serde_json::Value,
    /// Upstream resources — typically metrics and semantic models.
    /// Synthesized from `dbt.saved_queries.depends_on_nodes`; macros are
    /// excluded.
    pub depends_on: Vec<EdgeRef>,
    /// Downstream consumers (typically empty).
    pub referenced_by: Vec<EdgeRef>,
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

const SAVED_QUERY_DETAIL_NODE_SQL: &str = "\
SELECT unique_id, name, label, description, package_name, \
       original_file_path, file_path, \
       fqn, tags, group_name, created_at, \
       query_params, exports, depends_on_nodes \
FROM dbt.saved_queries \
WHERE unique_id = '{id}' \
LIMIT 1";

// ---------------------------------------------------------------------------
// Extractors
// ---------------------------------------------------------------------------

/// Map a unique_id like `metric.jaffle_shop.revenue` to the leading
/// dotted token (`"metric"`). When the id has no `.`, returns the id as
/// the type — covers unprefixed parquet rows seen in current sample
/// projects.
fn edge_type_from_prefix(unique_id: &str) -> String {
    match unique_id.split_once('.') {
        Some((prefix, _)) => prefix.to_owned(),
        None => unique_id.to_owned(),
    }
}

fn extract_saved_query_detail(batches: &[RecordBatch]) -> Option<SavedQueryDetail> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;

    let s = |name: &'static str| -> Option<String> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<StringArray>()?;
        opt_str(col, 0)
    };

    let created_at = batch
        .column_by_name("created_at")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .and_then(|c| if c.is_null(0) { None } else { Some(c.value(0)) });

    let query_params = json_parse_or_null(s("query_params").as_deref());
    let exports = json_parse_or_null(s("exports").as_deref());

    let depends_on_nodes = extract_str_list(batch, "depends_on_nodes");
    let depends_on = depends_on_nodes
        .into_iter()
        .map(|uid| EdgeRef {
            edge_type: edge_type_from_prefix(&uid),
            unique_id: uid,
        })
        .collect();

    Some(SavedQueryDetail {
        base: NodeBase {
            unique_id: s("unique_id").unwrap_or_default(),
            name: s("name").unwrap_or_default(),
            resource_type: "saved_query".to_owned(),
            package_name: s("package_name"),
            description: s("description"),
            original_file_path: s("original_file_path"),
        },
        label: s("label"),
        file_path: s("file_path"),
        fqn: extract_str_list(batch, "fqn"),
        tags: extract_str_list(batch, "tags"),
        group_name: s("group_name"),
        created_at,
        query_params,
        exports,
        depends_on,
        referenced_by: vec![],
    })
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `GET /api/v1/saved_queries/:id` — full saved query detail.
///
/// `query_params` and `exports` are parsed handler-side from the
/// JSON-string parquet columns; malformed blobs surface as `null`.
/// `depends_on` is synthesized from `depends_on_nodes`; `referenced_by`
/// reads downstream rows from `dbt.edges` (typically empty).
pub async fn get_saved_query(
    State(state): State<SharedState>,
    Path(unique_id): Path<String>,
) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }
    let id = escape_str(&unique_id);

    let node_sql = SAVED_QUERY_DETAIL_NODE_SQL.replace("{id}", &id);
    let downstream_sql = format!(
        "SELECT child_unique_id AS unique_id, edge_type \
         FROM dbt.edges WHERE parent_unique_id = '{id}' \
         ORDER BY child_unique_id"
    );

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let node_batches = backend.query_arrow(&node_sql).map_err(|e| e.to_string())?;
        // Missing dbt.edges view → no downstream refs; treat as empty.
        let downstream_batches = backend.query_arrow(&downstream_sql).ok();
        Ok((node_batches, downstream_batches))
    })
    .await;

    let (node_batches, downstream_batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let Some(mut detail) = extract_saved_query_detail(&node_batches) else {
        return not_found(format!("saved query {unique_id} not found"));
    };

    detail.referenced_by = downstream_batches
        .as_deref()
        .map(extract_edge_refs)
        .unwrap_or_default();

    Json(detail).into_response()
}

// ---------------------------------------------------------------------------
// Response types: GET /api/v1/saved_queries and GET /api/v1/saved_queries/facets
// ---------------------------------------------------------------------------

/// One row in `GET /api/v1/saved_queries`.
#[derive(Serialize)]
pub struct SavedQuerySummary {
    pub unique_id: String,
    pub name: String,
    pub package_name: Option<String>,
    pub group_name: Option<String>,
    pub tags: Vec<String>,
    pub description: Option<String>,
    pub created_at: Option<f64>,
    /// Upstream node unique_ids from `dbt.saved_queries.depends_on_nodes`.
    /// Capped at 500; see `depends_on_nodes_truncated`.
    pub depends_on_nodes: Vec<String>,
    /// `true` when the underlying list exceeded the cap and the response is
    /// truncated.
    pub depends_on_nodes_truncated: bool,
}

/// Cursor-paginated response for `GET /api/v1/saved_queries`.
#[derive(Serialize)]
pub struct SavedQueryListResponse {
    pub data: Vec<SavedQuerySummary>,
    pub page_info: PageInfo,
}

/// Response body for `GET /api/v1/saved_queries/facets`.
///
/// No filter dimensions in v0 — the saved-queries list exposes no filter
/// params. Returns `{}` rather than named empty arrays so that adding a
/// facet dimension later is wire-additive.
#[derive(Serialize)]
pub struct SavedQueryFacetsResponse {}

/// Query parameters for `GET /api/v1/saved_queries`.
#[derive(Debug, Default, Deserialize)]
pub struct SavedQueryListParams {
    /// Sort: `name:asc` (default) or `name:desc`. Any other key returns 400.
    pub sort: Option<String>,
    pub first: Option<u32>,
    pub after: Option<String>,
}

/// Maximum number of `depends_on_nodes` entries inlined per list row.
const DEPENDS_ON_NODES_CAP: usize = 500;

// ---------------------------------------------------------------------------
// SQL: GET /api/v1/saved_queries
// ---------------------------------------------------------------------------

/// Build `(count_sql, rows_sql)` for `GET /api/v1/saved_queries`.
///
/// Only `name` is sortable; any other column returns `Err`.
/// The count query excludes the cursor predicate so `total_count` reflects
/// the full set.
pub(crate) fn build_saved_query_list_sql(
    params: &SavedQueryListParams,
    first: u32,
    cursor: Option<&Cursor>,
) -> Result<(String, String), &'static str> {
    let (sort_col, dir) = parse_saved_query_sort(params.sort.as_deref())?;

    let filter_where = String::from("WHERE 1=1");

    let mut page_where = filter_where.clone();
    if let Some(c) = cursor {
        let frag = cursor_where_fragment(
            &format!("sq.{sort_col}"),
            "sq.unique_id",
            dir,
            c.sort_value.as_deref(),
            &c.unique_id,
        );
        let _ = write!(page_where, " AND {frag}");
    }

    let dir_sql = dir.as_sql();
    let count_sql = format!("SELECT count(*) FROM dbt.saved_queries sq {filter_where}");
    let peek = first + 1;
    let rows_sql = format!(
        "SELECT sq.unique_id, sq.name, sq.package_name, sq.group_name, \
                sq.tags, sq.description, sq.created_at, sq.depends_on_nodes \
         FROM dbt.saved_queries sq {page_where} \
         ORDER BY sq.{sort_col} {dir_sql} NULLS LAST, sq.unique_id ASC \
         LIMIT {peek}"
    );

    Ok((count_sql, rows_sql))
}

/// Parse the `?sort=` parameter for saved queries.
///
/// Returns `("name", Asc)` by default. Returns `Err` on unknown column or
/// unknown direction.
fn parse_saved_query_sort(sort: Option<&str>) -> Result<(&'static str, SortDir), &'static str> {
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
// Extraction helpers: GET /api/v1/saved_queries
// ---------------------------------------------------------------------------

/// Extract a `List<Utf8>` column at a specific row index.
/// Returns `[]` for missing column, null cell, or non-list child.
fn list_col_at(batch: &RecordBatch, col_name: &'static str, row: usize) -> Vec<String> {
    let Some(col) = batch.column_by_name(col_name) else {
        return vec![];
    };
    if col.is_null(row) {
        return vec![];
    }
    let Some(list) = col.as_any().downcast_ref::<ListArray>() else {
        return vec![];
    };
    let inner = list.value(row);
    let Some(strings) = inner.as_any().downcast_ref::<StringArray>() else {
        return vec![];
    };
    (0..strings.len())
        .filter(|&i| !strings.is_null(i))
        .map(|i| strings.value(i).to_owned())
        .collect()
}

fn batches_to_saved_query_summary_rows(batches: &[RecordBatch]) -> Vec<SavedQuerySummary> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let unique_id = str_col(batch, "unique_id");
        let name = str_col(batch, "name");
        let package_name = str_col(batch, "package_name");
        let group_name = str_col(batch, "group_name");
        let description = str_col(batch, "description");
        let created_at_col = batch
            .column_by_name("created_at")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());

        for i in 0..batch.num_rows() {
            let created_at = created_at_col.and_then(|col| {
                if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i))
                }
            });
            let mut dep_nodes = list_col_at(batch, "depends_on_nodes", i);
            let truncated = dep_nodes.len() > DEPENDS_ON_NODES_CAP;
            if truncated {
                dep_nodes.truncate(DEPENDS_ON_NODES_CAP);
            }
            rows.push(SavedQuerySummary {
                unique_id: unique_id.value(i).to_owned(),
                name: name.value(i).to_owned(),
                package_name: opt_str(package_name, i),
                group_name: opt_str(group_name, i),
                tags: list_col_at(batch, "tags", i),
                description: opt_str(description, i),
                created_at,
                depends_on_nodes: dep_nodes,
                depends_on_nodes_truncated: truncated,
            });
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// Handlers: GET /api/v1/saved_queries and GET /api/v1/saved_queries/facets
// ---------------------------------------------------------------------------

/// `GET /api/v1/saved_queries` — cursor-paginated list of saved query definitions.
///
/// Sort defaults to `name:asc`; only `name` is sortable. No filter params in v0.
/// `depends_on_nodes` is inlined per row, capped at 500.
pub async fn list_saved_queries(
    State(state): State<SharedState>,
    Query(params): Query<SavedQueryListParams>,
) -> Response {
    let first = clamp_first(params.first);

    let cursor = match params.after.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => match Cursor::decode(s) {
            Ok(c) => Some(c),
            Err(msg) => return bad_request(msg),
        },
        None => None,
    };

    let (count_sql, rows_sql) = match build_saved_query_list_sql(&params, first, cursor.as_ref()) {
        Ok(pair) => pair,
        Err(msg) => return bad_request(msg),
    };

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let total = backend
            .query_scalar(&count_sql)
            .ok_or_else(|| "count query returned no rows".to_string())?
            .parse::<u64>()
            .map_err(|e| format!("could not parse saved query count: {e}"))?;
        let batches = backend.query_arrow(&rows_sql).map_err(|e| e.to_string())?;
        Ok((total, batches))
    })
    .await;

    let (total_count, batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let mut rows = batches_to_saved_query_summary_rows(&batches);

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

    Json(SavedQueryListResponse {
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

/// `GET /api/v1/saved_queries/facets` — filter facet values for the saved
/// queries list.
///
/// No filter dimensions in v0: returns `{}`. When a future revision adds
/// filter params to the list endpoint, the corresponding facet keys will be
/// added here at the same time.
pub async fn list_saved_query_facets() -> Response {
    Json(SavedQueryFacetsResponse {}).into_response()
}

#[cfg(test)]
#[path = "saved_queries_tests.rs"]
mod tests;
