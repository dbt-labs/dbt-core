//! `GET /api/v1/metrics/:id` — typed metric detail.
//! `GET /api/v1/metrics` — cursor-paginated metric list.
//! `GET /api/v1/metrics/facets` — filter facet metadata (empty in v0).
//!
//! Metrics are Semantic Layer definitions, not warehouse-materialized
//! objects: no `execution_info`, no `catalog`, no `columns`. The shape
//! preserves the manifest's nested JSON for `type_params`, `filter`, and
//! `meta` — all three are JSON-string parquet columns deserialized
//! handler-side via [`crate::handlers::json::json_parse_or_null`].
//!
//! `depends_on` and `referenced_by` come from `dbt.edges`; for metrics the
//! upstream mix is `semantic_model.*` (simple/cumulative) or `metric.*`
//! (ratio/derived), and downstream is typically `saved_query.*`.
//!
//! Data sources:
//! - `dbt.metrics` — metric row
//! - `dbt.edges` — `depends_on` (upstream) and `referenced_by` (downstream)

use std::fmt::Write as _;

use arrow_array::{Array, Float64Array, RecordBatch, StringArray};
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

/// Response body for `GET /api/v1/metrics/:id`.
#[derive(Serialize)]
pub struct MetricDetail {
    #[serde(flatten)]
    pub base: NodeBase,
    /// Project-relative path of the `.yml` containing the metric block.
    pub file_path: Option<String>,
    pub fqn: Vec<String>,
    pub tags: Vec<String>,
    /// Human-readable label rendered in MetricView header.
    pub label: Option<String>,
    /// `"simple" | "ratio" | "derived" | "cumulative" | "conversion"`.
    pub metric_type: Option<String>,
    /// Parsed from JSON-string column `dbt.metrics.type_params`; shape
    /// varies by `metric_type` (see manifest v10 `metrics[].type_params`).
    pub type_params: serde_json::Value,
    /// Parsed from JSON-string column `dbt.metrics.metric_filter`. Manifest
    /// shape `{ where_filters: [{ where_sql_template }] }`.
    pub filter: serde_json::Value,
    pub time_granularity: Option<String>,
    pub semantic_model_name: Option<String>,
    pub input_metric_names: Vec<String>,
    pub group_name: Option<String>,
    /// Parsed JSON object, or `null` when absent / unparseable.
    pub meta: serde_json::Value,
    /// 1-hop upstream from `dbt.edges`. May mix `semantic_model.*` (simple/
    /// cumulative metrics) and `metric.*` (ratio/derived metrics).
    pub depends_on: Vec<EdgeRef>,
    /// 1-hop downstream from `dbt.edges`; typically `saved_query.*` or
    /// downstream `metric.*` consumers (derived/ratio).
    pub referenced_by: Vec<EdgeRef>,
    /// Epoch seconds; per-resource "Definition updated as of …" timestamp.
    pub created_at: Option<f64>,
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

// `dbt.metrics` has no `resource_type` column; this handler hardcodes
// `"metric"` on the response.
const METRIC_DETAIL_ROW_SQL: &str = "\
SELECT unique_id, name, package_name, description, \
       original_file_path, file_path, \
       label, metric_type, type_params, metric_filter, \
       time_granularity, semantic_model_name, group_name, \
       input_metric_names, fqn, tags, meta, created_at \
FROM dbt.metrics \
WHERE unique_id = '{id}' \
LIMIT 1";

// ---------------------------------------------------------------------------
// Extractors
// ---------------------------------------------------------------------------

fn extract_metric_detail(batches: &[RecordBatch]) -> Option<MetricDetail> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;

    let s = |name: &'static str| -> Option<String> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<StringArray>()?;
        opt_str(col, 0)
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

    let type_params = json_parse_or_null(s("type_params").as_deref());
    let filter = json_parse_or_null(s("metric_filter").as_deref());
    let meta = json_parse_or_null(s("meta").as_deref());

    Some(MetricDetail {
        base: NodeBase {
            unique_id: s("unique_id").unwrap_or_default(),
            name: s("name").unwrap_or_default(),
            resource_type: "metric".to_owned(),
            package_name: s("package_name"),
            description: s("description"),
            original_file_path: s("original_file_path"),
        },
        file_path: s("file_path"),
        fqn: extract_str_list(batch, "fqn"),
        tags: extract_str_list(batch, "tags"),
        label: s("label"),
        metric_type: s("metric_type"),
        type_params,
        filter,
        time_granularity: s("time_granularity"),
        semantic_model_name: s("semantic_model_name"),
        input_metric_names: extract_str_list(batch, "input_metric_names"),
        group_name: s("group_name"),
        meta,
        depends_on: vec![],
        referenced_by: vec![],
        created_at: f("created_at"),
    })
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `GET /api/v1/metrics/:id` — full metric detail.
///
/// `depends_on` and `referenced_by` are unbounded.
pub async fn get_metric(
    State(state): State<SharedState>,
    Path(unique_id): Path<String>,
) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }
    let id = escape_str(&unique_id);

    let row_sql = METRIC_DETAIL_ROW_SQL.replace("{id}", &id);
    let upstream_sql = format!(
        "SELECT parent_unique_id AS unique_id, edge_type \
         FROM dbt.edges WHERE child_unique_id = '{id}' \
         ORDER BY parent_unique_id"
    );
    let downstream_sql = format!(
        "SELECT child_unique_id AS unique_id, edge_type \
         FROM dbt.edges WHERE parent_unique_id = '{id}' \
         ORDER BY child_unique_id"
    );

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let row_batches = backend.query_arrow(&row_sql).map_err(|e| e.to_string())?;
        let upstream_batches = backend
            .query_arrow(&upstream_sql)
            .map_err(|e| e.to_string())?;
        let downstream_batches = backend
            .query_arrow(&downstream_sql)
            .map_err(|e| e.to_string())?;
        Ok((row_batches, upstream_batches, downstream_batches))
    })
    .await;

    let (row_batches, upstream_batches, downstream_batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let Some(mut detail) = extract_metric_detail(&row_batches) else {
        return not_found(format!("metric {unique_id} not found"));
    };

    detail.depends_on = extract_edge_refs(&upstream_batches);
    detail.referenced_by = extract_edge_refs(&downstream_batches);

    Json(detail).into_response()
}

// ---------------------------------------------------------------------------
// Response types: GET /api/v1/metrics and GET /api/v1/metrics/facets
// ---------------------------------------------------------------------------

/// One row in `GET /api/v1/metrics`.
#[derive(Serialize)]
pub struct MetricSummary {
    pub unique_id: String,
    pub name: String,
    pub package_name: Option<String>,
    pub group_name: Option<String>,
    pub metric_type: Option<String>,
    pub semantic_model_name: Option<String>,
    pub tags: Vec<String>,
    pub description: Option<String>,
    pub input_metric_names: Vec<String>,
    /// Always `false` in v0; included so the wire shape is stable when a
    /// per-row cap is added later.
    pub input_metric_names_truncated: bool,
    /// Epoch seconds; per-resource "Definition updated as of …" timestamp.
    pub created_at: Option<f64>,
}

/// Cursor-paginated response for `GET /api/v1/metrics`.
#[derive(Serialize)]
pub struct MetricListResponse {
    pub data: Vec<MetricSummary>,
    pub page_info: PageInfo,
}

/// Response body for `GET /api/v1/metrics/facets`.
///
/// No filter dimensions in v0 — the metrics list exposes no filter params.
/// Returns `{}` rather than named empty arrays so that adding a facet
/// dimension later is wire-additive.
#[derive(Serialize)]
pub struct MetricFacetsResponse {}

/// Query parameters for `GET /api/v1/metrics`.
#[derive(Debug, Default, Deserialize)]
pub struct MetricListParams {
    /// Sort: `name:asc` (default) or `name:desc`. Any other key returns 400.
    pub sort: Option<String>,
    pub first: Option<u32>,
    pub after: Option<String>,
}

// ---------------------------------------------------------------------------
// SQL: GET /api/v1/metrics
// ---------------------------------------------------------------------------

/// Build `(count_sql, rows_sql)` for `GET /api/v1/metrics`.
///
/// Only `name` is sortable; any other column returns `Err`.
/// The count query excludes the cursor predicate so `total_count` reflects
/// the full set.
pub(crate) fn build_metric_list_sql(
    params: &MetricListParams,
    first: u32,
    cursor: Option<&Cursor>,
) -> Result<(String, String), &'static str> {
    let (sort_col, dir) = parse_metric_sort(params.sort.as_deref())?;

    let filter_where = String::from("WHERE 1=1");

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
    let count_sql = format!("SELECT count(*) FROM dbt.metrics m {filter_where}");
    let peek = first + 1;
    let rows_sql = format!(
        "SELECT m.unique_id, m.name, m.package_name, m.group_name, \
                m.metric_type, m.semantic_model_name, m.tags, \
                m.description, m.input_metric_names, m.created_at \
         FROM dbt.metrics m {page_where} \
         ORDER BY m.{sort_col} {dir_sql} NULLS LAST, m.unique_id ASC \
         LIMIT {peek}"
    );

    Ok((count_sql, rows_sql))
}

/// Parse the `?sort=` parameter.
///
/// Returns `("name", Asc)` by default. Returns `Err` on unknown column or
/// unknown direction.
fn parse_metric_sort(sort: Option<&str>) -> Result<(&'static str, SortDir), &'static str> {
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
// Extraction helpers: GET /api/v1/metrics
// ---------------------------------------------------------------------------

/// Extract a `List<Utf8>` column value at a specific row index.
/// Returns `[]` for missing column, null cell, or non-list child.
fn list_col_at(batch: &RecordBatch, col_name: &'static str, row: usize) -> Vec<String> {
    use arrow_array::ListArray;
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

fn batches_to_metric_summary_rows(batches: &[RecordBatch]) -> Vec<MetricSummary> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let unique_id = str_col(batch, "unique_id");
        let name = str_col(batch, "name");
        let package_name = str_col(batch, "package_name");
        let group_name = str_col(batch, "group_name");
        let metric_type = str_col(batch, "metric_type");
        let semantic_model_name = str_col(batch, "semantic_model_name");
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
            rows.push(MetricSummary {
                unique_id: unique_id.value(i).to_owned(),
                name: name.value(i).to_owned(),
                package_name: opt_str(package_name, i),
                group_name: opt_str(group_name, i),
                metric_type: opt_str(metric_type, i),
                semantic_model_name: opt_str(semantic_model_name, i),
                tags: list_col_at(batch, "tags", i),
                description: opt_str(description, i),
                input_metric_names: list_col_at(batch, "input_metric_names", i),
                input_metric_names_truncated: false,
                created_at,
            });
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// Handlers: GET /api/v1/metrics and GET /api/v1/metrics/facets
// ---------------------------------------------------------------------------

/// `GET /api/v1/metrics` — cursor-paginated list of metric definitions.
///
/// Sort defaults to `name:asc`; only `name` is sortable. `?sort` with any
/// other column returns 400. No filter params in v0.
pub async fn list_metrics(
    State(state): State<SharedState>,
    Query(params): Query<MetricListParams>,
) -> Response {
    let first = clamp_first(params.first);

    let cursor = match params.after.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => match Cursor::decode(s) {
            Ok(c) => Some(c),
            Err(msg) => return bad_request(msg),
        },
        None => None,
    };

    let (count_sql, rows_sql) = match build_metric_list_sql(&params, first, cursor.as_ref()) {
        Ok(pair) => pair,
        Err(msg) => return bad_request(msg),
    };

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let total = backend
            .query_scalar(&count_sql)
            .ok_or_else(|| "count query returned no rows".to_string())?
            .parse::<u64>()
            .map_err(|e| format!("could not parse metric count: {e}"))?;
        let batches = backend.query_arrow(&rows_sql).map_err(|e| e.to_string())?;
        Ok((total, batches))
    })
    .await;

    let (total_count, batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let mut rows = batches_to_metric_summary_rows(&batches);

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

    Json(MetricListResponse {
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

/// `GET /api/v1/metrics/facets` — filter facet values for the metrics list.
///
/// No filter dimensions in v0: returns `{}`. When a future revision adds
/// filter params to the list endpoint, the corresponding facet keys will be
/// added here at the same time.
pub async fn list_metric_facets() -> Response {
    Json(MetricFacetsResponse {}).into_response()
}

#[cfg(test)]
#[path = "metrics_tests.rs"]
mod tests;
