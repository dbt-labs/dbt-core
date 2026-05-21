//! `GET /api/v1/metrics/:id` — typed metric detail.
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

use arrow_array::{Array, Float64Array, RecordBatch, StringArray};
use axum::Json;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::handlers::json::{bad_request, internal_error, json_parse_or_null, not_found};
use crate::handlers::node_base::{EdgeRef, NodeBase, extract_edge_refs, extract_str_list, opt_str};
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

#[cfg(test)]
#[path = "metrics_tests.rs"]
mod tests;
