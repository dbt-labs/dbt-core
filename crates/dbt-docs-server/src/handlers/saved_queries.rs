//! `GET /api/v1/saved_queries/:id` — typed saved query detail.
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

#[cfg(test)]
#[path = "saved_queries_tests.rs"]
mod tests;
