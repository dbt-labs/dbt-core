//! `GET /api/v1/exposures/:id` — typed exposure detail.
//!
//! Exposures are definition-only leaf consumers: no `execution_info`, no
//! `catalog`, no `columns`, no SQL body. The contract has `depends_on` but
//! no `referenced_by` (nothing refs an exposure).
//!
//! `meta` is a JSON-string parquet column; it's deserialized handler-side
//! via [`crate::handlers::json::json_parse_or_null`] so the response carries
//! a real JSON object, not an escaped string.
//!
//! `depends_on` is synthesized directly from `dbt.exposures.depends_on_nodes`
//! (a `List<Utf8>` of upstream unique_ids); `edge_type` is derived from the
//! `unique_id` prefix (`model.` → `"model"`, `source.` → `"source"`, etc.).
//!
//! Data source:
//! - `dbt.exposures` — single row per exposure (NOT `dbt.nodes`).

use arrow_array::{Array, Float64Array, RecordBatch, StringArray};
use axum::Json;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::handlers::json::{bad_request, internal_error, json_parse_or_null, not_found};
use crate::handlers::node_base::{EdgeRef, NodeBase, extract_str_list, opt_str};
use crate::handlers::sql::escape_str;
use crate::state::SharedState;

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Response body for `GET /api/v1/exposures/:id`.
#[derive(Serialize)]
pub struct ExposureDetail {
    #[serde(flatten)]
    pub base: NodeBase,
    /// Project-relative path of the `.yml` containing the exposure block.
    pub file_path: Option<String>,
    pub tags: Vec<String>,
    pub fqn: Vec<String>,
    pub label: Option<String>,
    /// One of `"dashboard"` · `"notebook"` · `"analysis"` · `"ml"` ·
    /// `"application"`. Plain `Utf8` in parquet — not enum-validated.
    pub exposure_type: Option<String>,
    pub maturity: Option<String>,
    pub url: Option<String>,
    pub owner_name: Option<String>,
    pub owner_email: Option<String>,
    /// Parsed JSON object, or `null` when absent / unparseable.
    pub meta: serde_json::Value,
    /// 1-hop upstream refs (models, sources, metrics, seeds, …). `edge_type`
    /// is derived from the `unique_id` prefix.
    pub depends_on: Vec<EdgeRef>,
    /// Epoch seconds (float) — definition-update timestamp.
    pub created_at: Option<f64>,
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------
//
// Single query against `dbt.exposures`. `meta` is a JSON-string column;
// `depends_on_nodes` is a `List<Utf8>` resolved to `EdgeRef[]` handler-side.

const EXPOSURE_DETAIL_SQL: &str = "\
SELECT unique_id, name, package_name, description, \
       original_file_path, file_path, \
       label, exposure_type, maturity, url, \
       owner_name, owner_email, meta, \
       tags, fqn, depends_on_nodes, created_at \
FROM dbt.exposures \
WHERE unique_id = '{id}' \
LIMIT 1";

// ---------------------------------------------------------------------------
// Extractors
// ---------------------------------------------------------------------------

/// Derive an `EdgeRef`'s `edge_type` from a `unique_id`'s prefix:
/// `model.x.y` → `"model"`, `source.x.y.z` → `"source"`, etc. The empty
/// string falls through if the id has no `.` — defensive only; production
/// ids always have a prefix.
fn edge_type_from_prefix(unique_id: &str) -> String {
    unique_id
        .split_once('.')
        .map(|(prefix, _)| prefix.to_owned())
        .unwrap_or_default()
}

fn extract_exposure_detail(batches: &[RecordBatch]) -> Option<ExposureDetail> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;

    let s = |name: &'static str| -> Option<String> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<StringArray>()?;
        opt_str(col, 0)
    };

    let meta_raw = s("meta");
    let meta = json_parse_or_null(meta_raw.as_deref());

    let depends_on = extract_str_list(batch, "depends_on_nodes")
        .into_iter()
        .map(|unique_id| EdgeRef {
            edge_type: edge_type_from_prefix(&unique_id),
            unique_id,
        })
        .collect();

    let created_at = batch
        .column_by_name("created_at")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .and_then(|c| if c.is_null(0) { None } else { Some(c.value(0)) });

    Some(ExposureDetail {
        base: NodeBase {
            unique_id: s("unique_id").unwrap_or_default(),
            name: s("name").unwrap_or_default(),
            // `dbt.exposures` has no `resource_type` column — hardcoded.
            resource_type: "exposure".to_owned(),
            package_name: s("package_name"),
            description: s("description"),
            original_file_path: s("original_file_path"),
        },
        file_path: s("file_path"),
        tags: extract_str_list(batch, "tags"),
        fqn: extract_str_list(batch, "fqn"),
        label: s("label"),
        exposure_type: s("exposure_type"),
        maturity: s("maturity"),
        url: s("url"),
        owner_name: s("owner_name"),
        owner_email: s("owner_email"),
        meta,
        depends_on,
        created_at,
    })
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `GET /api/v1/exposures/:id` — full exposure detail.
///
/// Exposures are leaf consumers — no `referenced_by` field (omitted, not
/// `[]`). No `execution_info`, `catalog`, `columns`, or SQL body.
pub async fn get_exposure(
    State(state): State<SharedState>,
    Path(unique_id): Path<String>,
) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }
    let id = escape_str(&unique_id);
    let sql = EXPOSURE_DETAIL_SQL.replace("{id}", &id);

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

    let Some(detail) = extract_exposure_detail(&batches) else {
        return not_found(format!("exposure {unique_id} not found"));
    };

    Json(detail).into_response()
}

#[cfg(test)]
#[path = "exposures_tests.rs"]
mod tests;
