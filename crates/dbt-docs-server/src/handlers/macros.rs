//! `GET /api/v1/macros/:id` — typed macro detail.
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

#[cfg(test)]
#[path = "macros_tests.rs"]
mod tests;
