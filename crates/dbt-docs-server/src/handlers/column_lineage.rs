//! Column-level lineage: `GET /api/v1/nodes/:unique_id/column-lineage`.
//!
//! Routes through the gated [`column_lineage`](crate::providers::ColumnLineageProvider)
//! provider. The SA distribution ships
//! [`UnavailableColumnLineage`](crate::providers::UnavailableColumnLineage),
//! which always returns [`LineageError::NotAvailable`] — surfaces as a
//! `412 Precondition Failed` with structured upgrade copy. The proprietary
//! distribution overrides this provider with a real impl backed by
//! `dbt.column_lineage` parquet.
//!
//! Single-hop only: returns every direct edge that touches any column of
//! the requested node. Multi-hop traversal at the *node* level isn't
//! exposed because the intermediate set explodes exponentially with column
//! fan-out and doesn't have a useful UI shape; per-column deep-dive (BFS
//! with a visited-set guard, like `dbt-index::core::impact`) is a future
//! addition that will take a separate `(node, column)` request shape.
//!
//! Response (200 OK) shape:
//! ```json
//! {
//!   "root": "model.foo.bar",
//!   "edges": [
//!     { "from_node": "...", "from_column": "...",
//!       "to_node": "...", "to_column": "...", "kind": "passthrough" }
//!   ]
//! }
//! ```
//!
//! Response (412 Precondition Failed) shape:
//! ```json
//! {
//!   "code": "column_lineage_unavailable",
//!   "message": "...",
//!   "upgrade_path": "..."
//! }
//! ```

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::handlers::json::{bad_request, internal_error};
use crate::providers::{ColumnLineageArgs, LineageError};
use crate::state::SharedState;

pub async fn get_column_lineage(
    State(state): State<SharedState>,
    Path(unique_id): Path<String>,
) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }

    let provider = state.providers.column_lineage.clone();
    let args = ColumnLineageArgs {
        unique_id: unique_id.clone(),
        column: None,
        upstream: false,
        downstream: false,
    };
    let result = tokio::task::spawn_blocking(move || provider.run(args)).await;

    match result {
        Ok(Ok(edges)) => Json(serde_json::json!({
            "root": unique_id,
            "edges": edges,
        }))
        .into_response(),
        Ok(Err(LineageError::NotAvailable)) => unavailable_response(),
        Ok(Err(LineageError::NodeNotFound(id))) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "node_not_found",
                "message": format!("node {id} not found"),
            })),
        )
            .into_response(),
        Ok(Err(LineageError::ColumnNotFound {
            node,
            column,
            available,
        })) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "column_not_found",
                "message": format!("column '{column}' not found in lineage for node '{node}'"),
                "available_columns": available,
            })),
        )
            .into_response(),
        Ok(Err(LineageError::Backend(msg))) => internal_error(msg),
        Err(err) => internal_error(err.to_string()),
    }
}

/// `412 Precondition Failed` with structured upgrade copy. The UI uses
/// the `code` to decide how to surface the upsell.
fn unavailable_response() -> Response {
    (
        StatusCode::PRECONDITION_FAILED,
        Json(serde_json::json!({
            "code": "column_lineage_unavailable",
            "message": "Column-level lineage is not available in this distribution + artifact set.",
            "upgrade_path": "Run `dbt --use-index <run|build|compile>` with static analysis enabled, in a distribution that ships the column-lineage provider.",
        })),
    )
        .into_response()
}
