//! Model-level lineage: `GET /api/v1/nodes/:unique_id/lineage`.
//!
//! Recursive CTE on `dbt.edges` walks both upstream and downstream from
//! the root, bounded by `max_depth` (default 5). The response includes
//! every node touched (joined with `dbt.nodes` for metadata) and every
//! edge traversed.
//!
//! Response shape:
//! ```json
//! {
//!   "root": "model.foo.bar",
//!   "max_depth": 5,
//!   "nodes": [
//!     { "unique_id": "model.foo.bar", "name": "bar",
//!       "resource_type": "model", "materialized": "view", "depth": 0 },
//!     { "unique_id": "source.foo.x", "name": "x",
//!       "resource_type": "source", "materialized": null, "depth": -1 }
//!   ],
//!   "edges": [
//!     { "from_id": "source.foo.x", "to_id": "model.foo.bar", "edge_type": "source" }
//!   ]
//! }
//! ```
//!
//! `depth` is signed: negative = upstream, 0 = root, positive = downstream.
//! Only model-level edges (from `dbt.edges`); column-level lineage lives
//! at `/column-lineage`.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::handlers::json::{bad_request, batches_as_value_array, internal_error, not_found};
use crate::handlers::sql::escape_str;
use crate::state::SharedState;

/// Default and maximum traversal depth. Capped to keep responses bounded
/// even if a misbehaving client requests something huge.
const DEFAULT_MAX_DEPTH: u32 = 5;
const HARD_MAX_DEPTH: u32 = 50;

#[derive(Debug, Deserialize)]
pub struct LineageParams {
    pub max_depth: Option<u32>,
}

pub async fn get_lineage(
    State(state): State<SharedState>,
    Path(unique_id): Path<String>,
    Query(params): Query<LineageParams>,
) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }
    let max_depth = params
        .max_depth
        .unwrap_or(DEFAULT_MAX_DEPTH)
        .clamp(1, HARD_MAX_DEPTH);
    let escaped = escape_str(&unique_id);

    let nodes_sql = build_nodes_sql(&escaped, max_depth);
    let edges_sql = build_edges_sql(&escaped, max_depth);

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let nodes = backend.query_arrow(&nodes_sql).map_err(|e| e.to_string())?;
        let edges = backend.query_arrow(&edges_sql).map_err(|e| e.to_string())?;
        Ok((nodes, edges))
    })
    .await;

    let (node_batches, edge_batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let nodes = match batches_as_value_array(&node_batches) {
        Ok(v) => v,
        Err(err) => return internal_error(err.to_string()),
    };
    // The `nodes` query always returns the root row when the node exists
    // in dbt.nodes, so an empty array means "node not found".
    if nodes.as_array().is_some_and(|a| a.is_empty()) {
        return not_found(format!("node {unique_id} not found"));
    }
    let edges = match batches_as_value_array(&edge_batches) {
        Ok(v) => v,
        Err(err) => return internal_error(err.to_string()),
    };

    Json(serde_json::json!({
        "root": unique_id,
        "max_depth": max_depth,
        "nodes": nodes,
        "edges": edges,
    }))
    .into_response()
}

/// Build the recursive-CTE query that returns every node within
/// `max_depth` hops of `<root_id>`, joined with metadata from `dbt.nodes`.
///
/// Output columns: `unique_id`, `name`, `resource_type`, `materialized`,
/// `depth` (signed: negative upstream, 0 root, positive downstream).
fn build_nodes_sql(root_id: &str, max_depth: u32) -> String {
    let max_neg = -(max_depth as i64);
    let max_pos = max_depth as i64;
    format!(
        "WITH RECURSIVE \
         upstream AS ( \
             SELECT parent_unique_id AS unique_id, -1 AS depth \
             FROM dbt.edges WHERE child_unique_id = '{root_id}' \
             UNION ALL \
             SELECT e.parent_unique_id, u.depth - 1 \
             FROM dbt.edges e JOIN upstream u \
                 ON e.child_unique_id = u.unique_id \
             WHERE u.depth > {max_neg} \
         ), \
         downstream AS ( \
             SELECT child_unique_id AS unique_id, 1 AS depth \
             FROM dbt.edges WHERE parent_unique_id = '{root_id}' \
             UNION ALL \
             SELECT e.child_unique_id, d.depth + 1 \
             FROM dbt.edges e JOIN downstream d \
                 ON e.parent_unique_id = d.unique_id \
             WHERE d.depth < {max_pos} \
         ), \
         all_ids AS ( \
             SELECT '{root_id}' AS unique_id, 0 AS depth \
             UNION ALL \
             SELECT unique_id, MIN(depth) FROM upstream GROUP BY unique_id \
             UNION ALL \
             SELECT unique_id, MAX(depth) FROM downstream GROUP BY unique_id \
         ) \
         SELECT n.unique_id, n.name, n.resource_type, n.materialized, \
                MIN(a.depth) AS depth \
         FROM all_ids a JOIN dbt.nodes n ON n.unique_id = a.unique_id \
         GROUP BY n.unique_id, n.name, n.resource_type, n.materialized \
         ORDER BY depth, n.resource_type, n.name"
    )
}

/// Edges within the lineage subgraph: any edge in `dbt.edges` whose both
/// endpoints fall within `max_depth` of `<root_id>`.
fn build_edges_sql(root_id: &str, max_depth: u32) -> String {
    let max_neg = -(max_depth as i64);
    let max_pos = max_depth as i64;
    format!(
        "WITH RECURSIVE \
         upstream AS ( \
             SELECT parent_unique_id AS unique_id, -1 AS depth \
             FROM dbt.edges WHERE child_unique_id = '{root_id}' \
             UNION ALL \
             SELECT e.parent_unique_id, u.depth - 1 \
             FROM dbt.edges e JOIN upstream u ON e.child_unique_id = u.unique_id \
             WHERE u.depth > {max_neg} \
         ), \
         downstream AS ( \
             SELECT child_unique_id AS unique_id, 1 AS depth \
             FROM dbt.edges WHERE parent_unique_id = '{root_id}' \
             UNION ALL \
             SELECT e.child_unique_id, d.depth + 1 \
             FROM dbt.edges e JOIN downstream d ON e.parent_unique_id = d.unique_id \
             WHERE d.depth < {max_pos} \
         ), \
         all_ids AS ( \
             SELECT '{root_id}' AS unique_id \
             UNION \
             SELECT unique_id FROM upstream \
             UNION \
             SELECT unique_id FROM downstream \
         ) \
         SELECT e.parent_unique_id AS from_id, \
                e.child_unique_id AS to_id, \
                e.edge_type \
         FROM dbt.edges e \
         WHERE e.parent_unique_id IN (SELECT unique_id FROM all_ids) \
           AND e.child_unique_id IN (SELECT unique_id FROM all_ids) \
         ORDER BY e.parent_unique_id, e.child_unique_id"
    )
}
