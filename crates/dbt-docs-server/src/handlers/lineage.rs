//! Model-level lineage: `GET /api/v1/nodes/:unique_id/lineage`.
//!
//! Recursive CTE on `dbt.edges` walks both upstream and downstream from
//! the root, bounded by `max_depth` (default 3, capped at 3). The response includes
//! every node touched (joined with `dbt.nodes` for metadata) and every
//! edge traversed.
//!
//! Response shape:
//! ```json
//! {
//!   "root": "model.foo.bar",
//!   "max_depth": 3,
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

use arrow_array::{Array, RecordBatch, StringArray};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::handlers::json::{bad_request, batches_as_value_array, internal_error, not_found};
use crate::handlers::node_base::extract_str_list;
use crate::handlers::sql::escape_str;
use crate::state::SharedState;

/// Default and maximum traversal depth. Capped to keep responses bounded
/// even if a misbehaving client requests something huge.
const DEFAULT_MAX_DEPTH: u32 = 3;
const HARD_MAX_DEPTH: u32 = 3;

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
    // Saved queries are absent from `dbt.edges` / `dbt.nodes` in current
    // parquet output; their upstream deps live on `dbt.saved_queries`. Probe
    // that table first so the recursive CTE path is reserved for nodes that
    // can actually populate it.
    let saved_query_sql = build_saved_query_probe_sql(&escaped);

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let sq_batches = backend
            .query_arrow(&saved_query_sql)
            .map_err(|e| e.to_string())?;
        if let Some(root) = extract_saved_query_root(&sq_batches) {
            return Ok(LineageQueryOutput::SavedQuery(root));
        }
        let nodes = backend.query_arrow(&nodes_sql).map_err(|e| e.to_string())?;
        let edges = backend.query_arrow(&edges_sql).map_err(|e| e.to_string())?;
        Ok(LineageQueryOutput::Node { nodes, edges })
    })
    .await;

    let output = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    match output {
        LineageQueryOutput::SavedQuery(root) => saved_query_response(&unique_id, max_depth, root),
        LineageQueryOutput::Node { nodes, edges } => {
            let nodes = match batches_as_value_array(&nodes) {
                Ok(v) => v,
                Err(err) => return internal_error(err.to_string()),
            };
            // The `nodes` query always returns the root row when the node exists
            // in dbt.nodes, so an empty array means "node not found".
            if nodes.as_array().is_some_and(|a| a.is_empty()) {
                return not_found(format!("node {unique_id} not found"));
            }
            let edges = match batches_as_value_array(&edges) {
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
    }
}

/// Internal: which table the root was found in. Drives whether we serve the
/// recursive-CTE response or the synthesized saved-query one.
enum LineageQueryOutput {
    SavedQuery(SavedQueryRoot),
    Node {
        nodes: Vec<RecordBatch>,
        edges: Vec<RecordBatch>,
    },
}

struct SavedQueryRoot {
    name: String,
    depends_on_nodes: Vec<String>,
}

fn build_saved_query_probe_sql(root_id: &str) -> String {
    format!(
        "SELECT unique_id, name, depends_on_nodes \
         FROM dbt.saved_queries WHERE unique_id = '{root_id}' LIMIT 1"
    )
}

fn extract_saved_query_root(batches: &[RecordBatch]) -> Option<SavedQueryRoot> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;
    let name = batch
        .column_by_name("name")?
        .as_any()
        .downcast_ref::<StringArray>()
        .and_then(|c| {
            if c.is_null(0) {
                None
            } else {
                Some(c.value(0).to_owned())
            }
        })?;
    Some(SavedQueryRoot {
        name,
        depends_on_nodes: extract_str_list(batch, "depends_on_nodes"),
    })
}

/// Build the lineage response for a saved_query root. Saved queries sit
/// outside `dbt.edges`, so we synthesize the root row + one upstream row per
/// `depends_on_nodes` entry, with edges pointing each upstream → root.
/// `resource_type` for each upstream is inferred from the dotted prefix
/// (`metric.x.y` → `metric`); bare ids fall back to `"node"`.
///
/// Doesn't walk further upstream from those nodes in this revision — the
/// recursive CTE would need a multi-root variant; saved queries today are
/// 1-hop deep in practice.
fn saved_query_response(unique_id: &str, max_depth: u32, root: SavedQueryRoot) -> Response {
    let mut nodes = Vec::with_capacity(root.depends_on_nodes.len() + 1);
    nodes.push(serde_json::json!({
        "unique_id": unique_id,
        "name": root.name,
        "resource_type": "saved_query",
        "materialized": serde_json::Value::Null,
        "depth": 0,
    }));
    let mut edges = Vec::with_capacity(root.depends_on_nodes.len());
    for upstream_id in &root.depends_on_nodes {
        let resource_type = match upstream_id.split_once('.') {
            Some((prefix, _)) => prefix.to_owned(),
            None => "node".to_owned(),
        };
        nodes.push(serde_json::json!({
            "unique_id": upstream_id,
            "name": upstream_id.rsplit('.').next().unwrap_or(upstream_id),
            "resource_type": resource_type,
            "materialized": serde_json::Value::Null,
            "depth": -1,
        }));
        edges.push(serde_json::json!({
            "from_id": upstream_id,
            "to_id": unique_id,
            "edge_type": "saved_query",
        }));
    }
    Json(serde_json::json!({
        "root": unique_id,
        "max_depth": max_depth,
        "nodes": nodes,
        "edges": edges,
    }))
    .into_response()
}

/// Build the recursive-CTE query that returns every node within
/// `max_depth` hops of `<root_id>`, joined with metadata from the union of
/// per-resource tables (`dbt.nodes` plus `dbt.metrics`, `dbt.semantic_models`,
/// `dbt.exposures` — none of which live in `dbt.nodes` but all of which can
/// appear in `dbt.edges`).
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
         ), \
         metadata AS ( \
             SELECT unique_id, name, resource_type, materialized FROM dbt.nodes \
             UNION ALL \
             SELECT unique_id, name, 'metric' AS resource_type, NULL AS materialized FROM dbt.metrics \
             UNION ALL \
             SELECT unique_id, name, 'semantic_model' AS resource_type, NULL AS materialized FROM dbt.semantic_models \
             UNION ALL \
             SELECT unique_id, name, 'exposure' AS resource_type, NULL AS materialized FROM dbt.exposures \
         ) \
         SELECT m.unique_id, m.name, m.resource_type, m.materialized, \
                MIN(a.depth) AS depth \
         FROM all_ids a JOIN metadata m ON m.unique_id = a.unique_id \
         GROUP BY m.unique_id, m.name, m.resource_type, m.materialized \
         ORDER BY depth, m.resource_type, m.name"
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

#[cfg(test)]
#[path = "lineage_tests.rs"]
mod tests;
