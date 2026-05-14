use std::fmt::Write as _;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::handlers::json::{
    bad_request, batches_as_value_array, first_row_as_object, internal_error, not_found,
    wrapped_list_response,
};
use crate::handlers::sql::{escape_str, is_safe_ident};
use crate::state::SharedState;

/// Default page size when `limit` is not specified. Matches the previous
/// hard cap so existing callers see no behavior change.
const DEFAULT_LIMIT: u32 = 1000;
/// Hard ceiling on a single page to keep response payloads bounded even
/// for misbehaving clients.
const HARD_MAX_LIMIT: u32 = 5000;

const NODE_LIST_COLUMNS: &str = "unique_id, name, resource_type, package_name, materialized, \
                                 description, database_name, schema_name, original_file_path";

#[derive(Debug, Deserialize)]
pub struct NodeListParams {
    #[serde(rename = "type")]
    pub resource_type: Option<String>,
    pub package: Option<String>,
    pub q: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

/// `GET /api/v1/nodes?type=&package=&q=&limit=&offset=` — paginated,
/// filterable list of nodes.
///
/// Response shape:
/// ```json
/// {
///   "nodes": [...],
///   "total": 4818,
///   "offset": 0,
///   "limit": 1000
/// }
/// ```
///
/// Caller computes `has_more` as `offset + nodes.length < total`. `total`
/// reflects the count *after* filters but before pagination.
pub async fn list_nodes(
    State(state): State<SharedState>,
    Query(params): Query<NodeListParams>,
) -> Response {
    let limit = params
        .limit
        .unwrap_or(DEFAULT_LIMIT)
        .clamp(1, HARD_MAX_LIMIT);
    let offset = params.offset.unwrap_or(0);

    let mut where_clause = String::from("WHERE 1=1");
    if let Some(rt) = params.resource_type.as_deref() {
        if !is_safe_ident(rt) {
            return bad_request("invalid type filter");
        }
        let _ = write!(where_clause, " AND resource_type = '{rt}'");
    }
    if let Some(pkg) = params.package.as_deref() {
        if !is_safe_ident(pkg) {
            return bad_request("invalid package filter");
        }
        let _ = write!(where_clause, " AND package_name = '{pkg}'");
    }
    if let Some(q) = params.q.as_deref().filter(|s| !s.is_empty()) {
        let escaped = escape_str(q);
        let _ = write!(
            where_clause,
            " AND (LOWER(name) LIKE LOWER('%{escaped}%') \
              OR LOWER(unique_id) LIKE LOWER('%{escaped}%'))"
        );
    }

    let count_sql = format!("SELECT count(*) FROM dbt.nodes {where_clause}");
    let rows_sql = format!(
        "SELECT {NODE_LIST_COLUMNS} FROM dbt.nodes {where_clause} \
         ORDER BY resource_type, name LIMIT {limit} OFFSET {offset}"
    );

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let total = backend
            .query_scalar(&count_sql)
            .ok_or_else(|| "count query returned no rows".to_string())?
            .parse::<u64>()
            .map_err(|e| format!("could not parse node count: {e}"))?;
        let batches = backend.query_arrow(&rows_sql).map_err(|e| e.to_string())?;
        Ok((total, batches))
    })
    .await;

    let (total, batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    wrapped_list_response(
        "nodes",
        &batches,
        &[
            ("total", &total.to_string()),
            ("offset", &offset.to_string()),
            ("limit", &limit.to_string()),
        ],
    )
}

/// `GET /api/v1/nodes/:unique_id` — full node detail with columns + edges.
pub async fn get_node(State(state): State<SharedState>, Path(unique_id): Path<String>) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }
    let escaped = escape_str(&unique_id);

    let node_sql = format!(
        "SELECT n.unique_id, n.name, n.resource_type, n.package_name, \
                n.materialized, n.description, n.database_name, n.schema_name, \
                n.relation_name, n.identifier, n.original_file_path, \
                n.access_level, n.group_name, n.raw_code \
         FROM dbt.nodes n WHERE n.unique_id = '{escaped}' LIMIT 1"
    );
    let columns_sql = format!(
        "SELECT column_name AS name, column_index AS index, \
                data_type, declared_type, inferred_type, catalog_type, \
                description, label, granularity \
         FROM dbt.node_columns WHERE unique_id = '{escaped}' \
         ORDER BY column_index NULLS LAST, column_name"
    );
    let upstream_sql = format!(
        "SELECT parent_unique_id AS unique_id, edge_type \
         FROM dbt.edges WHERE child_unique_id = '{escaped}' \
         ORDER BY parent_unique_id"
    );
    let downstream_sql = format!(
        "SELECT child_unique_id AS unique_id, edge_type \
         FROM dbt.edges WHERE parent_unique_id = '{escaped}' \
         ORDER BY child_unique_id"
    );

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let node_batches = backend.query_arrow(&node_sql).map_err(|e| e.to_string())?;
        let column_batches = backend
            .query_arrow(&columns_sql)
            .map_err(|e| e.to_string())?;
        let upstream_batches = backend
            .query_arrow(&upstream_sql)
            .map_err(|e| e.to_string())?;
        let downstream_batches = backend
            .query_arrow(&downstream_sql)
            .map_err(|e| e.to_string())?;
        Ok((
            node_batches,
            column_batches,
            upstream_batches,
            downstream_batches,
        ))
    })
    .await;

    let (node_batches, column_batches, upstream_batches, downstream_batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let node = match first_row_as_object(&node_batches) {
        Ok(Some(v)) => v,
        Ok(None) => return not_found(format!("node {unique_id} not found")),
        Err(err) => return internal_error(err.to_string()),
    };
    let columns = match batches_as_value_array(&column_batches) {
        Ok(v) => v,
        Err(err) => return internal_error(err.to_string()),
    };
    let depends_on = match batches_as_value_array(&upstream_batches) {
        Ok(v) => v,
        Err(err) => return internal_error(err.to_string()),
    };
    let referenced_by = match batches_as_value_array(&downstream_batches) {
        Ok(v) => v,
        Err(err) => return internal_error(err.to_string()),
    };

    let mut node = node;
    if let Some(obj) = node.as_object_mut() {
        obj.insert("columns".into(), columns);
        obj.insert("depends_on".into(), depends_on);
        obj.insert("referenced_by".into(), referenced_by);
    }
    Json(node).into_response()
}
