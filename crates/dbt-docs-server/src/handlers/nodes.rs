use std::fmt::Write as _;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
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

/// `dbt.nodes` holds: model, seed, test, unit_test, snapshot, analysis,
/// source, operation, sql_operation, function. The remaining resource types
/// live in their own parquet-backed views and must be counted separately.
///
/// `unit_test` is folded into `test` to match the UI's mapping (metadata-ui
/// maps unit_test → test). The outer GROUP BY SUM handles this correctly even
/// if `unit_test` rows appear in both `dbt.nodes` and `dbt.unit_tests`.
const NODE_COUNTS_SQL: &str = "\
WITH raw AS (\
  SELECT resource_type, COUNT(*) AS count FROM dbt.nodes GROUP BY resource_type \
  UNION ALL \
  SELECT 'exposure'       AS resource_type, COUNT(*) AS count FROM dbt.exposures \
  UNION ALL \
  SELECT 'group'          AS resource_type, COUNT(*) AS count FROM dbt.groups \
  UNION ALL \
  SELECT 'macro'          AS resource_type, COUNT(*) AS count FROM dbt.macros \
  UNION ALL \
  SELECT 'metric'         AS resource_type, COUNT(*) AS count FROM dbt.metrics \
  UNION ALL \
  SELECT 'saved_query'    AS resource_type, COUNT(*) AS count FROM dbt.saved_queries \
  UNION ALL \
  SELECT 'semantic_model' AS resource_type, COUNT(*) AS count FROM dbt.semantic_models \
) \
SELECT resource_type, CAST(SUM(count) AS BIGINT) AS count \
FROM raw \
GROUP BY resource_type \
ORDER BY resource_type";

/// `GET /api/v1/nodes/counts` — count of nodes per resource type.
///
/// Covers all resource types: `dbt.nodes` (model, seed, test, unit_test,
/// snapshot, analysis, source, function) plus the separate views for
/// exposure, group, macro, metric, saved_query, and semantic_model.
/// `unit_test` rows are summed under `test` by the caller ([`batches_to_counts`]).
///
/// Response shape (absent keys mean zero):
/// ```json
/// { "exposure": 12, "group": 5, "macro": 89, "model": 4521, "test": 9834 }
/// ```
pub async fn list_node_counts(State(state): State<SharedState>) -> Response {
    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || backend.query_arrow(NODE_COUNTS_SQL)).await;

    let batches = match result {
        Ok(Ok(b)) => b,
        Ok(Err(err)) => return internal_error(err.to_string()),
        Err(err) => return internal_error(err.to_string()),
    };

    Json(batches_to_counts(&batches)).into_response()
}

fn str_col<'a>(batch: &'a RecordBatch, name: &'static str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' missing from batch"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("column '{name}' is not a StringArray"))
}

fn i64_col<'a>(batch: &'a RecordBatch, name: &'static str) -> &'a Int64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' missing from batch"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("column '{name}' is not an Int64Array"))
}

/// Response body for `GET /api/v1/nodes/counts`.
///
/// Every field is always present in the JSON output — absent resource types
/// serialize as `0`. `unit_test` is folded into `test` to match the UI
/// mapping (metadata-ui: `unit_test → test`).
#[derive(Default)]
pub struct NodeCountsResponse {
    pub analysis: i64,
    pub exposure: i64,
    pub function: i64,
    pub group: i64,
    pub macro_: i64,
    pub metric: i64,
    pub model: i64,
    pub saved_query: i64,
    pub seed: i64,
    pub semantic_model: i64,
    pub snapshot: i64,
    pub source: i64,
    /// Includes `unit_test` rows from `dbt.nodes` (mapped by the server).
    pub test: i64,
}

impl serde::Serialize for NodeCountsResponse {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut m = s.serialize_map(Some(13))?;
        m.serialize_entry("analysis", &self.analysis)?;
        m.serialize_entry("exposure", &self.exposure)?;
        m.serialize_entry("function", &self.function)?;
        m.serialize_entry("group", &self.group)?;
        m.serialize_entry("macro", &self.macro_)?;
        m.serialize_entry("metric", &self.metric)?;
        m.serialize_entry("model", &self.model)?;
        m.serialize_entry("saved_query", &self.saved_query)?;
        m.serialize_entry("seed", &self.seed)?;
        m.serialize_entry("semantic_model", &self.semantic_model)?;
        m.serialize_entry("snapshot", &self.snapshot)?;
        m.serialize_entry("source", &self.source)?;
        m.serialize_entry("test", &self.test)?;
        m.end()
    }
}

fn batches_to_counts(batches: &[RecordBatch]) -> NodeCountsResponse {
    let mut resp = NodeCountsResponse::default();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let resource_type = str_col(batch, "resource_type");
        let count = i64_col(batch, "count");
        for i in 0..batch.num_rows() {
            if resource_type.is_null(i) {
                continue;
            }
            let n = count.value(i);
            match resource_type.value(i) {
                "analysis" => resp.analysis += n,
                "exposure" => resp.exposure += n,
                "function" => resp.function += n,
                "group" => resp.group += n,
                "macro" => resp.macro_ += n,
                "metric" => resp.metric += n,
                "model" => resp.model += n,
                "saved_query" => resp.saved_query += n,
                "seed" => resp.seed += n,
                "semantic_model" => resp.semantic_model += n,
                "snapshot" => resp.snapshot += n,
                "source" => resp.source += n,
                "test" | "unit_test" => resp.test += n,
                _ => {}
            }
        }
    }
    resp
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use axum::extract::State;

    use crate::providers::{BackendError, Providers};
    use crate::state::AppState;

    use super::{batches_to_counts, list_node_counts};

    struct MockBackend {
        batches: Vec<RecordBatch>,
        fail: bool,
    }

    impl MockBackend {
        fn with_batches(batches: Vec<RecordBatch>) -> Self {
            Self {
                batches,
                fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                batches: vec![],
                fail: true,
            }
        }
    }

    impl dbt_index_core::Backend for MockBackend {
        fn query_arrow(&self, _sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
            if self.fail {
                return Err(BackendError::Query("mock failure".into()));
            }
            Ok(self.batches.clone())
        }
    }

    fn make_state(backend: MockBackend) -> Arc<AppState> {
        let providers = Providers {
            backend: Arc::new(backend),
            ..Providers::default()
        };
        Arc::new(AppState::new("target/index".into(), providers))
    }

    fn counts_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("resource_type", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
        ]))
    }

    fn counts_batch(entries: &[(&str, i64)]) -> RecordBatch {
        let types: Vec<&str> = entries.iter().map(|(t, _)| *t).collect();
        let counts: Vec<i64> = entries.iter().map(|(_, c)| *c).collect();
        RecordBatch::try_new(
            counts_schema(),
            vec![
                Arc::new(StringArray::from(types)),
                Arc::new(Int64Array::from(counts)),
            ],
        )
        .expect("valid batch")
    }

    async fn response_body(response: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        serde_json::from_slice(&bytes).expect("valid json")
    }

    #[tokio::test]
    async fn returns_counts_for_known_resource_types() {
        let batch = counts_batch(&[("model", 142), ("source", 23), ("test", 456)]);
        let state = make_state(MockBackend::with_batches(vec![batch]));
        let response = list_node_counts(State(state)).await;
        assert_eq!(response.status(), 200);

        let body = response_body(response).await;
        assert_eq!(body["model"], 142);
        assert_eq!(body["source"], 23);
        assert_eq!(body["test"], 456);
        assert_eq!(body["macro"], 0);
        assert_eq!(body["exposure"], 0);
    }

    #[tokio::test]
    async fn empty_index_returns_all_zeros() {
        let state = make_state(MockBackend::with_batches(vec![]));
        let body = response_body(list_node_counts(State(state)).await).await;
        for key in [
            "model",
            "source",
            "test",
            "exposure",
            "group",
            "macro",
            "metric",
            "seed",
            "semantic_model",
            "snapshot",
            "saved_query",
            "function",
            "analysis",
        ] {
            assert_eq!(body[key], 0, "field '{key}' must be 0, not absent");
        }
    }

    #[tokio::test]
    async fn backend_error_returns_500() {
        let state = make_state(MockBackend::failing());
        let response = list_node_counts(State(state)).await;
        assert_eq!(response.status(), 500);
    }

    #[test]
    fn unit_test_rows_fold_into_test() {
        let batch = counts_batch(&[("test", 100), ("unit_test", 42)]);
        let counts = batches_to_counts(&[batch]);
        assert_eq!(counts.test, 142, "unit_test must be summed into test");
    }

    #[test]
    fn null_resource_type_rows_are_skipped() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("resource_type", DataType::Utf8, true),
            Field::new("count", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("model"), None])),
                Arc::new(Int64Array::from(vec![10i64, 5])),
            ],
        )
        .expect("valid batch");

        let counts = batches_to_counts(&[batch]);
        assert_eq!(counts.model, 10);
        assert_eq!(counts.source, 0);
    }
}
