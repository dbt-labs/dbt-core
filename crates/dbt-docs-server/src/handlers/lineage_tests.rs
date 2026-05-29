//! Tests for `GET /api/v1/nodes/:unique_id/lineage`.
//!
//! Covers the two code paths in [`get_lineage`]:
//!   - Saved-query roots — probed via `dbt.saved_queries` and served from
//!     synthesized `depends_on_nodes` rows (saved queries aren't in
//!     `dbt.edges`).
//!   - Generic nodes — recursive CTE over `dbt.edges` joined with
//!     `dbt.nodes`; 404 when the join is empty.
//!
//! Schema anchoring (#10255): the `RecordBatch` fixtures below are hand-
//! rolled and not enforced against the production parquet schemas. A column
//! rename or type change in `dbt-index` will pass these tests while
//! silently breaking the handler.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Int32Array, ListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use axum::extract::{Path, Query, State};
use axum::response::Response;

use super::*;
use crate::providers::{Backend, BackendError, Providers};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Mock backend
// ---------------------------------------------------------------------------

/// Routes Arrow queries to fixture batches based on the table the SQL hits.
/// `dbt.saved_queries` is probed first by the handler; if it returns rows the
/// node/edge branches are never hit, so the `node_batches`/`edge_batches`
/// fields can be empty for the saved-query tests.
struct LineageMockBackend {
    saved_query_batches: Vec<RecordBatch>,
    node_batches: Vec<RecordBatch>,
    edge_batches: Vec<RecordBatch>,
}

impl Backend for LineageMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, _sql: &str) -> Option<String> {
        Some("0".to_owned())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        if sql.contains("dbt.saved_queries") {
            return Ok(self.saved_query_batches.clone());
        }
        if sql.contains("dbt.edges") && !sql.contains("dbt.nodes") {
            return Ok(self.edge_batches.clone());
        }
        // The nodes recursive CTE references both tables.
        if sql.contains("dbt.nodes") {
            return Ok(self.node_batches.clone());
        }
        Err(BackendError::Query(format!("unrouted query: {sql}")))
    }
}

fn make_state(backend: LineageMockBackend) -> Arc<AppState> {
    let providers = Providers {
        backend: Arc::new(backend),
        ..Providers::default()
    };
    Arc::new(AppState {
        index_dir: PathBuf::from("/tmp"),
        providers,
    })
}

// ---------------------------------------------------------------------------
// Batch builders
// ---------------------------------------------------------------------------

fn make_str_list(values: &[&str]) -> ListArray {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for v in values {
        builder.values().append_value(v);
    }
    builder.append(true);
    builder.finish()
}

/// Build a single-row batch shaped like the `dbt.saved_queries` probe in
/// [`build_saved_query_probe_sql`]: `(unique_id, name, depends_on_nodes)`.
fn saved_query_probe_batch(unique_id: &str, name: &str, depends_on_nodes: &[&str]) -> RecordBatch {
    let deps = make_str_list(depends_on_nodes);
    let schema = Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("depends_on_nodes", deps.data_type().clone(), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![Some(name)])),
            Arc::new(deps),
        ],
    )
    .expect("valid saved_query probe batch")
}

/// Build a batch shaped like the lineage `nodes` SQL output:
/// `(unique_id, name, resource_type, materialized, depth)`.
fn node_lineage_batch(rows: &[(&str, &str, &str, Option<&str>, i32)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("resource_type", DataType::Utf8, true),
        Field::new("materialized", DataType::Utf8, true),
        Field::new("depth", DataType::Int32, false),
    ]));
    let uids: Vec<&str> = rows.iter().map(|(u, ..)| *u).collect();
    let names: Vec<Option<&str>> = rows.iter().map(|(_, n, ..)| Some(*n)).collect();
    let types: Vec<Option<&str>> = rows.iter().map(|(_, _, t, ..)| Some(*t)).collect();
    let mats: Vec<Option<&str>> = rows.iter().map(|(.., m, _)| *m).collect();
    let depths: Vec<i32> = rows.iter().map(|(.., d)| *d).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(uids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(mats)),
            Arc::new(Int32Array::from(depths)),
        ],
    )
    .expect("valid node lineage batch")
}

/// Build a batch shaped like the lineage `edges` SQL output:
/// `(from_id, to_id, edge_type)`.
fn edge_lineage_batch(rows: &[(&str, &str, &str)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("from_id", DataType::Utf8, false),
        Field::new("to_id", DataType::Utf8, false),
        Field::new("edge_type", DataType::Utf8, false),
    ]));
    let froms: Vec<&str> = rows.iter().map(|(f, ..)| *f).collect();
    let tos: Vec<&str> = rows.iter().map(|(_, t, _)| *t).collect();
    let etypes: Vec<&str> = rows.iter().map(|(.., e)| *e).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(froms)),
            Arc::new(StringArray::from(tos)),
            Arc::new(StringArray::from(etypes)),
        ],
    )
    .expect("valid edge lineage batch")
}

async fn response_body(response: Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&bytes).expect("valid json")
}

fn empty_params() -> Query<LineageParams> {
    Query(LineageParams { max_depth: None })
}

// ---------------------------------------------------------------------------
// Tests: validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn invalid_unique_id_returns_400() {
    let backend = LineageMockBackend {
        saved_query_batches: vec![],
        node_batches: vec![],
        edge_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_lineage(State(state), Path("bad'id".to_owned()), empty_params()).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn empty_unique_id_returns_400() {
    let backend = LineageMockBackend {
        saved_query_batches: vec![],
        node_batches: vec![],
        edge_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_lineage(State(state), Path(String::new()), empty_params()).await;
    assert_eq!(r.status(), 400);
}

// ---------------------------------------------------------------------------
// Tests: saved_query path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn saved_query_with_deps_returns_root_plus_upstream_rows() {
    let probe = saved_query_probe_batch(
        "dbt_invocations_by_billing_email",
        "dbt_invocations_by_billing_email",
        &[
            "metric.jaffle_shop.dbt_invocations",
            "semantic_model.jaffle_shop.accounts",
        ],
    );
    let backend = LineageMockBackend {
        saved_query_batches: vec![probe],
        node_batches: vec![],
        edge_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_lineage(
        State(state),
        Path("dbt_invocations_by_billing_email".to_owned()),
        empty_params(),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    assert_eq!(body["root"], "dbt_invocations_by_billing_email");
    let nodes = body["nodes"].as_array().expect("nodes is array");
    assert_eq!(nodes.len(), 3);

    // First row is the saved_query root at depth 0.
    assert_eq!(nodes[0]["unique_id"], "dbt_invocations_by_billing_email");
    assert_eq!(nodes[0]["resource_type"], "saved_query");
    assert_eq!(nodes[0]["depth"], 0);

    // Upstream rows are synthesized with resource_type inferred from prefix
    // and depth = -1.
    assert_eq!(nodes[1]["unique_id"], "metric.jaffle_shop.dbt_invocations");
    assert_eq!(nodes[1]["resource_type"], "metric");
    assert_eq!(nodes[1]["name"], "dbt_invocations");
    assert_eq!(nodes[1]["depth"], -1);

    assert_eq!(nodes[2]["unique_id"], "semantic_model.jaffle_shop.accounts");
    assert_eq!(nodes[2]["resource_type"], "semantic_model");
    assert_eq!(nodes[2]["name"], "accounts");

    let edges = body["edges"].as_array().expect("edges is array");
    assert_eq!(edges.len(), 2);
    assert_eq!(edges[0]["from_id"], "metric.jaffle_shop.dbt_invocations");
    assert_eq!(edges[0]["to_id"], "dbt_invocations_by_billing_email");
    assert_eq!(edges[0]["edge_type"], "saved_query");
}

#[tokio::test]
async fn saved_query_with_no_deps_still_returns_root() {
    // Empty depends_on_nodes is the common case in the current parquet output
    // (depends_on_nodes hasn't been populated for SL saved queries yet).
    // The handler must still succeed with a single-node graph rather than
    // 404, otherwise the detail page's lineage panel renders an error.
    let probe = saved_query_probe_batch("lonely_query", "lonely_query", &[]);
    let backend = LineageMockBackend {
        saved_query_batches: vec![probe],
        node_batches: vec![],
        edge_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_lineage(
        State(state),
        Path("lonely_query".to_owned()),
        empty_params(),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    let nodes = body["nodes"].as_array().expect("nodes is array");
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["resource_type"], "saved_query");
    assert_eq!(body["edges"].as_array().expect("edges is array").len(), 0);
}

#[tokio::test]
async fn saved_query_upstream_without_prefix_falls_back_to_node_type() {
    // Bare upstream ids (no dot) shouldn't crash; fall back to a sentinel
    // resource_type rather than empty string, so the UI has something to
    // render a generic icon for.
    let probe = saved_query_probe_batch("q", "q", &["bare_upstream_id"]);
    let backend = LineageMockBackend {
        saved_query_batches: vec![probe],
        node_batches: vec![],
        edge_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_lineage(State(state), Path("q".to_owned()), empty_params()).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    let nodes = body["nodes"].as_array().expect("nodes is array");
    assert_eq!(nodes[1]["unique_id"], "bare_upstream_id");
    assert_eq!(nodes[1]["resource_type"], "node");
}

#[tokio::test]
async fn saved_query_probe_short_circuits_generic_path() {
    // When the saved_query probe finds a row, the generic node lookup must
    // not run — otherwise a saved_query whose name happens to collide with
    // a `dbt.nodes` row could double-report.
    let probe = saved_query_probe_batch("my_query", "my_query", &[]);
    let backend = LineageMockBackend {
        saved_query_batches: vec![probe],
        // Decoy: if this batch were used the response would carry these rows.
        node_batches: vec![node_lineage_batch(&[(
            "model.x.collision",
            "collision",
            "model",
            Some("view"),
            0,
        )])],
        edge_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_lineage(State(state), Path("my_query".to_owned()), empty_params()).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    let nodes = body["nodes"].as_array().expect("nodes is array");
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["resource_type"], "saved_query");
}

// ---------------------------------------------------------------------------
// Tests: generic node path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn generic_node_returns_recursive_cte_result() {
    let backend = LineageMockBackend {
        saved_query_batches: vec![],
        node_batches: vec![node_lineage_batch(&[
            ("model.acme.orders", "orders", "model", Some("table"), 0),
            ("source.acme.raw_orders", "raw_orders", "source", None, -1),
        ])],
        edge_batches: vec![edge_lineage_batch(&[(
            "source.acme.raw_orders",
            "model.acme.orders",
            "source",
        )])],
    };
    let state = make_state(backend);
    let r = get_lineage(
        State(state),
        Path("model.acme.orders".to_owned()),
        empty_params(),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    assert_eq!(body["root"], "model.acme.orders");
    let nodes = body["nodes"].as_array().expect("nodes is array");
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0]["resource_type"], "model");
    assert_eq!(nodes[1]["resource_type"], "source");

    let edges = body["edges"].as_array().expect("edges is array");
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0]["edge_type"], "source");
}

#[tokio::test]
async fn metric_root_returns_lineage_when_only_metadata_batch_supplies_root() {
    // Metrics live in `dbt.metrics`, not `dbt.nodes`. The metadata CTE unions
    // both, so the mock can return a metric row from the same `node_batches`
    // field — what matters is that the handler doesn't 404 when the root's
    // resource_type is "metric".
    let backend = LineageMockBackend {
        saved_query_batches: vec![],
        node_batches: vec![node_lineage_batch(&[
            (
                "metric.acme.active_users",
                "active_users",
                "metric",
                None,
                0,
            ),
            (
                "semantic_model.acme.users",
                "users",
                "semantic_model",
                None,
                -1,
            ),
        ])],
        edge_batches: vec![edge_lineage_batch(&[(
            "semantic_model.acme.users",
            "metric.acme.active_users",
            "metric",
        )])],
    };
    let state = make_state(backend);
    let r = get_lineage(
        State(state),
        Path("metric.acme.active_users".to_owned()),
        empty_params(),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["root"], "metric.acme.active_users");
    let nodes = body["nodes"].as_array().expect("nodes is array");
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0]["resource_type"], "metric");
    assert_eq!(nodes[1]["resource_type"], "semantic_model");
}

#[tokio::test]
async fn unknown_node_returns_404() {
    let backend = LineageMockBackend {
        saved_query_batches: vec![],
        node_batches: vec![],
        edge_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_lineage(
        State(state),
        Path("model.acme.nonexistent".to_owned()),
        empty_params(),
    )
    .await;
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn max_depth_is_clamped_to_hard_max() {
    // Even when the caller asks for a giant depth, the response's max_depth
    // is reported as the clamped value. Locks in the contract documented at
    // [`HARD_MAX_DEPTH`].
    let backend = LineageMockBackend {
        saved_query_batches: vec![],
        node_batches: vec![node_lineage_batch(&[(
            "model.acme.orders",
            "orders",
            "model",
            Some("table"),
            0,
        )])],
        edge_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_lineage(
        State(state),
        Path("model.acme.orders".to_owned()),
        Query(LineageParams {
            max_depth: Some(9999),
        }),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["max_depth"], 3);
}
