//! Tests for `GET /api/v1/snapshots/:id`, `GET /api/v1/snapshots`, and
//! `GET /api/v1/snapshots/facets`.
//!
//! Schema anchoring (#10255): the `RecordBatch` fixtures below are hand-
//! rolled and not enforced against the production parquet schemas. A column
//! rename or type change in `dbt-index` will pass these tests while
//! silently breaking the handler. Once #10255 lands typed row builders,
//! replace these schemas to get compile-time coverage.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use axum::extract::{Path, Query, State};
use axum::response::Response;

use super::*;
use crate::providers::{Backend, BackendError, Providers};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Mock backend
// ---------------------------------------------------------------------------

/// Routes Arrow queries to the configured fixture batches based on which
/// `FROM` table the SQL mentions. Each optional fixture (run_results,
/// catalog, catalog_stats) doubles as the "view absent" knob: `None` → the
/// handler catches the error and renders the surface as JSON `null` / `[]`.
struct SnapshotDetailMockBackend {
    node_batches: Vec<RecordBatch>,
    column_batches: Vec<RecordBatch>,
    /// Upstream (depends_on) and downstream (referenced_by) edges share one
    /// batch in tests; the handler queries them with different WHERE clauses
    /// but the mock returns the same data for both.
    edge_batches: Vec<RecordBatch>,
    /// `Some(batches)` → query succeeds; `None` → view absent (query error).
    run_result_batches: Option<Vec<RecordBatch>>,
    /// `Some(batches)` → query succeeds; `None` → view absent (query error).
    catalog_batches: Option<Vec<RecordBatch>>,
    /// `Some(batches)` → query succeeds; `None` → view absent (query error).
    catalog_stats_batches: Option<Vec<RecordBatch>>,
}

impl Backend for SnapshotDetailMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, _sql: &str) -> Option<String> {
        Some("0".to_owned())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        // Order matters: more specific tables before more generic. `dbt.nodes`
        // is the fallback because it's the most general table the snapshot
        // handler queries.
        if sql.contains("dbt.node_columns") {
            return Ok(self.column_batches.clone());
        }
        if sql.contains("dbt.edges") {
            return Ok(self.edge_batches.clone());
        }
        if sql.contains("dbt_rt.run_results") {
            return self
                .run_result_batches
                .clone()
                .ok_or_else(|| BackendError::Query("run_results view absent".into()));
        }
        if sql.contains("dbt.catalog_stats") {
            return self
                .catalog_stats_batches
                .clone()
                .ok_or_else(|| BackendError::Query("catalog_stats view absent".into()));
        }
        if sql.contains("dbt.catalog_tables") {
            return self
                .catalog_batches
                .clone()
                .ok_or_else(|| BackendError::Query("catalog_tables view absent".into()));
        }
        if sql.contains("dbt.nodes") {
            return Ok(self.node_batches.clone());
        }
        Err(BackendError::Query(format!("unrouted query: {sql}")))
    }
}

/// Build a test `AppState` with explicitly-set capability flags so unit
/// tests don't depend on probe semantics. Production goes through
/// `AppState::new` which probes the backend at startup.
fn make_state(backend: SnapshotDetailMockBackend) -> Arc<AppState> {
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

/// Build a single-row `ListArray` from string slices.
///
/// TODO(#10255): replace with the typed `*RowBuilder` once dbt-index
/// exposes fixture builders bound to the production parquet schema.
fn make_str_list(values: &[&str]) -> ListArray {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for v in values {
        builder.values().append_value(v);
    }
    builder.append(true);
    builder.finish()
}

/// Schema for `dbt.nodes` rows as queried by `SNAPSHOT_DETAIL_NODE_SQL`.
/// Assumes `tags`/`fqn`/`primary_key` as `List(Utf8)`, `meta` as `Utf8`
/// holding a JSON string, scalar snapshot-specific fields as `Utf8`. Not
/// compile-checked against the production schema (#10255).
fn snapshot_node_schema(tags_field: &Field, fqn_field: &Field, pk_field: &Field) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("resource_type", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("patch_path", DataType::Utf8, true),
        Field::new("database_name", DataType::Utf8, true),
        Field::new("schema_name", DataType::Utf8, true),
        Field::new("identifier", DataType::Utf8, true),
        Field::new("relation_name", DataType::Utf8, true),
        Field::new("materialized", DataType::Utf8, true),
        Field::new("raw_code", DataType::Utf8, true),
        Field::new("compiled_code", DataType::Utf8, true),
        Field::new("meta", DataType::Utf8, true),
        tags_field.clone(),
        fqn_field.clone(),
        pk_field.clone(),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn make_snapshot_node_batch(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    description: Option<&str>,
    raw_code: Option<&str>,
    compiled_code: Option<&str>,
    meta_json: Option<&str>,
    tags: &[&str],
    fqn: &[&str],
    primary_key: &[&str],
) -> RecordBatch {
    let tags_arr = make_str_list(tags);
    let fqn_arr = make_str_list(fqn);
    let pk_arr = make_str_list(primary_key);
    let tags_field = Field::new("tags", tags_arr.data_type().clone(), true);
    let fqn_field = Field::new("fqn", fqn_arr.data_type().clone(), true);
    let pk_field = Field::new("primary_key", pk_arr.data_type().clone(), true);

    RecordBatch::try_new(
        snapshot_node_schema(&tags_field, &fqn_field, &pk_field),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec!["snapshot"])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![description])),
            Arc::new(StringArray::from(vec![Some(
                "snapshots/orders_snapshot.sql",
            )])),
            Arc::new(StringArray::from(vec![Some("snapshots/schema.yml")])),
            Arc::new(StringArray::from(vec![Some("prod")])),
            Arc::new(StringArray::from(vec![Some("dbt_prod")])),
            Arc::new(StringArray::from(vec![Some(name)])), // identifier defaults to name
            Arc::new(StringArray::from(vec![Some(
                "prod.dbt_prod.orders_snapshot",
            )])),
            Arc::new(StringArray::from(vec![Some("snapshot")])),
            Arc::new(StringArray::from(vec![raw_code])),
            Arc::new(StringArray::from(vec![compiled_code])),
            Arc::new(StringArray::from(vec![meta_json])),
            Arc::new(tags_arr),
            Arc::new(fqn_arr),
            Arc::new(pk_arr),
        ],
    )
    .expect("valid snapshot node batch")
}

fn column_batch_one(
    name: &str,
    data_type: Option<&str>,
    catalog_type: Option<&str>,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("index", DataType::Int64, true),
        Field::new("data_type", DataType::Utf8, true),
        Field::new("declared_type", DataType::Utf8, true),
        Field::new("inferred_type", DataType::Utf8, true),
        Field::new("catalog_type", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("granularity", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![name])),
            Arc::new(Int64Array::from(vec![Some(0i64)])),
            Arc::new(StringArray::from(vec![data_type])),
            Arc::new(StringArray::from(vec![data_type])), // declared_type
            Arc::new(StringArray::from(vec![None::<&str>])), // inferred_type
            Arc::new(StringArray::from(vec![catalog_type])),
            Arc::new(StringArray::from(vec![Some("desc")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )
    .expect("valid column batch")
}

fn edge_batch(rows: &[(&str, &str)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("edge_type", DataType::Utf8, false),
    ]));
    let uids: Vec<&str> = rows.iter().map(|(u, _)| *u).collect();
    let etypes: Vec<&str> = rows.iter().map(|(_, e)| *e).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(uids)),
            Arc::new(StringArray::from(etypes)),
        ],
    )
    .expect("valid edge batch")
}

fn run_result_batch(status: &str, execution_time: f64, completed_at: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("status", DataType::Utf8, true),
        Field::new("execution_time", DataType::Float64, true),
        Field::new("completed_at", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![status])),
            Arc::new(Float64Array::from(vec![execution_time])),
            Arc::new(StringArray::from(vec![completed_at])),
        ],
    )
    .expect("valid run result batch")
}

fn catalog_batch(table_type: Option<&str>, owner: Option<&str>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("type", DataType::Utf8, true),
        Field::new("owner", DataType::Utf8, true),
        Field::new("bytes_stat", DataType::Int64, true),
        Field::new("row_count_stat", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![table_type])),
            Arc::new(StringArray::from(vec![owner])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
        ],
    )
    .expect("valid catalog batch")
}

fn catalog_stats_batch(rows: &[(&str, &str, &str, &str, bool)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("include", DataType::Boolean, true),
    ]));
    let ids: Vec<&str> = rows.iter().map(|r| r.0).collect();
    let labels: Vec<&str> = rows.iter().map(|r| r.1).collect();
    let values: Vec<&str> = rows.iter().map(|r| r.2).collect();
    let descs: Vec<&str> = rows.iter().map(|r| r.3).collect();
    let includes: Vec<bool> = rows.iter().map(|r| r.4).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(labels)),
            Arc::new(StringArray::from(values)),
            Arc::new(StringArray::from(descs)),
            Arc::new(BooleanArray::from(includes)),
        ],
    )
    .expect("valid catalog_stats batch")
}

async fn response_body(response: Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&bytes).expect("valid json")
}

fn full_node_batch() -> RecordBatch {
    make_snapshot_node_batch(
        "snapshot.jaffle_shop.orders_snapshot",
        "orders_snapshot",
        Some("jaffle_shop"),
        Some("Snapshot of the orders table"),
        Some("{%- snapshot orders_snapshot -%}...{%- endsnapshot -%}"),
        Some("select * from prod.dbt_prod.orders"),
        Some(r#"{"owner":"data-eng","priority":"high"}"#),
        &["finance", "snapshot"],
        &["jaffle_shop", "orders_snapshot"],
        &["order_id"],
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn invalid_unique_id_returns_400() {
    let backend = SnapshotDetailMockBackend {
        node_batches: vec![],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_snapshot(State(state), Path("bad'id".to_owned())).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn missing_snapshot_returns_404() {
    let backend = SnapshotDetailMockBackend {
        node_batches: vec![],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_snapshot(State(state), Path("snapshot.x.y".to_owned())).await;
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn all_fields_hydrated() {
    let backend = SnapshotDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![column_batch_one("order_id", Some("integer"), Some("INT64"))],
        edge_batches: vec![edge_batch(&[("model.jaffle_shop.orders", "ref")])],
        run_result_batches: Some(vec![run_result_batch(
            "success",
            12.7,
            "2026-05-15T10:32:11Z",
        )]),
        catalog_batches: Some(vec![catalog_batch(Some("table"), Some("dbt_runner"))]),
        catalog_stats_batches: Some(vec![catalog_stats_batch(&[(
            "has_stats",
            "Has Stats?",
            "true",
            "Indicates whether there are statistics",
            false,
        )])]),
    };
    let state = make_state(backend);
    let r = get_snapshot(
        State(state),
        Path("snapshot.jaffle_shop.orders_snapshot".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    // NodeBase fields flatten into top-level.
    assert_eq!(body["unique_id"], "snapshot.jaffle_shop.orders_snapshot");
    assert_eq!(body["name"], "orders_snapshot");
    assert_eq!(body["resource_type"], "snapshot");
    assert_eq!(body["package_name"], "jaffle_shop");
    assert_eq!(body["description"], "Snapshot of the orders table");

    // Snapshot-specific scalars.
    assert_eq!(body["patch_path"], "snapshots/schema.yml");
    assert_eq!(body["materialized"], "snapshot");
    assert_eq!(body["database_name"], "prod");
    assert_eq!(body["relation_name"], "prod.dbt_prod.orders_snapshot");
    assert!(body["raw_code"].as_str().unwrap().contains("snapshot"));
    assert_eq!(body["compiled_code"], "select * from prod.dbt_prod.orders");

    // List-typed fields surface as JSON arrays.
    assert_eq!(body["tags"], serde_json::json!(["finance", "snapshot"]));
    assert_eq!(
        body["fqn"],
        serde_json::json!(["jaffle_shop", "orders_snapshot"])
    );

    // meta is parsed JSON, NOT an escaped string.
    assert_eq!(
        body["meta"],
        serde_json::json!({"owner": "data-eng", "priority": "high"}),
        "meta must be parsed as JSON object, not escaped string"
    );

    // Both depends_on and referenced_by populated (mock returns same batch for both).
    assert_eq!(
        body["depends_on"][0]["unique_id"],
        "model.jaffle_shop.orders"
    );
    assert_eq!(body["depends_on"][0]["edge_type"], "ref");
    assert_eq!(
        body["referenced_by"][0]["unique_id"],
        "model.jaffle_shop.orders"
    );

    // Execution info round-trip.
    assert_eq!(body["execution_info"]["status"], "success");
    assert_eq!(
        body["execution_info"]["completed_at"],
        "2026-05-15T10:32:11Z"
    );
    assert_eq!(
        body["execution_info"]["execution_time"],
        serde_json::json!(12.7)
    );

    // Catalog with primary_key from dbt.nodes, stats from catalog_stats.
    assert_eq!(body["catalog"]["type"], "table");
    assert_eq!(body["catalog"]["owner"], "dbt_runner");
    assert_eq!(
        body["catalog"]["primary_key"],
        serde_json::json!(["order_id"]),
        "primary_key comes from dbt.nodes.primary_key, not catalog_tables"
    );
    assert_eq!(body["catalog"]["stats"][0]["id"], "has_stats");
    assert_eq!(body["catalog"]["stats"][0]["value"], "true");
    assert_eq!(body["catalog"]["stats"][0]["include"], false);
    // Snapshot catalog must NOT carry a `comment` field (that's source-only).
    assert!(
        body["catalog"].get("comment").is_none(),
        "snapshot catalog must omit comment"
    );

    // Columns echoed.
    assert_eq!(body["columns"][0]["name"], "order_id");
    assert_eq!(body["columns"][0]["catalog_type"], "INT64");
}

#[tokio::test]
async fn meta_null_when_absent() {
    let backend = SnapshotDetailMockBackend {
        node_batches: vec![make_snapshot_node_batch(
            "snapshot.pkg.s",
            "s",
            None,
            None,
            None,
            None,
            None, // meta absent
            &[],
            &[],
            &[],
        )],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_snapshot(State(state), Path("snapshot.pkg.s".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn meta_null_when_malformed() {
    // Malformed JSON must serialise as null, NOT bubble a parse error to the client.
    // This protects against partial / corrupted writes in the parquet index.
    let backend = SnapshotDetailMockBackend {
        node_batches: vec![make_snapshot_node_batch(
            "snapshot.pkg.s",
            "s",
            None,
            None,
            None,
            None,
            Some("not{valid:json"),
            &[],
            &[],
            &[],
        )],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_snapshot(State(state), Path("snapshot.pkg.s".to_owned())).await;
    assert_eq!(r.status(), 200, "malformed meta must not 500 the response");
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn execution_info_null_when_view_absent() {
    let backend = SnapshotDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        // None = view absent at query time.
        run_result_batches: None,
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_snapshot(
        State(state),
        Path("snapshot.jaffle_shop.orders_snapshot".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(
        body["execution_info"],
        serde_json::Value::Null,
        "execution_info must be null when the parquet view is absent"
    );
}

#[tokio::test]
async fn execution_info_null_when_no_row_for_snapshot() {
    // View present but no row for this snapshot — equivalent semantics from
    // the FE perspective: execution_info is null.
    let backend = SnapshotDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_snapshot(
        State(state),
        Path("snapshot.jaffle_shop.orders_snapshot".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["execution_info"], serde_json::Value::Null);
}

#[tokio::test]
async fn catalog_null_when_view_absent() {
    let backend = SnapshotDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: None,
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_snapshot(
        State(state),
        Path("snapshot.jaffle_shop.orders_snapshot".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["catalog"], serde_json::Value::Null);
}

#[tokio::test]
async fn catalog_stats_empty_array_when_no_rows() {
    // catalog_tables has a row but catalog_stats is empty: catalog.stats=[],
    // catalog itself is present.
    let backend = SnapshotDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: Some(vec![catalog_batch(Some("table"), Some("svc"))]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_snapshot(
        State(state),
        Path("snapshot.jaffle_shop.orders_snapshot".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(
        body["catalog"]["stats"],
        serde_json::json!([]),
        "stats[] must be empty array (not null) when the table is present but no stats rows"
    );
    assert_eq!(body["catalog"]["type"], "table");
}

#[tokio::test]
async fn primary_key_empty_array_when_no_pk_declared() {
    let backend = SnapshotDetailMockBackend {
        node_batches: vec![make_snapshot_node_batch(
            "snapshot.pkg.s",
            "s",
            None,
            None,
            None,
            None,
            None,
            &[],
            &[],
            &[], // no PK
        )],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: Some(vec![catalog_batch(Some("table"), None)]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_snapshot(State(state), Path("snapshot.pkg.s".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["catalog"]["primary_key"], serde_json::json!([]));
}

#[tokio::test]
async fn depends_on_and_referenced_by_empty_when_no_edges() {
    let backend = SnapshotDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_snapshot(
        State(state),
        Path("snapshot.jaffle_shop.orders_snapshot".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["depends_on"], serde_json::json!([]));
    assert_eq!(body["referenced_by"], serde_json::json!([]));
}

// ===========================================================================
// Tests: GET /api/v1/snapshots and GET /api/v1/snapshots/facets
// ===========================================================================
//
// TODO(#10255): replace hand-rolled RecordBatch schemas with typed row
// builders once dbt-index exposes fixture builders bound to the production
// parquet schema.

// ---------------------------------------------------------------------------
// Mock backend for list + facets
// ---------------------------------------------------------------------------

struct SnapshotListMockBackend {
    total_count: u64,
    row_batches: Vec<RecordBatch>,
    /// When `true`, the with-run_results count query returns `None` to trigger
    /// the no-run_results fallback path.
    run_results_absent: bool,
    /// When `true`, the with-catalog query also returns `None`.
    catalog_absent: bool,
}

impl SnapshotListMockBackend {
    fn new(total: u64, rows: Vec<RecordBatch>) -> Self {
        Self {
            total_count: total,
            row_batches: rows,
            run_results_absent: false,
            catalog_absent: false,
        }
    }
    fn without_run_results(mut self) -> Self {
        self.run_results_absent = true;
        self
    }
    fn without_catalog(mut self) -> Self {
        self.catalog_absent = true;
        self
    }
}

impl Backend for SnapshotListMockBackend {
    fn is_available(&self) -> bool {
        true
    }
    fn query_scalar(&self, sql: &str) -> Option<String> {
        if !sql.contains("count(*)") {
            return None;
        }
        if self.run_results_absent && sql.contains("run_results") {
            return None;
        }
        if self.catalog_absent && sql.contains("catalog_stats") {
            return None;
        }
        Some(self.total_count.to_string())
    }
    fn query_arrow(&self, _sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        Ok(self.row_batches.clone())
    }
}

fn make_list_state(backend: SnapshotListMockBackend) -> Arc<AppState> {
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
// Batch builders for list
// ---------------------------------------------------------------------------

fn snapshot_list_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("resource_type", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("materialized", DataType::Utf8, true),
        Field::new("strategy", DataType::Utf8, true),
        Field::new("config_updated_at", DataType::Utf8, true),
        Field::new("ei_completed_at", DataType::Utf8, true),
        Field::new("ei_status", DataType::Utf8, true),
        Field::new("ei_error", DataType::Utf8, true),
        Field::new("has_run_result", DataType::Int32, false),
        Field::new("has_catalog", DataType::Int32, false),
        Field::new("row_count_stat", DataType::Int64, true),
        Field::new("bytes_stat", DataType::Int64, true),
        Field::new("last_modified_stat", DataType::Utf8, true),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn snapshot_list_row(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    strategy: Option<&str>,
    config_updated_at: Option<&str>,
    has_run_result: i32,
    ei_status: Option<&str>,
    ei_completed_at: Option<&str>,
    ei_error: Option<&str>,
    has_catalog: i32,
    row_count_stat: Option<i64>,
    bytes_stat: Option<i64>,
    last_modified_stat: Option<&str>,
) -> RecordBatch {
    RecordBatch::try_new(
        snapshot_list_schema(),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec!["snapshot"])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![Some("snapshot")])),
            Arc::new(StringArray::from(vec![strategy])),
            Arc::new(StringArray::from(vec![config_updated_at])),
            Arc::new(StringArray::from(vec![ei_completed_at])),
            Arc::new(StringArray::from(vec![ei_status])),
            Arc::new(StringArray::from(vec![ei_error])),
            Arc::new(arrow_array::Int32Array::from(vec![has_run_result])),
            Arc::new(arrow_array::Int32Array::from(vec![has_catalog])),
            Arc::new(Int64Array::from(vec![row_count_stat])),
            Arc::new(Int64Array::from(vec![bytes_stat])),
            Arc::new(StringArray::from(vec![last_modified_stat])),
        ],
    )
    .expect("valid snapshot list row batch")
}

// ---------------------------------------------------------------------------
// Unit tests: SQL builders
// ---------------------------------------------------------------------------

#[test]
fn unknown_sort_column_returns_err() {
    let params = SnapshotListParams {
        sort: Some("tags:asc".into()),
        ..Default::default()
    };
    assert!(build_snapshot_list_sql(&params, true, true, 10, None).is_err());
}

#[test]
fn sort_without_direction_returns_err() {
    let params = SnapshotListParams {
        sort: Some("name".into()),
        ..Default::default()
    };
    assert!(build_snapshot_list_sql(&params, true, true, 10, None).is_err());
}

#[test]
fn sort_invalid_direction_returns_err() {
    let params = SnapshotListParams {
        sort: Some("name:random".into()),
        ..Default::default()
    };
    assert!(build_snapshot_list_sql(&params, true, true, 10, None).is_err());
}

#[test]
fn allowlisted_sort_columns_succeed() {
    for col in &["name", "package_name", "updated_at"] {
        let params = SnapshotListParams {
            sort: Some(format!("{col}:asc")),
            ..Default::default()
        };
        assert!(
            build_snapshot_list_sql(&params, true, true, 10, None).is_ok(),
            "expected ok for sort={col}:asc"
        );
        let params_desc = SnapshotListParams {
            sort: Some(format!("{col}:desc")),
            ..Default::default()
        };
        assert!(
            build_snapshot_list_sql(&params_desc, true, true, 10, None).is_ok(),
            "expected ok for sort={col}:desc"
        );
    }
}

#[test]
fn count_sql_excludes_cursor_page_sql_includes_cursor() {
    let c = Cursor {
        sort_value: Some("orders_snapshot".into()),
        unique_id: "snapshot.pkg.orders_snapshot".into(),
    };
    let params = SnapshotListParams::default();
    let (count, rows) = build_snapshot_list_sql(&params, true, true, 10, Some(&c)).unwrap();
    assert!(
        !count.contains("orders_snapshot"),
        "count must exclude cursor predicate"
    );
    assert!(
        rows.contains("orders_snapshot"),
        "rows must include cursor predicate"
    );
}

#[test]
fn no_run_results_flag_emits_null_execution_info() {
    let params = SnapshotListParams::default();
    let (_, rows) = build_snapshot_list_sql(&params, false, false, 10, None).unwrap();
    assert!(
        rows.contains("NULL::VARCHAR AS ei_status"),
        "fallback must emit NULL ei_status"
    );
    assert!(
        rows.contains("NULL::VARCHAR AS ei_completed_at"),
        "fallback must emit NULL ei_completed_at"
    );
}

#[test]
fn no_catalog_flag_emits_null_catalog_stats() {
    let params = SnapshotListParams::default();
    let (_, rows) = build_snapshot_list_sql(&params, false, false, 10, None).unwrap();
    assert!(
        rows.contains("NULL::BIGINT AS row_count_stat"),
        "fallback must emit NULL row_count_stat"
    );
    assert!(
        rows.contains("NULL::BIGINT AS bytes_stat"),
        "fallback must emit NULL bytes_stat"
    );
}

// ---------------------------------------------------------------------------
// Integration tests: list_snapshots
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_snapshots_empty_catalog() {
    let state = make_list_state(SnapshotListMockBackend::new(0, vec![]));
    let r = list_snapshots(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"], serde_json::json!([]));
    assert_eq!(body["page_info"]["total_count"], 0);
    assert_eq!(body["page_info"]["has_next_page"], false);
    assert_eq!(body["page_info"]["start_cursor"], serde_json::Value::Null);
    assert_eq!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_snapshots_all_fields_hydrated() {
    let row = snapshot_list_row(
        "snapshot.jaffle_shop.orders_snapshot",
        "orders_snapshot",
        Some("jaffle_shop"),
        Some("timestamp"),
        Some("updated_at"),
        1, // has_run_result
        Some("success"),
        Some("2026-05-15T10:32:11Z"),
        None, // no error
        1,    // has_catalog
        Some(42000),
        Some(3145728),
        Some("2026-05-15 10:30:00"),
    );
    let state = make_list_state(SnapshotListMockBackend::new(1, vec![row]));
    let r = list_snapshots(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    assert_eq!(
        body["data"][0]["unique_id"],
        "snapshot.jaffle_shop.orders_snapshot"
    );
    assert_eq!(body["data"][0]["name"], "orders_snapshot");
    assert_eq!(body["data"][0]["resource_type"], "snapshot");
    assert_eq!(body["data"][0]["package_name"], "jaffle_shop");
    assert_eq!(body["data"][0]["materialized"], "snapshot");
    assert_eq!(body["data"][0]["strategy"], "timestamp");
    assert_eq!(body["data"][0]["updated_at"], "updated_at");

    // execution_info populated.
    assert_eq!(body["data"][0]["execution_info"]["status"], "success");
    assert_eq!(
        body["data"][0]["execution_info"]["completed_at"],
        "2026-05-15T10:32:11Z"
    );
    assert_eq!(
        body["data"][0]["execution_info"]["error"],
        serde_json::Value::Null
    );

    // catalog populated.
    assert_eq!(body["data"][0]["catalog"]["row_count_stat"], 42000);
    assert_eq!(body["data"][0]["catalog"]["bytes_stat"], 3145728);
    assert_eq!(
        body["data"][0]["catalog"]["last_modified_stat"],
        "2026-05-15 10:30:00"
    );

    assert_eq!(body["page_info"]["total_count"], 1);
}

#[tokio::test]
async fn list_snapshots_check_strategy_null_updated_at() {
    let row = snapshot_list_row(
        "snapshot.jaffle_shop.customers_snapshot",
        "customers_snapshot",
        Some("jaffle_shop"),
        Some("check"),
        None, // updated_at is null for check strategy
        0,    // no run result
        None,
        None,
        None,
        0, // no catalog
        None,
        None,
        None,
    );
    let state = make_list_state(SnapshotListMockBackend::new(1, vec![row]));
    let r = list_snapshots(State(state), Query(Default::default())).await;
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["strategy"], "check");
    assert_eq!(body["data"][0]["updated_at"], serde_json::Value::Null);
    assert_eq!(body["data"][0]["execution_info"], serde_json::Value::Null);
    assert_eq!(body["data"][0]["catalog"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_snapshots_execution_info_null_when_run_results_absent() {
    let row = snapshot_list_row(
        "snapshot.pkg.t",
        "t",
        None,
        None,
        None,
        0,
        None,
        None,
        None,
        0,
        None,
        None,
        None,
    );
    let state = make_list_state(SnapshotListMockBackend::new(1, vec![row]).without_run_results());
    let r = list_snapshots(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["execution_info"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_snapshots_catalog_null_when_catalog_absent() {
    let row = snapshot_list_row(
        "snapshot.pkg.t",
        "t",
        None,
        None,
        None,
        0,
        None,
        None,
        None,
        0,
        None,
        None,
        None,
    );
    let state = make_list_state(
        SnapshotListMockBackend::new(1, vec![row])
            .without_run_results()
            .without_catalog(),
    );
    let r = list_snapshots(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["catalog"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_snapshots_unknown_sort_returns_400() {
    let state = make_list_state(SnapshotListMockBackend::new(0, vec![]));
    let params = SnapshotListParams {
        sort: Some("database_name:asc".into()),
        ..Default::default()
    };
    let r = list_snapshots(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_snapshots_invalid_cursor_returns_400() {
    let state = make_list_state(SnapshotListMockBackend::new(0, vec![]));
    let params = SnapshotListParams {
        after: Some("not-valid-base64!!!".into()),
        ..Default::default()
    };
    let r = list_snapshots(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_snapshots_multi_page_has_next_page_true() {
    let row_a = snapshot_list_row(
        "snapshot.pkg.alpha",
        "alpha",
        None,
        None,
        None,
        0,
        None,
        None,
        None,
        0,
        None,
        None,
        None,
    );
    let row_b = snapshot_list_row(
        "snapshot.pkg.beta",
        "beta",
        None,
        None,
        None,
        0,
        None,
        None,
        None,
        0,
        None,
        None,
        None,
    );
    let state = make_list_state(SnapshotListMockBackend::new(2, vec![row_a, row_b]));
    let params = SnapshotListParams {
        first: Some(1),
        ..Default::default()
    };
    let r = list_snapshots(State(state), Query(params)).await;
    let body = response_body(r).await;
    assert_eq!(body["page_info"]["has_next_page"], true);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_ne!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_snapshots_last_page_end_cursor_null() {
    let row = snapshot_list_row(
        "snapshot.pkg.z",
        "z",
        None,
        None,
        None,
        0,
        None,
        None,
        None,
        0,
        None,
        None,
        None,
    );
    let state = make_list_state(SnapshotListMockBackend::new(1, vec![row]));
    let r = list_snapshots(State(state), Query(Default::default())).await;
    let body = response_body(r).await;
    assert_eq!(body["page_info"]["has_next_page"], false);
    assert_eq!(
        body["page_info"]["end_cursor"],
        serde_json::Value::Null,
        "end_cursor must be null on last page per ADR-6"
    );
    assert_ne!(body["page_info"]["start_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_snapshots_cursor_advances_page() {
    let after = Cursor {
        sort_value: Some("alpha".into()),
        unique_id: "snapshot.pkg.alpha".into(),
    }
    .encode();
    let row = snapshot_list_row(
        "snapshot.pkg.beta",
        "beta",
        None,
        None,
        None,
        0,
        None,
        None,
        None,
        0,
        None,
        None,
        None,
    );
    let state = make_list_state(SnapshotListMockBackend::new(1, vec![row]));
    let params = SnapshotListParams {
        after: Some(after),
        ..Default::default()
    };
    let r = list_snapshots(State(state), Query(params)).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["name"], "beta");
}

// ---------------------------------------------------------------------------
// Integration tests: list_snapshot_facets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_snapshot_facets_returns_empty_object() {
    let r = list_snapshot_facets().await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(
        body,
        serde_json::json!({}),
        "snapshots facets must be an empty JSON object per contract"
    );
}
