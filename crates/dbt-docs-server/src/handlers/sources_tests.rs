//! Tests for `GET /api/v1/sources/:id`.
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
use axum::extract::{Path, State};
use axum::response::Response;

use super::*;
use crate::providers::{Backend, BackendError, Providers};
use crate::state::{AppState, Capabilities};

// ---------------------------------------------------------------------------
// Mock backend
// ---------------------------------------------------------------------------

/// Routes Arrow queries to the configured fixture batches based on which
/// `FROM` table the SQL mentions. Each optional fixture (freshness, catalog,
/// catalog_stats) doubles as the "view absent" knob: `None` → the handler
/// catches the error and renders the surface as JSON `null` / `[]`.
struct SourceDetailMockBackend {
    node_batches: Vec<RecordBatch>,
    column_batches: Vec<RecordBatch>,
    edge_batches: Vec<RecordBatch>,
    /// `Some(batches)` → query succeeds; `None` → view absent (query error).
    freshness_batches: Option<Vec<RecordBatch>>,
    /// `Some(batches)` → query succeeds; `None` → view absent (query error).
    catalog_batches: Option<Vec<RecordBatch>>,
    /// `Some(batches)` → query succeeds; `None` → view absent (query error).
    catalog_stats_batches: Option<Vec<RecordBatch>>,
}

impl Backend for SourceDetailMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, _sql: &str) -> Option<String> {
        Some("0".to_owned())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        // Order matters: more specific tables before more generic. `dbt.nodes`
        // is the fallback because it's the most general table the source
        // handler queries.
        if sql.contains("dbt.node_columns") {
            return Ok(self.column_batches.clone());
        }
        if sql.contains("dbt.edges") {
            return Ok(self.edge_batches.clone());
        }
        if sql.contains("dbt.source_freshness") {
            return self
                .freshness_batches
                .clone()
                .ok_or_else(|| BackendError::Query("source_freshness view absent".into()));
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
fn make_state(backend: SourceDetailMockBackend) -> Arc<AppState> {
    let providers = Providers {
        backend: Arc::new(backend),
        ..Providers::unavailable()
    };
    Arc::new(AppState {
        index_dir: PathBuf::from("/tmp"),
        providers,
        capabilities: Capabilities::default(),
        server_version: env!("CARGO_PKG_VERSION"),
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

/// Schema for `dbt.nodes` rows as queried by `SOURCE_DETAIL_NODE_SQL`.
/// Assumes `tags`/`fqn`/`primary_key` as `List(Utf8)`, `meta` as `Utf8`
/// holding a JSON string, scalar source-specific fields as `Utf8`. Not
/// compile-checked against the production schema (#10255).
fn source_node_schema(tags_field: &Field, fqn_field: &Field, pk_field: &Field) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("resource_type", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("file_path", DataType::Utf8, true),
        Field::new("database_name", DataType::Utf8, true),
        Field::new("schema_name", DataType::Utf8, true),
        Field::new("identifier", DataType::Utf8, true),
        Field::new("source_name", DataType::Utf8, true),
        Field::new("source_description", DataType::Utf8, true),
        Field::new("loader", DataType::Utf8, true),
        Field::new("meta", DataType::Utf8, true),
        tags_field.clone(),
        fqn_field.clone(),
        pk_field.clone(),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn make_source_node_batch(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    description: Option<&str>,
    source_name: Option<&str>,
    source_description: Option<&str>,
    loader: Option<&str>,
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
        source_node_schema(&tags_field, &fqn_field, &pk_field),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec!["source"])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![description])),
            // original_file_path same as file_path for sources (YAML-only).
            Arc::new(StringArray::from(vec![Some("models/staging/sources.yml")])),
            Arc::new(StringArray::from(vec![Some("models/staging/sources.yml")])),
            Arc::new(StringArray::from(vec![Some("raw")])),
            Arc::new(StringArray::from(vec![Some("jaffle_shop")])),
            Arc::new(StringArray::from(vec![Some(name)])), // identifier defaults to name
            Arc::new(StringArray::from(vec![source_name])),
            Arc::new(StringArray::from(vec![source_description])),
            Arc::new(StringArray::from(vec![loader])),
            Arc::new(StringArray::from(vec![meta_json])),
            Arc::new(tags_arr),
            Arc::new(fqn_arr),
            Arc::new(pk_arr),
        ],
    )
    .expect("valid source node batch")
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

/// Build a freshness fixture batch covering all top-level fields. Columns
/// match the projection in `SOURCE_DETAIL_FRESHNESS_SQL` (timestamps already
/// cast to VARCHAR).
#[allow(clippy::too_many_arguments)]
fn freshness_batch(
    status: &str,
    snapshotted_at: Option<&str>,
    max_loaded_at: Option<&str>,
    time_ago: Option<f64>,
    warn_count: Option<i64>,
    warn_period: Option<&str>,
    error_count: Option<i64>,
    error_period: Option<&str>,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("status", DataType::Utf8, false),
        Field::new("snapshotted_at", DataType::Utf8, true),
        Field::new("max_loaded_at", DataType::Utf8, true),
        Field::new("max_loaded_at_time_ago", DataType::Float64, true),
        Field::new("warn_after_count", DataType::Int64, true),
        Field::new("warn_after_period", DataType::Utf8, true),
        Field::new("error_after_count", DataType::Int64, true),
        Field::new("error_after_period", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some(status)])),
            Arc::new(StringArray::from(vec![snapshotted_at])),
            Arc::new(StringArray::from(vec![max_loaded_at])),
            Arc::new(Float64Array::from(vec![time_ago])),
            Arc::new(Int64Array::from(vec![warn_count])),
            Arc::new(StringArray::from(vec![warn_period])),
            Arc::new(Int64Array::from(vec![error_count])),
            Arc::new(StringArray::from(vec![error_period])),
        ],
    )
    .expect("valid freshness batch")
}

fn catalog_batch(
    table_type: Option<&str>,
    owner: Option<&str>,
    comment: Option<&str>,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("type", DataType::Utf8, true),
        Field::new("owner", DataType::Utf8, true),
        Field::new("comment", DataType::Utf8, true),
        Field::new("bytes_stat", DataType::Int64, true),
        Field::new("row_count_stat", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![table_type])),
            Arc::new(StringArray::from(vec![owner])),
            Arc::new(StringArray::from(vec![comment])),
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
    make_source_node_batch(
        "source.jaffle_shop.raw_jaffle.orders",
        "orders",
        Some("jaffle_shop"),
        Some("Raw orders table"),
        Some("raw_jaffle"),
        Some("Raw tables from production Postgres"),
        Some("fivetran"),
        Some(r#"{"owner":"data-eng","priority":"high"}"#),
        &["raw", "jaffle"],
        &["jaffle_shop", "raw_jaffle", "orders"],
        &["id"],
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn invalid_unique_id_returns_400() {
    let backend = SourceDetailMockBackend {
        node_batches: vec![],
        column_batches: vec![],
        edge_batches: vec![],
        freshness_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_source(State(state), Path("bad'id".to_owned())).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn missing_source_returns_404() {
    let backend = SourceDetailMockBackend {
        node_batches: vec![],
        column_batches: vec![],
        edge_batches: vec![],
        freshness_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_source(State(state), Path("source.x.y.z".to_owned())).await;
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn all_fields_hydrated() {
    let backend = SourceDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![column_batch_one("id", Some("integer"), Some("INT64"))],
        edge_batches: vec![edge_batch(&[("model.jaffle_shop.stg_orders", "model")])],
        freshness_batches: Some(vec![freshness_batch(
            "pass",
            Some("2026-05-15 10:00:00"),
            Some("2026-05-15 09:45:00"),
            Some(900.0),
            Some(12),
            Some("hour"),
            Some(24),
            Some("hour"),
        )]),
        catalog_batches: Some(vec![catalog_batch(
            Some("table"),
            Some("fivetran"),
            Some("Raw orders synced"),
        )]),
        catalog_stats_batches: Some(vec![catalog_stats_batch(&[(
            "has_stats",
            "Has Stats?",
            "true",
            "Indicates whether there are statistics",
            false,
        )])]),
    };
    let state = make_state(backend);
    let r = get_source(
        State(state),
        Path("source.jaffle_shop.raw_jaffle.orders".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    // NodeBase fields flatten into top-level.
    assert_eq!(body["unique_id"], "source.jaffle_shop.raw_jaffle.orders");
    assert_eq!(body["name"], "orders");
    assert_eq!(body["resource_type"], "source");
    assert_eq!(body["package_name"], "jaffle_shop");
    assert_eq!(body["description"], "Raw orders table");

    // Source-specific scalars.
    assert_eq!(body["source_name"], "raw_jaffle");
    assert_eq!(body["loader"], "fivetran");
    assert_eq!(body["database_name"], "raw");

    // List-typed fields surface as JSON arrays, not as nested objects.
    assert_eq!(
        body["tags"],
        serde_json::json!(["raw", "jaffle"]),
        "tags must be a flat string array"
    );
    assert_eq!(
        body["fqn"],
        serde_json::json!(["jaffle_shop", "raw_jaffle", "orders"])
    );

    // meta is parsed JSON, NOT an escaped string.
    assert_eq!(
        body["meta"],
        serde_json::json!({"owner": "data-eng", "priority": "high"}),
        "meta must be parsed as JSON object, not escaped string"
    );

    // referenced_by populated; depends_on key must be ABSENT (not empty array).
    assert_eq!(
        body["referenced_by"][0]["unique_id"],
        "model.jaffle_shop.stg_orders"
    );
    assert!(
        body.get("depends_on").is_none(),
        "sources must omit depends_on entirely, not return []"
    );

    // Freshness round-trip.
    assert_eq!(body["freshness"]["status"], "pass");
    assert_eq!(body["freshness"]["max_loaded_at_time_ago"], 900.0);
    assert_eq!(body["freshness"]["criteria"]["error_after"]["count"], 24);
    assert_eq!(
        body["freshness"]["criteria"]["warn_after"]["period"],
        "hour"
    );

    // Catalog with primary_key from dbt.nodes, stats from catalog_stats.
    assert_eq!(body["catalog"]["type"], "table");
    assert_eq!(body["catalog"]["owner"], "fivetran");
    assert_eq!(body["catalog"]["comment"], "Raw orders synced");
    assert_eq!(
        body["catalog"]["primary_key"],
        serde_json::json!(["id"]),
        "primary_key comes from dbt.nodes.primary_key, not catalog_tables"
    );
    assert_eq!(body["catalog"]["stats"][0]["id"], "has_stats");
    assert_eq!(body["catalog"]["stats"][0]["value"], "true");
    assert_eq!(body["catalog"]["stats"][0]["include"], false);

    // Columns echoed; declared_type and data_type both populated.
    assert_eq!(body["columns"][0]["name"], "id");
    assert_eq!(body["columns"][0]["catalog_type"], "INT64");
}

#[tokio::test]
async fn meta_null_when_absent() {
    let backend = SourceDetailMockBackend {
        node_batches: vec![make_source_node_batch(
            "source.pkg.src.t",
            "t",
            None,
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
        freshness_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_source(State(state), Path("source.pkg.src.t".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn meta_null_when_malformed() {
    // Malformed JSON must serialise as null, NOT bubble a parse error to the client.
    // This protects against partial / corrupted writes in the parquet index.
    let backend = SourceDetailMockBackend {
        node_batches: vec![make_source_node_batch(
            "source.pkg.src.t",
            "t",
            None,
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
        freshness_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_source(State(state), Path("source.pkg.src.t".to_owned())).await;
    assert_eq!(r.status(), 200, "malformed meta must not 500 the response");
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn freshness_null_when_view_absent() {
    let backend = SourceDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        // None = view absent at query time.
        freshness_batches: None,
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_source(
        State(state),
        Path("source.jaffle_shop.raw_jaffle.orders".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(
        body["freshness"],
        serde_json::Value::Null,
        "freshness must be null when the parquet view is absent"
    );
}

#[tokio::test]
async fn freshness_null_when_no_row_for_source() {
    // View present but no row for this source — equivalent semantics from
    // the FE perspective: freshness is null.
    let backend = SourceDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        freshness_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_source(
        State(state),
        Path("source.jaffle_shop.raw_jaffle.orders".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["freshness"], serde_json::Value::Null);
}

#[tokio::test]
async fn catalog_null_when_view_absent() {
    let backend = SourceDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        freshness_batches: Some(vec![]),
        catalog_batches: None,
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_source(
        State(state),
        Path("source.jaffle_shop.raw_jaffle.orders".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["catalog"], serde_json::Value::Null);
}

#[tokio::test]
async fn catalog_stats_empty_array_when_no_rows() {
    // catalog_tables has a row but catalog_stats is empty: catalog.stats=[],
    // catalog itself is present. This is the common state for projects that
    // ran `dbt docs generate` against an adapter that doesn't emit stats.
    let backend = SourceDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        freshness_batches: Some(vec![]),
        catalog_batches: Some(vec![catalog_batch(Some("table"), Some("svc"), None)]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_source(
        State(state),
        Path("source.jaffle_shop.raw_jaffle.orders".to_owned()),
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
    let backend = SourceDetailMockBackend {
        node_batches: vec![make_source_node_batch(
            "source.pkg.src.t",
            "t",
            None,
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
        freshness_batches: Some(vec![]),
        catalog_batches: Some(vec![catalog_batch(Some("table"), None, None)]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_source(State(state), Path("source.pkg.src.t".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["catalog"]["primary_key"], serde_json::json!([]));
}

#[tokio::test]
async fn referenced_by_empty_array_when_no_downstream() {
    let backend = SourceDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        freshness_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_source(
        State(state),
        Path("source.jaffle_shop.raw_jaffle.orders".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["referenced_by"], serde_json::json!([]));
    assert!(body.get("depends_on").is_none());
}

#[tokio::test]
async fn freshness_criteria_null_when_no_thresholds() {
    // A source with `loaded_at_field` set but no warn/error thresholds in YAML
    // still produces a freshness row (status, loaded timestamps populated)
    // but all four threshold columns are null. The handler must emit
    // `criteria: null`, not `criteria: { error_after: null, warn_after: null }`.
    let backend = SourceDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        freshness_batches: Some(vec![freshness_batch(
            "pass",
            Some("2026-05-15 10:00:00"),
            Some("2026-05-15 09:45:00"),
            Some(900.0),
            None,
            None,
            None,
            None,
        )]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_source(
        State(state),
        Path("source.jaffle_shop.raw_jaffle.orders".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["freshness"]["status"], "pass");
    assert_eq!(body["freshness"]["criteria"], serde_json::Value::Null);
}
