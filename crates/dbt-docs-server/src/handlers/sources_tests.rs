//! Tests for `GET /api/v1/sources/:id`, `GET /api/v1/sources`, and
//! `GET /api/v1/sources/facets`.
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

// ===========================================================================
// Tests: GET /api/v1/sources and GET /api/v1/sources/facets
// ===========================================================================
//
// TODO(#10255): replace hand-rolled RecordBatch schemas with typed row
// builders once dbt-index exposes fixture builders bound to the production
// parquet schema.

// ---------------------------------------------------------------------------
// Mock backend for list + facets
// ---------------------------------------------------------------------------

struct SourceListMockBackend {
    total_count: u64,
    row_batches: Vec<RecordBatch>,
    /// When `true`, the WITH-freshness count query returns `None` to trigger
    /// the no-join fallback path.
    freshness_view_absent: bool,
    db_facet_batches: Vec<RecordBatch>,
    schema_facet_batches: Vec<RecordBatch>,
}

impl SourceListMockBackend {
    fn new(total: u64, rows: Vec<RecordBatch>) -> Self {
        Self {
            total_count: total,
            row_batches: rows,
            freshness_view_absent: false,
            db_facet_batches: vec![],
            schema_facet_batches: vec![],
        }
    }
    fn without_freshness(mut self) -> Self {
        self.freshness_view_absent = true;
        self
    }
    fn with_db_facets(mut self, batches: Vec<RecordBatch>) -> Self {
        self.db_facet_batches = batches;
        self
    }
    fn with_schema_facets(mut self, batches: Vec<RecordBatch>) -> Self {
        self.schema_facet_batches = batches;
        self
    }
}

impl Backend for SourceListMockBackend {
    fn is_available(&self) -> bool {
        true
    }
    fn query_scalar(&self, sql: &str) -> Option<String> {
        if !sql.contains("count(*)") {
            return None;
        }
        if self.freshness_view_absent && sql.contains("source_freshness") {
            return None;
        }
        Some(self.total_count.to_string())
    }
    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        if sql.contains("database_name AS value") {
            return Ok(self.db_facet_batches.clone());
        }
        if sql.contains("schema_name AS value") {
            return Ok(self.schema_facet_batches.clone());
        }
        Ok(self.row_batches.clone())
    }
}

fn make_list_state(backend: SourceListMockBackend) -> Arc<AppState> {
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

fn source_list_schema(tags_field: &Field) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("resource_type", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("source_name", DataType::Utf8, true),
        Field::new("source_description", DataType::Utf8, true),
        Field::new("database_name", DataType::Utf8, true),
        Field::new("schema_name", DataType::Utf8, true),
        Field::new("identifier", DataType::Utf8, true),
        Field::new("loader", DataType::Utf8, true),
        tags_field.clone(),
        Field::new("freshness_status", DataType::Utf8, true),
        Field::new("freshness_snapshotted_at", DataType::Utf8, true),
        Field::new("freshness_max_loaded_at", DataType::Utf8, true),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn source_list_row(
    unique_id: &str,
    name: &str,
    database_name: Option<&str>,
    schema_name: Option<&str>,
    tags: &[&str],
    freshness_status: Option<&str>,
    snapshotted_at: Option<&str>,
    max_loaded_at: Option<&str>,
) -> RecordBatch {
    let tags_arr = make_str_list(tags);
    let tags_field = Field::new("tags", tags_arr.data_type().clone(), true);
    RecordBatch::try_new(
        source_list_schema(&tags_field),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec!["source"])),
            Arc::new(StringArray::from(vec![Some("jaffle_shop")])),
            Arc::new(StringArray::from(vec![Some("raw_jaffle")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![database_name])),
            Arc::new(StringArray::from(vec![schema_name])),
            Arc::new(StringArray::from(vec![Some(name)])),
            Arc::new(StringArray::from(vec![Some("fivetran")])),
            Arc::new(tags_arr),
            Arc::new(StringArray::from(vec![freshness_status])),
            Arc::new(StringArray::from(vec![snapshotted_at])),
            Arc::new(StringArray::from(vec![max_loaded_at])),
        ],
    )
    .expect("valid source list row batch")
}

fn facet_value_batch(values: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.to_vec()))])
        .expect("valid facet batch")
}

// ---------------------------------------------------------------------------
// Unit tests: SQL builders
// ---------------------------------------------------------------------------

#[test]
fn sort_param_returns_err() {
    let params = SourceListParams {
        sort: Some("name:asc".into()),
        ..Default::default()
    };
    assert!(build_source_list_sql(&params, true, 10, None).is_err());
}

#[test]
fn invalid_freshness_status_returns_err() {
    let params = SourceListParams {
        freshness_status: Some("stale".into()),
        ..Default::default()
    };
    assert!(build_source_list_sql(&params, true, 10, None).is_err());
}

#[test]
fn freshness_predicate_runtime_error_maps_to_sql_space() {
    let pred = build_freshness_predicate("runtime_error", true)
        .unwrap()
        .unwrap();
    assert!(pred.contains("'runtime error'"));
}

#[test]
fn freshness_predicate_no_data_with_join_uses_null_check() {
    let pred = build_freshness_predicate("no_data", true).unwrap().unwrap();
    assert!(pred.contains("sf.unique_id IS NULL"));
}

#[test]
fn freshness_predicate_no_data_without_join_is_vacuous() {
    assert!(
        build_freshness_predicate("no_data", false)
            .unwrap()
            .is_none()
    );
}

#[test]
fn freshness_predicate_specific_status_without_join_is_impossible() {
    let pred = build_freshness_predicate("warn", false).unwrap().unwrap();
    assert_eq!(pred, "1=0");
}

#[test]
fn count_sql_excludes_cursor_page_sql_includes_cursor() {
    let c = Cursor {
        sort_value: Some("orders".into()),
        unique_id: "source.pkg.src.orders".into(),
    };
    let params = SourceListParams::default();
    let (count, rows) = build_source_list_sql(&params, true, 10, Some(&c)).unwrap();
    assert!(
        !count.contains("orders"),
        "count must exclude cursor predicate"
    );
    assert!(
        rows.contains("orders"),
        "rows must include cursor predicate"
    );
}

// ---------------------------------------------------------------------------
// Integration tests: list_sources
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_sources_empty_catalog() {
    let state = make_list_state(SourceListMockBackend::new(0, vec![]));
    let r = list_sources(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"], serde_json::json!([]));
    assert_eq!(body["page_info"]["total_count"], 0);
    assert_eq!(body["page_info"]["has_next_page"], false);
    assert_eq!(body["page_info"]["start_cursor"], serde_json::Value::Null);
    assert_eq!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_sources_all_fields_hydrated() {
    let row = source_list_row(
        "source.jaffle_shop.raw_jaffle.orders",
        "orders",
        Some("raw"),
        Some("jaffle_shop"),
        &["tag_a"],
        Some("pass"),
        Some("2026-05-19T10:00:00Z"),
        Some("2026-05-19T09:45:00Z"),
    );
    let state = make_list_state(SourceListMockBackend::new(1, vec![row]));
    let r = list_sources(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(
        body["data"][0]["unique_id"],
        "source.jaffle_shop.raw_jaffle.orders"
    );
    assert_eq!(body["data"][0]["name"], "orders");
    assert_eq!(body["data"][0]["resource_type"], "source");
    assert_eq!(body["data"][0]["database_name"], "raw");
    assert_eq!(body["data"][0]["schema_name"], "jaffle_shop");
    assert_eq!(body["data"][0]["loader"], "fivetran");
    assert_eq!(body["data"][0]["tags"], serde_json::json!(["tag_a"]));
    assert_eq!(body["data"][0]["freshness"]["status"], "pass");
    assert_eq!(
        body["data"][0]["freshness"]["snapshotted_at"],
        "2026-05-19T10:00:00Z"
    );
    assert!(
        body["data"][0]["freshness"].get("criteria").is_none(),
        "list freshness must not expose criteria"
    );
    assert_eq!(body["page_info"]["total_count"], 1);
}

#[tokio::test]
async fn list_sources_freshness_null_when_view_absent() {
    let row = source_list_row("source.pkg.src.t", "t", None, None, &[], None, None, None);
    let state = make_list_state(SourceListMockBackend::new(1, vec![row]).without_freshness());
    let r = list_sources(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["freshness"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_sources_sort_param_returns_400() {
    let state = make_list_state(SourceListMockBackend::new(0, vec![]));
    let params = SourceListParams {
        sort: Some("name:asc".into()),
        ..Default::default()
    };
    let r = list_sources(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_sources_invalid_freshness_status_returns_400() {
    let state = make_list_state(SourceListMockBackend::new(0, vec![]));
    let params = SourceListParams {
        freshness_status: Some("stale".into()),
        ..Default::default()
    };
    let r = list_sources(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_sources_invalid_cursor_returns_400() {
    let state = make_list_state(SourceListMockBackend::new(0, vec![]));
    let params = SourceListParams {
        after: Some("not-valid-base64!!!".into()),
        ..Default::default()
    };
    let r = list_sources(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_sources_multi_page_has_next_page_true() {
    let row_a = source_list_row(
        "source.pkg.src.alpha",
        "alpha",
        Some("db"),
        Some("sc"),
        &[],
        None,
        None,
        None,
    );
    let row_b = source_list_row(
        "source.pkg.src.beta",
        "beta",
        Some("db"),
        Some("sc"),
        &[],
        None,
        None,
        None,
    );
    let state = make_list_state(SourceListMockBackend::new(2, vec![row_a, row_b]));
    let params = SourceListParams {
        first: Some(1),
        ..Default::default()
    };
    let r = list_sources(State(state), Query(params)).await;
    let body = response_body(r).await;
    assert_eq!(body["page_info"]["has_next_page"], true);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_ne!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_sources_last_page_end_cursor_null() {
    let row = source_list_row("source.pkg.src.z", "z", None, None, &[], None, None, None);
    let state = make_list_state(SourceListMockBackend::new(1, vec![row]));
    let r = list_sources(State(state), Query(Default::default())).await;
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
async fn list_sources_cursor_advances_page() {
    let after = Cursor {
        sort_value: Some("alpha".into()),
        unique_id: "source.pkg.src.alpha".into(),
    }
    .encode();
    let row = source_list_row(
        "source.pkg.src.beta",
        "beta",
        None,
        None,
        &[],
        None,
        None,
        None,
    );
    let state = make_list_state(SourceListMockBackend::new(1, vec![row]));
    let params = SourceListParams {
        after: Some(after),
        ..Default::default()
    };
    let r = list_sources(State(state), Query(params)).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["name"], "beta");
}

#[tokio::test]
async fn list_sources_tags_extracted_per_row() {
    let row_a = source_list_row(
        "source.pkg.src.a",
        "a",
        None,
        None,
        &["tag_a1", "tag_a2"],
        None,
        None,
        None,
    );
    let row_b = source_list_row(
        "source.pkg.src.b",
        "b",
        None,
        None,
        &["tag_b1"],
        None,
        None,
        None,
    );
    let state = make_list_state(SourceListMockBackend::new(2, vec![row_a, row_b]));
    let r = list_sources(State(state), Query(Default::default())).await;
    let body = response_body(r).await;
    assert_eq!(
        body["data"][0]["tags"],
        serde_json::json!(["tag_a1", "tag_a2"])
    );
    assert_eq!(body["data"][1]["tags"], serde_json::json!(["tag_b1"]));
}

// ---------------------------------------------------------------------------
// Integration tests: list_source_facets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_source_facets_freshness_status_is_static() {
    let state = make_list_state(SourceListMockBackend::new(0, vec![]));
    let r = list_source_facets(State(state)).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    let statuses: Vec<&str> = body["freshness_status"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["value"].as_str().unwrap())
        .collect();
    assert_eq!(
        statuses,
        vec!["no_data", "pass", "warn", "error", "runtime_error"]
    );
}

#[tokio::test]
async fn list_source_facets_all_counts_null() {
    let state = make_list_state(SourceListMockBackend::new(0, vec![]));
    let r = list_source_facets(State(state)).await;
    let body = response_body(r).await;
    for v in body["freshness_status"].as_array().unwrap() {
        assert_eq!(v["count"], serde_json::Value::Null);
    }
}

#[tokio::test]
async fn list_source_facets_empty_databases_on_empty_catalog() {
    let state = make_list_state(SourceListMockBackend::new(0, vec![]));
    let r = list_source_facets(State(state)).await;
    let body = response_body(r).await;
    assert_eq!(body["databases"], serde_json::json!([]));
    assert_eq!(body["schemas"], serde_json::json!([]));
}

#[tokio::test]
async fn list_source_facets_databases_and_schemas_populated() {
    let state = make_list_state(
        SourceListMockBackend::new(0, vec![])
            .with_db_facets(vec![facet_value_batch(&["raw", "analytics"])])
            .with_schema_facets(vec![facet_value_batch(&["jaffle_shop", "stripe"])]),
    );
    let r = list_source_facets(State(state)).await;
    let body = response_body(r).await;
    let dbs: Vec<&str> = body["databases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["value"].as_str().unwrap())
        .collect();
    assert_eq!(dbs, vec!["raw", "analytics"]);
    let schemas: Vec<&str> = body["schemas"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["value"].as_str().unwrap())
        .collect();
    assert_eq!(schemas, vec!["jaffle_shop", "stripe"]);
}
