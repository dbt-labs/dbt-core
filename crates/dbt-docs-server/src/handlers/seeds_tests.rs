//! Tests for `GET /api/v1/seeds/:id`.
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
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Mock backend
// ---------------------------------------------------------------------------

/// Routes Arrow queries to the configured fixture batches based on which
/// `FROM` table the SQL mentions. Each optional fixture (run_results,
/// catalog, catalog_stats) doubles as the "view absent" knob: `None` → the
/// handler catches the error and renders the surface as JSON `null` / `[]`.
struct SeedDetailMockBackend {
    node_batches: Vec<RecordBatch>,
    column_batches: Vec<RecordBatch>,
    edge_batches: Vec<RecordBatch>,
    /// `Some(batches)` → query succeeds; `None` → view absent (query error).
    run_result_batches: Option<Vec<RecordBatch>>,
    /// `Some(batches)` → query succeeds; `None` → view absent (query error).
    catalog_batches: Option<Vec<RecordBatch>>,
    /// `Some(batches)` → query succeeds; `None` → view absent (query error).
    catalog_stats_batches: Option<Vec<RecordBatch>>,
}

impl Backend for SeedDetailMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, _sql: &str) -> Option<String> {
        Some("0".to_owned())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        // Order matters: more specific tables before more generic. `dbt.nodes`
        // is the fallback because it's the most general table the seed
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
fn make_state(backend: SeedDetailMockBackend) -> Arc<AppState> {
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

/// Schema for `dbt.nodes` rows as queried by `SEED_DETAIL_NODE_SQL`.
/// Assumes `tags`/`fqn` as `List(Utf8)`, `meta` as `Utf8` holding a JSON
/// string, scalar fields as `Utf8`. Not compile-checked against the
/// production schema (#10255).
fn seed_node_schema(tags_field: &Field, fqn_field: &Field) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("resource_type", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("file_path", DataType::Utf8, true),
        Field::new("patch_path", DataType::Utf8, true),
        Field::new("database_name", DataType::Utf8, true),
        Field::new("schema_name", DataType::Utf8, true),
        Field::new("identifier", DataType::Utf8, true),
        Field::new("meta", DataType::Utf8, true),
        tags_field.clone(),
        fqn_field.clone(),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn make_seed_node_batch(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    description: Option<&str>,
    patch_path: Option<&str>,
    identifier: Option<&str>,
    meta_json: Option<&str>,
    tags: &[&str],
    fqn: &[&str],
) -> RecordBatch {
    let tags_arr = make_str_list(tags);
    let fqn_arr = make_str_list(fqn);
    let tags_field = Field::new("tags", tags_arr.data_type().clone(), true);
    let fqn_field = Field::new("fqn", fqn_arr.data_type().clone(), true);

    RecordBatch::try_new(
        seed_node_schema(&tags_field, &fqn_field),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec!["seed"])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![description])),
            Arc::new(StringArray::from(vec![Some("seeds/raw_customers.csv")])),
            Arc::new(StringArray::from(vec![Some("seeds/raw_customers.csv")])),
            Arc::new(StringArray::from(vec![patch_path])),
            Arc::new(StringArray::from(vec![Some("prod")])),
            Arc::new(StringArray::from(vec![Some("dbt_prod")])),
            Arc::new(StringArray::from(vec![identifier])),
            Arc::new(StringArray::from(vec![meta_json])),
            Arc::new(tags_arr),
            Arc::new(fqn_arr),
        ],
    )
    .expect("valid seed node batch")
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

/// Build a run_results fixture batch matching `SEED_DETAIL_RUN_RESULT_SQL`.
fn run_result_batch(
    status: &str,
    execution_time: Option<f64>,
    completed_at: Option<&str>,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("status", DataType::Utf8, true),
        Field::new("execution_time", DataType::Float64, true),
        Field::new("completed_at", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some(status)])),
            Arc::new(Float64Array::from(vec![execution_time])),
            Arc::new(StringArray::from(vec![completed_at])),
        ],
    )
    .expect("valid run_result batch")
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
    make_seed_node_batch(
        "seed.jaffle_shop.raw_customers",
        "raw_customers",
        Some("jaffle_shop"),
        Some("Raw customer seed file loaded from CSV."),
        Some("seeds/_schema.yml"),
        Some("raw_customers"),
        Some(r#"{"owner":"data-eng"}"#),
        &["raw", "seed"],
        &["jaffle_shop", "raw_customers"],
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn invalid_unique_id_returns_400() {
    let backend = SeedDetailMockBackend {
        node_batches: vec![],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_seed(State(state), Path("bad'id".to_owned())).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn missing_seed_returns_404() {
    let backend = SeedDetailMockBackend {
        node_batches: vec![],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_seed(State(state), Path("seed.x.y".to_owned())).await;
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn all_fields_hydrated() {
    let backend = SeedDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![column_batch_one("id", Some("integer"), Some("INT64"))],
        edge_batches: vec![edge_batch(&[("model.jaffle_shop.stg_customers", "ref")])],
        run_result_batches: Some(vec![run_result_batch(
            "success",
            Some(1.8),
            Some("2026-05-15 10:28:03"),
        )]),
        catalog_batches: Some(vec![catalog_batch(Some("table"), Some("dbt_runner"))]),
        catalog_stats_batches: Some(vec![catalog_stats_batch(&[(
            "has_stats",
            "Has Stats?",
            "true",
            "Indicates whether there are statistics for this table",
            false,
        )])]),
    };
    let state = make_state(backend);
    let r = get_seed(
        State(state),
        Path("seed.jaffle_shop.raw_customers".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    // NodeBase fields flatten into top-level.
    assert_eq!(body["unique_id"], "seed.jaffle_shop.raw_customers");
    assert_eq!(body["name"], "raw_customers");
    assert_eq!(body["resource_type"], "seed");
    assert_eq!(body["package_name"], "jaffle_shop");
    assert_eq!(
        body["description"],
        "Raw customer seed file loaded from CSV."
    );

    // Seed-specific scalars.
    assert_eq!(body["identifier"], "raw_customers");
    assert_eq!(body["patch_path"], "seeds/_schema.yml");
    assert_eq!(body["database_name"], "prod");
    assert_eq!(body["schema_name"], "dbt_prod");

    // List-typed fields surface as JSON arrays.
    assert_eq!(body["tags"], serde_json::json!(["raw", "seed"]));
    assert_eq!(
        body["fqn"],
        serde_json::json!(["jaffle_shop", "raw_customers"])
    );

    // meta is parsed JSON, NOT an escaped string.
    assert_eq!(
        body["meta"],
        serde_json::json!({"owner": "data-eng"}),
        "meta must be parsed as JSON object, not escaped string"
    );

    // referenced_by populated; depends_on key must be ABSENT (not empty array).
    assert_eq!(
        body["referenced_by"][0]["unique_id"],
        "model.jaffle_shop.stg_customers"
    );
    assert_eq!(body["referenced_by"][0]["edge_type"], "ref");
    assert!(
        body.get("depends_on").is_none(),
        "seeds must omit depends_on entirely, not return []"
    );

    // execution_info populated.
    assert_eq!(body["execution_info"]["status"], "success");
    assert_eq!(body["execution_info"]["execution_time"], 1.8);
    assert_eq!(
        body["execution_info"]["completed_at"],
        "2026-05-15 10:28:03"
    );

    // Catalog populated; stats[0] populated.
    assert_eq!(body["catalog"]["type"], "table");
    assert_eq!(body["catalog"]["owner"], "dbt_runner");
    assert_eq!(body["catalog"]["stats"][0]["id"], "has_stats");
    assert_eq!(body["catalog"]["stats"][0]["value"], "true");
    assert_eq!(body["catalog"]["stats"][0]["include"], false);
    // SeedCatalogInfo does not include comment or primary_key.
    assert!(
        body["catalog"].get("comment").is_none(),
        "seeds catalog must not include comment"
    );
    assert!(
        body["catalog"].get("primary_key").is_none(),
        "seeds catalog must not include primary_key"
    );

    // Columns echoed.
    assert_eq!(body["columns"][0]["name"], "id");
    assert_eq!(body["columns"][0]["catalog_type"], "INT64");
}

#[tokio::test]
async fn meta_null_when_absent() {
    let backend = SeedDetailMockBackend {
        node_batches: vec![make_seed_node_batch(
            "seed.pkg.t",
            "t",
            None,
            None,
            None,
            None,
            None, // meta absent
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
    let r = get_seed(State(state), Path("seed.pkg.t".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn meta_null_when_malformed() {
    // Malformed JSON must serialise as null, NOT bubble a parse error to the client.
    let backend = SeedDetailMockBackend {
        node_batches: vec![make_seed_node_batch(
            "seed.pkg.t",
            "t",
            None,
            None,
            None,
            None,
            Some("not{valid:json"),
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
    let r = get_seed(State(state), Path("seed.pkg.t".to_owned())).await;
    assert_eq!(r.status(), 200, "malformed meta must not 500 the response");
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn execution_info_null_when_view_absent() {
    let backend = SeedDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        // None = view absent at query time.
        run_result_batches: None,
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_seed(
        State(state),
        Path("seed.jaffle_shop.raw_customers".to_owned()),
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
async fn execution_info_null_when_no_row_for_seed() {
    // View present but no row for this seed — seed never ran.
    let backend = SeedDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_seed(
        State(state),
        Path("seed.jaffle_shop.raw_customers".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["execution_info"], serde_json::Value::Null);
}

#[tokio::test]
async fn catalog_null_when_view_absent() {
    let backend = SeedDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: None,
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_seed(
        State(state),
        Path("seed.jaffle_shop.raw_customers".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["catalog"], serde_json::Value::Null);
}

#[tokio::test]
async fn catalog_stats_empty_array_when_no_rows() {
    // catalog_tables has a row but catalog_stats is empty: catalog.stats=[],
    // catalog itself is present.
    let backend = SeedDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: Some(vec![catalog_batch(Some("table"), Some("svc"))]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_seed(
        State(state),
        Path("seed.jaffle_shop.raw_customers".to_owned()),
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
async fn referenced_by_empty_array_when_no_downstream() {
    let backend = SeedDetailMockBackend {
        node_batches: vec![full_node_batch()],
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
        catalog_batches: Some(vec![]),
        catalog_stats_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_seed(
        State(state),
        Path("seed.jaffle_shop.raw_customers".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["referenced_by"], serde_json::json!([]));
    assert!(body.get("depends_on").is_none());
}
