//! Tests for `GET /api/v1/seeds/:id`, `GET /api/v1/seeds`, and
//! `GET /api/v1/seeds/facets`.
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
        has_dbt_state: false,
        index_dir: PathBuf::from("/tmp"),
        project_loaded: false,
        generation: None,
        providers,
        do_not_track: false,
        send_anonymous_usage_stats: true,
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

// ===========================================================================
// Tests: GET /api/v1/seeds and GET /api/v1/seeds/facets
// ===========================================================================
//
// TODO(#10255): replace hand-rolled RecordBatch schemas with typed row
// builders once dbt-index exposes fixture builders bound to the production
// parquet schema.

// ---------------------------------------------------------------------------
// Mock backend for list + facets
// ---------------------------------------------------------------------------

struct SeedListMockBackend {
    total_count: u64,
    row_batches: Vec<RecordBatch>,
    /// When `true`, the with-run_results count query returns `None` to trigger
    /// the no-run_results fallback path.
    run_results_absent: bool,
    /// When `true`, the with-catalog_stats query also returns `None`.
    catalog_stats_absent: bool,
}

impl SeedListMockBackend {
    fn new(total: u64, rows: Vec<RecordBatch>) -> Self {
        Self {
            total_count: total,
            row_batches: rows,
            run_results_absent: false,
            catalog_stats_absent: false,
        }
    }
    fn without_run_results(mut self) -> Self {
        self.run_results_absent = true;
        self
    }
    fn without_catalog_stats(mut self) -> Self {
        self.catalog_stats_absent = true;
        self
    }
}

impl Backend for SeedListMockBackend {
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
        if self.catalog_stats_absent && sql.contains("catalog_stats") {
            return None;
        }
        Some(self.total_count.to_string())
    }
    fn query_arrow(&self, _sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        Ok(self.row_batches.clone())
    }
}

fn make_list_state(backend: SeedListMockBackend) -> Arc<AppState> {
    let providers = Providers {
        backend: Arc::new(backend),
        ..Providers::default()
    };
    Arc::new(AppState {
        has_dbt_state: false,
        index_dir: PathBuf::from("/tmp"),
        project_loaded: false,
        generation: None,
        providers,
        do_not_track: false,
        send_anonymous_usage_stats: true,
    })
}

// ---------------------------------------------------------------------------
// Batch builders for list
// ---------------------------------------------------------------------------

fn seed_list_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("resource_type", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("row_count", DataType::Int64, true),
        Field::new("executed_at", DataType::Utf8, true),
    ]))
}

fn seed_list_row(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    description: Option<&str>,
    original_file_path: Option<&str>,
    row_count: Option<i64>,
    executed_at: Option<&str>,
) -> RecordBatch {
    RecordBatch::try_new(
        seed_list_schema(),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec!["seed"])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![description])),
            Arc::new(StringArray::from(vec![original_file_path])),
            Arc::new(Int64Array::from(vec![row_count])),
            Arc::new(StringArray::from(vec![executed_at])),
        ],
    )
    .expect("valid seed list row batch")
}

// ---------------------------------------------------------------------------
// Unit tests: SQL builders
// ---------------------------------------------------------------------------

#[test]
fn unknown_sort_column_returns_err() {
    let params = SeedListParams {
        sort: Some("tags:asc".into()),
        ..Default::default()
    };
    assert!(build_seed_list_sql(&params, true, true, 10, None).is_err());
}

#[test]
fn sort_without_direction_returns_err() {
    let params = SeedListParams {
        sort: Some("name".into()),
        ..Default::default()
    };
    assert!(build_seed_list_sql(&params, true, true, 10, None).is_err());
}

#[test]
fn sort_invalid_direction_returns_err() {
    let params = SeedListParams {
        sort: Some("name:random".into()),
        ..Default::default()
    };
    assert!(build_seed_list_sql(&params, true, true, 10, None).is_err());
}

#[test]
fn allowlisted_sort_columns_succeed() {
    for col in &["name", "row_count", "executed_at"] {
        let params = SeedListParams {
            sort: Some(format!("{col}:asc")),
            ..Default::default()
        };
        assert!(
            build_seed_list_sql(&params, true, true, 10, None).is_ok(),
            "expected ok for sort={col}:asc"
        );
        let params_desc = SeedListParams {
            sort: Some(format!("{col}:desc")),
            ..Default::default()
        };
        assert!(
            build_seed_list_sql(&params_desc, true, true, 10, None).is_ok(),
            "expected ok for sort={col}:desc"
        );
    }
}

#[test]
fn count_sql_excludes_cursor_page_sql_includes_cursor() {
    let c = Cursor {
        sort_value: Some("raw_customers".into()),
        unique_id: "seed.pkg.raw_customers".into(),
    };
    let params = SeedListParams::default();
    let (count, rows) = build_seed_list_sql(&params, true, true, 10, Some(&c)).unwrap();
    assert!(
        !count.contains("raw_customers"),
        "count must exclude cursor predicate"
    );
    assert!(
        rows.contains("raw_customers"),
        "rows must include cursor predicate"
    );
}

#[test]
fn no_run_results_flag_emits_null_executed_at() {
    let params = SeedListParams::default();
    let (_, rows) = build_seed_list_sql(&params, false, false, 10, None).unwrap();
    assert!(
        rows.contains("NULL::VARCHAR AS executed_at"),
        "fallback must emit NULL executed_at"
    );
}

#[test]
fn no_catalog_stats_flag_emits_null_row_count() {
    let params = SeedListParams::default();
    let (_, rows) = build_seed_list_sql(&params, false, false, 10, None).unwrap();
    assert!(
        rows.contains("NULL::BIGINT AS row_count"),
        "fallback must emit NULL row_count"
    );
}

// ---------------------------------------------------------------------------
// Integration tests: list_seeds
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_seeds_empty_catalog() {
    let state = make_list_state(SeedListMockBackend::new(0, vec![]));
    let r = list_seeds(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"], serde_json::json!([]));
    assert_eq!(body["page_info"]["total_count"], 0);
    assert_eq!(body["page_info"]["has_next_page"], false);
    assert_eq!(body["page_info"]["start_cursor"], serde_json::Value::Null);
    assert_eq!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_seeds_all_fields_hydrated() {
    let row = seed_list_row(
        "seed.jaffle_shop.raw_customers",
        "raw_customers",
        Some("jaffle_shop"),
        Some("Raw customer seed file loaded from CSV."),
        Some("seeds/raw_customers.csv"),
        Some(935),
        Some("2026-05-15T10:28:03Z"),
    );
    let state = make_list_state(SeedListMockBackend::new(1, vec![row]));
    let r = list_seeds(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(
        body["data"][0]["unique_id"],
        "seed.jaffle_shop.raw_customers"
    );
    assert_eq!(body["data"][0]["name"], "raw_customers");
    assert_eq!(body["data"][0]["resource_type"], "seed");
    assert_eq!(body["data"][0]["package_name"], "jaffle_shop");
    assert_eq!(
        body["data"][0]["description"],
        "Raw customer seed file loaded from CSV."
    );
    assert_eq!(
        body["data"][0]["original_file_path"],
        "seeds/raw_customers.csv"
    );
    assert_eq!(body["data"][0]["row_count"], 935);
    assert_eq!(body["data"][0]["executed_at"], "2026-05-15T10:28:03Z");
    assert_eq!(body["page_info"]["total_count"], 1);
}

#[tokio::test]
async fn list_seeds_executed_at_null_when_run_results_absent() {
    let row = seed_list_row(
        "seed.pkg.t",
        "t",
        None,
        None,
        None,
        None,
        None, // executed_at null
    );
    let state = make_list_state(SeedListMockBackend::new(1, vec![row]).without_run_results());
    let r = list_seeds(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["executed_at"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_seeds_row_count_null_when_catalog_stats_absent() {
    let row = seed_list_row(
        "seed.pkg.t",
        "t",
        None,
        None,
        None,
        None, // row_count null
        None,
    );
    let state = make_list_state(
        SeedListMockBackend::new(1, vec![row])
            .without_run_results()
            .without_catalog_stats(),
    );
    let r = list_seeds(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["row_count"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_seeds_unknown_sort_returns_400() {
    let state = make_list_state(SeedListMockBackend::new(0, vec![]));
    let params = SeedListParams {
        sort: Some("database_name:asc".into()),
        ..Default::default()
    };
    let r = list_seeds(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_seeds_invalid_cursor_returns_400() {
    let state = make_list_state(SeedListMockBackend::new(0, vec![]));
    let params = SeedListParams {
        after: Some("not-valid-base64!!!".into()),
        ..Default::default()
    };
    let r = list_seeds(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_seeds_multi_page_has_next_page_true() {
    let row_a = seed_list_row("seed.pkg.alpha", "alpha", None, None, None, None, None);
    let row_b = seed_list_row("seed.pkg.beta", "beta", None, None, None, None, None);
    let state = make_list_state(SeedListMockBackend::new(2, vec![row_a, row_b]));
    let params = SeedListParams {
        first: Some(1),
        ..Default::default()
    };
    let r = list_seeds(State(state), Query(params)).await;
    let body = response_body(r).await;
    assert_eq!(body["page_info"]["has_next_page"], true);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_ne!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_seeds_last_page_end_cursor_null() {
    let row = seed_list_row("seed.pkg.z", "z", None, None, None, None, None);
    let state = make_list_state(SeedListMockBackend::new(1, vec![row]));
    let r = list_seeds(State(state), Query(Default::default())).await;
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
async fn list_seeds_cursor_advances_page() {
    let after = Cursor {
        sort_value: Some("alpha".into()),
        unique_id: "seed.pkg.alpha".into(),
    }
    .encode();
    let row = seed_list_row("seed.pkg.beta", "beta", None, None, None, None, None);
    let state = make_list_state(SeedListMockBackend::new(1, vec![row]));
    let params = SeedListParams {
        after: Some(after),
        ..Default::default()
    };
    let r = list_seeds(State(state), Query(params)).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["name"], "beta");
}

// ---------------------------------------------------------------------------
// Integration tests: list_seed_facets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_seed_facets_returns_empty_object() {
    let r = list_seed_facets().await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(
        body,
        serde_json::json!({}),
        "seeds facets must be an empty JSON object per contract"
    );
}
