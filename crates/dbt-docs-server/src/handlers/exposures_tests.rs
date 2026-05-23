//! Tests for `GET /api/v1/exposures/:id`.
//!
//! Schema anchoring (#10255): the `RecordBatch` fixtures below are hand-
//! rolled and not enforced against the production parquet schemas. A column
//! rename or type change in `dbt-index` will pass these tests while
//! silently breaking the handler. Once #10255 lands typed row builders,
//! replace these schemas to get compile-time coverage.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Float64Array, ListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use axum::extract::{Path, State};
use axum::response::Response;

use super::*;
use crate::providers::{Backend, BackendError, Providers};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Mock backend
// ---------------------------------------------------------------------------

/// Routes Arrow queries to the configured fixture batches. The exposure
/// handler issues a single query against `dbt.exposures`, so the mock only
/// needs one slot.
struct ExposureDetailMockBackend {
    batches: Vec<RecordBatch>,
}

impl Backend for ExposureDetailMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, _sql: &str) -> Option<String> {
        Some("0".to_owned())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        if sql.contains("dbt.exposures") {
            return Ok(self.batches.clone());
        }
        Err(BackendError::Query(format!("unrouted query: {sql}")))
    }
}

/// Build a test `AppState` with explicitly-set capability flags so unit
/// tests don't depend on probe semantics. Production goes through
/// `AppState::new` which probes the backend at startup.
fn make_state(backend: ExposureDetailMockBackend) -> Arc<AppState> {
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

/// Schema for `dbt.exposures` rows as queried by `EXPOSURE_DETAIL_SQL`.
/// Assumes `tags`/`fqn`/`depends_on_nodes` as `List(Utf8)`, `meta` as `Utf8`
/// holding a JSON string, `created_at` as `Float64`, scalar fields as
/// `Utf8`. Not compile-checked against the production schema (#10255).
fn exposure_schema(tags_field: &Field, fqn_field: &Field, depends_on_field: &Field) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("file_path", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("exposure_type", DataType::Utf8, true),
        Field::new("maturity", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
        Field::new("owner_name", DataType::Utf8, true),
        Field::new("owner_email", DataType::Utf8, true),
        Field::new("meta", DataType::Utf8, true),
        tags_field.clone(),
        fqn_field.clone(),
        depends_on_field.clone(),
        Field::new("created_at", DataType::Float64, true),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn make_exposure_batch(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    description: Option<&str>,
    label: Option<&str>,
    exposure_type: Option<&str>,
    maturity: Option<&str>,
    url: Option<&str>,
    owner_name: Option<&str>,
    owner_email: Option<&str>,
    meta_json: Option<&str>,
    tags: &[&str],
    fqn: &[&str],
    depends_on_nodes: &[&str],
    created_at: Option<f64>,
) -> RecordBatch {
    let tags_arr = make_str_list(tags);
    let fqn_arr = make_str_list(fqn);
    let deps_arr = make_str_list(depends_on_nodes);
    let tags_field = Field::new("tags", tags_arr.data_type().clone(), true);
    let fqn_field = Field::new("fqn", fqn_arr.data_type().clone(), true);
    let deps_field = Field::new("depends_on_nodes", deps_arr.data_type().clone(), true);

    RecordBatch::try_new(
        exposure_schema(&tags_field, &fqn_field, &deps_field),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![description])),
            Arc::new(StringArray::from(vec![Some("models/exposures.yml")])),
            Arc::new(StringArray::from(vec![Some("models/exposures.yml")])),
            Arc::new(StringArray::from(vec![label])),
            Arc::new(StringArray::from(vec![exposure_type])),
            Arc::new(StringArray::from(vec![maturity])),
            Arc::new(StringArray::from(vec![url])),
            Arc::new(StringArray::from(vec![owner_name])),
            Arc::new(StringArray::from(vec![owner_email])),
            Arc::new(StringArray::from(vec![meta_json])),
            Arc::new(tags_arr),
            Arc::new(fqn_arr),
            Arc::new(deps_arr),
            Arc::new(Float64Array::from(vec![created_at])),
        ],
    )
    .expect("valid exposure batch")
}

fn full_exposure_batch() -> RecordBatch {
    make_exposure_batch(
        "exposure.jaffle_shop.revenue_dashboard",
        "revenue_dashboard",
        Some("jaffle_shop"),
        Some("Top-line revenue dashboard used by the finance team."),
        Some("Revenue Dashboard"),
        Some("dashboard"),
        Some("high"),
        Some("https://bi.example.com/dashboards/revenue"),
        Some("Jane Doe"),
        Some("jane.doe@example.com"),
        Some(r#"{"team":"finance","priority":"high"}"#),
        &["finance", "exec"],
        &["jaffle_shop", "revenue_dashboard"],
        &[
            "model.jaffle_shop.orders",
            "source.jaffle_shop.raw_jaffle.orders",
        ],
        Some(1_747_432_300.5),
    )
}

async fn response_body(response: Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&bytes).expect("valid json")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn invalid_unique_id_returns_400() {
    let backend = ExposureDetailMockBackend { batches: vec![] };
    let state = make_state(backend);
    let r = get_exposure(State(state), Path("bad'id".to_owned())).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn missing_exposure_returns_404() {
    let backend = ExposureDetailMockBackend { batches: vec![] };
    let state = make_state(backend);
    let r = get_exposure(State(state), Path("exposure.x.y".to_owned())).await;
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn all_fields_hydrated() {
    let backend = ExposureDetailMockBackend {
        batches: vec![full_exposure_batch()],
    };
    let state = make_state(backend);
    let r = get_exposure(
        State(state),
        Path("exposure.jaffle_shop.revenue_dashboard".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    // NodeBase fields flatten into top-level.
    assert_eq!(body["unique_id"], "exposure.jaffle_shop.revenue_dashboard");
    assert_eq!(body["name"], "revenue_dashboard");
    assert_eq!(
        body["resource_type"], "exposure",
        "resource_type must be hardcoded since dbt.exposures has no such column"
    );
    assert_eq!(body["package_name"], "jaffle_shop");
    assert_eq!(
        body["description"],
        "Top-line revenue dashboard used by the finance team."
    );

    // Exposure-specific scalars.
    assert_eq!(body["label"], "Revenue Dashboard");
    assert_eq!(body["exposure_type"], "dashboard");
    assert_eq!(body["maturity"], "high");
    assert_eq!(body["url"], "https://bi.example.com/dashboards/revenue");
    assert_eq!(body["owner_name"], "Jane Doe");
    assert_eq!(body["owner_email"], "jane.doe@example.com");

    // List-typed fields surface as JSON arrays.
    assert_eq!(body["tags"], serde_json::json!(["finance", "exec"]));
    assert_eq!(
        body["fqn"],
        serde_json::json!(["jaffle_shop", "revenue_dashboard"])
    );

    // meta is parsed JSON, NOT an escaped string.
    assert_eq!(
        body["meta"],
        serde_json::json!({"team": "finance", "priority": "high"}),
        "meta must be parsed as JSON object, not escaped string"
    );

    // depends_on populated with edge_type derived from each unique_id prefix.
    assert_eq!(
        body["depends_on"],
        serde_json::json!([
            {"unique_id": "model.jaffle_shop.orders", "edge_type": "model"},
            {"unique_id": "source.jaffle_shop.raw_jaffle.orders", "edge_type": "source"},
        ])
    );

    // created_at populated as a float.
    assert_eq!(body["created_at"], 1_747_432_300.5);

    // referenced_by key must be ABSENT — exposures are leaf consumers.
    assert!(
        body.get("referenced_by").is_none(),
        "exposures must omit referenced_by entirely, not return []"
    );
}

#[tokio::test]
async fn meta_null_when_absent() {
    let backend = ExposureDetailMockBackend {
        batches: vec![make_exposure_batch(
            "exposure.pkg.e",
            "e",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None, // meta absent
            &[],
            &[],
            &[],
            None,
        )],
    };
    let state = make_state(backend);
    let r = get_exposure(State(state), Path("exposure.pkg.e".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn meta_null_when_malformed() {
    // Malformed JSON must serialise as null, NOT bubble a parse error to the client.
    // This protects against partial / corrupted writes in the parquet index.
    let backend = ExposureDetailMockBackend {
        batches: vec![make_exposure_batch(
            "exposure.pkg.e",
            "e",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("not{valid:json"),
            &[],
            &[],
            &[],
            None,
        )],
    };
    let state = make_state(backend);
    let r = get_exposure(State(state), Path("exposure.pkg.e".to_owned())).await;
    assert_eq!(r.status(), 200, "malformed meta must not 500 the response");
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn depends_on_empty_when_no_upstream() {
    let backend = ExposureDetailMockBackend {
        batches: vec![make_exposure_batch(
            "exposure.pkg.e",
            "e",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &[],
            &[],
            &[], // no upstream
            None,
        )],
    };
    let state = make_state(backend);
    let r = get_exposure(State(state), Path("exposure.pkg.e".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["depends_on"], serde_json::json!([]));
}

#[tokio::test]
async fn depends_on_edge_type_derived_from_prefix() {
    // Each upstream unique_id prefix should map to its resource type.
    let backend = ExposureDetailMockBackend {
        batches: vec![make_exposure_batch(
            "exposure.pkg.e",
            "e",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &[],
            &[],
            &[
                "model.pkg.m",
                "source.pkg.src.t",
                "metric.pkg.mt",
                "seed.pkg.s",
            ],
            None,
        )],
    };
    let state = make_state(backend);
    let r = get_exposure(State(state), Path("exposure.pkg.e".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(
        body["depends_on"],
        serde_json::json!([
            {"unique_id": "model.pkg.m", "edge_type": "model"},
            {"unique_id": "source.pkg.src.t", "edge_type": "source"},
            {"unique_id": "metric.pkg.mt", "edge_type": "metric"},
            {"unique_id": "seed.pkg.s", "edge_type": "seed"},
        ])
    );
}
