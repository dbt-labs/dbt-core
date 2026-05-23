//! Tests for `GET /api/v1/metrics/:id`.
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

/// Routes Arrow queries based on the `FROM` table mentioned in the SQL.
struct MetricDetailMockBackend {
    row_batches: Vec<RecordBatch>,
    upstream_batches: Vec<RecordBatch>,
    downstream_batches: Vec<RecordBatch>,
}

impl Backend for MetricDetailMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, _sql: &str) -> Option<String> {
        Some("0".to_owned())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        if sql.contains("dbt.edges") {
            // child_unique_id = upstream lookup; parent_unique_id = downstream.
            if sql.contains("child_unique_id =") {
                return Ok(self.upstream_batches.clone());
            }
            return Ok(self.downstream_batches.clone());
        }
        if sql.contains("dbt.metrics") {
            return Ok(self.row_batches.clone());
        }
        Err(BackendError::Query(format!("unrouted query: {sql}")))
    }
}

fn make_state(backend: MetricDetailMockBackend) -> Arc<AppState> {
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

/// Schema for `dbt.metrics` rows as queried by `METRIC_DETAIL_ROW_SQL`.
fn metric_row_schema(
    input_metrics_field: &Field,
    fqn_field: &Field,
    tags_field: &Field,
) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("file_path", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("metric_type", DataType::Utf8, true),
        Field::new("type_params", DataType::Utf8, true),
        Field::new("metric_filter", DataType::Utf8, true),
        Field::new("time_granularity", DataType::Utf8, true),
        Field::new("semantic_model_name", DataType::Utf8, true),
        Field::new("group_name", DataType::Utf8, true),
        input_metrics_field.clone(),
        fqn_field.clone(),
        tags_field.clone(),
        Field::new("meta", DataType::Utf8, true),
        Field::new("created_at", DataType::Float64, true),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn make_metric_row_batch(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    description: Option<&str>,
    label: Option<&str>,
    metric_type: Option<&str>,
    type_params_json: Option<&str>,
    metric_filter_json: Option<&str>,
    time_granularity: Option<&str>,
    semantic_model_name: Option<&str>,
    group_name: Option<&str>,
    input_metric_names: &[&str],
    fqn: &[&str],
    tags: &[&str],
    meta_json: Option<&str>,
    created_at: Option<f64>,
) -> RecordBatch {
    let input_metrics_arr = make_str_list(input_metric_names);
    let fqn_arr = make_str_list(fqn);
    let tags_arr = make_str_list(tags);
    let input_metrics_field = Field::new(
        "input_metric_names",
        input_metrics_arr.data_type().clone(),
        true,
    );
    let fqn_field = Field::new("fqn", fqn_arr.data_type().clone(), true);
    let tags_field = Field::new("tags", tags_arr.data_type().clone(), true);

    RecordBatch::try_new(
        metric_row_schema(&input_metrics_field, &fqn_field, &tags_field),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![description])),
            Arc::new(StringArray::from(vec![Some("models/marts/metrics.yml")])),
            Arc::new(StringArray::from(vec![Some("models/marts/metrics.yml")])),
            Arc::new(StringArray::from(vec![label])),
            Arc::new(StringArray::from(vec![metric_type])),
            Arc::new(StringArray::from(vec![type_params_json])),
            Arc::new(StringArray::from(vec![metric_filter_json])),
            Arc::new(StringArray::from(vec![time_granularity])),
            Arc::new(StringArray::from(vec![semantic_model_name])),
            Arc::new(StringArray::from(vec![group_name])),
            Arc::new(input_metrics_arr),
            Arc::new(fqn_arr),
            Arc::new(tags_arr),
            Arc::new(StringArray::from(vec![meta_json])),
            Arc::new(Float64Array::from(vec![created_at])),
        ],
    )
    .expect("valid metric row batch")
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

async fn response_body(response: Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&bytes).expect("valid json")
}

fn full_metric_row() -> RecordBatch {
    make_metric_row_batch(
        "metric.jaffle_shop.total_revenue",
        "total_revenue",
        Some("jaffle_shop"),
        Some("Sum of order amounts across all completed orders."),
        Some("Total revenue"),
        Some("simple"),
        Some(r#"{"measure":{"name":"order_amount","alias":null,"filter":null}}"#),
        Some(
            r#"{"where_filters":[{"where_sql_template":"{{ Dimension('orders__status') }} = 'completed'"}]}"#,
        ),
        Some("day"),
        Some("orders"),
        Some("finance"),
        &[],
        &["jaffle_shop", "total_revenue"],
        &["finance"],
        Some(r#"{"owner":"data-eng"}"#),
        Some(1_747_432_300.5),
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn invalid_unique_id_returns_400() {
    let backend = MetricDetailMockBackend {
        row_batches: vec![],
        upstream_batches: vec![],
        downstream_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_metric(State(state), Path("bad'id".to_owned())).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn missing_metric_returns_404() {
    let backend = MetricDetailMockBackend {
        row_batches: vec![],
        upstream_batches: vec![],
        downstream_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_metric(State(state), Path("metric.x.missing".to_owned())).await;
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn all_fields_hydrated() {
    let backend = MetricDetailMockBackend {
        row_batches: vec![full_metric_row()],
        upstream_batches: vec![edge_batch(&[(
            "semantic_model.jaffle_shop.orders",
            "semantic_model",
        )])],
        downstream_batches: vec![edge_batch(&[(
            "saved_query.jaffle_shop.weekly_revenue",
            "saved_query",
        )])],
    };
    let state = make_state(backend);
    let r = get_metric(
        State(state),
        Path("metric.jaffle_shop.total_revenue".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    // NodeBase fields flatten into top-level; resource_type is hardcoded.
    assert_eq!(body["unique_id"], "metric.jaffle_shop.total_revenue");
    assert_eq!(body["name"], "total_revenue");
    assert_eq!(body["resource_type"], "metric");
    assert_eq!(body["package_name"], "jaffle_shop");

    // Metric-specific scalars.
    assert_eq!(body["label"], "Total revenue");
    assert_eq!(body["metric_type"], "simple");
    assert_eq!(body["time_granularity"], "day");
    assert_eq!(body["semantic_model_name"], "orders");
    assert_eq!(body["group_name"], "finance");

    // List columns as flat arrays.
    assert_eq!(body["tags"], serde_json::json!(["finance"]));
    assert_eq!(
        body["fqn"],
        serde_json::json!(["jaffle_shop", "total_revenue"])
    );
    assert_eq!(body["input_metric_names"], serde_json::json!([]));

    // JSON-string columns surface as parsed objects.
    assert_eq!(
        body["type_params"],
        serde_json::json!({
            "measure": {"name": "order_amount", "alias": null, "filter": null}
        }),
        "type_params must be parsed JSON, not escaped string"
    );
    assert_eq!(
        body["filter"],
        serde_json::json!({
            "where_filters": [
                {"where_sql_template": "{{ Dimension('orders__status') }} = 'completed'"}
            ]
        }),
        "filter must be parsed JSON object preserving where_filters[] shape"
    );
    assert_eq!(
        body["meta"],
        serde_json::json!({"owner": "data-eng"}),
        "meta must be parsed JSON object"
    );

    // Edges synthesised from dbt.edges.
    assert_eq!(
        body["depends_on"][0]["unique_id"],
        "semantic_model.jaffle_shop.orders"
    );
    assert_eq!(body["depends_on"][0]["edge_type"], "semantic_model");
    assert_eq!(
        body["referenced_by"][0]["unique_id"],
        "saved_query.jaffle_shop.weekly_revenue"
    );
    assert_eq!(body["referenced_by"][0]["edge_type"], "saved_query");

    // Timestamp surfaces as a number, not a string.
    assert_eq!(body["created_at"], 1_747_432_300.5);

    // Fields the contract excludes must be ABSENT, not null.
    assert!(body.get("execution_info").is_none());
    assert!(body.get("catalog").is_none());
    assert!(body.get("columns").is_none());
    assert!(body.get("compiled_code").is_none());
    assert!(body.get("raw_code").is_none());
}

#[tokio::test]
async fn meta_null_when_absent() {
    let row = make_metric_row_batch(
        "metric.pkg.m",
        "m",
        None,
        None,
        None,
        Some("simple"),
        None,
        None,
        None,
        None,
        None,
        &[],
        &[],
        &[],
        None, // meta absent
        None,
    );
    let backend = MetricDetailMockBackend {
        row_batches: vec![row],
        upstream_batches: vec![],
        downstream_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_metric(State(state), Path("metric.pkg.m".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn meta_null_when_malformed() {
    // Malformed JSON must serialise as null, NOT bubble a parse error.
    let row = make_metric_row_batch(
        "metric.pkg.m",
        "m",
        None,
        None,
        None,
        Some("simple"),
        None,
        None,
        None,
        None,
        None,
        &[],
        &[],
        &[],
        Some("not{valid:json"),
        None,
    );
    let backend = MetricDetailMockBackend {
        row_batches: vec![row],
        upstream_batches: vec![],
        downstream_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_metric(State(state), Path("metric.pkg.m".to_owned())).await;
    assert_eq!(r.status(), 200, "malformed meta must not 500 the response");
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn type_params_null_when_absent() {
    let row = make_metric_row_batch(
        "metric.pkg.m",
        "m",
        None,
        None,
        None,
        Some("simple"),
        None, // type_params absent
        None,
        None,
        None,
        None,
        &[],
        &[],
        &[],
        None,
        None,
    );
    let backend = MetricDetailMockBackend {
        row_batches: vec![row],
        upstream_batches: vec![],
        downstream_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_metric(State(state), Path("metric.pkg.m".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["type_params"], serde_json::Value::Null);
}

#[tokio::test]
async fn type_params_null_when_malformed() {
    let row = make_metric_row_batch(
        "metric.pkg.m",
        "m",
        None,
        None,
        None,
        Some("simple"),
        Some("{not valid"),
        None,
        None,
        None,
        None,
        &[],
        &[],
        &[],
        None,
        None,
    );
    let backend = MetricDetailMockBackend {
        row_batches: vec![row],
        upstream_batches: vec![],
        downstream_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_metric(State(state), Path("metric.pkg.m".to_owned())).await;
    assert_eq!(
        r.status(),
        200,
        "malformed type_params must not 500 the response"
    );
    let body = response_body(r).await;
    assert_eq!(body["type_params"], serde_json::Value::Null);
}

#[tokio::test]
async fn filter_null_when_absent() {
    let row = make_metric_row_batch(
        "metric.pkg.m",
        "m",
        None,
        None,
        None,
        Some("simple"),
        None,
        None, // filter absent
        None,
        None,
        None,
        &[],
        &[],
        &[],
        None,
        None,
    );
    let backend = MetricDetailMockBackend {
        row_batches: vec![row],
        upstream_batches: vec![],
        downstream_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_metric(State(state), Path("metric.pkg.m".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["filter"], serde_json::Value::Null);
}

#[tokio::test]
async fn depends_on_mixes_semantic_model_and_metric_edge_types() {
    // Risk #5 — derived/ratio metrics depend on other metrics; simple/cumulative
    // depend on a semantic_model. The handler must preserve the edge_type as
    // returned by dbt.edges; the FE relies on this distinction.
    let backend = MetricDetailMockBackend {
        row_batches: vec![full_metric_row()],
        upstream_batches: vec![edge_batch(&[
            ("metric.jaffle_shop.revenue", "metric"),
            ("metric.jaffle_shop.order_count", "metric"),
            ("semantic_model.jaffle_shop.orders", "semantic_model"),
        ])],
        downstream_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_metric(
        State(state),
        Path("metric.jaffle_shop.total_revenue".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    let deps = body["depends_on"].as_array().expect("array");
    assert_eq!(deps.len(), 3);
    assert_eq!(deps[0]["edge_type"], "metric");
    assert_eq!(deps[1]["edge_type"], "metric");
    assert_eq!(deps[2]["edge_type"], "semantic_model");
}

#[tokio::test]
async fn referenced_by_empty_array_when_no_downstream() {
    let backend = MetricDetailMockBackend {
        row_batches: vec![full_metric_row()],
        upstream_batches: vec![],
        downstream_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_metric(
        State(state),
        Path("metric.jaffle_shop.total_revenue".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["referenced_by"], serde_json::json!([]));
    assert_eq!(body["depends_on"], serde_json::json!([]));
}

#[tokio::test]
async fn created_at_null_when_absent() {
    let row = make_metric_row_batch(
        "metric.pkg.m",
        "m",
        None,
        None,
        None,
        Some("simple"),
        None,
        None,
        None,
        None,
        None,
        &[],
        &[],
        &[],
        None,
        None, // created_at absent
    );
    let backend = MetricDetailMockBackend {
        row_batches: vec![row],
        upstream_batches: vec![],
        downstream_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_metric(State(state), Path("metric.pkg.m".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["created_at"], serde_json::Value::Null);
}
