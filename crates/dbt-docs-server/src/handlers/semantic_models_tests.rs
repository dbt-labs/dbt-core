//! Tests for `GET /api/v1/semantic_models/:id`,
//! `GET /api/v1/semantic_models`, and `GET /api/v1/semantic_models/facets`.
//!
//! Schema anchoring (#10255): the `RecordBatch` fixtures below are hand-
//! rolled and not enforced against the production parquet schemas. A column
//! rename or type change in `dbt-index` will pass these tests while
//! silently breaking the handler. Once #10255 lands typed row builders,
//! replace these schemas to get compile-time coverage.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder, StructBuilder};
use arrow_array::{BooleanArray, Float64Array, ListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Fields, Schema};
use axum::extract::{Path, Query, State};
use axum::response::Response;

use super::*;
use crate::handlers::pagination::Cursor;
use crate::providers::{Backend, BackendError, Providers};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Mock backend
// ---------------------------------------------------------------------------

/// Routes Arrow queries to the configured fixture batches based on which
/// `FROM` table the SQL mentions. `entity_batches` / `dimension_batches` /
/// `measure_batches` use `Option<Vec<RecordBatch>>` so each one can be
/// independently signalled as "view absent" (query error) vs. "view
/// present but empty" (empty `[]`).
struct SemanticModelMockBackend {
    node_batches: Vec<RecordBatch>,
    entity_batches: Option<Vec<RecordBatch>>,
    dimension_batches: Option<Vec<RecordBatch>>,
    measure_batches: Option<Vec<RecordBatch>>,
    upstream_batches: Vec<RecordBatch>,
    downstream_batches: Vec<RecordBatch>,
}

impl Backend for SemanticModelMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, _sql: &str) -> Option<String> {
        Some("0".to_owned())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        // Order matters: more specific tables before more generic.
        if sql.contains("dbt.semantic_entities") {
            return self
                .entity_batches
                .clone()
                .ok_or_else(|| BackendError::Query("semantic_entities view absent".into()));
        }
        if sql.contains("dbt.semantic_dimensions") {
            return self
                .dimension_batches
                .clone()
                .ok_or_else(|| BackendError::Query("semantic_dimensions view absent".into()));
        }
        if sql.contains("dbt.semantic_measures") {
            return self
                .measure_batches
                .clone()
                .ok_or_else(|| BackendError::Query("semantic_measures view absent".into()));
        }
        if sql.contains("dbt.semantic_models") {
            return Ok(self.node_batches.clone());
        }
        if sql.contains("child_unique_id = ") {
            return Ok(self.upstream_batches.clone());
        }
        if sql.contains("parent_unique_id = ") {
            return Ok(self.downstream_batches.clone());
        }
        Err(BackendError::Query(format!("unrouted query: {sql}")))
    }
}

fn make_state(backend: SemanticModelMockBackend) -> Arc<AppState> {
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

/// Schema for the node SELECT (semantic_models LEFT JOIN nodes).
fn node_schema(fqn_field: &Field) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("file_path", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
        fqn_field.clone(),
        Field::new("node_relation", DataType::Utf8, true),
        Field::new("primary_entity", DataType::Utf8, true),
        Field::new("defaults", DataType::Utf8, true),
        Field::new("group_name", DataType::Utf8, true),
        Field::new("config", DataType::Utf8, true),
        Field::new("created_at", DataType::Float64, true),
        Field::new("model_unique_id", DataType::Utf8, true),
        Field::new("model_name", DataType::Utf8, true),
        Field::new("model_access_level", DataType::Utf8, true),
        Field::new("model_alias", DataType::Utf8, true),
    ]))
}

#[derive(Default)]
struct NodeFixture<'a> {
    unique_id: &'a str,
    name: &'a str,
    package_name: Option<&'a str>,
    description: Option<&'a str>,
    label: Option<&'a str>,
    fqn: &'a [&'a str],
    node_relation: Option<&'a str>,
    primary_entity: Option<&'a str>,
    defaults: Option<&'a str>,
    group_name: Option<&'a str>,
    config: Option<&'a str>,
    created_at: Option<f64>,
    model_unique_id: Option<&'a str>,
    model_name: Option<&'a str>,
    model_access_level: Option<&'a str>,
    model_alias: Option<&'a str>,
}

fn make_node_batch(f: NodeFixture<'_>) -> RecordBatch {
    let fqn_arr = make_str_list(f.fqn);
    let fqn_field = Field::new("fqn", fqn_arr.data_type().clone(), true);
    RecordBatch::try_new(
        node_schema(&fqn_field),
        vec![
            Arc::new(StringArray::from(vec![f.unique_id])),
            Arc::new(StringArray::from(vec![f.name])),
            Arc::new(StringArray::from(vec![f.package_name])),
            Arc::new(StringArray::from(vec![f.description])),
            Arc::new(StringArray::from(vec![Some("models/semantic_models.yml")])),
            Arc::new(StringArray::from(vec![Some("semantic_models.yml")])),
            Arc::new(StringArray::from(vec![f.label])),
            Arc::new(fqn_arr),
            Arc::new(StringArray::from(vec![f.node_relation])),
            Arc::new(StringArray::from(vec![f.primary_entity])),
            Arc::new(StringArray::from(vec![f.defaults])),
            Arc::new(StringArray::from(vec![f.group_name])),
            Arc::new(StringArray::from(vec![f.config])),
            Arc::new(Float64Array::from(vec![f.created_at])),
            Arc::new(StringArray::from(vec![f.model_unique_id])),
            Arc::new(StringArray::from(vec![f.model_name])),
            Arc::new(StringArray::from(vec![f.model_access_level])),
            Arc::new(StringArray::from(vec![f.model_alias])),
        ],
    )
    .expect("valid node batch")
}

fn full_node_batch() -> RecordBatch {
    make_node_batch(NodeFixture {
        unique_id: "semantic_model.jaffle_shop.orders",
        name: "orders",
        package_name: Some("jaffle_shop"),
        description: Some("Semantic model over orders"),
        label: Some("Orders"),
        fqn: &["jaffle_shop", "semantic_models", "orders"],
        node_relation: Some(
            r#"{"alias":"fct_orders","database":"analytics","schema_name":"main","relation_name":"analytics.main.fct_orders"}"#,
        ),
        primary_entity: Some("order"),
        defaults: Some(r#"{"agg_time_dimension":"ordered_at"}"#),
        group_name: Some("finance"),
        config: Some(r#"{"tags":["finance","semantic"],"meta":{"owner":"data-eng"}}"#),
        created_at: Some(1_747_432_300.5),
        model_unique_id: Some("model.jaffle_shop.fct_orders"),
        model_name: Some("fct_orders"),
        model_access_level: Some("public"),
        model_alias: Some("fct_orders"),
    })
}

fn entity_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("entity_type", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("expr", DataType::Utf8, true),
        Field::new("role", DataType::Utf8, true),
    ]))
}

#[allow(clippy::type_complexity)]
fn entity_batch(rows: &[(&str, Option<&str>, Option<&str>, Option<&str>)]) -> RecordBatch {
    let names: Vec<&str> = rows.iter().map(|r| r.0).collect();
    let etypes: Vec<Option<&str>> = rows.iter().map(|r| r.1).collect();
    let descs: Vec<Option<&str>> = rows.iter().map(|r| r.2).collect();
    let exprs: Vec<Option<&str>> = rows.iter().map(|r| r.3).collect();
    RecordBatch::try_new(
        entity_schema(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(etypes)),
            Arc::new(StringArray::from(descs)),
            Arc::new(StringArray::from(vec![None::<&str>; rows.len()])),
            Arc::new(StringArray::from(exprs)),
            Arc::new(StringArray::from(vec![None::<&str>; rows.len()])),
        ],
    )
    .expect("valid entity batch")
}

fn dimension_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("dimension_type", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("expr", DataType::Utf8, true),
        Field::new("is_partition", DataType::Boolean, true),
        Field::new("time_granularity", DataType::Utf8, true),
        Field::new("validity_params", DataType::Utf8, true),
    ]))
}

#[allow(clippy::type_complexity)]
fn dimension_batch(
    rows: &[(
        &str,
        Option<&str>,
        Option<&str>,
        Option<bool>,
        Option<&str>,
        Option<&str>,
    )],
) -> RecordBatch {
    let names: Vec<&str> = rows.iter().map(|r| r.0).collect();
    let types: Vec<Option<&str>> = rows.iter().map(|r| r.1).collect();
    let exprs: Vec<Option<&str>> = rows.iter().map(|r| r.2).collect();
    let parts: Vec<Option<bool>> = rows.iter().map(|r| r.3).collect();
    let grans: Vec<Option<&str>> = rows.iter().map(|r| r.4).collect();
    let vps: Vec<Option<&str>> = rows.iter().map(|r| r.5).collect();
    RecordBatch::try_new(
        dimension_schema(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(vec![None::<&str>; rows.len()])),
            Arc::new(StringArray::from(vec![None::<&str>; rows.len()])),
            Arc::new(StringArray::from(exprs)),
            Arc::new(BooleanArray::from(parts)),
            Arc::new(StringArray::from(grans)),
            Arc::new(StringArray::from(vps)),
        ],
    )
    .expect("valid dimension batch")
}

fn measure_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("agg", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("expr", DataType::Utf8, true),
        Field::new("create_metric", DataType::Boolean, true),
        Field::new("agg_time_dimension", DataType::Utf8, true),
        Field::new("agg_params", DataType::Utf8, true),
        Field::new("non_additive_dimension", DataType::Utf8, true),
    ]))
}

#[allow(clippy::type_complexity)]
fn measure_batch(
    rows: &[(
        &str,
        Option<&str>,
        Option<&str>,
        Option<bool>,
        Option<&str>,
        Option<&str>,
    )],
) -> RecordBatch {
    let names: Vec<&str> = rows.iter().map(|r| r.0).collect();
    let aggs: Vec<Option<&str>> = rows.iter().map(|r| r.1).collect();
    let exprs: Vec<Option<&str>> = rows.iter().map(|r| r.2).collect();
    let metrics: Vec<Option<bool>> = rows.iter().map(|r| r.3).collect();
    let atds: Vec<Option<&str>> = rows.iter().map(|r| r.4).collect();
    let aps: Vec<Option<&str>> = rows.iter().map(|r| r.5).collect();
    RecordBatch::try_new(
        measure_schema(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(aggs)),
            Arc::new(StringArray::from(vec![None::<&str>; rows.len()])),
            Arc::new(StringArray::from(vec![None::<&str>; rows.len()])),
            Arc::new(StringArray::from(exprs)),
            Arc::new(BooleanArray::from(metrics)),
            Arc::new(StringArray::from(atds)),
            Arc::new(StringArray::from(aps)),
            Arc::new(StringArray::from(vec![None::<&str>; rows.len()])),
        ],
    )
    .expect("valid measure batch")
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn empty_backend() -> SemanticModelMockBackend {
    SemanticModelMockBackend {
        node_batches: vec![],
        entity_batches: Some(vec![]),
        dimension_batches: Some(vec![]),
        measure_batches: Some(vec![]),
        upstream_batches: vec![],
        downstream_batches: vec![],
    }
}

#[tokio::test]
async fn invalid_unique_id_returns_400() {
    let state = make_state(empty_backend());
    let r = get_semantic_model(State(state), Path("bad'id".to_owned())).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn missing_semantic_model_returns_404() {
    let state = make_state(empty_backend());
    let r = get_semantic_model(
        State(state),
        Path("semantic_model.x.does_not_exist".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn all_fields_hydrated() {
    let backend = SemanticModelMockBackend {
        node_batches: vec![full_node_batch()],
        entity_batches: Some(vec![entity_batch(&[
            (
                "order",
                Some("primary"),
                Some("Unique order identifier."),
                Some("order_id"),
            ),
            (
                "customer",
                Some("foreign"),
                Some("Customer that placed the order."),
                Some("customer_id"),
            ),
        ])]),
        dimension_batches: Some(vec![dimension_batch(&[
            (
                "ordered_at",
                Some("time"),
                Some("ordered_at"),
                Some(false),
                Some("day"),
                None,
            ),
            (
                "status",
                Some("categorical"),
                Some("status"),
                Some(false),
                None,
                None,
            ),
        ])]),
        measure_batches: Some(vec![measure_batch(&[
            (
                "order_total",
                Some("sum"),
                Some("amount"),
                Some(true),
                Some("ordered_at"),
                None,
            ),
            (
                "p95_latency",
                Some("percentile"),
                Some("latency_ms"),
                Some(false),
                Some("ordered_at"),
                Some(r#"{"percentile":0.95,"use_discrete_percentile":false}"#),
            ),
        ])]),
        upstream_batches: vec![edge_batch(&[("model.jaffle_shop.fct_orders", "ref")])],
        downstream_batches: vec![edge_batch(&[
            ("metric.jaffle_shop.total_orders", "metric"),
            ("saved_query.jaffle_shop.orders_by_month", "saved_query"),
        ])],
    };
    let state = make_state(backend);
    let r = get_semantic_model(
        State(state),
        Path("semantic_model.jaffle_shop.orders".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    // NodeBase fields flatten to top-level.
    assert_eq!(body["unique_id"], "semantic_model.jaffle_shop.orders");
    assert_eq!(body["name"], "orders");
    assert_eq!(body["resource_type"], "semantic_model");
    assert_eq!(body["package_name"], "jaffle_shop");
    assert_eq!(body["label"], "Orders");
    assert_eq!(body["group_name"], "finance");
    assert_eq!(body["primary_entity"], "order");
    assert_eq!(body["created_at"], 1_747_432_300.5);

    // fqn list passthrough.
    assert_eq!(
        body["fqn"],
        serde_json::json!(["jaffle_shop", "semantic_models", "orders"])
    );

    // tags and meta lifted from config blob.
    assert_eq!(body["tags"], serde_json::json!(["finance", "semantic"]));
    assert_eq!(body["meta"], serde_json::json!({"owner": "data-eng"}));

    // node_relation and defaults parsed as nested JSON.
    assert_eq!(body["node_relation"]["alias"], "fct_orders");
    assert_eq!(body["node_relation"]["database"], "analytics");
    assert_eq!(body["defaults"]["agg_time_dimension"], "ordered_at");

    // UpstreamModelRef populated from JOIN.
    assert_eq!(body["model"]["unique_id"], "model.jaffle_shop.fct_orders");
    assert_eq!(body["model"]["name"], "fct_orders");
    assert_eq!(body["model"]["access_level"], "public");
    assert_eq!(body["model"]["alias"], "fct_orders");

    // Inline sub-resource arrays.
    assert_eq!(body["entities"][0]["name"], "order");
    assert_eq!(body["entities"][0]["type"], "primary");
    assert_eq!(body["entities"][0]["expr"], "order_id");
    assert_eq!(body["entities"][1]["type"], "foreign");

    assert_eq!(body["dimensions"][0]["name"], "ordered_at");
    assert_eq!(body["dimensions"][0]["type"], "time");
    assert_eq!(body["dimensions"][0]["is_partition"], false);
    assert_eq!(body["dimensions"][0]["time_granularity"], "day");
    assert_eq!(body["dimensions"][1]["type"], "categorical");

    assert_eq!(body["measures"][0]["name"], "order_total");
    assert_eq!(body["measures"][0]["agg"], "sum");
    assert_eq!(body["measures"][0]["create_metric"], true);
    // agg_params parsed as nested JSON object, not escaped string.
    assert_eq!(
        body["measures"][1]["agg_params"],
        serde_json::json!({"percentile": 0.95, "use_discrete_percentile": false})
    );

    // Edges.
    assert_eq!(
        body["depends_on"][0]["unique_id"],
        "model.jaffle_shop.fct_orders"
    );
    assert_eq!(body["depends_on"][0]["edge_type"], "ref");
    assert_eq!(body["referenced_by"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn node_relation_null_when_absent() {
    let backend = SemanticModelMockBackend {
        node_batches: vec![make_node_batch(NodeFixture {
            unique_id: "semantic_model.pkg.x",
            name: "x",
            ..Default::default()
        })],
        ..empty_backend()
    };
    let state = make_state(backend);
    let r = get_semantic_model(State(state), Path("semantic_model.pkg.x".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["node_relation"], serde_json::Value::Null);
}

#[tokio::test]
async fn node_relation_null_when_malformed() {
    let backend = SemanticModelMockBackend {
        node_batches: vec![make_node_batch(NodeFixture {
            unique_id: "semantic_model.pkg.x",
            name: "x",
            node_relation: Some("not{valid:json"),
            ..Default::default()
        })],
        ..empty_backend()
    };
    let state = make_state(backend);
    let r = get_semantic_model(State(state), Path("semantic_model.pkg.x".to_owned())).await;
    assert_eq!(
        r.status(),
        200,
        "malformed node_relation must not 500 the response"
    );
    let body = response_body(r).await;
    assert_eq!(body["node_relation"], serde_json::Value::Null);
}

#[tokio::test]
async fn defaults_null_when_absent() {
    let backend = SemanticModelMockBackend {
        node_batches: vec![make_node_batch(NodeFixture {
            unique_id: "semantic_model.pkg.x",
            name: "x",
            ..Default::default()
        })],
        ..empty_backend()
    };
    let state = make_state(backend);
    let r = get_semantic_model(State(state), Path("semantic_model.pkg.x".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["defaults"], serde_json::Value::Null);
}

#[tokio::test]
async fn defaults_null_when_malformed() {
    let backend = SemanticModelMockBackend {
        node_batches: vec![make_node_batch(NodeFixture {
            unique_id: "semantic_model.pkg.x",
            name: "x",
            defaults: Some("}{bad"),
            ..Default::default()
        })],
        ..empty_backend()
    };
    let state = make_state(backend);
    let r = get_semantic_model(State(state), Path("semantic_model.pkg.x".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["defaults"], serde_json::Value::Null);
}

#[tokio::test]
async fn entities_empty_when_no_rows() {
    let backend = SemanticModelMockBackend {
        node_batches: vec![full_node_batch()],
        ..empty_backend()
    };
    let state = make_state(backend);
    let r = get_semantic_model(
        State(state),
        Path("semantic_model.jaffle_shop.orders".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["entities"], serde_json::json!([]));
}

#[tokio::test]
async fn dimensions_empty_when_no_rows() {
    let backend = SemanticModelMockBackend {
        node_batches: vec![full_node_batch()],
        ..empty_backend()
    };
    let state = make_state(backend);
    let r = get_semantic_model(
        State(state),
        Path("semantic_model.jaffle_shop.orders".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["dimensions"], serde_json::json!([]));
}

#[tokio::test]
async fn measures_empty_when_no_rows() {
    let backend = SemanticModelMockBackend {
        node_batches: vec![full_node_batch()],
        ..empty_backend()
    };
    let state = make_state(backend);
    let r = get_semantic_model(
        State(state),
        Path("semantic_model.jaffle_shop.orders".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["measures"], serde_json::json!([]));
}

#[tokio::test]
async fn depends_on_edge_type_derived_from_prefix() {
    // Edge rows carry their edge_type directly from `dbt.edges` — exercise
    // that the handler echoes it verbatim and that the array is shaped
    // even for a single 1-hop upstream.
    let backend = SemanticModelMockBackend {
        node_batches: vec![full_node_batch()],
        upstream_batches: vec![edge_batch(&[("model.jaffle_shop.fct_orders", "ref")])],
        ..empty_backend()
    };
    let state = make_state(backend);
    let r = get_semantic_model(
        State(state),
        Path("semantic_model.jaffle_shop.orders".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(body["depends_on"].as_array().unwrap().len(), 1);
    assert_eq!(body["depends_on"][0]["edge_type"], "ref");
    assert_eq!(
        body["depends_on"][0]["unique_id"],
        "model.jaffle_shop.fct_orders"
    );
}

// ---------------------------------------------------------------------------
// Mock backend: list_semantic_models / list_semantic_model_facets
// ---------------------------------------------------------------------------

// TODO(#10255): replace hand-rolled RecordBatch schemas with typed row
// builders once dbt-index ships them.

struct SemanticModelListMockBackend {
    total_count: u64,
    row_batches: Vec<RecordBatch>,
}

impl SemanticModelListMockBackend {
    fn new(total: u64, rows: Vec<RecordBatch>) -> Self {
        Self {
            total_count: total,
            row_batches: rows,
        }
    }
}

impl Backend for SemanticModelListMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, sql: &str) -> Option<String> {
        if sql.contains("count(*)") {
            Some(self.total_count.to_string())
        } else {
            None
        }
    }

    fn query_arrow(&self, _sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        Ok(self.row_batches.clone())
    }
}

fn make_list_state(backend: SemanticModelListMockBackend) -> Arc<AppState> {
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
// Batch builders: list rows
// ---------------------------------------------------------------------------

/// Build a `RecordBatch` for the semantic model list query.
///
/// `entities` is a `List<Struct<name: Utf8, entity_type: Utf8>>` — the DuckDB
/// aggregation result shape.
#[allow(clippy::too_many_arguments)]
fn semantic_model_list_row(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    group_name: Option<&str>,
    primary_entity: Option<&str>,
    description: Option<&str>,
    created_at: Option<f64>,
    entities: &[(&str, Option<&str>)],
) -> RecordBatch {
    // Build the struct array for entities.
    let entity_fields = Fields::from(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("entity_type", DataType::Utf8, true),
    ]);
    let _entity_struct_field = Field::new_list_field(DataType::Struct(entity_fields.clone()), true);
    let mut list_builder = ListBuilder::new(StructBuilder::from_fields(entity_fields, 0));
    {
        let sb = list_builder.values();
        for (ename, etype) in entities {
            sb.field_builder::<StringBuilder>(0)
                .expect("name builder")
                .append_value(ename);
            match etype {
                Some(t) => sb
                    .field_builder::<StringBuilder>(1)
                    .expect("entity_type builder")
                    .append_value(t),
                None => sb
                    .field_builder::<StringBuilder>(1)
                    .expect("entity_type builder")
                    .append_null(),
            }
            sb.append(true);
        }
        list_builder.append(true); // always append (even empty)
    }
    let entities_arr = list_builder.finish();
    let entities_list_field = Field::new("entities", entities_arr.data_type().clone(), true);

    let schema = Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("group_name", DataType::Utf8, true),
        Field::new("primary_entity", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("created_at", DataType::Float64, true),
        entities_list_field,
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![group_name])),
            Arc::new(StringArray::from(vec![primary_entity])),
            Arc::new(StringArray::from(vec![description])),
            Arc::new(Float64Array::from(vec![created_at])),
            Arc::new(entities_arr),
        ],
    )
    .expect("valid semantic model list row batch")
}

// ---------------------------------------------------------------------------
// Unit tests: SQL builder
// ---------------------------------------------------------------------------

#[test]
fn sort_default_is_name_asc() {
    let params = SemanticModelListParams::default();
    let (_, rows) = build_semantic_model_list_sql(&params, 10, None).unwrap();
    assert!(rows.contains("ORDER BY sm.name ASC NULLS LAST"));
}

#[test]
fn sort_name_desc_accepted() {
    let params = SemanticModelListParams {
        sort: Some("name:desc".into()),
        ..Default::default()
    };
    let (_, rows) = build_semantic_model_list_sql(&params, 10, None).unwrap();
    assert!(rows.contains("ORDER BY sm.name DESC NULLS LAST"));
}

#[test]
fn sort_unknown_column_returns_err() {
    let params = SemanticModelListParams {
        sort: Some("package_name:asc".into()),
        ..Default::default()
    };
    assert!(build_semantic_model_list_sql(&params, 10, None).is_err());
}

#[test]
fn sort_unknown_direction_returns_err() {
    let params = SemanticModelListParams {
        sort: Some("name:random".into()),
        ..Default::default()
    };
    assert!(build_semantic_model_list_sql(&params, 10, None).is_err());
}

#[test]
fn count_sql_excludes_cursor_rows_sql_includes_cursor() {
    let c = Cursor {
        sort_value: Some("orders".into()),
        unique_id: "semantic_model.jaffle_shop.orders".into(),
    };
    let params = SemanticModelListParams::default();
    let (count, rows) = build_semantic_model_list_sql(&params, 10, Some(&c)).unwrap();
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
// Integration tests: list_semantic_models
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_semantic_models_empty_catalog() {
    let state = make_list_state(SemanticModelListMockBackend::new(0, vec![]));
    let r = list_semantic_models(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"], serde_json::json!([]));
    assert_eq!(body["page_info"]["total_count"], 0);
    assert_eq!(body["page_info"]["has_next_page"], false);
    assert_eq!(body["page_info"]["start_cursor"], serde_json::Value::Null);
    assert_eq!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_semantic_models_all_fields_hydrated() {
    let row = semantic_model_list_row(
        "semantic_model.jaffle_shop.orders",
        "orders",
        Some("jaffle_shop"),
        Some("finance"),
        Some("order"),
        Some("Semantic model over orders."),
        Some(1_747_432_300.5),
        &[("customer", Some("foreign")), ("order", Some("primary"))],
    );
    let state = make_list_state(SemanticModelListMockBackend::new(1, vec![row]));
    let r = list_semantic_models(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    let row = &body["data"][0];
    assert_eq!(row["unique_id"], "semantic_model.jaffle_shop.orders");
    assert_eq!(row["name"], "orders");
    assert_eq!(row["package_name"], "jaffle_shop");
    assert_eq!(row["group_name"], "finance");
    assert_eq!(row["primary_entity"], "order");
    assert_eq!(row["description"], "Semantic model over orders.");
    assert_eq!(row["created_at"], 1_747_432_300.5);
    assert_eq!(row["truncated"], false);
    assert_eq!(row["entities"][0]["name"], "customer");
    assert_eq!(row["entities"][0]["type"], "foreign");
    assert_eq!(row["entities"][1]["name"], "order");
    assert_eq!(row["entities"][1]["type"], "primary");
    assert_eq!(body["page_info"]["total_count"], 1);
    assert_eq!(
        body["page_info"]["end_cursor"],
        serde_json::Value::Null,
        "end_cursor must be null on last page"
    );
    assert_ne!(body["page_info"]["start_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_semantic_models_nullable_fields() {
    let row = semantic_model_list_row(
        "semantic_model.pkg.x",
        "x",
        None,
        None,
        None,
        None,
        None,
        &[],
    );
    let state = make_list_state(SemanticModelListMockBackend::new(1, vec![row]));
    let r = list_semantic_models(State(state), Query(Default::default())).await;
    let body = response_body(r).await;
    let row = &body["data"][0];
    assert_eq!(row["package_name"], serde_json::Value::Null);
    assert_eq!(row["group_name"], serde_json::Value::Null);
    assert_eq!(row["primary_entity"], serde_json::Value::Null);
    assert_eq!(row["description"], serde_json::Value::Null);
    assert_eq!(row["created_at"], serde_json::Value::Null);
    assert_eq!(row["entities"], serde_json::json!([]));
    assert_eq!(row["truncated"], false);
}

#[tokio::test]
async fn list_semantic_models_sort_unknown_column_returns_400() {
    let state = make_list_state(SemanticModelListMockBackend::new(0, vec![]));
    let params = SemanticModelListParams {
        sort: Some("package_name:asc".into()),
        ..Default::default()
    };
    let r = list_semantic_models(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_semantic_models_invalid_cursor_returns_400() {
    let state = make_list_state(SemanticModelListMockBackend::new(0, vec![]));
    let params = SemanticModelListParams {
        after: Some("not-valid-base64!!!".into()),
        ..Default::default()
    };
    let r = list_semantic_models(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_semantic_models_multi_page_has_next_page_true() {
    let row_a = semantic_model_list_row(
        "semantic_model.pkg.alpha",
        "alpha",
        None,
        None,
        None,
        None,
        None,
        &[],
    );
    let row_b = semantic_model_list_row(
        "semantic_model.pkg.beta",
        "beta",
        None,
        None,
        None,
        None,
        None,
        &[],
    );
    let state = make_list_state(SemanticModelListMockBackend::new(2, vec![row_a, row_b]));
    let params = SemanticModelListParams {
        first: Some(1),
        ..Default::default()
    };
    let r = list_semantic_models(State(state), Query(params)).await;
    let body = response_body(r).await;
    assert_eq!(body["page_info"]["has_next_page"], true);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_ne!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_semantic_models_last_page_end_cursor_null() {
    let row = semantic_model_list_row(
        "semantic_model.pkg.z",
        "z",
        None,
        None,
        None,
        None,
        None,
        &[],
    );
    let state = make_list_state(SemanticModelListMockBackend::new(1, vec![row]));
    let r = list_semantic_models(State(state), Query(Default::default())).await;
    let body = response_body(r).await;
    assert_eq!(body["page_info"]["has_next_page"], false);
    assert_eq!(
        body["page_info"]["end_cursor"],
        serde_json::Value::Null,
        "end_cursor must be null on last page"
    );
}

#[tokio::test]
async fn list_semantic_models_cursor_advances_page() {
    let after = Cursor {
        sort_value: Some("alpha".into()),
        unique_id: "semantic_model.pkg.alpha".into(),
    }
    .encode();
    let row = semantic_model_list_row(
        "semantic_model.pkg.beta",
        "beta",
        None,
        None,
        None,
        None,
        None,
        &[],
    );
    let state = make_list_state(SemanticModelListMockBackend::new(1, vec![row]));
    let params = SemanticModelListParams {
        after: Some(after),
        ..Default::default()
    };
    let r = list_semantic_models(State(state), Query(params)).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["name"], "beta");
}

#[tokio::test]
async fn list_semantic_models_entity_type_null_allowed() {
    let row = semantic_model_list_row(
        "semantic_model.pkg.x",
        "x",
        None,
        None,
        None,
        None,
        None,
        &[("entity_without_type", None)],
    );
    let state = make_list_state(SemanticModelListMockBackend::new(1, vec![row]));
    let r = list_semantic_models(State(state), Query(Default::default())).await;
    let body = response_body(r).await;
    assert_eq!(
        body["data"][0]["entities"][0]["name"],
        "entity_without_type"
    );
    assert_eq!(
        body["data"][0]["entities"][0]["type"],
        serde_json::Value::Null
    );
}

// ---------------------------------------------------------------------------
// Integration tests: list_semantic_model_facets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_semantic_model_facets_returns_empty_object() {
    let r = list_semantic_model_facets().await;
    assert_eq!(r.status(), 200);
    let bytes = axum::body::to_bytes(r.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("valid json");
    assert_eq!(
        body,
        serde_json::json!({}),
        "facets must be empty object in v0"
    );
}
