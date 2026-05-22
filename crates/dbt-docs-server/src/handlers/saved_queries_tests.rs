//! Tests for `GET /api/v1/saved_queries/:id`.
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
use crate::state::{AppState, Capabilities};

// ---------------------------------------------------------------------------
// Mock backend
// ---------------------------------------------------------------------------

/// Routes Arrow queries to fixture batches based on which `FROM` table
/// the SQL mentions. The `edge_batches: None` knob simulates an absent
/// `dbt.edges` view — the handler must still return a valid response
/// with `referenced_by: []`.
struct SavedQueryDetailMockBackend {
    node_batches: Vec<RecordBatch>,
    /// `Some(batches)` → query succeeds; `None` → view absent (query error).
    edge_batches: Option<Vec<RecordBatch>>,
}

impl Backend for SavedQueryDetailMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, _sql: &str) -> Option<String> {
        Some("0".to_owned())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        if sql.contains("dbt.edges") {
            return self
                .edge_batches
                .clone()
                .ok_or_else(|| BackendError::Query("edges view absent".into()));
        }
        if sql.contains("dbt.saved_queries") {
            return Ok(self.node_batches.clone());
        }
        Err(BackendError::Query(format!("unrouted query: {sql}")))
    }
}

fn make_state(backend: SavedQueryDetailMockBackend) -> Arc<AppState> {
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

fn saved_query_node_schema(
    fqn_field: &Field,
    tags_field: &Field,
    deps_field: &Field,
) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("file_path", DataType::Utf8, true),
        fqn_field.clone(),
        tags_field.clone(),
        Field::new("group_name", DataType::Utf8, true),
        Field::new("created_at", DataType::Float64, true),
        Field::new("query_params", DataType::Utf8, true),
        Field::new("exports", DataType::Utf8, true),
        deps_field.clone(),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn make_saved_query_node_batch(
    unique_id: &str,
    name: &str,
    label: Option<&str>,
    description: Option<&str>,
    package_name: Option<&str>,
    original_file_path: Option<&str>,
    file_path: Option<&str>,
    fqn: &[&str],
    tags: &[&str],
    group_name: Option<&str>,
    created_at: Option<f64>,
    query_params: Option<&str>,
    exports: Option<&str>,
    depends_on_nodes: &[&str],
) -> RecordBatch {
    let fqn_arr = make_str_list(fqn);
    let tags_arr = make_str_list(tags);
    let deps_arr = make_str_list(depends_on_nodes);
    let fqn_field = Field::new("fqn", fqn_arr.data_type().clone(), true);
    let tags_field = Field::new("tags", tags_arr.data_type().clone(), true);
    let deps_field = Field::new("depends_on_nodes", deps_arr.data_type().clone(), true);

    RecordBatch::try_new(
        saved_query_node_schema(&fqn_field, &tags_field, &deps_field),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![Some(name)])),
            Arc::new(StringArray::from(vec![label])),
            Arc::new(StringArray::from(vec![description])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![original_file_path])),
            Arc::new(StringArray::from(vec![file_path])),
            Arc::new(fqn_arr),
            Arc::new(tags_arr),
            Arc::new(StringArray::from(vec![group_name])),
            Arc::new(Float64Array::from(vec![created_at])),
            Arc::new(StringArray::from(vec![query_params])),
            Arc::new(StringArray::from(vec![exports])),
            Arc::new(deps_arr),
        ],
    )
    .expect("valid saved_query node batch")
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

fn full_node_batch() -> RecordBatch {
    make_saved_query_node_batch(
        "saved_query.jaffle_shop.weekly_revenue_summary",
        "weekly_revenue_summary",
        Some("Weekly Revenue Summary"),
        Some("Weekly revenue by region"),
        Some("jaffle_shop"),
        Some("models/semantic/saved_queries.yml"),
        Some("models/semantic/saved_queries.yml"),
        &["jaffle_shop", "semantic", "weekly_revenue_summary"],
        &["finance", "weekly"],
        Some("finance"),
        Some(1_747_320_731.0),
        Some(
            r#"{"metrics":["revenue","order_count"],"group_by":["customer__region","metric_time__week"],"order_by":["-metric_time__week"],"limit":1000,"where":{"where_filters":[{"where_sql_template":"{{ Dimension('customer__region') }} != 'INTERNAL'"}]}}"#,
        ),
        Some(
            r#"[{"name":"weekly_revenue_summary__warehouse","config":{"alias":"weekly_revenue_summary","export_as":"table","schema":"analytics","database":"prod"}}]"#,
        ),
        &[
            "metric.jaffle_shop.revenue",
            "metric.jaffle_shop.order_count",
            "semantic_model.jaffle_shop.customers",
        ],
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn invalid_unique_id_returns_400() {
    let backend = SavedQueryDetailMockBackend {
        node_batches: vec![],
        edge_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_saved_query(State(state), Path("bad'id".to_owned())).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn missing_saved_query_returns_404() {
    let backend = SavedQueryDetailMockBackend {
        node_batches: vec![],
        edge_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_saved_query(State(state), Path("saved_query.x.y".to_owned())).await;
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn all_fields_hydrated() {
    let backend = SavedQueryDetailMockBackend {
        node_batches: vec![full_node_batch()],
        edge_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_saved_query(
        State(state),
        Path("saved_query.jaffle_shop.weekly_revenue_summary".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    // NodeBase fields flatten into top-level.
    assert_eq!(
        body["unique_id"],
        "saved_query.jaffle_shop.weekly_revenue_summary"
    );
    assert_eq!(body["name"], "weekly_revenue_summary");
    assert_eq!(body["resource_type"], "saved_query");
    assert_eq!(body["package_name"], "jaffle_shop");
    assert_eq!(body["description"], "Weekly revenue by region");

    // Saved-query-specific scalars.
    assert_eq!(body["label"], "Weekly Revenue Summary");
    assert_eq!(body["group_name"], "finance");
    assert_eq!(body["created_at"], 1_747_320_731.0);

    // List-typed fields surface as flat JSON arrays.
    assert_eq!(body["tags"], serde_json::json!(["finance", "weekly"]));
    assert_eq!(
        body["fqn"],
        serde_json::json!(["jaffle_shop", "semantic", "weekly_revenue_summary"])
    );

    // query_params is parsed JSON, NOT an escaped string.
    assert_eq!(
        body["query_params"]["metrics"],
        serde_json::json!(["revenue", "order_count"])
    );
    assert_eq!(
        body["query_params"]["group_by"],
        serde_json::json!(["customer__region", "metric_time__week"])
    );
    assert_eq!(
        body["query_params"]["order_by"],
        serde_json::json!(["-metric_time__week"])
    );
    assert_eq!(body["query_params"]["limit"], 1000);
    assert_eq!(
        body["query_params"]["where"]["where_filters"][0]["where_sql_template"],
        "{{ Dimension('customer__region') }} != 'INTERNAL'"
    );

    // exports is a parsed JSON array of export objects.
    assert_eq!(
        body["exports"][0]["name"],
        "weekly_revenue_summary__warehouse"
    );
    assert_eq!(
        body["exports"][0]["config"]["alias"],
        "weekly_revenue_summary"
    );
    assert_eq!(body["exports"][0]["config"]["export_as"], "table");
    assert_eq!(body["exports"][0]["config"]["schema"], "analytics");
    assert_eq!(body["exports"][0]["config"]["database"], "prod");

    // depends_on synthesized from depends_on_nodes with edge_type from prefix.
    assert_eq!(body["depends_on"].as_array().unwrap().len(), 3);
    assert_eq!(
        body["depends_on"][0]["unique_id"],
        "metric.jaffle_shop.revenue"
    );
    assert_eq!(body["depends_on"][0]["edge_type"], "metric");
    assert_eq!(
        body["depends_on"][2]["unique_id"],
        "semantic_model.jaffle_shop.customers"
    );
    assert_eq!(body["depends_on"][2]["edge_type"], "semantic_model");

    // referenced_by empty (no exposures in this fixture).
    assert_eq!(body["referenced_by"], serde_json::json!([]));

    // No execution_info, catalog, freshness, columns, raw_code, compiled_code.
    assert!(body.get("execution_info").is_none());
    assert!(body.get("catalog").is_none());
    assert!(body.get("freshness").is_none());
    assert!(body.get("columns").is_none());
    assert!(body.get("raw_code").is_none());
    assert!(body.get("compiled_code").is_none());
}

#[tokio::test]
async fn query_params_null_when_absent() {
    let backend = SavedQueryDetailMockBackend {
        node_batches: vec![make_saved_query_node_batch(
            "saved_query.pkg.q",
            "q",
            None,
            None,
            None,
            None,
            None,
            &[],
            &[],
            None,
            None,
            None, // query_params absent
            Some("[]"),
            &[],
        )],
        edge_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_saved_query(State(state), Path("saved_query.pkg.q".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["query_params"], serde_json::Value::Null);
}

#[tokio::test]
async fn query_params_null_when_malformed() {
    let backend = SavedQueryDetailMockBackend {
        node_batches: vec![make_saved_query_node_batch(
            "saved_query.pkg.q",
            "q",
            None,
            None,
            None,
            None,
            None,
            &[],
            &[],
            None,
            None,
            Some("not{valid:json"),
            Some("[]"),
            &[],
        )],
        edge_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_saved_query(State(state), Path("saved_query.pkg.q".to_owned())).await;
    assert_eq!(r.status(), 200, "malformed query_params must not 500");
    let body = response_body(r).await;
    assert_eq!(body["query_params"], serde_json::Value::Null);
}

#[tokio::test]
async fn exports_null_when_absent() {
    let backend = SavedQueryDetailMockBackend {
        node_batches: vec![make_saved_query_node_batch(
            "saved_query.pkg.q",
            "q",
            None,
            None,
            None,
            None,
            None,
            &[],
            &[],
            None,
            None,
            Some("{}"),
            None, // exports absent
            &[],
        )],
        edge_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_saved_query(State(state), Path("saved_query.pkg.q".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["exports"], serde_json::Value::Null);
}

#[tokio::test]
async fn exports_null_when_malformed() {
    let backend = SavedQueryDetailMockBackend {
        node_batches: vec![make_saved_query_node_batch(
            "saved_query.pkg.q",
            "q",
            None,
            None,
            None,
            None,
            None,
            &[],
            &[],
            None,
            None,
            Some("{}"),
            Some("[{not valid"),
            &[],
        )],
        edge_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_saved_query(State(state), Path("saved_query.pkg.q".to_owned())).await;
    assert_eq!(r.status(), 200, "malformed exports must not 500");
    let body = response_body(r).await;
    assert_eq!(body["exports"], serde_json::Value::Null);
}

#[tokio::test]
async fn depends_on_edge_type_derived_from_prefix() {
    let backend = SavedQueryDetailMockBackend {
        node_batches: vec![make_saved_query_node_batch(
            "saved_query.pkg.q",
            "q",
            None,
            None,
            None,
            None,
            None,
            &[],
            &[],
            None,
            None,
            Some("{}"),
            Some("[]"),
            &[
                "metric.pkg.m1",
                "semantic_model.pkg.sm1",
                "model.pkg.mod1",
                "bare_unprefixed_id",
            ],
        )],
        edge_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_saved_query(State(state), Path("saved_query.pkg.q".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["depends_on"][0]["edge_type"], "metric");
    assert_eq!(body["depends_on"][1]["edge_type"], "semantic_model");
    assert_eq!(body["depends_on"][2]["edge_type"], "model");
    // Unprefixed unique_id falls back to using the whole id as the type.
    assert_eq!(body["depends_on"][3]["edge_type"], "bare_unprefixed_id");
}

#[tokio::test]
async fn referenced_by_empty_when_edges_view_absent() {
    let backend = SavedQueryDetailMockBackend {
        node_batches: vec![full_node_batch()],
        // None = view absent at query time.
        edge_batches: None,
    };
    let state = make_state(backend);
    let r = get_saved_query(
        State(state),
        Path("saved_query.jaffle_shop.weekly_revenue_summary".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["referenced_by"], serde_json::json!([]));
}

#[tokio::test]
async fn referenced_by_populated_from_downstream_edges() {
    let backend = SavedQueryDetailMockBackend {
        node_batches: vec![full_node_batch()],
        edge_batches: Some(vec![edge_batch(&[(
            "exposure.jaffle_shop.weekly_dashboard",
            "exposure",
        )])]),
    };
    let state = make_state(backend);
    let r = get_saved_query(
        State(state),
        Path("saved_query.jaffle_shop.weekly_revenue_summary".to_owned()),
    )
    .await;
    let body = response_body(r).await;
    assert_eq!(
        body["referenced_by"][0]["unique_id"],
        "exposure.jaffle_shop.weekly_dashboard"
    );
    assert_eq!(body["referenced_by"][0]["edge_type"], "exposure");
}
