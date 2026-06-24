//! Tests for `GET /api/v1/tests/:id`, `GET /api/v1/tests`, and
//! `GET /api/v1/tests/facets`.
//!
//! TODO(#10255): the `RecordBatch` fixtures below are hand-rolled and not
//! enforced against the production parquet schemas. A column rename or type
//! change in `dbt-index` will pass these tests while silently breaking the
//! handler. Replace once typed `*RowBuilder`s land.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Float64Array, ListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
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
/// `FROM` table the SQL mentions. Optional fixtures double as the "view
/// absent" knob: `None` → query errors → handler treats as no row.
struct TestDetailMockBackend {
    node_batches: Vec<RecordBatch>,
    test_metadata_batches: Vec<RecordBatch>,
    unit_test_batches: Vec<RecordBatch>,
    edge_batches: Vec<RecordBatch>,
    /// `Some(batches)` → query succeeds; `None` → view absent (query error).
    run_result_batches: Option<Vec<RecordBatch>>,
}

impl Backend for TestDetailMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, _sql: &str) -> Option<String> {
        Some("0".to_owned())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        if sql.contains("dbt.test_metadata") {
            return Ok(self.test_metadata_batches.clone());
        }
        if sql.contains("dbt.unit_tests") {
            return Ok(self.unit_test_batches.clone());
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
        if sql.contains("dbt.nodes") {
            return Ok(self.node_batches.clone());
        }
        Err(BackendError::Query(format!("unrouted query: {sql}")))
    }
}

fn make_state(backend: TestDetailMockBackend) -> Arc<AppState> {
    let providers = Providers {
        backend: Arc::new(backend),
        ..Providers::default()
    };
    Arc::new(AppState {
        has_dbt_state: false,
        index_dir: PathBuf::from("/tmp"),
        providers,
        do_not_track: false,
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

#[allow(clippy::too_many_arguments)]
fn data_test_node_batch(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    description: Option<&str>,
    original_file_path: Option<&str>,
    file_path: Option<&str>,
    patch_path: Option<&str>,
    meta_json: Option<&str>,
    raw_code: Option<&str>,
    compiled_code: Option<&str>,
    tags: &[&str],
    fqn: &[&str],
) -> RecordBatch {
    let tags_arr = make_str_list(tags);
    let fqn_arr = make_str_list(fqn);
    let tags_field = Field::new("tags", tags_arr.data_type().clone(), true);
    let fqn_field = Field::new("fqn", fqn_arr.data_type().clone(), true);
    let schema = Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("resource_type", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("file_path", DataType::Utf8, true),
        Field::new("patch_path", DataType::Utf8, true),
        Field::new("meta", DataType::Utf8, true),
        Field::new("raw_code", DataType::Utf8, true),
        Field::new("compiled_code", DataType::Utf8, true),
        tags_field,
        fqn_field,
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec!["test"])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![description])),
            Arc::new(StringArray::from(vec![original_file_path])),
            Arc::new(StringArray::from(vec![file_path])),
            Arc::new(StringArray::from(vec![patch_path])),
            Arc::new(StringArray::from(vec![meta_json])),
            Arc::new(StringArray::from(vec![raw_code])),
            Arc::new(StringArray::from(vec![compiled_code])),
            Arc::new(tags_arr),
            Arc::new(fqn_arr),
        ],
    )
    .expect("valid data-test node batch")
}

fn test_metadata_batch(
    test_name: &str,
    kwargs_json: Option<&str>,
    column_name: Option<&str>,
    severity: Option<&str>,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("test_name", DataType::Utf8, true),
        Field::new("kwargs", DataType::Utf8, true),
        Field::new("column_name", DataType::Utf8, true),
        Field::new("severity", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some(test_name)])),
            Arc::new(StringArray::from(vec![kwargs_json])),
            Arc::new(StringArray::from(vec![column_name])),
            Arc::new(StringArray::from(vec![severity])),
        ],
    )
    .expect("valid test_metadata batch")
}

#[allow(clippy::too_many_arguments)]
fn unit_test_batch(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    description: Option<&str>,
    original_file_path: Option<&str>,
    file_path: Option<&str>,
    model: Option<&str>,
    given_json: Option<&str>,
    expect_json: Option<&str>,
    fqn: &[&str],
    depends_on_nodes: &[&str],
) -> RecordBatch {
    let fqn_arr = make_str_list(fqn);
    let dep_arr = make_str_list(depends_on_nodes);
    let fqn_field = Field::new("fqn", fqn_arr.data_type().clone(), true);
    let dep_field = Field::new("depends_on_nodes", dep_arr.data_type().clone(), true);
    let schema = Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("file_path", DataType::Utf8, true),
        fqn_field,
        Field::new("model", DataType::Utf8, true),
        Field::new("given", DataType::Utf8, true),
        Field::new("expect", DataType::Utf8, true),
        dep_field,
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![description])),
            Arc::new(StringArray::from(vec![original_file_path])),
            Arc::new(StringArray::from(vec![file_path])),
            Arc::new(fqn_arr),
            Arc::new(StringArray::from(vec![model])),
            Arc::new(StringArray::from(vec![given_json])),
            Arc::new(StringArray::from(vec![expect_json])),
            Arc::new(dep_arr),
        ],
    )
    .expect("valid unit_test batch")
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

fn run_result_batch(
    status: &str,
    execution_time: Option<f64>,
    message: Option<&str>,
    completed_at: Option<&str>,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("status", DataType::Utf8, true),
        Field::new("execution_time", DataType::Float64, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("completed_at", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some(status)])),
            Arc::new(Float64Array::from(vec![execution_time])),
            Arc::new(StringArray::from(vec![message])),
            Arc::new(StringArray::from(vec![completed_at])),
        ],
    )
    .expect("valid run_result batch")
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
    let backend = TestDetailMockBackend {
        node_batches: vec![],
        test_metadata_batches: vec![],
        unit_test_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_test(State(state), Path("bad'id".to_owned())).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn missing_test_returns_404() {
    let backend = TestDetailMockBackend {
        node_batches: vec![],
        test_metadata_batches: vec![],
        unit_test_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_test(State(state), Path("test.pkg.missing".to_owned())).await;
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn data_test_all_fields_hydrated() {
    let backend = TestDetailMockBackend {
        node_batches: vec![data_test_node_batch(
            "test.jaffle_shop.not_null_orders_order_id",
            "not_null_orders_order_id",
            Some("jaffle_shop"),
            Some("Asserts that order_id is never null."),
            Some("models/schema.yml"),
            Some("models/schema.yml"),
            None,
            Some(r#"{"owner":"data-eng"}"#),
            Some("select order_id from {{ model }} where order_id is null"),
            Some("select order_id from prod.dbt_prod.orders where order_id is null"),
            &["data-quality"],
            &["jaffle_shop", "not_null_orders_order_id"],
        )],
        test_metadata_batches: vec![test_metadata_batch(
            "not_null",
            Some(r#"{"column_name":"order_id","model":"ref('orders')"}"#),
            Some("order_id"),
            Some("ERROR"),
        )],
        unit_test_batches: vec![],
        edge_batches: vec![edge_batch(&[("model.jaffle_shop.orders", "ref")])],
        run_result_batches: Some(vec![run_result_batch(
            "pass",
            Some(1.4),
            None,
            Some("2026-05-15T10:32:11Z"),
        )]),
    };
    let state = make_state(backend);
    let r = get_test(
        State(state),
        Path("test.jaffle_shop.not_null_orders_order_id".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    assert_eq!(
        body["unique_id"],
        "test.jaffle_shop.not_null_orders_order_id"
    );
    assert_eq!(body["name"], "not_null_orders_order_id");
    assert_eq!(body["resource_type"], "test");
    assert_eq!(body["package_name"], "jaffle_shop");
    assert_eq!(body["description"], "Asserts that order_id is never null.");

    // Common fields.
    assert_eq!(body["tags"], serde_json::json!(["data-quality"]));
    assert_eq!(
        body["fqn"],
        serde_json::json!(["jaffle_shop", "not_null_orders_order_id"])
    );
    assert_eq!(body["file_path"], "models/schema.yml");
    assert_eq!(body["patch_path"], serde_json::Value::Null);
    assert_eq!(body["meta"], serde_json::json!({"owner": "data-eng"}));

    // Data-test specifics.
    assert_eq!(body["column_name"], "order_id");
    assert_eq!(body["test_type"], "generic");
    assert_eq!(body["severity"], "ERROR");
    assert_eq!(body["test_metadata"]["name"], "not_null");
    assert_eq!(
        body["test_metadata"]["kwargs"],
        serde_json::json!({"column_name": "order_id", "model": "ref('orders')"}),
        "kwargs must be parsed as JSON object, not escaped string"
    );
    assert!(
        body["raw_code"]
            .as_str()
            .is_some_and(|s| s.contains("where order_id is null"))
    );
    assert!(
        body["compiled_code"]
            .as_str()
            .is_some_and(|s| s.contains("prod.dbt_prod.orders"))
    );

    // depends_on populated; referenced_by absent (leaf).
    assert_eq!(
        body["depends_on"][0]["unique_id"],
        "model.jaffle_shop.orders"
    );
    assert!(
        body.get("referenced_by").is_none(),
        "tests must omit referenced_by entirely"
    );

    // ExecutionInfo.
    assert_eq!(body["execution_info"]["status"], "pass");
    assert_eq!(body["execution_info"]["error"], serde_json::Value::Null);
    assert_eq!(body["execution_info"]["execution_time"], 1.4);
    assert_eq!(
        body["execution_info"]["completed_at"],
        "2026-05-15T10:32:11Z"
    );
}

#[tokio::test]
async fn data_test_resource_type_field_is_present() {
    let backend = TestDetailMockBackend {
        node_batches: vec![data_test_node_batch(
            "test.pkg.t",
            "t",
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
        )],
        test_metadata_batches: vec![],
        unit_test_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_test(State(state), Path("test.pkg.t".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["resource_type"], "test");
}

#[tokio::test]
async fn data_test_singular_has_no_test_metadata() {
    // Singular tests don't have a row in `dbt.test_metadata`. `test_type`
    // must be "singular" and `test_metadata`, `column_name`, `severity` null.
    let backend = TestDetailMockBackend {
        node_batches: vec![data_test_node_batch(
            "test.pkg.singular",
            "singular",
            Some("pkg"),
            None,
            None,
            None,
            None,
            None,
            Some("select 1 where false"),
            None,
            &[],
            &[],
        )],
        test_metadata_batches: vec![],
        unit_test_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_test(State(state), Path("test.pkg.singular".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["test_type"], "singular");
    assert_eq!(body["test_metadata"], serde_json::Value::Null);
    assert_eq!(body["column_name"], serde_json::Value::Null);
    assert_eq!(body["severity"], serde_json::Value::Null);
}

#[tokio::test]
async fn data_test_metadata_kwargs_null_when_malformed() {
    let backend = TestDetailMockBackend {
        node_batches: vec![data_test_node_batch(
            "test.pkg.t",
            "t",
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
        )],
        test_metadata_batches: vec![test_metadata_batch(
            "unique",
            Some("not{json"),
            Some("id"),
            None,
        )],
        unit_test_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_test(State(state), Path("test.pkg.t".to_owned())).await;
    assert_eq!(r.status(), 200, "malformed kwargs must not 500");
    let body = response_body(r).await;
    assert_eq!(body["test_metadata"]["name"], "unique");
    assert_eq!(body["test_metadata"]["kwargs"], serde_json::Value::Null);
}

#[tokio::test]
async fn unit_test_all_fields_hydrated() {
    let given_json = r#"[
        {"input":"ref('stg_orders')","rows":[
            {"order_id":1,"status":"completed"},
            {"order_id":2,"status":"pending"}
        ],"format":"dict"}
    ]"#;
    let expect_json = r#"{"rows":[
        {"order_id":1,"amount":25.00}
    ],"format":"dict"}"#;
    let backend = TestDetailMockBackend {
        // Nothing in `dbt.nodes` for unit tests.
        node_batches: vec![],
        test_metadata_batches: vec![],
        unit_test_batches: vec![unit_test_batch(
            "unit_test.jaffle_shop.test_orders_completed_status",
            "test_orders_completed_status",
            Some("jaffle_shop"),
            Some("Checks that completed orders always have a non-null amount."),
            Some("models/schema.yml"),
            Some("models/schema.yml"),
            Some("orders"),
            Some(given_json),
            Some(expect_json),
            &["jaffle_shop", "test_orders_completed_status"],
            &["model.jaffle_shop.orders", "model.jaffle_shop.stg_orders"],
        )],
        edge_batches: vec![],
        run_result_batches: Some(vec![run_result_batch(
            "pass",
            Some(0.8),
            None,
            Some("2026-05-15T10:32:15Z"),
        )]),
    };
    let state = make_state(backend);
    let r = get_test(
        State(state),
        Path("unit_test.jaffle_shop.test_orders_completed_status".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    assert_eq!(
        body["unique_id"],
        "unit_test.jaffle_shop.test_orders_completed_status"
    );
    assert_eq!(body["name"], "test_orders_completed_status");
    assert_eq!(body["resource_type"], "unit_test");
    assert_eq!(body["package_name"], "jaffle_shop");

    // model + given + expect.
    assert_eq!(body["model"], "orders");
    assert_eq!(body["given"][0]["input"], "ref('stg_orders')");
    assert_eq!(body["given"][0]["rows"][0]["order_id"], 1);
    assert_eq!(body["given"][0]["rows"][0]["status"], "completed");
    assert_eq!(body["expect"]["rows"][0]["order_id"], 1);
    assert_eq!(body["expect"]["rows"][0]["amount"], 25.00);
    assert_eq!(body["num_given"], 1);
    assert_eq!(body["num_given_rows"], 2);
    assert_eq!(body["num_expect_rows"], 1);

    // depends_on synthesised from `depends_on_nodes` since edges are empty.
    assert_eq!(
        body["depends_on"][0]["unique_id"],
        "model.jaffle_shop.orders"
    );
    assert_eq!(body["depends_on"][0]["edge_type"], "ref");
    assert!(body.get("referenced_by").is_none());

    // ExecutionInfo.
    assert_eq!(body["execution_info"]["status"], "pass");
    assert_eq!(body["execution_info"]["error"], serde_json::Value::Null);
}

#[tokio::test]
async fn unit_test_resource_type_field_is_present() {
    let backend = TestDetailMockBackend {
        node_batches: vec![],
        test_metadata_batches: vec![],
        unit_test_batches: vec![unit_test_batch(
            "unit_test.pkg.t",
            "t",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &[],
            &[],
        )],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_test(State(state), Path("unit_test.pkg.t".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["resource_type"], "unit_test");
}

#[tokio::test]
async fn unit_test_given_empty_array_when_no_fixtures() {
    let backend = TestDetailMockBackend {
        node_batches: vec![],
        test_metadata_batches: vec![],
        unit_test_batches: vec![unit_test_batch(
            "unit_test.pkg.t",
            "t",
            None,
            None,
            None,
            None,
            None,
            Some("[]"),
            Some(r#"{"rows":[]}"#),
            &[],
            &[],
        )],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_test(State(state), Path("unit_test.pkg.t".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["given"], serde_json::json!([]));
    assert_eq!(body["expect"]["rows"], serde_json::json!([]));
    assert_eq!(body["num_given"], 0);
    assert_eq!(body["num_given_rows"], 0);
    assert_eq!(body["num_expect_rows"], 0);
}

#[tokio::test]
async fn meta_null_when_absent() {
    let backend = TestDetailMockBackend {
        node_batches: vec![data_test_node_batch(
            "test.pkg.t",
            "t",
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
        )],
        test_metadata_batches: vec![],
        unit_test_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_test(State(state), Path("test.pkg.t".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn meta_null_when_malformed() {
    let backend = TestDetailMockBackend {
        node_batches: vec![data_test_node_batch(
            "test.pkg.t",
            "t",
            None,
            None,
            None,
            None,
            None,
            Some("not{json"),
            None,
            None,
            &[],
            &[],
        )],
        test_metadata_batches: vec![],
        unit_test_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![]),
    };
    let state = make_state(backend);
    let r = get_test(State(state), Path("test.pkg.t".to_owned())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn execution_info_null_when_view_absent() {
    let backend = TestDetailMockBackend {
        node_batches: vec![data_test_node_batch(
            "test.pkg.t",
            "t",
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
        )],
        test_metadata_batches: vec![],
        unit_test_batches: vec![],
        edge_batches: vec![],
        run_result_batches: None,
    };
    let state = make_state(backend);
    let r = get_test(State(state), Path("test.pkg.t".to_owned())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["execution_info"], serde_json::Value::Null);
}

#[tokio::test]
async fn execution_info_error_populated_on_failure() {
    let backend = TestDetailMockBackend {
        node_batches: vec![data_test_node_batch(
            "test.pkg.t",
            "t",
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
        )],
        test_metadata_batches: vec![],
        unit_test_batches: vec![],
        edge_batches: vec![],
        run_result_batches: Some(vec![run_result_batch(
            "error",
            Some(0.1),
            Some("Compilation Error: invalid sql"),
            Some("2026-05-15T10:32:11Z"),
        )]),
    };
    let state = make_state(backend);
    let r = get_test(State(state), Path("test.pkg.t".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["execution_info"]["status"], "error");
    assert_eq!(
        body["execution_info"]["error"],
        "Compilation Error: invalid sql"
    );
}

// ===========================================================================
// Tests: GET /api/v1/tests and GET /api/v1/tests/facets
// ===========================================================================
//
// TODO(#10255): replace hand-rolled RecordBatch schemas with typed row
// builders once dbt-index exposes fixture builders bound to the production
// parquet schema.

// ---------------------------------------------------------------------------
// Mock backend for list + facets
// ---------------------------------------------------------------------------

/// Mock backend for list + facets. Routes COUNT(*) queries and row queries
/// by sniffing keywords in the SQL. The UNION ALL query always touches
/// `dbt.nodes`, `dbt.test_metadata`, and `dbt.unit_tests`.
struct TestListMockBackend {
    total_count: u64,
    row_batches: Vec<RecordBatch>,
    /// When `true`, the with-run_results count query returns `None`.
    run_results_absent: bool,
}

impl TestListMockBackend {
    fn new(total: u64, rows: Vec<RecordBatch>) -> Self {
        Self {
            total_count: total,
            row_batches: rows,
            run_results_absent: false,
        }
    }
    fn without_run_results(mut self) -> Self {
        self.run_results_absent = true;
        self
    }
}

impl Backend for TestListMockBackend {
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
        Some(self.total_count.to_string())
    }
    fn query_arrow(&self, _sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        Ok(self.row_batches.clone())
    }
}

fn make_list_state(backend: TestListMockBackend) -> Arc<AppState> {
    let providers = Providers {
        backend: Arc::new(backend),
        ..Providers::default()
    };
    Arc::new(AppState {
        has_dbt_state: false,
        index_dir: PathBuf::from("/tmp"),
        providers,
        do_not_track: false,
    })
}

// ---------------------------------------------------------------------------
// Batch builders for list
// ---------------------------------------------------------------------------

/// Schema for the UNION ALL output — matches the SELECT list in
/// `build_test_list_sql`.
fn test_list_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("resource_type", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("test_type", DataType::Utf8, true),
        Field::new("tested_node_unique_id", DataType::Utf8, true),
        Field::new("tested_column", DataType::Utf8, true),
        Field::new("severity", DataType::Utf8, true),
        Field::new("test_namespace", DataType::Utf8, true),
        Field::new("kwargs_raw", DataType::Utf8, true),
        Field::new("given_raw", DataType::Utf8, true),
        Field::new("expect_raw", DataType::Utf8, true),
        Field::new("rr_status", DataType::Utf8, true),
        Field::new("rr_message", DataType::Utf8, true),
        Field::new("rr_completed_at", DataType::Utf8, true),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn data_test_list_row(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    test_type: Option<&str>,
    tested_node: Option<&str>,
    tested_column: Option<&str>,
    severity: Option<&str>,
    namespace: Option<&str>,
    kwargs_raw: Option<&str>,
    rr_status: Option<&str>,
    rr_message: Option<&str>,
    rr_completed_at: Option<&str>,
) -> RecordBatch {
    RecordBatch::try_new(
        test_list_schema(),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec!["test"])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![test_type])),
            Arc::new(StringArray::from(vec![tested_node])),
            Arc::new(StringArray::from(vec![tested_column])),
            Arc::new(StringArray::from(vec![severity])),
            Arc::new(StringArray::from(vec![namespace])),
            Arc::new(StringArray::from(vec![kwargs_raw])),
            Arc::new(StringArray::from(vec![None::<&str>])), // given_raw
            Arc::new(StringArray::from(vec![None::<&str>])), // expect_raw
            Arc::new(StringArray::from(vec![rr_status])),
            Arc::new(StringArray::from(vec![rr_message])),
            Arc::new(StringArray::from(vec![rr_completed_at])),
        ],
    )
    .expect("valid data test list row")
}

#[allow(clippy::too_many_arguments)]
fn unit_test_list_row(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    tested_node: Option<&str>,
    given_raw: Option<&str>,
    expect_raw: Option<&str>,
    rr_status: Option<&str>,
    rr_completed_at: Option<&str>,
) -> RecordBatch {
    RecordBatch::try_new(
        test_list_schema(),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec!["unit_test"])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![Some("unit")])), // test_type
            Arc::new(StringArray::from(vec![tested_node])),
            Arc::new(StringArray::from(vec![None::<&str>])), // tested_column
            Arc::new(StringArray::from(vec![None::<&str>])), // severity
            Arc::new(StringArray::from(vec![None::<&str>])), // test_namespace
            Arc::new(StringArray::from(vec![None::<&str>])), // kwargs_raw
            Arc::new(StringArray::from(vec![given_raw])),
            Arc::new(StringArray::from(vec![expect_raw])),
            Arc::new(StringArray::from(vec![rr_status])),
            Arc::new(StringArray::from(vec![None::<&str>])), // rr_message
            Arc::new(StringArray::from(vec![rr_completed_at])),
        ],
    )
    .expect("valid unit test list row")
}

// ---------------------------------------------------------------------------
// Unit tests: SQL builder
// ---------------------------------------------------------------------------

#[test]
fn unknown_sort_column_returns_err() {
    let params = TestListParams {
        sort: Some("severity:asc".into()),
        ..Default::default()
    };
    assert!(build_test_list_sql(&params, true, 10, None).is_err());
}

#[test]
fn sort_without_direction_returns_err() {
    let params = TestListParams {
        sort: Some("name".into()),
        ..Default::default()
    };
    assert!(build_test_list_sql(&params, true, 10, None).is_err());
}

#[test]
fn sort_name_asc_succeeds() {
    let params = TestListParams {
        sort: Some("name:asc".into()),
        ..Default::default()
    };
    assert!(build_test_list_sql(&params, true, 10, None).is_ok());
}

#[test]
fn sort_name_desc_succeeds() {
    let params = TestListParams {
        sort: Some("name:desc".into()),
        ..Default::default()
    };
    assert!(build_test_list_sql(&params, true, 10, None).is_ok());
}

#[test]
fn invalid_test_type_param_returns_err() {
    let params = TestListParams {
        test_type: Some("unknown".into()),
        ..Default::default()
    };
    assert!(build_test_list_sql(&params, true, 10, None).is_err());
}

#[test]
fn test_type_unit_filters_resource_type() {
    let params = TestListParams {
        test_type: Some("unit".into()),
        ..Default::default()
    };
    let (count_sql, rows_sql) = build_test_list_sql(&params, true, 10, None).unwrap();
    assert!(count_sql.contains("'unit_test'"));
    assert!(rows_sql.contains("'unit_test'"));
}

#[test]
fn test_type_data_filters_resource_type() {
    let params = TestListParams {
        test_type: Some("data".into()),
        ..Default::default()
    };
    let (_, rows_sql) = build_test_list_sql(&params, true, 10, None).unwrap();
    // The WHERE clause must include 'test' but not 'unit_test' as a filter
    // (note: 'unit_test' still appears in the CTE's UNION ALL construction,
    // but the outer WHERE restricts it).
    assert!(
        rows_sql.contains("resource_type IN ('test')"),
        "rows_sql must filter to 'test' resource_type"
    );
}

#[test]
fn count_sql_excludes_cursor_page_sql_includes_cursor() {
    let c = Cursor {
        sort_value: Some("not_null_orders".into()),
        unique_id: "test.pkg.not_null_orders".into(),
    };
    let params = TestListParams::default();
    let (count, rows) = build_test_list_sql(&params, true, 10, Some(&c)).unwrap();
    assert!(
        !count.contains("not_null_orders"),
        "count must exclude cursor predicate"
    );
    assert!(
        rows.contains("not_null_orders"),
        "rows must include cursor predicate"
    );
}

#[test]
fn no_run_results_emits_null_rr_columns() {
    let params = TestListParams::default();
    let (_, rows) = build_test_list_sql(&params, false, 10, None).unwrap();
    assert!(rows.contains("NULL::VARCHAR AS rr_status"));
}

// ---------------------------------------------------------------------------
// Integration tests: list_tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_tests_empty_catalog() {
    let state = make_list_state(TestListMockBackend::new(0, vec![]));
    let r = list_tests(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"], serde_json::json!([]));
    assert_eq!(body["page_info"]["total_count"], 0);
    assert_eq!(body["page_info"]["has_next_page"], false);
    assert_eq!(body["page_info"]["start_cursor"], serde_json::Value::Null);
    assert_eq!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_tests_data_test_fields_hydrated() {
    let row = data_test_list_row(
        "test.jaffle_shop.not_null_orders_order_id.d12f0947c8",
        "not_null_orders_order_id",
        Some("jaffle_shop"),
        Some("not_null"),
        Some("model.jaffle_shop.orders"),
        Some("order_id"),
        None, // severity null in sample project
        None,
        Some(r#"{"column_name":"order_id","model":"ref('orders')"}"#),
        Some("pass"),
        None,
        Some("2026-05-15T10:32:11Z"),
    );
    let state = make_list_state(TestListMockBackend::new(1, vec![row]));
    let r = list_tests(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    let item = &body["data"][0];
    assert_eq!(item["resource_type"], "test");
    assert_eq!(
        item["unique_id"],
        "test.jaffle_shop.not_null_orders_order_id.d12f0947c8"
    );
    assert_eq!(item["name"], "not_null_orders_order_id");
    assert_eq!(item["package_name"], "jaffle_shop");
    assert_eq!(item["test_type"], "not_null");
    assert_eq!(item["tested_node_unique_id"], "model.jaffle_shop.orders");
    assert_eq!(item["tested_column"], "order_id");
    assert_eq!(item["severity"], serde_json::Value::Null);
    assert_eq!(item["execution_info"]["status"], "pass");
    assert_eq!(
        item["execution_info"]["completed_at"],
        "2026-05-15T10:32:11Z"
    );
    // test_metadata present (has kwargs_raw).
    assert_eq!(item["test_metadata"]["kwargs"]["column_name"], "order_id");
    // Unit-test-only fields must be ABSENT (not null) on data-test rows.
    assert!(
        item.get("num_given").is_none(),
        "num_given must be absent on data test rows"
    );
    assert_eq!(body["page_info"]["total_count"], 1);
}

#[tokio::test]
async fn list_tests_unit_test_fields_hydrated() {
    let given_json =
        r#"[{"input":"ref('stg_orders')","rows":[{"id":1},{"id":2}],"format":"dict"}]"#;
    let expect_json = r#"{"rows":[{"id":1}],"format":"dict"}"#;
    let row = unit_test_list_row(
        "unit_test.jaffle_shop.orders.test_supply_costs",
        "test_supply_costs",
        Some("jaffle_shop"),
        Some("model.jaffle_shop.orders"),
        Some(given_json),
        Some(expect_json),
        Some("pass"),
        Some("2026-05-15T10:32:15Z"),
    );
    let state = make_list_state(TestListMockBackend::new(1, vec![row]));
    let r = list_tests(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    let item = &body["data"][0];
    assert_eq!(item["resource_type"], "unit_test");
    assert_eq!(item["test_type"], "unit");
    assert_eq!(item["tested_node_unique_id"], "model.jaffle_shop.orders");
    assert_eq!(item["tested_column"], serde_json::Value::Null);
    assert_eq!(item["severity"], serde_json::Value::Null);
    assert_eq!(item["num_given"], 1);
    assert_eq!(item["num_given_rows"], 2);
    assert_eq!(item["num_expect_rows"], 1);
    assert_eq!(item["execution_info"]["status"], "pass");
    // Data-test-only field must be ABSENT on unit-test rows.
    assert!(
        item.get("test_metadata").is_none(),
        "test_metadata must be absent on unit test rows"
    );
}

#[tokio::test]
async fn list_tests_execution_info_null_when_run_results_absent() {
    let row = data_test_list_row(
        "test.pkg.t",
        "t",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None, // rr_status null
        None,
        None,
    );
    let state = make_list_state(TestListMockBackend::new(1, vec![row]).without_run_results());
    let r = list_tests(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["execution_info"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_tests_unknown_sort_returns_400() {
    let state = make_list_state(TestListMockBackend::new(0, vec![]));
    let params = TestListParams {
        sort: Some("test_type:asc".into()),
        ..Default::default()
    };
    let r = list_tests(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_tests_invalid_cursor_returns_400() {
    let state = make_list_state(TestListMockBackend::new(0, vec![]));
    let params = TestListParams {
        after: Some("not-valid-base64!!!".into()),
        ..Default::default()
    };
    let r = list_tests(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_tests_multi_page_has_next_page_true() {
    let row_a = data_test_list_row(
        "test.pkg.alpha",
        "alpha",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    );
    let row_b = data_test_list_row(
        "test.pkg.beta",
        "beta",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    );
    let state = make_list_state(TestListMockBackend::new(2, vec![row_a, row_b]));
    let params = TestListParams {
        first: Some(1),
        ..Default::default()
    };
    let r = list_tests(State(state), Query(params)).await;
    let body = response_body(r).await;
    assert_eq!(body["page_info"]["has_next_page"], true);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_ne!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_tests_last_page_end_cursor_null() {
    let row = data_test_list_row(
        "test.pkg.z",
        "z",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    );
    let state = make_list_state(TestListMockBackend::new(1, vec![row]));
    let r = list_tests(State(state), Query(Default::default())).await;
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
async fn list_tests_cursor_advances_page() {
    let after = Cursor {
        sort_value: Some("alpha".into()),
        unique_id: "test.pkg.alpha".into(),
    }
    .encode();
    let row = data_test_list_row(
        "test.pkg.beta",
        "beta",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    );
    let state = make_list_state(TestListMockBackend::new(1, vec![row]));
    let params = TestListParams {
        after: Some(after),
        ..Default::default()
    };
    let r = list_tests(State(state), Query(params)).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["name"], "beta");
}

#[tokio::test]
async fn list_tests_invalid_test_type_param_returns_400() {
    let state = make_list_state(TestListMockBackend::new(0, vec![]));
    let params = TestListParams {
        test_type: Some("unknown_type".into()),
        ..Default::default()
    };
    let r = list_tests(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

// ---------------------------------------------------------------------------
// Integration tests: list_test_facets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_test_facets_results_exact_set() {
    let r = list_test_facets().await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    let results: Vec<String> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["value"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        results,
        vec!["pass", "fail", "warn", "error", "skipped", "unknown"],
        "results must match testStatusesPlusUnknown exactly"
    );
}

#[tokio::test]
async fn list_test_facets_run_statuses_exact_set() {
    let r = list_test_facets().await;
    let body = response_body(r).await;
    let run_statuses: Vec<String> = body["run_statuses"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["value"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        run_statuses,
        vec!["success", "error", "skipped", "reused"],
        "run_statuses must match runStatuses exactly"
    );
}

#[tokio::test]
async fn list_test_facets_test_types_exact_set() {
    let r = list_test_facets().await;
    let body = response_body(r).await;
    let test_types: Vec<String> = body["test_types"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["value"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        test_types,
        vec!["unit", "data"],
        "test_types must match testTypesDisplay exactly"
    );
}

#[tokio::test]
async fn list_test_facets_count_always_null() {
    let r = list_test_facets().await;
    let body = response_body(r).await;
    // All values in all arrays must have count: null.
    for key in &["results", "run_statuses", "test_types"] {
        for item in body[key].as_array().unwrap() {
            assert_eq!(
                item["count"],
                serde_json::Value::Null,
                "{key}[*].count must be null"
            );
        }
    }
}
