//! Tests for `GET /api/v1/tests/:id`.
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
use axum::extract::{Path, State};
use axum::response::Response;

use super::*;
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
        index_dir: PathBuf::from("/tmp"),
        providers,
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
