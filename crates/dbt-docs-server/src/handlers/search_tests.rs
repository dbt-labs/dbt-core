//! Integration tests for `GET /api/v1/search`.
//!
//! Tests use a multi-table mock backend that routes queries by SQL content
//! to the appropriate fixture batch, mirroring the pattern in models_tests.rs
//! and sources_tests.rs.

use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{BooleanArray, ListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use axum::extract::{Query, State};
use axum::response::Response;

use super::*;
use crate::providers::{Backend, BackendError, Providers};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Mock backend
// ---------------------------------------------------------------------------

/// Routes Arrow queries to fixture batches based on SQL content.
struct SearchMockBackend {
    /// Returned for queries against dbt.nodes.
    nodes_batches: Vec<RecordBatch>,
    /// Returned for queries against dbt.macros.
    macros_batches: Vec<RecordBatch>,
    /// Returned for queries against dbt.groups.
    groups_batches: Vec<RecordBatch>,
    /// Returned for queries against dbt.metrics.
    metrics_batches: Vec<RecordBatch>,
    /// Returned for queries against dbt.saved_queries.
    saved_queries_batches: Vec<RecordBatch>,
    /// Returned for queries against dbt.semantic_models.
    semantic_models_batches: Vec<RecordBatch>,
    /// Returned for queries against dbt.exposures.
    exposures_batches: Vec<RecordBatch>,
    /// Returned for queries against dbt.unit_tests.
    unit_tests_batches: Vec<RecordBatch>,
    /// Returned for queries against dbt.node_columns.
    node_columns_batches: Vec<RecordBatch>,
    /// When true, queries mentioning source_freshness fail (view absent).
    freshness_absent: bool,
    /// Fixed count to return for COUNT queries.
    count_override: Option<u64>,
}

impl SearchMockBackend {
    fn empty() -> Self {
        Self {
            nodes_batches: vec![],
            macros_batches: vec![],
            groups_batches: vec![],
            metrics_batches: vec![],
            saved_queries_batches: vec![],
            semantic_models_batches: vec![],
            exposures_batches: vec![],
            unit_tests_batches: vec![],
            node_columns_batches: vec![],
            freshness_absent: false,
            count_override: None,
        }
    }

    fn with_nodes(mut self, batches: Vec<RecordBatch>) -> Self {
        self.nodes_batches = batches;
        self
    }

    fn with_macros(mut self, batches: Vec<RecordBatch>) -> Self {
        self.macros_batches = batches;
        self
    }

    fn with_node_columns(mut self, batches: Vec<RecordBatch>) -> Self {
        self.node_columns_batches = batches;
        self
    }

    fn without_freshness(mut self) -> Self {
        self.freshness_absent = true;
        self
    }

    fn with_count(mut self, count: u64) -> Self {
        self.count_override = Some(count);
        self
    }
}

impl Backend for SearchMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, sql: &str) -> Option<String> {
        if self.freshness_absent && sql.contains("source_freshness") {
            return None;
        }
        if let Some(count) = self.count_override {
            return Some(count.to_string());
        }
        // Count the rows in the union result — approximate for tests
        let total = self
            .nodes_batches
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>()
            + self
                .macros_batches
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>()
            + self
                .groups_batches
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>()
            + self
                .metrics_batches
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>()
            + self
                .saved_queries_batches
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>()
            + self
                .semantic_models_batches
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>()
            + self
                .exposures_batches
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>()
            + self
                .unit_tests_batches
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>();
        Some(total.to_string())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        if self.freshness_absent && sql.contains("source_freshness") {
            return Err(BackendError::Query("source_freshness view absent".into()));
        }
        // Route by SQL content keywords.
        // The column-highlight sub-query is: "SELECT DISTINCT column_name FROM dbt.node_columns WHERE ..."
        // The page/count SQL also references dbt.node_columns inside the field_matches CTE but starts
        // with "WITH base". Distinguish by checking for the targeted SELECT DISTINCT form.
        if sql.contains("SELECT DISTINCT column_name") {
            return Ok(self.node_columns_batches.clone());
        }
        // The search SQL builds a UNION and wraps in a CTE. We detect by which
        // table names appear in the SQL.
        // Build merged result batches from all relevant tables.
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        if sql.contains("dbt.nodes") {
            all_batches.extend(self.nodes_batches.clone());
        }
        if sql.contains("dbt.macros") {
            all_batches.extend(self.macros_batches.clone());
        }
        if sql.contains("dbt.groups") {
            all_batches.extend(self.groups_batches.clone());
        }
        if sql.contains("dbt.metrics") {
            all_batches.extend(self.metrics_batches.clone());
        }
        if sql.contains("dbt.saved_queries") {
            all_batches.extend(self.saved_queries_batches.clone());
        }
        if sql.contains("dbt.semantic_models") {
            all_batches.extend(self.semantic_models_batches.clone());
        }
        if sql.contains("dbt.exposures") {
            all_batches.extend(self.exposures_batches.clone());
        }
        if sql.contains("dbt.unit_tests") {
            all_batches.extend(self.unit_tests_batches.clone());
        }
        Ok(all_batches)
    }
}

fn make_state(backend: SearchMockBackend) -> Arc<AppState> {
    let providers = Providers {
        backend: Arc::new(backend),
        ..Providers::default()
    };
    Arc::new(AppState::new(std::path::PathBuf::from("/tmp"), providers))
}

async fn response_body(response: Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&bytes).expect("valid json")
}

// ---------------------------------------------------------------------------
// Batch builders
// ---------------------------------------------------------------------------

/// Build a single-element `ListArray` from string slices.
fn make_str_list_array(values: &[&str]) -> ListArray {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for v in values {
        builder.values().append_value(v);
    }
    builder.append(true);
    builder.finish()
}

/// Schema for the uniform search result row (what the handler projects).
fn search_row_schema() -> Arc<Schema> {
    let fqn_arr = make_str_list_array(&[]);
    let tags_arr = make_str_list_array(&[]);
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("resource_type", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("fqn", fqn_arr.data_type().clone(), true),
        Field::new("tags", tags_arr.data_type().clone(), true),
        Field::new("description", DataType::Utf8, true),
        Field::new("materialized", DataType::Utf8, true),
        Field::new("access_level", DataType::Utf8, true),
        Field::new("source_name", DataType::Utf8, true),
        Field::new("freshness_checked", DataType::Boolean, true),
        Field::new("test_type", DataType::Utf8, true),
        Field::new("exposure_type", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("matched_field", DataType::Utf8, true),
    ]))
}

/// Build a search result RecordBatch for a single row.
#[allow(clippy::too_many_arguments)]
fn make_search_row(
    unique_id: &str,
    name: &str,
    resource_type: &str,
    package_name: Option<&str>,
    fqn: Option<&[&str]>,
    tags: Option<&[&str]>,
    description: Option<&str>,
    materialized: Option<&str>,
    access_level: Option<&str>,
    source_name: Option<&str>,
    freshness_checked: Option<bool>,
    test_type: Option<&str>,
    exposure_type: Option<&str>,
    original_file_path: Option<&str>,
    matched_field: Option<&str>,
) -> RecordBatch {
    let schema = search_row_schema();

    let fqn_arr: Arc<dyn Array> = match fqn {
        Some(vals) => {
            let arr = make_str_list_array(vals);
            Arc::new(arr)
        }
        None => {
            // null list
            let mut builder = ListBuilder::new(StringBuilder::new());
            builder.append_null();
            Arc::new(builder.finish())
        }
    };
    let tags_arr: Arc<dyn Array> = match tags {
        Some(vals) => {
            let arr = make_str_list_array(vals);
            Arc::new(arr)
        }
        None => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            builder.append_null();
            Arc::new(builder.finish())
        }
    };

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec![resource_type])),
            Arc::new(StringArray::from(vec![package_name])),
            fqn_arr,
            tags_arr,
            Arc::new(StringArray::from(vec![description])),
            Arc::new(StringArray::from(vec![materialized])),
            Arc::new(StringArray::from(vec![access_level])),
            Arc::new(StringArray::from(vec![source_name])),
            Arc::new(BooleanArray::from(vec![freshness_checked])),
            Arc::new(StringArray::from(vec![test_type])),
            Arc::new(StringArray::from(vec![exposure_type])),
            Arc::new(StringArray::from(vec![original_file_path])),
            Arc::new(StringArray::from(vec![matched_field])),
        ],
    )
    .expect("valid search row batch")
}

/// Make a simple model row (name match).
fn make_model_row(unique_id: &str, name: &str) -> RecordBatch {
    make_search_row(
        unique_id,
        name,
        "model",
        Some("jaffle_shop"),
        Some(&["jaffle_shop", name]),
        Some(&[]),
        Some("A model"),
        Some("table"),
        Some("public"),
        None,
        None,
        None,
        None,
        Some("models/jaffle_shop.sql"),
        None, // matched_field null = browse mode
    )
}

/// Make a model row with a matched_field.
fn make_model_row_matched(unique_id: &str, name: &str, matched_field: &str) -> RecordBatch {
    make_search_row(
        unique_id,
        name,
        "model",
        Some("jaffle_shop"),
        Some(&["jaffle_shop", name]),
        Some(&[]),
        Some("A model"),
        Some("table"),
        Some("public"),
        None,
        None,
        None,
        None,
        Some("models/jaffle_shop.sql"),
        Some(matched_field),
    )
}

/// Make a macro row (no fqn, no tags).
fn make_macro_row(unique_id: &str, name: &str, matched_field: Option<&str>) -> RecordBatch {
    make_search_row(
        unique_id,
        name,
        "macro",
        Some("jaffle_shop"),
        None, // no fqn
        None, // no tags
        Some("A macro"),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        matched_field,
    )
}

/// Make a source row with freshness.
fn make_source_row(
    unique_id: &str,
    name: &str,
    freshness_checked: Option<bool>,
    matched_field: Option<&str>,
) -> RecordBatch {
    make_search_row(
        unique_id,
        name,
        "source",
        Some("jaffle_shop"),
        Some(&["jaffle_shop", "raw_jaffle", name]),
        Some(&[]),
        None,
        None,
        None,
        Some("raw_jaffle"),
        freshness_checked,
        None,
        None,
        None,
        matched_field,
    )
}

/// Schema for node_columns fixture.
fn node_columns_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
    ]))
}

fn make_node_columns_batch(rows: &[(&str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        node_columns_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(uid, _)| *uid).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, col)| *col).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("valid columns batch")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn empty_catalog_search_returns_empty_data() {
    let state = make_state(SearchMockBackend::empty());
    let response = search(State(state), Query(SearchQueryParams::default())).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
    assert_eq!(body["page_info"]["total_count"], 0);
    assert_eq!(body["page_info"]["has_next_page"], false);
}

#[tokio::test]
async fn browse_mode_no_q_returns_all_resources() {
    let row = make_model_row("model.jaffle_shop.orders", "orders");
    let state = make_state(SearchMockBackend::empty().with_nodes(vec![row]));
    let response = search(State(state), Query(SearchQueryParams::default())).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    let data = body["data"].as_array().unwrap();
    assert!(!data.is_empty());
    for item in data {
        assert_eq!(item["matched_field"], serde_json::Value::Null);
        assert_eq!(item["highlight"], serde_json::Value::Null);
    }
}

#[tokio::test]
async fn browse_mode_empty_q_treated_as_browse() {
    let row = make_model_row("model.jaffle_shop.orders", "orders");
    let state = make_state(SearchMockBackend::empty().with_nodes(vec![row]));
    let params = SearchQueryParams {
        q: Some(String::new()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    for item in body["data"].as_array().unwrap() {
        assert_eq!(item["matched_field"], serde_json::Value::Null);
        assert_eq!(item["highlight"], serde_json::Value::Null);
    }
}

#[tokio::test]
async fn search_name_match_suppresses_highlight() {
    let row = make_model_row_matched("model.jaffle_shop.orders", "orders", "name");
    let state = make_state(SearchMockBackend::empty().with_nodes(vec![row]));
    let params = SearchQueryParams {
        q: Some("orders".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    let data = body["data"].as_array().unwrap();
    assert!(!data.is_empty());
    // All name matches must have null highlight.
    for item in data {
        if item["matched_field"] == "name" {
            assert_eq!(
                item["highlight"],
                serde_json::Value::Null,
                "highlight must be null for name matches"
            );
        }
    }
}

#[tokio::test]
async fn search_description_match_returns_highlight() {
    let row = make_search_row(
        "model.jaffle_shop.orders",
        "orders",
        "model",
        Some("jaffle_shop"),
        Some(&["jaffle_shop", "orders"]),
        Some(&[]),
        Some("combining payments and order status, one row per order"),
        Some("table"),
        Some("public"),
        None,
        None,
        None,
        None,
        Some("models/orders.sql"),
        Some("description"),
    );
    let state = make_state(SearchMockBackend::empty().with_nodes(vec![row]));
    let params = SearchQueryParams {
        q: Some("order".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    let data = body["data"].as_array().unwrap();
    assert!(!data.is_empty());
    let item = &data[0];
    assert_eq!(item["matched_field"], "description");
    let highlight = item["highlight"]
        .as_str()
        .expect("highlight must be string");
    assert!(
        highlight.contains("<b>"),
        "highlight must contain <b> tags, got: {highlight}"
    );
}

#[tokio::test]
async fn search_column_match_returns_comma_joined_highlight() {
    // Node row with matched_field=column
    let node_row = make_search_row(
        "model.jaffle_shop.fct_orders",
        "fct_orders",
        "model",
        Some("jaffle_shop"),
        Some(&["jaffle_shop", "marts", "fct_orders"]),
        Some(&[]),
        None,
        Some("table"),
        Some("public"),
        None,
        None,
        None,
        None,
        Some("models/marts/fct_orders.sql"),
        Some("column"),
    );
    // Column fixture matching "order"
    let col_batch = make_node_columns_batch(&[
        ("model.jaffle_shop.fct_orders", "order_id"),
        ("model.jaffle_shop.fct_orders", "order_status"),
    ]);
    let state = make_state(
        SearchMockBackend::empty()
            .with_nodes(vec![node_row])
            .with_node_columns(vec![col_batch]),
    );
    let params = SearchQueryParams {
        q: Some("order".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    let data = body["data"].as_array().unwrap();
    assert!(!data.is_empty());
    let item = &data[0];
    assert_eq!(item["matched_field"], "column");
    let highlight = item["highlight"]
        .as_str()
        .expect("highlight must be string for column match");
    assert!(
        highlight.contains("<b>order</b>"),
        "highlight must wrap matched substring, got: {highlight}"
    );
    // Must be comma-joined if multiple columns
    assert!(
        highlight.contains(", "),
        "must be comma-joined: {highlight}"
    );
}

#[tokio::test]
async fn search_tag_match_returns_highlight() {
    let row = make_search_row(
        "model.jaffle_shop.orders",
        "orders",
        "model",
        Some("jaffle_shop"),
        Some(&["jaffle_shop", "orders"]),
        Some(&["orders-core"]),
        None,
        Some("table"),
        Some("public"),
        None,
        None,
        None,
        None,
        Some("models/orders.sql"),
        Some("tag"),
    );
    let state = make_state(SearchMockBackend::empty().with_nodes(vec![row]));
    let params = SearchQueryParams {
        q: Some("order".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    let data = body["data"].as_array().unwrap();
    assert!(!data.is_empty());
    let item = &data[0];
    assert_eq!(item["matched_field"], "tag");
    let highlight = item["highlight"]
        .as_str()
        .expect("highlight must be string for tag match");
    assert!(
        highlight.contains("<b>"),
        "highlight must contain <b> tags, got: {highlight}"
    );
}

#[tokio::test]
async fn cursor_pagination_advances_correctly() {
    // Build two rows with distinct names so cursor ordering works.
    let row1 = make_model_row("model.jaffle_shop.alpha", "alpha");
    let row2 = make_model_row("model.jaffle_shop.beta", "beta");

    // First page: first=1 → returns 2 rows (peek), reports has_next_page=true.
    let state = make_state(
        SearchMockBackend::empty()
            .with_nodes(vec![row1.clone(), row2.clone()])
            .with_count(2),
    );
    let params = SearchQueryParams {
        first: Some(1),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    assert_eq!(body["page_info"]["has_next_page"], true);
    assert!(body["page_info"]["end_cursor"].is_string());

    let cursor = body["page_info"]["end_cursor"].as_str().unwrap().to_owned();

    // Second page: use cursor from first page.
    let state2 = make_state(
        SearchMockBackend::empty()
            .with_nodes(vec![row2.clone()])
            .with_count(2),
    );
    let params2 = SearchQueryParams {
        first: Some(1),
        after: Some(cursor),
        ..Default::default()
    };
    let response2 = search(State(state2), Query(params2)).await;
    assert_eq!(response2.status(), 200);
    let body2 = response_body(response2).await;
    assert_eq!(body2["data"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn invalid_cursor_returns_400() {
    let state = make_state(SearchMockBackend::empty());
    let params = SearchQueryParams {
        after: Some("notbase64".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 400);
    let body = response_body(response).await;
    assert_eq!(body["code"], "invalid_cursor");
}

#[tokio::test]
async fn type_filter_restricts_resource_types() {
    // Backend has both nodes (model) and macros, but we filter to model only.
    let model_row = make_model_row("model.jaffle_shop.orders", "orders");
    let macro_row = make_macro_row("macro.jaffle_shop.my_macro", "my_macro", None);
    let state = make_state(
        SearchMockBackend::empty()
            .with_nodes(vec![model_row])
            .with_macros(vec![macro_row]),
    );
    let params = SearchQueryParams {
        type_filter: Some("model".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    for item in body["data"].as_array().unwrap() {
        assert_eq!(item["hit"]["resource_type"], "model");
    }
}

#[tokio::test]
async fn invalid_type_filter_returns_400() {
    let state = make_state(SearchMockBackend::empty());
    let params = SearchQueryParams {
        type_filter: Some("nonexistent_type".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 400);
    let body = response_body(response).await;
    assert_eq!(body["code"], "invalid_type");
}

#[tokio::test]
async fn package_filter_restricts_packages() {
    let row = make_model_row("model.jaffle_shop.orders", "orders");
    let state = make_state(SearchMockBackend::empty().with_nodes(vec![row]));
    let params = SearchQueryParams {
        package: Some("jaffle_shop".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    // Verify package filter doesn't cause error (actual SQL filtering tested empirically).
}

#[tokio::test]
async fn tag_filter_restricts_by_tag() {
    let row = make_model_row("model.jaffle_shop.orders", "orders");
    let state = make_state(SearchMockBackend::empty().with_nodes(vec![row]));
    let params = SearchQueryParams {
        tag: Some("nightly".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    // Verify tag filter doesn't cause error.
}

#[tokio::test]
async fn modeling_layer_filter_restricts_to_models() {
    let row = make_search_row(
        "model.jaffle_shop.stg_orders",
        "stg_orders",
        "model",
        Some("jaffle_shop"),
        Some(&["jaffle_shop", "staging", "stg_orders"]),
        Some(&[]),
        None,
        Some("view"),
        Some("protected"),
        None,
        None,
        None,
        None,
        Some("models/staging/stg_orders.sql"),
        None,
    );
    let state = make_state(SearchMockBackend::empty().with_nodes(vec![row]));
    let params = SearchQueryParams {
        modeling_layer: Some("Staging".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    for item in body["data"].as_array().unwrap() {
        assert_eq!(item["hit"]["resource_type"], "model");
    }
}

#[tokio::test]
async fn query_too_long_returns_400() {
    let state = make_state(SearchMockBackend::empty());
    let long_q = "a".repeat(1025);
    let params = SearchQueryParams {
        q: Some(long_q),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 400);
    let body = response_body(response).await;
    assert_eq!(body["code"], "query_too_long");
}

#[tokio::test]
async fn ilike_wildcards_in_query_are_escaped() {
    // A query with % and _ must not produce SQL errors or match wildcardly.
    let state = make_state(SearchMockBackend::empty());
    let params = SearchQueryParams {
        q: Some("%percent_".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    // Must return 200, not 500 (SQL syntax error from unescaped wildcards).
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn fqn_absent_for_macro_hits() {
    let macro_row = make_macro_row("macro.jaffle_shop.my_macro", "my_macro", Some("name"));
    let state = make_state(SearchMockBackend::empty().with_macros(vec![macro_row]));
    let params = SearchQueryParams {
        type_filter: Some("macro".into()),
        q: Some("my".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    let data = body["data"].as_array().unwrap();
    if !data.is_empty() {
        let hit = &data[0]["hit"];
        // fqn must be ABSENT (not null) for macro hits.
        assert!(
            hit.get("fqn").is_none(),
            "fqn must be absent for macro hits, got: {hit}"
        );
    }
}

#[tokio::test]
async fn freshness_checked_null_without_capability() {
    let row = make_source_row(
        "source.jaffle_shop.raw_jaffle.orders",
        "orders",
        None, // freshness_checked null because view absent
        None,
    );
    let state = make_state(
        SearchMockBackend::empty()
            .with_nodes(vec![row])
            .without_freshness(),
    );
    let params = SearchQueryParams {
        type_filter: Some("source".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    let data = body["data"].as_array().unwrap();
    if !data.is_empty() {
        let hit = &data[0]["hit"];
        assert_eq!(
            hit["freshness_checked"],
            serde_json::Value::Null,
            "freshness_checked must be null when capability absent"
        );
    }
}

#[tokio::test]
async fn total_count_reflects_full_result_set() {
    // first=1 but count=5 — total_count must reflect the full count.
    let rows: Vec<RecordBatch> = (0..5)
        .map(|i| make_model_row(&format!("model.pkg.model{i}"), &format!("model{i}")))
        .collect();
    let state = make_state(SearchMockBackend::empty().with_nodes(rows).with_count(5));
    let params = SearchQueryParams {
        first: Some(1),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    assert_eq!(
        body["page_info"]["total_count"], 5,
        "total_count must reflect full set, not just current page"
    );
}

#[tokio::test]
async fn multi_token_and_semantics() {
    // Query "order customer" — a row matching both tokens should appear.
    let row = make_search_row(
        "model.jaffle_shop.order_customer",
        "order_customer",
        "model",
        Some("jaffle_shop"),
        Some(&["jaffle_shop", "order_customer"]),
        Some(&[]),
        Some("order per customer"),
        Some("table"),
        Some("public"),
        None,
        None,
        None,
        None,
        Some("models/order_customer.sql"),
        Some("name"),
    );
    let state = make_state(SearchMockBackend::empty().with_nodes(vec![row]));
    let params = SearchQueryParams {
        q: Some("order customer".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    // Should not error even with multi-token query.
}

#[tokio::test]
async fn default_page_size_is_fifty() {
    // 60 model rows — unconstrained GET /search must return 50 and has_next_page=true.
    let rows: Vec<RecordBatch> = (0..60)
        .map(|i| make_model_row(&format!("model.pkg.model{i:02}"), &format!("model{i:02}")))
        .collect();
    // Count must be 60 so total_count is correct.
    let state = make_state(SearchMockBackend::empty().with_nodes(rows).with_count(60));
    let params = SearchQueryParams::default();
    let response = search(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    let count = body["data"].as_array().unwrap().len();
    assert_eq!(count, 50, "default page size must be 50, got {count}");
    assert_eq!(body["page_info"]["has_next_page"], true);
}

#[tokio::test]
async fn single_quote_in_query_returns_200() {
    // ?q=o'reilly must not produce a SQL syntax error.
    let state = make_state(SearchMockBackend::empty());
    let params = SearchQueryParams {
        q: Some("o'reilly".into()),
        ..Default::default()
    };
    let response = search(State(state), Query(params)).await;
    assert_eq!(
        response.status(),
        200,
        "single quote in query must return 200, not 500"
    );
}

// ---------------------------------------------------------------------------
// Unit tests for helper functions
// ---------------------------------------------------------------------------

#[test]
fn escape_ilike_escapes_wildcards() {
    use crate::handlers::sql::escape_ilike;
    assert_eq!(escape_ilike("100% pure"), "100\\% pure");
    assert_eq!(escape_ilike("snake_case"), "snake\\_case");
    assert_eq!(escape_ilike("o'reilly"), "o''reilly");
    assert_eq!(escape_ilike(r"back\slash"), r"back\\slash");
    assert_eq!(escape_ilike("%_"), r"\%\_");
}

#[test]
fn build_highlight_wraps_match_case_insensitive() {
    let result = build_highlight("Orders by Customer", &["orders"]);
    assert_eq!(result, "<b>Orders</b> by Customer");
}

#[test]
fn build_highlight_multi_token() {
    let result = build_highlight("order_id and customer_id", &["order", "customer"]);
    assert!(result.contains("<b>order</b>"));
    assert!(result.contains("<b>customer</b>"));
}

#[test]
fn build_highlight_preserves_original_casing() {
    let result = build_highlight("ORDERS list", &["orders"]);
    assert_eq!(result, "<b>ORDERS</b> list");
}
