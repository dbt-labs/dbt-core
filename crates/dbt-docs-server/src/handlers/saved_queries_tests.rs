//! Tests for `GET /api/v1/saved_queries/:id`, `GET /api/v1/saved_queries`,
//! and `GET /api/v1/saved_queries/facets`.
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
use axum::extract::{Path, Query, State};
use axum::response::Response;

use super::*;
use crate::handlers::pagination::Cursor;
use crate::providers::{Backend, BackendError, Providers};
use crate::state::AppState;

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
        ..Providers::default()
    };
    Arc::new(AppState {
        has_dbt_state: false,
        index_dir: PathBuf::from("/tmp"),
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

// ---------------------------------------------------------------------------
// Mock backend: list_saved_queries / list_saved_query_facets
// ---------------------------------------------------------------------------

// TODO(#10255): replace hand-rolled RecordBatch schemas with typed row
// builders once dbt-index ships them.

struct SavedQueryListMockBackend {
    total_count: u64,
    row_batches: Vec<RecordBatch>,
}

impl SavedQueryListMockBackend {
    fn new(total: u64, rows: Vec<RecordBatch>) -> Self {
        Self {
            total_count: total,
            row_batches: rows,
        }
    }
}

impl Backend for SavedQueryListMockBackend {
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

fn make_list_state(backend: SavedQueryListMockBackend) -> Arc<AppState> {
    let providers = Providers {
        backend: Arc::new(backend),
        ..Providers::default()
    };
    Arc::new(AppState {
        has_dbt_state: false,
        index_dir: PathBuf::from("/tmp"),
        providers,
        do_not_track: false,
        send_anonymous_usage_stats: true,
    })
}

// ---------------------------------------------------------------------------
// Batch builders: list rows
// ---------------------------------------------------------------------------

fn saved_query_list_schema(tags_field: &Field, deps_field: &Field) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("group_name", DataType::Utf8, true),
        tags_field.clone(),
        Field::new("description", DataType::Utf8, true),
        Field::new("created_at", DataType::Float64, true),
        deps_field.clone(),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn saved_query_list_row(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    group_name: Option<&str>,
    tags: &[&str],
    description: Option<&str>,
    created_at: Option<f64>,
    depends_on_nodes: &[&str],
) -> RecordBatch {
    let tags_arr = make_str_list(tags);
    let deps_arr = make_str_list(depends_on_nodes);
    let tags_field = Field::new("tags", tags_arr.data_type().clone(), true);
    let deps_field = Field::new("depends_on_nodes", deps_arr.data_type().clone(), true);

    RecordBatch::try_new(
        saved_query_list_schema(&tags_field, &deps_field),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![group_name])),
            Arc::new(tags_arr),
            Arc::new(StringArray::from(vec![description])),
            Arc::new(Float64Array::from(vec![created_at])),
            Arc::new(deps_arr),
        ],
    )
    .expect("valid saved query list row batch")
}

// ---------------------------------------------------------------------------
// Unit tests: SQL builder
// ---------------------------------------------------------------------------

#[test]
fn sort_default_is_name_asc() {
    let params = SavedQueryListParams::default();
    let (_, rows) = build_saved_query_list_sql(&params, 10, None).unwrap();
    assert!(rows.contains("ORDER BY sq.name ASC NULLS LAST"));
}

#[test]
fn sort_name_desc_accepted() {
    let params = SavedQueryListParams {
        sort: Some("name:desc".into()),
        ..Default::default()
    };
    let (_, rows) = build_saved_query_list_sql(&params, 10, None).unwrap();
    assert!(rows.contains("ORDER BY sq.name DESC NULLS LAST"));
}

#[test]
fn sort_unknown_column_returns_err() {
    let params = SavedQueryListParams {
        sort: Some("package_name:asc".into()),
        ..Default::default()
    };
    assert!(build_saved_query_list_sql(&params, 10, None).is_err());
}

#[test]
fn sort_unknown_direction_returns_err() {
    let params = SavedQueryListParams {
        sort: Some("name:random".into()),
        ..Default::default()
    };
    assert!(build_saved_query_list_sql(&params, 10, None).is_err());
}

#[test]
fn count_sql_excludes_cursor_rows_sql_includes_cursor() {
    let c = Cursor {
        sort_value: Some("weekly_revenue".into()),
        unique_id: "saved_query.jaffle_shop.weekly_revenue".into(),
    };
    let params = SavedQueryListParams::default();
    let (count, rows) = build_saved_query_list_sql(&params, 10, Some(&c)).unwrap();
    assert!(
        !count.contains("weekly_revenue"),
        "count must exclude cursor predicate"
    );
    assert!(
        rows.contains("weekly_revenue"),
        "rows must include cursor predicate"
    );
}

// ---------------------------------------------------------------------------
// Integration tests: list_saved_queries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_saved_queries_empty_catalog() {
    let state = make_list_state(SavedQueryListMockBackend::new(0, vec![]));
    let r = list_saved_queries(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"], serde_json::json!([]));
    assert_eq!(body["page_info"]["total_count"], 0);
    assert_eq!(body["page_info"]["has_next_page"], false);
    assert_eq!(body["page_info"]["start_cursor"], serde_json::Value::Null);
    assert_eq!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_saved_queries_all_fields_hydrated() {
    let row = saved_query_list_row(
        "saved_query.jaffle_shop.weekly_revenue_summary",
        "weekly_revenue_summary",
        Some("jaffle_shop"),
        Some("finance"),
        &["finance", "weekly"],
        Some("Weekly revenue by region."),
        Some(1_747_320_731.0),
        &[
            "metric.jaffle_shop.revenue",
            "metric.jaffle_shop.order_count",
            "semantic_model.jaffle_shop.customers",
        ],
    );
    let state = make_list_state(SavedQueryListMockBackend::new(1, vec![row]));
    let r = list_saved_queries(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    let row = &body["data"][0];
    assert_eq!(
        row["unique_id"],
        "saved_query.jaffle_shop.weekly_revenue_summary"
    );
    assert_eq!(row["name"], "weekly_revenue_summary");
    assert_eq!(row["package_name"], "jaffle_shop");
    assert_eq!(row["group_name"], "finance");
    assert_eq!(row["tags"], serde_json::json!(["finance", "weekly"]));
    assert_eq!(row["description"], "Weekly revenue by region.");
    assert_eq!(row["created_at"], 1_747_320_731.0);
    assert_eq!(
        row["depends_on_nodes"],
        serde_json::json!([
            "metric.jaffle_shop.revenue",
            "metric.jaffle_shop.order_count",
            "semantic_model.jaffle_shop.customers"
        ])
    );
    assert_eq!(row["depends_on_nodes_truncated"], false);
    assert_eq!(body["page_info"]["total_count"], 1);
    assert_eq!(
        body["page_info"]["end_cursor"],
        serde_json::Value::Null,
        "end_cursor must be null on last page"
    );
    assert_ne!(body["page_info"]["start_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_saved_queries_nullable_fields() {
    let row = saved_query_list_row("saved_query.pkg.q", "q", None, None, &[], None, None, &[]);
    let state = make_list_state(SavedQueryListMockBackend::new(1, vec![row]));
    let r = list_saved_queries(State(state), Query(Default::default())).await;
    let body = response_body(r).await;
    let row = &body["data"][0];
    assert_eq!(row["package_name"], serde_json::Value::Null);
    assert_eq!(row["group_name"], serde_json::Value::Null);
    assert_eq!(row["description"], serde_json::Value::Null);
    assert_eq!(row["created_at"], serde_json::Value::Null);
    assert_eq!(row["tags"], serde_json::json!([]));
    assert_eq!(row["depends_on_nodes"], serde_json::json!([]));
    assert_eq!(row["depends_on_nodes_truncated"], false);
}

#[tokio::test]
async fn list_saved_queries_sort_unknown_column_returns_400() {
    let state = make_list_state(SavedQueryListMockBackend::new(0, vec![]));
    let params = SavedQueryListParams {
        sort: Some("package_name:asc".into()),
        ..Default::default()
    };
    let r = list_saved_queries(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_saved_queries_invalid_cursor_returns_400() {
    let state = make_list_state(SavedQueryListMockBackend::new(0, vec![]));
    let params = SavedQueryListParams {
        after: Some("not-valid-base64!!!".into()),
        ..Default::default()
    };
    let r = list_saved_queries(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_saved_queries_multi_page_has_next_page_true() {
    let row_a = saved_query_list_row(
        "saved_query.pkg.alpha",
        "alpha",
        None,
        None,
        &[],
        None,
        None,
        &[],
    );
    let row_b = saved_query_list_row(
        "saved_query.pkg.beta",
        "beta",
        None,
        None,
        &[],
        None,
        None,
        &[],
    );
    let state = make_list_state(SavedQueryListMockBackend::new(2, vec![row_a, row_b]));
    let params = SavedQueryListParams {
        first: Some(1),
        ..Default::default()
    };
    let r = list_saved_queries(State(state), Query(params)).await;
    let body = response_body(r).await;
    assert_eq!(body["page_info"]["has_next_page"], true);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_ne!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_saved_queries_last_page_end_cursor_null() {
    let row = saved_query_list_row("saved_query.pkg.z", "z", None, None, &[], None, None, &[]);
    let state = make_list_state(SavedQueryListMockBackend::new(1, vec![row]));
    let r = list_saved_queries(State(state), Query(Default::default())).await;
    let body = response_body(r).await;
    assert_eq!(body["page_info"]["has_next_page"], false);
    assert_eq!(
        body["page_info"]["end_cursor"],
        serde_json::Value::Null,
        "end_cursor must be null on last page"
    );
}

#[tokio::test]
async fn list_saved_queries_cursor_advances_page() {
    let after = Cursor {
        sort_value: Some("alpha".into()),
        unique_id: "saved_query.pkg.alpha".into(),
    }
    .encode();
    let row = saved_query_list_row(
        "saved_query.pkg.beta",
        "beta",
        None,
        None,
        &[],
        None,
        None,
        &[],
    );
    let state = make_list_state(SavedQueryListMockBackend::new(1, vec![row]));
    let params = SavedQueryListParams {
        after: Some(after),
        ..Default::default()
    };
    let r = list_saved_queries(State(state), Query(params)).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["name"], "beta");
}

// ---------------------------------------------------------------------------
// Integration tests: list_saved_query_facets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_saved_query_facets_returns_empty_object() {
    let r = list_saved_query_facets().await;
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
