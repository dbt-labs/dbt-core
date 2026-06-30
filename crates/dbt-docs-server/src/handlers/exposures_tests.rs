//! Tests for `GET /api/v1/exposures/:id`, `GET /api/v1/exposures`, and
//! `GET /api/v1/exposures/facets`.
//!
//! Schema anchoring (#10255): the `RecordBatch` fixtures below are hand-
//! rolled and not enforced against the production parquet schemas. A column
//! rename or type change in `dbt-index` will pass these tests while
//! silently breaking the handler. Once #10255 lands typed row builders,
//! replace these schemas to get compile-time coverage.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{BooleanArray, Float64Array, ListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use axum::extract::{Path, Query, State};
use axum::response::Response;

use super::*;
use crate::providers::{Backend, BackendError, Providers};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Mock backend
// ---------------------------------------------------------------------------

struct ExposureMockBackend {
    /// Fixture for `dbt.exposures` queries.
    exposure_batches: Vec<RecordBatch>,
    /// `query_scalar` return value — controls the total count for LIST.
    scalar_value: String,
}

impl Backend for ExposureMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, _sql: &str) -> Option<String> {
        Some(self.scalar_value.clone())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        if sql.contains("dbt.exposures") {
            return Ok(self.exposure_batches.clone());
        }
        Err(BackendError::Query(format!("unrouted query: {sql}")))
    }
}

fn make_state(backend: ExposureMockBackend) -> Arc<AppState> {
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

fn make_str_list(values: &[&str]) -> ListArray {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for v in values {
        builder.values().append_value(v);
    }
    builder.append(true);
    builder.finish()
}

fn make_str_list_multi(rows: &[&[&str]]) -> ListArray {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for &row in rows {
        for v in row {
            builder.values().append_value(v);
        }
        builder.append(true);
    }
    builder.finish()
}

/// Schema for `dbt.exposures` detail queries (single-row SELECT).
fn exposure_detail_schema(
    tags_field: &Field,
    fqn_field: &Field,
    depends_on_field: &Field,
) -> Arc<Schema> {
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
fn make_detail_batch(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    description: Option<&str>,
    exposure_type: Option<&str>,
    maturity: Option<&str>,
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
    let dep_arr = make_str_list(depends_on_nodes);

    let tags_field = Field::new("tags", tags_arr.data_type().clone(), true);
    let fqn_field = Field::new("fqn", fqn_arr.data_type().clone(), true);
    let dep_field = Field::new("depends_on_nodes", dep_arr.data_type().clone(), true);

    RecordBatch::try_new(
        exposure_detail_schema(&tags_field, &fqn_field, &dep_field),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![description])),
            Arc::new(StringArray::from(vec![None::<&str>])), // original_file_path
            Arc::new(StringArray::from(vec![None::<&str>])), // file_path
            Arc::new(StringArray::from(vec![None::<&str>])), // label
            Arc::new(StringArray::from(vec![exposure_type])),
            Arc::new(StringArray::from(vec![maturity])),
            Arc::new(StringArray::from(vec![None::<&str>])), // url
            Arc::new(StringArray::from(vec![owner_name])),
            Arc::new(StringArray::from(vec![owner_email])),
            Arc::new(StringArray::from(vec![meta_json])),
            Arc::new(tags_arr),
            Arc::new(fqn_arr),
            Arc::new(dep_arr),
            Arc::new(Float64Array::from(vec![created_at])),
        ],
    )
    .expect("valid exposure detail batch")
}

/// Schema for `dbt.exposures` list queries (multi-row SELECT with capped depends_on).
fn list_schema(tags_field: &Field, dep_capped_field: &Field) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("exposure_type", DataType::Utf8, true),
        Field::new("maturity", DataType::Utf8, true),
        Field::new("owner_name", DataType::Utf8, true),
        Field::new("owner_email", DataType::Utf8, true),
        tags_field.clone(),
        Field::new("created_at", DataType::Float64, true),
        dep_capped_field.clone(),
        Field::new("depends_on_truncated", DataType::Boolean, true),
    ]))
}

struct ListRow<'a> {
    unique_id: &'a str,
    name: &'a str,
    exposure_type: Option<&'a str>,
    maturity: Option<&'a str>,
    owner_name: Option<&'a str>,
    owner_email: Option<&'a str>,
    tags: &'a [&'a str],
    created_at: Option<f64>,
    depends_on_nodes: &'a [&'a str],
    depends_on_truncated: bool,
}

fn make_list_batch(rows: &[ListRow<'_>]) -> RecordBatch {
    let tags_arr = make_str_list_multi(&rows.iter().map(|r| r.tags).collect::<Vec<_>>());
    let dep_arr = make_str_list_multi(&rows.iter().map(|r| r.depends_on_nodes).collect::<Vec<_>>());

    let tags_field = Field::new("tags", tags_arr.data_type().clone(), true);
    let dep_field = Field::new("depends_on_nodes_capped", dep_arr.data_type().clone(), true);

    let unique_ids: Vec<&str> = rows.iter().map(|r| r.unique_id).collect();
    let names: Vec<&str> = rows.iter().map(|r| r.name).collect();
    let exposure_types: Vec<Option<&str>> = rows.iter().map(|r| r.exposure_type).collect();
    let maturities: Vec<Option<&str>> = rows.iter().map(|r| r.maturity).collect();
    let owner_names: Vec<Option<&str>> = rows.iter().map(|r| r.owner_name).collect();
    let owner_emails: Vec<Option<&str>> = rows.iter().map(|r| r.owner_email).collect();
    let created_ats: Vec<Option<f64>> = rows.iter().map(|r| r.created_at).collect();
    let truncated: Vec<bool> = rows.iter().map(|r| r.depends_on_truncated).collect();

    RecordBatch::try_new(
        list_schema(&tags_field, &dep_field),
        vec![
            Arc::new(StringArray::from(unique_ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(exposure_types)),
            Arc::new(StringArray::from(maturities)),
            Arc::new(StringArray::from(owner_names)),
            Arc::new(StringArray::from(owner_emails)),
            Arc::new(tags_arr),
            Arc::new(Float64Array::from(created_ats)),
            Arc::new(dep_arr),
            Arc::new(BooleanArray::from(truncated)),
        ],
    )
    .expect("valid list batch")
}

fn facets_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
}

fn make_facets_batch(owners: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        facets_schema(),
        vec![Arc::new(StringArray::from(owners.to_vec()))],
    )
    .expect("valid facets batch")
}

async fn response_body(response: Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&bytes).expect("valid json")
}

// ---------------------------------------------------------------------------
// Tests: GET /api/v1/exposures/:id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn detail_invalid_unique_id_returns_400() {
    let backend = ExposureMockBackend {
        exposure_batches: vec![],
        scalar_value: "0".into(),
    };
    let state = make_state(backend);
    let r = get_exposure(State(state), Path("bad'id".to_owned())).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn detail_missing_returns_404() {
    let backend = ExposureMockBackend {
        exposure_batches: vec![],
        scalar_value: "0".into(),
    };
    let state = make_state(backend);
    let r = get_exposure(State(state), Path("exposure.x.y".to_owned())).await;
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn detail_all_fields_hydrated() {
    let batch = make_detail_batch(
        "exposure.jaffle_shop.revenue_dashboard",
        "revenue_dashboard",
        Some("jaffle_shop"),
        Some("Dashboard of revenue metrics."),
        Some("dashboard"),
        Some("high"),
        Some("Jane Doe"),
        Some("jane@example.com"),
        Some(r#"{"owner":"finance"}"#),
        &["finance", "exec"],
        &["jaffle_shop", "revenue_dashboard"],
        &["model.jaffle_shop.orders", "source.jaffle_shop.raw.orders"],
        Some(1_747_432_300.5),
    );
    let backend = ExposureMockBackend {
        exposure_batches: vec![batch],
        scalar_value: "0".into(),
    };
    let state = make_state(backend);
    let r = get_exposure(
        State(state),
        Path("exposure.jaffle_shop.revenue_dashboard".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    assert_eq!(body["unique_id"], "exposure.jaffle_shop.revenue_dashboard");
    assert_eq!(body["name"], "revenue_dashboard");
    assert_eq!(body["resource_type"], "exposure");
    assert_eq!(body["package_name"], "jaffle_shop");
    assert_eq!(body["description"], "Dashboard of revenue metrics.");
    assert_eq!(body["exposure_type"], "dashboard");
    assert_eq!(body["maturity"], "high");
    assert_eq!(body["owner_name"], "Jane Doe");
    assert_eq!(body["owner_email"], "jane@example.com");
    assert_eq!(body["meta"], serde_json::json!({"owner": "finance"}));
    assert_eq!(body["tags"], serde_json::json!(["finance", "exec"]));
    assert_eq!(body["created_at"], 1_747_432_300.5_f64);

    assert_eq!(
        body["depends_on"][0]["unique_id"],
        "model.jaffle_shop.orders"
    );
    assert_eq!(body["depends_on"][0]["edge_type"], "model");
    assert_eq!(
        body["depends_on"][1]["unique_id"],
        "source.jaffle_shop.raw.orders"
    );
    assert_eq!(body["depends_on"][1]["edge_type"], "source");

    // Exposures have no referenced_by (leaf nodes).
    assert!(body.get("referenced_by").is_none());
    // No execution_info, catalog, or columns.
    assert!(body.get("execution_info").is_none());
    assert!(body.get("catalog").is_none());
    assert!(body.get("columns").is_none());
}

#[tokio::test]
async fn detail_meta_null_when_absent() {
    let batch = make_detail_batch(
        "exposure.pkg.t",
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
        &[],
        None,
    );
    let backend = ExposureMockBackend {
        exposure_batches: vec![batch],
        scalar_value: "0".into(),
    };
    let state = make_state(backend);
    let r = get_exposure(State(state), Path("exposure.pkg.t".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn detail_meta_null_when_malformed() {
    let batch = make_detail_batch(
        "exposure.pkg.t",
        "t",
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
    );
    let backend = ExposureMockBackend {
        exposure_batches: vec![batch],
        scalar_value: "0".into(),
    };
    let state = make_state(backend);
    let r = get_exposure(State(state), Path("exposure.pkg.t".to_owned())).await;
    assert_eq!(r.status(), 200, "malformed meta must not 500 the response");
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

// ---------------------------------------------------------------------------
// Tests: GET /api/v1/exposures
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_empty_catalog_returns_empty_data() {
    let backend = ExposureMockBackend {
        exposure_batches: vec![],
        scalar_value: "0".into(),
    };
    let state = make_state(backend);
    let r = list_exposures(State(state), Query(ExposureListParams::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"], serde_json::json!([]));
    assert_eq!(body["page_info"]["total_count"], 0);
    assert_eq!(body["page_info"]["has_next_page"], false);
    assert_eq!(body["page_info"]["start_cursor"], serde_json::Value::Null);
    assert_eq!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_single_page_returns_all_rows() {
    let batch = make_list_batch(&[
        ListRow {
            unique_id: "exposure.jaffle_shop.churn_notebook",
            name: "churn_notebook",
            exposure_type: Some("notebook"),
            maturity: Some("medium"),
            owner_name: Some("Alex Park"),
            owner_email: Some("alex@example.com"),
            tags: &[],
            created_at: Some(1_747_104_900.0),
            depends_on_nodes: &["model.jaffle_shop.customers"],
            depends_on_truncated: false,
        },
        ListRow {
            unique_id: "exposure.jaffle_shop.revenue_dashboard",
            name: "revenue_dashboard",
            exposure_type: Some("dashboard"),
            maturity: Some("high"),
            owner_name: Some("Jane Doe"),
            owner_email: Some("jane@example.com"),
            tags: &["finance"],
            created_at: Some(1_747_432_300.5),
            depends_on_nodes: &["model.jaffle_shop.orders", "source.jaffle_shop.raw.orders"],
            depends_on_truncated: false,
        },
    ]);
    let backend = ExposureMockBackend {
        exposure_batches: vec![batch],
        scalar_value: "2".into(),
    };
    let state = make_state(backend);
    let r = list_exposures(State(state), Query(ExposureListParams::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
    assert_eq!(body["page_info"]["total_count"], 2);
    assert_eq!(body["page_info"]["has_next_page"], false);
    assert_eq!(
        body["data"][0]["unique_id"],
        "exposure.jaffle_shop.churn_notebook"
    );
    assert_eq!(body["data"][0]["exposure_type"], "notebook");
    assert_eq!(
        body["data"][0]["depends_on"][0]["unique_id"],
        "model.jaffle_shop.customers"
    );
    assert_eq!(body["data"][0]["depends_on"][0]["edge_type"], "model");
    assert_eq!(
        body["data"][1]["unique_id"],
        "exposure.jaffle_shop.revenue_dashboard"
    );
    assert_eq!(body["data"][1]["tags"], serde_json::json!(["finance"]));
}

#[tokio::test]
async fn list_has_next_page_when_more_rows_available() {
    // Request first=1; backend returns 2 rows (peek = first+1 = 2).
    let batch = make_list_batch(&[
        ListRow {
            unique_id: "exposure.pkg.alpha",
            name: "alpha",
            exposure_type: None,
            maturity: None,
            owner_name: None,
            owner_email: None,
            tags: &[],
            created_at: None,
            depends_on_nodes: &[],
            depends_on_truncated: false,
        },
        ListRow {
            unique_id: "exposure.pkg.beta",
            name: "beta",
            exposure_type: None,
            maturity: None,
            owner_name: None,
            owner_email: None,
            tags: &[],
            created_at: None,
            depends_on_nodes: &[],
            depends_on_truncated: false,
        },
    ]);
    let backend = ExposureMockBackend {
        exposure_batches: vec![batch],
        scalar_value: "3".into(),
    };
    let state = make_state(backend);
    let r = list_exposures(
        State(state),
        Query(ExposureListParams {
            first: Some(1),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["page_info"]["has_next_page"], true);
    assert_eq!(body["page_info"]["total_count"], 3);
    // end_cursor must be non-null when has_next_page=true.
    assert!(body["page_info"]["end_cursor"].is_string());
    // start_cursor must be non-null when data is non-empty.
    assert!(body["page_info"]["start_cursor"].is_string());
}

#[tokio::test]
async fn list_sort_param_returns_400() {
    let backend = ExposureMockBackend {
        exposure_batches: vec![],
        scalar_value: "0".into(),
    };
    let state = make_state(backend);
    let r = list_exposures(
        State(state),
        Query(ExposureListParams {
            sort: Some("name:asc".to_owned()),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_invalid_cursor_returns_400() {
    let backend = ExposureMockBackend {
        exposure_batches: vec![],
        scalar_value: "0".into(),
    };
    let state = make_state(backend);
    let r = list_exposures(
        State(state),
        Query(ExposureListParams {
            after: Some("not-valid-base64!!!".to_owned()),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_owner_filter_passes_through() {
    // Backend returns one row; validates the owner filter doesn't break the handler.
    let batch = make_list_batch(&[ListRow {
        unique_id: "exposure.jaffle_shop.revenue_dashboard",
        name: "revenue_dashboard",
        exposure_type: Some("dashboard"),
        maturity: None,
        owner_name: Some("Jane Doe"),
        owner_email: None,
        tags: &[],
        created_at: None,
        depends_on_nodes: &[],
        depends_on_truncated: false,
    }]);
    let backend = ExposureMockBackend {
        exposure_batches: vec![batch],
        scalar_value: "1".into(),
    };
    let state = make_state(backend);
    let r = list_exposures(
        State(state),
        Query(ExposureListParams {
            owner: Some("Jane Doe".to_owned()),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["owner_name"], "Jane Doe");
}

#[tokio::test]
async fn list_cursor_pagination_roundtrip() {
    // Encode a cursor and pass it back as ?after — the handler must accept it.
    let cursor = Cursor {
        sort_value: Some("alpha".into()),
        unique_id: "exposure.pkg.alpha".into(),
    };
    let encoded = cursor.encode();

    let backend = ExposureMockBackend {
        exposure_batches: vec![],
        scalar_value: "5".into(),
    };
    let state = make_state(backend);
    let r = list_exposures(
        State(state),
        Query(ExposureListParams {
            after: Some(encoded),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(r.status(), 200);
}

#[tokio::test]
async fn list_depends_on_truncated_flag_propagated() {
    let batch = make_list_batch(&[ListRow {
        unique_id: "exposure.pkg.large",
        name: "large",
        exposure_type: None,
        maturity: None,
        owner_name: None,
        owner_email: None,
        tags: &[],
        created_at: None,
        depends_on_nodes: &["model.pkg.a", "model.pkg.b"],
        depends_on_truncated: true,
    }]);
    let backend = ExposureMockBackend {
        exposure_batches: vec![batch],
        scalar_value: "1".into(),
    };
    let state = make_state(backend);
    let r = list_exposures(State(state), Query(ExposureListParams::default())).await;
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["depends_on_truncated"], true);
}

// ---------------------------------------------------------------------------
// Tests: GET /api/v1/exposures/facets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn facets_empty_project_returns_empty_owners() {
    let backend = ExposureMockBackend {
        exposure_batches: vec![],
        scalar_value: "0".into(),
    };
    let state = make_state(backend);
    let r = list_exposure_facets(State(state)).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["owners"], serde_json::json!([]));
}

#[tokio::test]
async fn facets_returns_distinct_owner_names() {
    let batch = make_facets_batch(&["Alex Park", "Jane Doe"]);
    let backend = ExposureMockBackend {
        exposure_batches: vec![batch],
        scalar_value: "0".into(),
    };
    let state = make_state(backend);
    let r = list_exposure_facets(State(state)).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    let owners = body["owners"].as_array().unwrap();
    assert_eq!(owners.len(), 2);
    assert_eq!(owners[0]["value"], "Alex Park");
    assert_eq!(owners[1]["value"], "Jane Doe");
    // count is always null today.
    assert_eq!(owners[0]["count"], serde_json::Value::Null);
}

#[tokio::test]
async fn facets_no_auto_bi_providers_or_exposure_modes() {
    // The facets response must not contain auto_bi_providers or exposure_modes
    // (Class C Platform-only fields with no parquet column).
    let batch = make_facets_batch(&["Jane Doe"]);
    let backend = ExposureMockBackend {
        exposure_batches: vec![batch],
        scalar_value: "0".into(),
    };
    let state = make_state(backend);
    let r = list_exposure_facets(State(state)).await;
    let body = response_body(r).await;
    assert!(
        body.get("auto_bi_providers").is_none(),
        "auto_bi_providers must not appear in facets response"
    );
    assert!(
        body.get("exposure_modes").is_none(),
        "exposure_modes must not appear in facets response"
    );
}
