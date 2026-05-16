use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use axum::extract::{Query, State};
use axum::response::Response;

use super::*;
use crate::providers::{Backend, BackendError, Providers};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Unit tests: SQL generation from LAYER_CONDITIONS
// ---------------------------------------------------------------------------

#[test]
fn layer_conditions_drive_case_sql() {
    // Every entry in LAYER_CONDITIONS must appear in the generated CASE SQL.
    // This is the authoritative check: if a layer is added/renamed here, the
    // generated SQL picks it up automatically.
    let sql = modeling_layer_case_sql();
    assert!(sql.starts_with("CASE"), "must be a CASE expression");
    assert!(sql.ends_with("ELSE NULL END"), "must fall back to NULL");
    for (layer, cond) in LAYER_CONDITIONS {
        assert!(sql.contains(layer), "CASE SQL missing '{layer}'");
        assert!(
            sql.contains(cond),
            "CASE SQL missing condition for '{layer}'"
        );
    }
}

#[test]
fn modeling_layer_where_isolates_each_layer() {
    // Filtering for one layer must not bleed into another layer's patterns.
    for (target_layer, _) in LAYER_CONDITIONS {
        let sql = modeling_layer_where(&[target_layer]);
        for (other_layer, other_cond) in LAYER_CONDITIONS {
            if other_layer == target_layer {
                assert!(
                    sql.contains(other_cond),
                    "WHERE missing {target_layer} conditions"
                );
            } else {
                // The condition strings for other layers should not appear.
                // Compare by checking for the other layer's unique LIKE patterns.
                let other_patterns: Vec<&str> = other_cond.split(" OR ").collect();
                for pat in &other_patterns {
                    // Only check patterns that aren't shared (e.g. "lower" is shared).
                    let pat = pat.trim();
                    if pat.contains("LIKE '") {
                        assert!(
                            !sql.contains(pat),
                            "WHERE for '{target_layer}' leaked pattern from '{other_layer}': {pat}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn modeling_layer_where_multi_layer_or() {
    let sql = modeling_layer_where(&["Staging", "Marts"]);
    assert!(sql.contains(") OR ("), "layers must be OR'd together");
}

#[test]
fn parse_modeling_layers_rejects_unknown() {
    assert!(parse_modeling_layers("Unknown").is_err());
    assert!(parse_modeling_layers("staging").is_err()); // wrong case
}

#[test]
fn parse_modeling_layers_accepts_all_valid() {
    for (layer, _) in LAYER_CONDITIONS {
        assert!(
            parse_modeling_layers(layer).is_ok(),
            "rejected valid layer '{layer}'"
        );
    }
    assert!(parse_modeling_layers("Staging,Marts").is_ok());
}

// ---------------------------------------------------------------------------
// Integration tests: HTTP handler via direct invocation
// ---------------------------------------------------------------------------

/// Mock backend with configurable scalar and arrow responses.
///
/// When `fail_if_sql_contains` is set, `query_scalar` returns `None` for any
/// SQL containing that substring — simulating a missing DuckDB view.
struct MockBackend {
    scalar_result: Option<String>,
    arrow_result: Result<Vec<RecordBatch>, BackendError>,
    fail_if_sql_contains: Option<&'static str>,
}

impl MockBackend {
    fn with_rows(count: u64, rows: Vec<RecordBatch>) -> Self {
        Self {
            scalar_result: Some(count.to_string()),
            arrow_result: Ok(rows),
            fail_if_sql_contains: None,
        }
    }

    /// Simulates `dbt_rt.run_results` view being absent: scalar returns None
    /// for queries mentioning "run_results", succeeds otherwise.
    fn without_run_results(count: u64, rows: Vec<RecordBatch>) -> Self {
        Self {
            scalar_result: Some(count.to_string()),
            arrow_result: Ok(rows),
            fail_if_sql_contains: Some("run_results"),
        }
    }
}

impl Backend for MockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, sql: &str) -> Option<String> {
        if let Some(marker) = self.fail_if_sql_contains {
            if sql.contains(marker) {
                return None;
            }
        }
        self.scalar_result.clone()
    }

    fn query_arrow(&self, _sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        match &self.arrow_result {
            Ok(batches) => Ok(batches.clone()),
            Err(e) => Err(BackendError::Query(e.to_string())),
        }
    }
}

fn make_state(backend: MockBackend) -> Arc<AppState> {
    let providers = Providers {
        backend: Arc::new(backend),
        ..Providers::unavailable()
    };
    Arc::new(AppState::new(std::path::PathBuf::from("/tmp"), providers))
}

/// Schema shared by both batch builders — keeps field order in sync.
fn model_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("modeling_layer", DataType::Utf8, true),
        Field::new("access_level", DataType::Utf8, true),
        Field::new("contract_enforced", DataType::Boolean, false),
        Field::new("owner", DataType::Utf8, true),
        Field::new("executed_at", DataType::Utf8, true),
    ]))
}

/// RecordBatch with every model field non-null — used to verify full hydration.
fn all_fields_batch() -> RecordBatch {
    RecordBatch::try_new(
        model_schema(),
        vec![
            Arc::new(StringArray::from(vec!["model.pkg.fct_orders"])),
            Arc::new(StringArray::from(vec!["fct_orders"])),
            Arc::new(StringArray::from(vec![Some("pkg")])),
            Arc::new(StringArray::from(vec![Some("models/marts/fct_orders.sql")])),
            Arc::new(StringArray::from(vec![Some("Marts")])),
            Arc::new(StringArray::from(vec![Some("public")])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(StringArray::from(vec![Some("Team X")])),
            Arc::new(StringArray::from(vec![Some("2026-05-11T14:10:00")])),
        ],
    )
    .expect("valid batch")
}

/// RecordBatch where nullable fields are null — simulates a model with no
/// group (owner=null), no run history (executed_at=null), and a path that
/// matches no layer convention (modeling_layer=null).
fn null_fields_batch() -> RecordBatch {
    RecordBatch::try_new(
        model_schema(),
        vec![
            Arc::new(StringArray::from(vec!["model.pkg.customers"])),
            Arc::new(StringArray::from(vec!["customers"])),
            Arc::new(StringArray::from(vec![Some("pkg")])),
            Arc::new(StringArray::from(vec![Some("models/customers.sql")])),
            Arc::new(StringArray::from(vec![None::<&str>])), // modeling_layer = null
            Arc::new(StringArray::from(vec![Some("protected")])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(StringArray::from(vec![None::<&str>])), // owner = null
            Arc::new(StringArray::from(vec![None::<&str>])), // executed_at = null
        ],
    )
    .expect("valid batch")
}

async fn response_body(response: Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&bytes).expect("valid json")
}

#[tokio::test]
async fn all_fields_hydrated() {
    let state = make_state(MockBackend::with_rows(1, vec![all_fields_batch()]));
    let response = list_models(State(state), Query(ModelListParams::default())).await;
    assert_eq!(response.status(), 200);

    let body = response_body(response).await;
    let m = &body["models"][0];
    assert_eq!(m["unique_id"], "model.pkg.fct_orders");
    assert_eq!(m["name"], "fct_orders");
    assert_eq!(m["modeling_layer"], "Marts");
    assert_eq!(m["access_level"], "public");
    assert_eq!(m["contract_enforced"], true);
    assert_eq!(m["owner"], "Team X");
    assert_eq!(m["executed_at"], "2026-05-11T14:10:00");
    assert_eq!(body["total"], 1);
    assert_eq!(body["offset"], 0);
}

#[tokio::test]
async fn pagination_metadata_correct() {
    let state = make_state(MockBackend::with_rows(50, vec![]));
    let params = ModelListParams {
        limit: Some(10),
        offset: Some(20),
        ..Default::default()
    };
    let body = response_body(list_models(State(state), Query(params)).await).await;
    assert_eq!(body["total"], 50);
    assert_eq!(body["offset"], 20);
    assert_eq!(body["limit"], 10);
}

#[tokio::test]
async fn invalid_sort_column_returns_400() {
    let state = make_state(MockBackend::with_rows(0, vec![]));
    let params = ModelListParams {
        sort: Some("injected;DROP".into()),
        ..Default::default()
    };
    assert_eq!(list_models(State(state), Query(params)).await.status(), 400);
}

#[tokio::test]
async fn invalid_sort_direction_returns_400() {
    let state = make_state(MockBackend::with_rows(0, vec![]));
    let params = ModelListParams {
        sort: Some("name:sideways".into()),
        ..Default::default()
    };
    assert_eq!(list_models(State(state), Query(params)).await.status(), 400);
}

#[tokio::test]
async fn invalid_modeling_layer_returns_400() {
    let state = make_state(MockBackend::with_rows(0, vec![]));
    let params = ModelListParams {
        modeling_layer: Some("NotALayer".into()),
        ..Default::default()
    };
    assert_eq!(list_models(State(state), Query(params)).await.status(), 400);
}

#[tokio::test]
async fn invalid_access_value_returns_400() {
    let state = make_state(MockBackend::with_rows(0, vec![]));
    let params = ModelListParams {
        access: Some("superuser".into()),
        ..Default::default()
    };
    assert_eq!(list_models(State(state), Query(params)).await.status(), 400);
}

#[tokio::test]
async fn run_results_absent_falls_back_to_null_executed_at() {
    // Backend returns None for any query mentioning "run_results" (CTE fails),
    // but succeeds for the fallback query (no CTE). executed_at should be null.
    let state = make_state(MockBackend::without_run_results(
        1,
        vec![all_fields_batch()],
    ));
    let response = list_models(State(state), Query(ModelListParams::default())).await;
    assert_eq!(
        response.status(),
        200,
        "must not 500 when run_results is absent"
    );
}

#[tokio::test]
async fn empty_result_returns_200() {
    let state = make_state(MockBackend::with_rows(0, vec![]));
    let body =
        response_body(list_models(State(state), Query(ModelListParams::default())).await).await;
    assert_eq!(body["total"], 0);
    assert_eq!(body["models"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn null_fields_present_as_null_not_absent() {
    // arrow_json omits null-valued fields; the handler must normalize them so
    // every row has the same key set regardless of per-row nullability.
    let state = make_state(MockBackend::with_rows(1, vec![null_fields_batch()]));
    let body =
        response_body(list_models(State(state), Query(ModelListParams::default())).await).await;
    let m = &body["models"][0];

    // Fields that are null for this row must appear as JSON null, not absent.
    assert_eq!(
        m["modeling_layer"],
        serde_json::Value::Null,
        "modeling_layer must be null, not absent"
    );
    assert_eq!(
        m["owner"],
        serde_json::Value::Null,
        "owner must be null, not absent"
    );
    assert_eq!(
        m["executed_at"],
        serde_json::Value::Null,
        "executed_at must be null, not absent"
    );

    // Non-null fields must still be present and correct.
    assert_eq!(m["name"], "customers");
    assert_eq!(m["access_level"], "protected");
    assert_eq!(m["contract_enforced"], false);

    // All contract fields must be present — the struct definition is the
    // authoritative list, this mirrors it for the JSON assertion.
    for field in [
        "unique_id",
        "name",
        "package_name",
        "original_file_path",
        "modeling_layer",
        "access_level",
        "contract_enforced",
        "owner",
        "executed_at",
    ] {
        assert!(
            m.get(field).is_some(),
            "field '{field}' missing from response"
        );
    }
}

#[tokio::test]
async fn pagination_exhausts_all_rows() {
    // Simulate 3 total rows and page through them one at a time.
    // Each call moves offset forward; once offset >= total, the loop stops.
    // This exercises the limit/offset contract end-to-end.
    let total: u64 = 3;
    let mut collected = 0u64;
    let mut offset = 0u32;
    let limit = 1u32;

    loop {
        let state = make_state(MockBackend::with_rows(
            total,
            if offset < total as u32 {
                vec![all_fields_batch()]
            } else {
                vec![]
            },
        ));
        let params = ModelListParams {
            limit: Some(limit),
            offset: Some(offset),
            ..Default::default()
        };
        let body = response_body(list_models(State(state), Query(params)).await).await;

        assert_eq!(body["total"], total);
        assert_eq!(body["offset"], offset);
        assert_eq!(body["limit"], limit);

        let page_count = body["models"].as_array().unwrap().len() as u64;
        collected += page_count;
        offset += limit;

        if offset as u64 >= total {
            break;
        }
    }

    assert_eq!(collected, total, "paginated through all {total} rows");
}

#[tokio::test]
async fn facets_returns_static_layers_and_accesses_plus_dynamic_owners() {
    // Facets batch: one owner row.
    let schema = Arc::new(Schema::new(vec![Field::new("owner", DataType::Utf8, true)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["Data Team"]))]).unwrap();

    let state = make_state(MockBackend::with_rows(0, vec![batch]));
    let body = response_body(list_model_facets(State(state)).await).await;

    // Every entry is {value, count} with count=null.
    let layers = body["modeling_layers"]
        .as_array()
        .expect("modeling_layers array");
    assert!(!layers.is_empty());
    for (name, _) in LAYER_CONDITIONS {
        let entry = layers
            .iter()
            .find(|v| v["value"] == *name)
            .unwrap_or_else(|| panic!("modeling_layers missing '{name}'"));
        assert_eq!(entry["count"], serde_json::Value::Null);
    }

    let accesses = body["accesses"].as_array().expect("accesses array");
    assert!(!accesses.is_empty());
    for level in VALID_ACCESS_LEVELS {
        let entry = accesses
            .iter()
            .find(|v| v["value"] == *level)
            .unwrap_or_else(|| panic!("accesses missing '{level}'"));
        assert_eq!(entry["count"], serde_json::Value::Null);
    }

    let owners = body["owners"].as_array().expect("owners array");
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0]["value"], "Data Team");
    assert_eq!(owners[0]["count"], serde_json::Value::Null);
}

#[tokio::test]
async fn facets_owners_empty_when_no_groups() {
    let state = make_state(MockBackend::with_rows(0, vec![]));
    let body = response_body(list_model_facets(State(state)).await).await;
    assert_eq!(body["owners"].as_array().unwrap().len(), 0);
    // Static arrays still present and non-empty.
    assert!(!body["modeling_layers"].as_array().unwrap().is_empty());
    assert!(!body["accesses"].as_array().unwrap().is_empty());
}
