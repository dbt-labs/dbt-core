use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use axum::extract::{Path, Query, State};
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
    /// When set, returned for SQL containing `"materialized AS value"` (the
    /// materialization facets query). Allows `list_model_facets` tests to return
    /// a different batch for owners vs. materializations within one spawn_blocking.
    materialization_result: Option<Vec<RecordBatch>>,
}

impl MockBackend {
    fn with_rows(count: u64, rows: Vec<RecordBatch>) -> Self {
        Self {
            scalar_result: Some(count.to_string()),
            arrow_result: Ok(rows),
            fail_if_sql_contains: None,
            materialization_result: None,
        }
    }

    /// Simulates `dbt_rt.run_results` view being absent: scalar returns None
    /// for queries mentioning "run_results", succeeds otherwise.
    fn without_run_results(count: u64, rows: Vec<RecordBatch>) -> Self {
        Self {
            scalar_result: Some(count.to_string()),
            arrow_result: Ok(rows),
            fail_if_sql_contains: Some("run_results"),
            materialization_result: None,
        }
    }

    fn with_materialization_facets(mut self, batches: Vec<RecordBatch>) -> Self {
        self.materialization_result = Some(batches);
        self
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

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        // Materialization facets query — return fixture if set, else empty vec.
        // Must route here (not fall through) to avoid "column 'value' missing" panics
        // when the owners batch is returned for this query instead.
        if sql.contains("materialized AS value") {
            return Ok(self.materialization_result.clone().unwrap_or_default());
        }
        match &self.arrow_result {
            Ok(batches) => Ok(batches.clone()),
            Err(e) => Err(BackendError::Query(e.to_string())),
        }
    }
}

fn make_state(backend: MockBackend) -> Arc<AppState> {
    let providers = Providers {
        backend: Arc::new(backend),
        ..Providers::default()
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
    let m = &body["data"][0];
    assert_eq!(m["unique_id"], "model.pkg.fct_orders");
    assert_eq!(m["name"], "fct_orders");
    assert_eq!(m["modeling_layer"], "Marts");
    assert_eq!(m["access_level"], "public");
    assert_eq!(m["contract_enforced"], true);
    assert_eq!(m["owner"], "Team X");
    assert_eq!(m["executed_at"], "2026-05-11T14:10:00");
    assert_eq!(body["page_info"]["total_count"], 1);
    assert_eq!(body["page_info"]["has_next_page"], false);
    // Single-row page: start_cursor is set, end_cursor is null (no more pages).
    assert!(
        body["page_info"]["start_cursor"].is_string(),
        "start_cursor must be a base64 string on a non-empty page"
    );
    assert!(
        body["page_info"]["end_cursor"].is_null(),
        "end_cursor must be null when has_next_page is false"
    );
}

#[tokio::test]
async fn page_info_envelope_uses_cursor_shape() {
    // ADR-6: page_info has four fields (total_count, start_cursor, end_cursor,
    // has_next_page). The pre-doctrine offset/limit fields must NOT appear.
    let state = make_state(MockBackend::with_rows(1, vec![all_fields_batch()]));
    let response = list_models(State(state), Query(ModelListParams::default())).await;
    assert_eq!(response.status(), 200);

    let body = response_body(response).await;
    assert!(
        body.get("page_info").is_some(),
        "expected top-level \"page_info\" per ADR-6; got: {body}"
    );
    assert!(
        body.get("total").is_none(),
        "expected NO top-level \"total\" (pre-doctrine); got: {body}"
    );
    assert!(
        body.get("offset").is_none(),
        "expected NO top-level \"offset\" (pre-doctrine); got: {body}"
    );
    assert!(
        body.get("limit").is_none(),
        "expected NO top-level \"limit\" (pre-doctrine); got: {body}"
    );

    let pi = &body["page_info"];
    for field in ["total_count", "start_cursor", "end_cursor", "has_next_page"] {
        assert!(pi.get(field).is_some(), "page_info.{field} missing");
    }
}

#[tokio::test]
async fn has_next_page_true_when_backend_returns_peek_row() {
    // The handler queries `LIMIT first + 1` to peek for `has_next_page`. When
    // the mock returns more rows than `first`, has_next_page must be true and
    // the response array must be trimmed to `first`.
    let state = make_state(MockBackend::with_rows(
        100,
        vec![all_fields_batch(), all_fields_batch()], // 2 rows, first=1 + peek
    ));
    let params = ModelListParams {
        first: Some(1),
        ..Default::default()
    };
    let body = response_body(list_models(State(state), Query(params)).await).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["page_info"]["has_next_page"], true);
    assert!(
        body["page_info"]["end_cursor"].is_string(),
        "end_cursor must be a base64 string when more pages exist"
    );
    assert_eq!(body["page_info"]["total_count"], 100);
}

#[tokio::test]
async fn invalid_cursor_returns_400() {
    let state = make_state(MockBackend::with_rows(0, vec![]));
    let params = ModelListParams {
        after: Some("not-base64-and-not-json".into()),
        ..Default::default()
    };
    assert_eq!(list_models(State(state), Query(params)).await.status(), 400);
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
    assert_eq!(body["page_info"]["total_count"], 0);
    assert_eq!(body["page_info"]["has_next_page"], false);
    assert!(body["page_info"]["start_cursor"].is_null());
    assert!(body["page_info"]["end_cursor"].is_null());
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn null_fields_present_as_null_not_absent() {
    // arrow_json omits null-valued fields; the handler must normalize them so
    // every row has the same key set regardless of per-row nullability.
    let state = make_state(MockBackend::with_rows(1, vec![null_fields_batch()]));
    let body =
        response_body(list_models(State(state), Query(ModelListParams::default())).await).await;
    let m = &body["data"][0];

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
async fn pagination_exhausts_all_rows_via_cursor() {
    // Cursor pagination end-to-end: page through all rows by passing
    // page_info.end_cursor back as ?after on each iteration. Stops when
    // has_next_page is false.
    //
    // The MockBackend can only return a fixed batch per state, so we
    // construct a fresh state per page and use the test's total = 3 with
    // first = 1 (one row per page) to drive three forward steps.
    let total: u64 = 3;
    let first = 1u32;
    let mut collected = 0u64;
    let mut after: Option<String> = None;

    loop {
        // Per-page mock: returns 2 rows when more pages are expected (so the
        // handler's peek detects has_next_page), 1 row on the final page.
        let remaining = total - collected;
        let rows = if remaining > first as u64 {
            vec![all_fields_batch(), all_fields_batch()]
        } else if remaining > 0 {
            vec![all_fields_batch()]
        } else {
            vec![]
        };
        let state = make_state(MockBackend::with_rows(total, rows));

        let params = ModelListParams {
            first: Some(first),
            after: after.clone(),
            ..Default::default()
        };
        let body = response_body(list_models(State(state), Query(params)).await).await;

        assert_eq!(body["page_info"]["total_count"], total);
        let page_count = body["data"].as_array().unwrap().len() as u64;
        assert!(page_count <= first as u64);
        collected += page_count;

        let has_next = body["page_info"]["has_next_page"].as_bool().unwrap();
        if !has_next {
            break;
        }
        after = Some(
            body["page_info"]["end_cursor"]
                .as_str()
                .expect("end_cursor must be set when has_next_page is true")
                .to_owned(),
        );
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

// ===========================================================================
// Tests: get_model (GET /api/v1/models/:id)
// ===========================================================================
//
// Schema anchoring — known limitations (see https://github.com/dbt-labs/fs/issues/10255):
//
// The mock RecordBatch schemas below are hand-written approximations of the
// actual parquet schemas defined in `crates/dbt-index/src/parquet.rs` via the
// `define_row!` macro. This means:
//
//   - Column NAME changes in dbt-index are not caught at compile time. A rename
//     of `contract_enforced` to `enforced_contract` would pass all these tests
//     while silently breaking the handler's SQL query in production.
//
//   - Column TYPE changes are not caught unless the affected field is used in an
//     extraction function that is explicitly exercised here. For example, if
//     `tags` were changed from `List(Utf8)` to `Utf8` in the parquet schema,
//     `extract_str_list` would silently return `[]` for every model.
//
// Resolution tracked in #10255: `dbt-index` should expose a `test-fixtures`
// feature that generates typed `NodeRowBuilder`, `RunResultRowBuilder`, and
// `CatalogTableRowBuilder` from the same `define_row!` macro that defines the
// production schema. Once available, replace the hand-rolled schemas below with
// those builders to get compile-time and type-level coverage.
//
// What these tests DO reliably cover:
//   - Handler control flow: routing, 404/400 paths, capability-gated nulls
//   - Extraction function behaviour given a conforming batch
//   - Response shape: all Option<T> fields serialise as null, not absent
//   - List-type extraction (tags, fqn): exercises extract_str_list end-to-end
//   - Column and edge sub-resource extraction: exercises extract_model_columns
//     and extract_edge_refs with real multi-row batches

/// Routes Arrow queries to the correct fixture batch based on SQL content.
struct DetailMockBackend {
    node_batches: Vec<RecordBatch>,
    column_batches: Vec<RecordBatch>,
    /// Upstream (depends_on) and downstream (referenced_by) edges share one
    /// batch in tests; the handler queries them with different WHERE clauses but
    /// the mock returns the same data for both, which is fine for unit tests.
    edge_batches: Vec<RecordBatch>,
    /// `Some(batches)` → query succeeds; `None` → view absent (query error).
    run_result_batches: Option<Vec<RecordBatch>>,
    /// `Some(batches)` → query succeeds; `None` → view absent (query error).
    catalog_batches: Option<Vec<RecordBatch>>,
}

impl Backend for DetailMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, _sql: &str) -> Option<String> {
        Some("0".to_owned())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        if sql.contains("dbt.node_columns") {
            return Ok(self.column_batches.clone());
        }
        if sql.contains("dbt_rt.run_results") {
            return self
                .run_result_batches
                .clone()
                .ok_or_else(|| BackendError::Query("run_results view absent".into()));
        }
        if sql.contains("dbt.catalog_tables") {
            return self
                .catalog_batches
                .clone()
                .ok_or_else(|| BackendError::Query("catalog_tables view absent".into()));
        }
        if sql.contains("dbt.edges") {
            return Ok(self.edge_batches.clone());
        }
        if sql.contains("dbt.nodes") {
            return Ok(self.node_batches.clone());
        }
        // Fallback — should not happen in well-structured tests.
        Err(BackendError::Query(format!("unrouted query: {sql}")))
    }
}

fn make_detail_state(backend: DetailMockBackend) -> Arc<AppState> {
    let providers = Providers {
        backend: Arc::new(backend),
        ..Providers::default()
    };
    Arc::new(AppState::new(std::path::PathBuf::from("/tmp"), providers))
}

// ── Batch builders ───────────────────────────────────────────────────────────

/// Build a single-element `ListArray` from string slices.
///
/// The resulting column type is `List(Field { name: "item", type: Utf8 })`,
/// which is what `ListBuilder<StringBuilder>` produces by default.
///
/// TODO (#10255): replace with `NodeRowBuilder::push(tags: &[&str])` once
/// dbt-index exposes typed fixture builders. That builder would use
/// `schema_nodes()` and guarantee `List(Field { name: "l", type: Utf8 })`
/// matching the production parquet schema.
fn make_str_list_array(values: &[&str]) -> ListArray {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for v in values {
        builder.values().append_value(v);
    }
    builder.append(true);
    builder.finish()
}

/// Schema for `dbt.nodes` rows as queried by `MODEL_DETAIL_NODE_SQL`.
///
/// Field types are assumed to match `NodeRow` in `crates/dbt-index/src/parquet.rs`.
/// Verified empirically against a real index (2026-05-18). No compile-time
/// enforcement — see issue #10255 for the fix.
///
/// Assumptions that would silently break if wrong:
///   - `tags` and `fqn` are `List(Utf8)`, not JSON strings (verified: List(Utf8) ✓)
///   - `contract_enforced` is `Boolean`, not `Int8` (verified: Boolean ✓)
fn node_detail_schema(tags_field: &Field, fqn_field: &Field) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("resource_type", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("materialized", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("database_name", DataType::Utf8, true),
        Field::new("schema_name", DataType::Utf8, true),
        Field::new("relation_name", DataType::Utf8, true),
        Field::new("identifier", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("access_level", DataType::Utf8, true),
        Field::new("group_name", DataType::Utf8, true),
        Field::new("raw_code", DataType::Utf8, true),
        Field::new("compiled_code", DataType::Utf8, true),
        Field::new("contract_enforced", DataType::Boolean, true),
        tags_field.clone(),
        fqn_field.clone(),
    ]))
}

fn make_node_batch_with_lists(
    unique_id: &str,
    name: &str,
    tags: &[&str],
    fqn: &[&str],
) -> RecordBatch {
    let tags_arr = Arc::new(make_str_list_array(tags));
    let fqn_arr = Arc::new(make_str_list_array(fqn));
    let tags_field = Field::new("tags", tags_arr.data_type().clone(), false);
    let fqn_field = Field::new("fqn", fqn_arr.data_type().clone(), false);
    let schema = node_detail_schema(&tags_field, &fqn_field);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec!["model"])),
            Arc::new(StringArray::from(vec![Some("jaffle_shop")])),
            Arc::new(StringArray::from(vec![Some("table")])),
            Arc::new(StringArray::from(vec![Some("Order model")])),
            Arc::new(StringArray::from(vec![Some("prod")])),
            Arc::new(StringArray::from(vec![Some("dbt_prod")])),
            Arc::new(StringArray::from(vec![Some("prod.dbt_prod.orders")])),
            Arc::new(StringArray::from(vec![Some("orders")])),
            Arc::new(StringArray::from(vec![Some("models/orders.sql")])),
            Arc::new(StringArray::from(vec![Some("public")])),
            Arc::new(StringArray::from(vec![Some("finance")])),
            Arc::new(StringArray::from(vec![Some("select * from ...")])),
            Arc::new(StringArray::from(vec![None::<&str>])), // compiled_code
            Arc::new(BooleanArray::from(vec![Some(true)])),
            tags_arr,
            fqn_arr,
        ],
    )
    .expect("valid node batch")
}

/// Convenience wrapper with representative test data.
fn make_node_batch(unique_id: &str, name: &str) -> RecordBatch {
    make_node_batch_with_lists(
        unique_id,
        name,
        &["finance", "core"],
        &["jaffle_shop", name],
    )
}

/// Schema for `dbt.node_columns` as queried by the columns sub-query.
fn column_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("index", DataType::Int64, true),
        Field::new("data_type", DataType::Utf8, true),
        Field::new("declared_type", DataType::Utf8, true),
        Field::new("inferred_type", DataType::Utf8, true),
        Field::new("catalog_type", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
        Field::new("granularity", DataType::Utf8, true),
    ]))
}

fn make_columns_batch(cols: &[(&str, Option<i64>, Option<&str>)]) -> RecordBatch {
    let n = cols.len();
    let schema = column_schema();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                cols.iter().map(|(name, _, _)| *name).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                cols.iter().map(|(_, idx, _)| *idx).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                cols.iter().map(|(_, _, dt)| *dt).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec![None::<&str>; n])), // declared_type
            Arc::new(StringArray::from(vec![None::<&str>; n])), // inferred_type
            Arc::new(StringArray::from(vec![None::<&str>; n])), // catalog_type
            Arc::new(StringArray::from(vec![None::<&str>; n])), // description
            Arc::new(StringArray::from(vec![None::<&str>; n])), // label
            Arc::new(StringArray::from(vec![None::<&str>; n])), // granularity
        ],
    )
    .expect("valid columns batch")
}

/// Schema for `dbt.edges` as queried by the upstream/downstream sub-queries.
fn edges_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("edge_type", DataType::Utf8, false),
    ]))
}

fn make_edges_batch(edges: &[(&str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        edges_schema(),
        vec![
            Arc::new(StringArray::from(
                edges.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                edges.iter().map(|(_, et)| *et).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("valid edges batch")
}

fn run_result_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("status", DataType::Utf8, true),
        Field::new("execution_time", DataType::Float64, true),
        Field::new("completed_at", DataType::Utf8, true),
    ]))
}

fn make_run_result_batch(status: &str, execution_time: f64, completed_at: &str) -> RecordBatch {
    RecordBatch::try_new(
        run_result_schema(),
        vec![
            Arc::new(StringArray::from(vec![status])),
            Arc::new(Float64Array::from(vec![execution_time])),
            Arc::new(StringArray::from(vec![completed_at])),
        ],
    )
    .expect("valid run result batch")
}

fn catalog_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("type", DataType::Utf8, true),
        Field::new("owner", DataType::Utf8, true),
        Field::new("bytes_stat", DataType::Int64, true),
        Field::new("row_count_stat", DataType::Int64, true),
    ]))
}

fn make_catalog_batch(typ: &str, owner: &str, bytes_stat: i64, row_count_stat: i64) -> RecordBatch {
    RecordBatch::try_new(
        catalog_schema(),
        vec![
            Arc::new(StringArray::from(vec![typ])),
            Arc::new(StringArray::from(vec![owner])),
            Arc::new(Int64Array::from(vec![bytes_stat])),
            Arc::new(Int64Array::from(vec![row_count_stat])),
        ],
    )
    .expect("valid catalog batch")
}

// ── Backend helpers ───────────────────────────────────────────────────────────

/// Minimal backend: node row only, empty columns and edges.
/// Use for tests that care about capability gating, not sub-resource extraction.
fn detail_backend(
    node: Vec<RecordBatch>,
    run_result: Option<RecordBatch>,
    catalog: Option<RecordBatch>,
) -> DetailMockBackend {
    DetailMockBackend {
        node_batches: node,
        column_batches: vec![],
        edge_batches: vec![],
        run_result_batches: run_result.map(|b| vec![b]),
        catalog_batches: catalog.map(|b| vec![b]),
    }
}

/// Full backend: all sub-resources supplied. Use for extraction correctness tests.
fn full_detail_backend(
    node: Vec<RecordBatch>,
    columns: Vec<RecordBatch>,
    edges: Vec<RecordBatch>,
    run_result: Option<RecordBatch>,
    catalog: Option<RecordBatch>,
) -> DetailMockBackend {
    DetailMockBackend {
        node_batches: node,
        column_batches: columns,
        edge_batches: edges,
        run_result_batches: run_result.map(|b| vec![b]),
        catalog_batches: catalog.map(|b| vec![b]),
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn get_model_returns_200_with_core_fields() {
    let backend = detail_backend(
        vec![make_node_batch("model.jaffle_shop.orders", "orders")],
        None,
        None,
    );
    let state = make_detail_state(backend);
    let response = get_model(State(state), Path("model.jaffle_shop.orders".to_owned())).await;
    assert_eq!(response.status(), 200);

    let body = response_body(response).await;
    assert_eq!(body["unique_id"], "model.jaffle_shop.orders");
    assert_eq!(body["name"], "orders");
    assert_eq!(body["resource_type"], "model");
    assert_eq!(body["materialized"], "table");
    assert_eq!(body["access_level"], "public");
    assert_eq!(body["contract_enforced"], true);
}

#[tokio::test]
async fn get_model_returns_404_when_not_found() {
    let backend = detail_backend(vec![], None, None);
    let state = make_detail_state(backend);
    let response = get_model(State(state), Path("model.jaffle_shop.missing".to_owned())).await;
    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn get_model_returns_400_for_invalid_unique_id() {
    let backend = detail_backend(vec![], None, None);
    let state = make_detail_state(backend);
    let response = get_model(
        State(state),
        Path("model'; DROP TABLE nodes; --".to_owned()),
    )
    .await;
    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn get_model_execution_info_null_when_run_results_absent() {
    let backend = detail_backend(
        vec![make_node_batch("model.jaffle_shop.orders", "orders")],
        None,
        None,
    );
    let state = make_detail_state(backend);
    let body =
        response_body(get_model(State(state), Path("model.jaffle_shop.orders".to_owned())).await)
            .await;
    assert_eq!(body["execution_info"], serde_json::Value::Null);
}

#[tokio::test]
async fn get_model_execution_info_populated_when_run_results_present() {
    let backend = detail_backend(
        vec![make_node_batch("model.jaffle_shop.orders", "orders")],
        Some(make_run_result_batch(
            "success",
            4.2,
            "2026-05-15T10:32:11Z",
        )),
        None,
    );
    let state = make_detail_state(backend);
    let body =
        response_body(get_model(State(state), Path("model.jaffle_shop.orders".to_owned())).await)
            .await;
    let ei = &body["execution_info"];
    assert_eq!(ei["status"], "success");
    assert_eq!(ei["completed_at"], "2026-05-15T10:32:11Z");
    assert_eq!(ei["execution_time"], serde_json::json!(4.2));
}

#[tokio::test]
async fn get_model_catalog_null_when_catalog_stats_absent() {
    let backend = detail_backend(
        vec![make_node_batch("model.jaffle_shop.orders", "orders")],
        None,
        None,
    );
    let state = make_detail_state(backend);
    let body =
        response_body(get_model(State(state), Path("model.jaffle_shop.orders".to_owned())).await)
            .await;
    assert_eq!(body["catalog"], serde_json::Value::Null);
}

#[tokio::test]
async fn get_model_catalog_populated_when_catalog_stats_present() {
    let backend = detail_backend(
        vec![make_node_batch("model.jaffle_shop.orders", "orders")],
        None,
        Some(make_catalog_batch("table", "dbt_runner", 1_048_576, 10_500)),
    );
    let state = make_detail_state(backend);
    let body =
        response_body(get_model(State(state), Path("model.jaffle_shop.orders".to_owned())).await)
            .await;
    let cat = &body["catalog"];
    assert_eq!(cat["type"], "table");
    assert_eq!(cat["owner"], "dbt_runner");
    assert_eq!(cat["bytes_stat"], serde_json::json!(1_048_576_i64));
    assert_eq!(cat["row_count_stat"], serde_json::json!(10_500_i64));
}

#[tokio::test]
async fn get_model_all_capabilities_present() {
    let backend = detail_backend(
        vec![make_node_batch("model.jaffle_shop.orders", "orders")],
        Some(make_run_result_batch(
            "success",
            4.2,
            "2026-05-15T10:32:11Z",
        )),
        Some(make_catalog_batch("table", "dbt_runner", 1_048_576, 10_500)),
    );
    let state = make_detail_state(backend);
    let body =
        response_body(get_model(State(state), Path("model.jaffle_shop.orders".to_owned())).await)
            .await;
    assert_eq!(body["unique_id"], "model.jaffle_shop.orders");
    assert_eq!(body["execution_info"]["status"], "success");
    assert_eq!(body["catalog"]["type"], "table");
    // Verify sub-resource arrays are present and empty (batch has no columns/edges).
    assert_eq!(body["columns"].as_array().unwrap().len(), 0);
    assert_eq!(body["depends_on"].as_array().unwrap().len(), 0);
    assert_eq!(body["referenced_by"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn get_model_nullable_fields_present_as_null_not_absent() {
    // Typed struct guarantees Option<T> fields serialize as JSON null, not absent.
    let backend = detail_backend(
        vec![make_node_batch("model.jaffle_shop.orders", "orders")],
        None,
        None,
    );
    let state = make_detail_state(backend);
    let body =
        response_body(get_model(State(state), Path("model.jaffle_shop.orders".to_owned())).await)
            .await;
    assert_eq!(body["compiled_code"], serde_json::Value::Null);
    assert_eq!(body["execution_info"], serde_json::Value::Null);
    assert_eq!(body["catalog"], serde_json::Value::Null);
}

// ── Extraction correctness tests ─────────────────────────────────────────────
// These exercise the actual extraction logic with realistic data. They are the
// canonical reference for future endpoints (sources, seeds, snapshots, tests)
// that will copy this handler's pattern.

#[tokio::test]
async fn get_model_tags_and_fqn_extracted_as_arrays() {
    // Exercises extract_str_list against a real ListArray batch.
    // This is the one path that would silently return [] if tags/fqn were
    // stored as JSON strings rather than List(Utf8) — see issue #10255.
    let backend = detail_backend(
        vec![make_node_batch_with_lists(
            "model.jaffle_shop.orders",
            "orders",
            &["finance", "core"],
            &["jaffle_shop", "orders"],
        )],
        None,
        None,
    );
    let state = make_detail_state(backend);
    let body =
        response_body(get_model(State(state), Path("model.jaffle_shop.orders".to_owned())).await)
            .await;
    assert_eq!(body["tags"], serde_json::json!(["finance", "core"]));
    assert_eq!(body["fqn"], serde_json::json!(["jaffle_shop", "orders"]));
}

#[tokio::test]
async fn get_model_empty_tags_and_fqn_serialize_as_empty_arrays() {
    let backend = detail_backend(
        vec![make_node_batch_with_lists(
            "model.jaffle_shop.orders",
            "orders",
            &[],
            &[],
        )],
        None,
        None,
    );
    let state = make_detail_state(backend);
    let body =
        response_body(get_model(State(state), Path("model.jaffle_shop.orders".to_owned())).await)
            .await;
    assert_eq!(body["tags"], serde_json::json!([]));
    assert_eq!(body["fqn"], serde_json::json!([]));
}

#[tokio::test]
async fn get_model_columns_extracted_with_real_rows() {
    // Exercises extract_model_columns with multiple rows and mixed nullability.
    let columns_batch = make_columns_batch(&[
        ("order_id", Some(0), Some("integer")),
        ("amount", Some(1), None), // data_type null
        ("status", Some(2), Some("varchar")),
    ]);
    let backend = full_detail_backend(
        vec![make_node_batch("model.jaffle_shop.orders", "orders")],
        vec![columns_batch],
        vec![],
        None,
        None,
    );
    let state = make_detail_state(backend);
    let body =
        response_body(get_model(State(state), Path("model.jaffle_shop.orders".to_owned())).await)
            .await;
    let cols = body["columns"].as_array().unwrap();
    assert_eq!(cols.len(), 3);
    assert_eq!(cols[0]["name"], "order_id");
    assert_eq!(cols[0]["index"], serde_json::json!(0_i64));
    assert_eq!(cols[0]["data_type"], "integer");
    assert_eq!(cols[1]["name"], "amount");
    assert_eq!(cols[1]["data_type"], serde_json::Value::Null);
    assert_eq!(cols[2]["name"], "status");
    // Every column row must include all nullable sub-fields, not just non-null ones.
    for col in cols {
        for field in [
            "declared_type",
            "inferred_type",
            "catalog_type",
            "description",
            "label",
            "granularity",
        ] {
            assert!(
                col.get(field).is_some(),
                "column field '{field}' must be present"
            );
        }
    }
}

#[tokio::test]
async fn get_model_edges_extracted_into_depends_on_and_referenced_by() {
    // Both depends_on and referenced_by use the same edges batch in the mock.
    // Verifies extract_edge_refs with real multi-row data.
    let edges_batch = make_edges_batch(&[
        ("model.jaffle_shop.stg_orders", "ref"),
        ("model.jaffle_shop.stg_payments", "ref"),
    ]);
    let backend = full_detail_backend(
        vec![make_node_batch("model.jaffle_shop.orders", "orders")],
        vec![],
        vec![edges_batch],
        None,
        None,
    );
    let state = make_detail_state(backend);
    let body =
        response_body(get_model(State(state), Path("model.jaffle_shop.orders".to_owned())).await)
            .await;
    // Both arrays populated from the same mock batch.
    for key in ["depends_on", "referenced_by"] {
        let edges = body[key].as_array().unwrap();
        assert_eq!(edges.len(), 2, "{key} should have 2 edges");
        assert_eq!(edges[0]["unique_id"], "model.jaffle_shop.stg_orders");
        assert_eq!(edges[0]["edge_type"], "ref");
        assert_eq!(edges[1]["unique_id"], "model.jaffle_shop.stg_payments");
    }
}

// ---------------------------------------------------------------------------
// ?materialization= filter tests
// ---------------------------------------------------------------------------

fn value_batch(values: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.to_vec()))])
        .expect("valid value batch")
}

#[tokio::test]
async fn materialization_filter_narrows_results() {
    let batch = all_fields_batch();
    let backend = MockBackend::with_rows(1, vec![batch]);
    let state = make_state(backend);
    let params = ModelListParams {
        materialization: Some("table".into()),
        ..Default::default()
    };
    let response = list_models(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn materialization_filter_accepts_custom_value() {
    // Custom materialization like "iceberg" must return 200, never 400.
    let backend = MockBackend::with_rows(0, vec![]);
    let state = make_state(backend);
    let params = ModelListParams {
        materialization: Some("iceberg".into()),
        ..Default::default()
    };
    let response = list_models(State(state), Query(params)).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    assert_eq!(body["page_info"]["total_count"], 0);
}

#[tokio::test]
async fn model_facets_returns_materializations() {
    let owners_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("owner", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["Data Team"]))],
    )
    .unwrap();
    let mat_batch = value_batch(&["table", "view"]);
    let backend =
        MockBackend::with_rows(0, vec![owners_batch]).with_materialization_facets(vec![mat_batch]);
    let state = make_state(backend);
    let response = list_model_facets(State(state)).await;
    assert_eq!(response.status(), 200);
    let body = response_body(response).await;
    let mats = body["materializations"].as_array().unwrap();
    assert_eq!(mats.len(), 2);
    assert_eq!(mats[0]["value"], "table");
    assert_eq!(mats[1]["value"], "view");
    assert_eq!(mats[0]["count"], serde_json::Value::Null);
}
