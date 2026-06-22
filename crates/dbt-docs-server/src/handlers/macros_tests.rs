//! Tests for `GET /api/v1/macros/:id`, `GET /api/v1/macros`, and
//! `GET /api/v1/macros/facets`.
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
use crate::handlers::pagination::Cursor;
use crate::providers::{Backend, BackendError, Providers};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Mock backend
// ---------------------------------------------------------------------------

/// Routes the single `dbt.macros` query to the configured batches.
struct MacroDetailMockBackend {
    macro_batches: Vec<RecordBatch>,
}

impl Backend for MacroDetailMockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn query_scalar(&self, _sql: &str) -> Option<String> {
        Some("0".to_owned())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        if sql.contains("dbt.macros") {
            return Ok(self.macro_batches.clone());
        }
        Err(BackendError::Query(format!("unrouted query: {sql}")))
    }
}

fn make_state(backend: MacroDetailMockBackend) -> Arc<AppState> {
    let providers = Providers {
        backend: Arc::new(backend),
        ..Providers::default()
    };
    Arc::new(AppState {
        has_dbt_state: false,
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

/// Schema for `dbt.macros` rows as queried by `MACRO_DETAIL_SQL`. Not
/// compile-checked against the production schema (#10255).
fn macro_schema(deps_field: &Field, langs_field: &Field) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("file_path", DataType::Utf8, true),
        Field::new("patch_path", DataType::Utf8, true),
        Field::new("macro_sql", DataType::Utf8, true),
        Field::new("arguments", DataType::Utf8, true),
        Field::new("meta", DataType::Utf8, true),
        Field::new("docs_show", DataType::Boolean, true),
        langs_field.clone(),
        deps_field.clone(),
        Field::new("created_at", DataType::Float64, true),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn make_macro_batch(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    description: Option<&str>,
    macro_sql: Option<&str>,
    arguments_json: Option<&str>,
    meta_json: Option<&str>,
    docs_show: Option<bool>,
    supported_languages: &[&str],
    depends_on_macros: &[&str],
    created_at: Option<f64>,
) -> RecordBatch {
    let langs_arr = make_str_list(supported_languages);
    let deps_arr = make_str_list(depends_on_macros);
    let langs_field = Field::new("supported_languages", langs_arr.data_type().clone(), true);
    let deps_field = Field::new("depends_on_macros", deps_arr.data_type().clone(), true);

    RecordBatch::try_new(
        macro_schema(&deps_field, &langs_field),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![description])),
            Arc::new(StringArray::from(vec![Some("macros/m.sql")])),
            Arc::new(StringArray::from(vec![Some("macros/m.sql")])),
            Arc::new(StringArray::from(vec![Some("macros/schema.yml")])),
            Arc::new(StringArray::from(vec![macro_sql])),
            Arc::new(StringArray::from(vec![arguments_json])),
            Arc::new(StringArray::from(vec![meta_json])),
            Arc::new(BooleanArray::from(vec![docs_show])),
            Arc::new(langs_arr),
            Arc::new(deps_arr),
            Arc::new(Float64Array::from(vec![created_at])),
        ],
    )
    .expect("valid macro batch")
}

async fn response_body(response: Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&bytes).expect("valid json")
}

fn full_macro_batch() -> RecordBatch {
    make_macro_batch(
        "macro.jaffle_shop.cents_to_dollars",
        "cents_to_dollars",
        Some("jaffle_shop"),
        Some("Convert cents to dollars."),
        Some("{% macro cents_to_dollars(c) -%}({{ c }} / 100)::numeric{%- endmacro %}"),
        Some(r#"[{"name":"column_name","type":"string","description":"cents col"}]"#),
        Some(r#"{"owner":"data-eng"}"#),
        Some(true),
        &["sql"],
        &["macro.dbt.type_numeric"],
        Some(1_746_000_000.0),
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn invalid_unique_id_returns_400() {
    let backend = MacroDetailMockBackend {
        macro_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_macro(State(state), Path("bad'id".to_owned())).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn missing_macro_returns_404() {
    let backend = MacroDetailMockBackend {
        macro_batches: vec![],
    };
    let state = make_state(backend);
    let r = get_macro(State(state), Path("macro.x.y".to_owned())).await;
    assert_eq!(r.status(), 404);
}

#[tokio::test]
async fn all_fields_hydrated() {
    let backend = MacroDetailMockBackend {
        macro_batches: vec![full_macro_batch()],
    };
    let state = make_state(backend);
    let r = get_macro(
        State(state),
        Path("macro.jaffle_shop.cents_to_dollars".to_owned()),
    )
    .await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;

    // NodeBase fields flatten into top-level.
    assert_eq!(body["unique_id"], "macro.jaffle_shop.cents_to_dollars");
    assert_eq!(body["name"], "cents_to_dollars");
    assert_eq!(body["resource_type"], "macro");
    assert_eq!(body["package_name"], "jaffle_shop");
    assert_eq!(body["description"], "Convert cents to dollars.");

    // Macro-specific scalars.
    assert_eq!(body["file_path"], "macros/m.sql");
    assert_eq!(body["patch_path"], "macros/schema.yml");
    assert!(
        body["macro_sql"]
            .as_str()
            .unwrap()
            .contains("cents_to_dollars"),
        "macro_sql must round-trip the template source"
    );
    assert_eq!(body["docs_show"], true);
    assert_eq!(body["supported_languages"], serde_json::json!(["sql"]));
    assert_eq!(body["created_at"], 1_746_000_000.0);

    // meta parsed as JSON object.
    assert_eq!(
        body["meta"],
        serde_json::json!({"owner": "data-eng"}),
        "meta must be parsed as JSON object, not escaped string"
    );

    // arguments parsed as JSON array.
    assert_eq!(
        body["arguments"],
        serde_json::json!([{"name":"column_name","type":"string","description":"cents col"}]),
        "arguments must be parsed as JSON array, not escaped string"
    );

    // depends_on synthesised from depends_on_macros list with edge_type "macro".
    assert_eq!(
        body["depends_on"],
        serde_json::json!([{"unique_id":"macro.dbt.type_numeric","edge_type":"macro"}]),
    );

    // Excluded fields must NOT appear.
    assert!(body.get("referenced_by").is_none());
    assert!(body.get("execution_info").is_none());
    assert!(body.get("catalog").is_none());
    assert!(body.get("columns").is_none());
    assert!(body.get("tags").is_none());
    assert!(body.get("fqn").is_none());
    assert!(body.get("materialized").is_none());
    assert!(body.get("raw_code").is_none());
}

#[tokio::test]
async fn meta_null_when_absent() {
    let backend = MacroDetailMockBackend {
        macro_batches: vec![make_macro_batch(
            "macro.pkg.m",
            "m",
            None,
            None,
            None,
            None,
            None, // meta absent
            None,
            &[],
            &[],
            None,
        )],
    };
    let state = make_state(backend);
    let r = get_macro(State(state), Path("macro.pkg.m".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn meta_null_when_malformed() {
    // Malformed JSON must serialise as null, NOT bubble a parse error to the client.
    let backend = MacroDetailMockBackend {
        macro_batches: vec![make_macro_batch(
            "macro.pkg.m",
            "m",
            None,
            None,
            None,
            None,
            Some("not{valid:json"),
            None,
            &[],
            &[],
            None,
        )],
    };
    let state = make_state(backend);
    let r = get_macro(State(state), Path("macro.pkg.m".to_owned())).await;
    assert_eq!(r.status(), 200, "malformed meta must not 500 the response");
    let body = response_body(r).await;
    assert_eq!(body["meta"], serde_json::Value::Null);
}

#[tokio::test]
async fn arguments_null_when_absent() {
    let backend = MacroDetailMockBackend {
        macro_batches: vec![make_macro_batch(
            "macro.pkg.m",
            "m",
            None,
            None,
            None,
            None, // arguments absent
            None,
            None,
            &[],
            &[],
            None,
        )],
    };
    let state = make_state(backend);
    let r = get_macro(State(state), Path("macro.pkg.m".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["arguments"], serde_json::Value::Null);
}

#[tokio::test]
async fn arguments_null_when_malformed() {
    let backend = MacroDetailMockBackend {
        macro_batches: vec![make_macro_batch(
            "macro.pkg.m",
            "m",
            None,
            None,
            None,
            Some("[not valid"),
            None,
            None,
            &[],
            &[],
            None,
        )],
    };
    let state = make_state(backend);
    let r = get_macro(State(state), Path("macro.pkg.m".to_owned())).await;
    assert_eq!(
        r.status(),
        200,
        "malformed arguments must not 500 the response"
    );
    let body = response_body(r).await;
    assert_eq!(body["arguments"], serde_json::Value::Null);
}

#[tokio::test]
async fn depends_on_empty_when_no_macro_deps() {
    let backend = MacroDetailMockBackend {
        macro_batches: vec![make_macro_batch(
            "macro.pkg.m",
            "m",
            None,
            None,
            None,
            None,
            None,
            None,
            &[],
            &[], // no deps
            None,
        )],
    };
    let state = make_state(backend);
    let r = get_macro(State(state), Path("macro.pkg.m".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["depends_on"], serde_json::json!([]));
}

#[tokio::test]
async fn supported_languages_empty_when_absent() {
    let backend = MacroDetailMockBackend {
        macro_batches: vec![make_macro_batch(
            "macro.pkg.m",
            "m",
            None,
            None,
            None,
            None,
            None,
            None,
            &[], // no languages
            &[],
            None,
        )],
    };
    let state = make_state(backend);
    let r = get_macro(State(state), Path("macro.pkg.m".to_owned())).await;
    let body = response_body(r).await;
    assert_eq!(body["supported_languages"], serde_json::json!([]));
}

// ---------------------------------------------------------------------------
// Mock backend: list_macros / list_macro_facets
// ---------------------------------------------------------------------------

// TODO(#10255): replace hand-rolled RecordBatch schemas with typed row
// builders once dbt-index ships them. A column rename in `dbt.macros`
// will pass these tests while silently breaking the handler.

struct MacroListMockBackend {
    total_count: u64,
    row_batches: Vec<RecordBatch>,
    /// `Some(batches)` for facets query; `None` routes to `row_batches`.
    facet_batches: Option<Vec<RecordBatch>>,
}

impl MacroListMockBackend {
    fn new(total: u64, rows: Vec<RecordBatch>) -> Self {
        Self {
            total_count: total,
            row_batches: rows,
            facet_batches: None,
        }
    }

    fn with_facets(total: u64, rows: Vec<RecordBatch>, facets: Vec<RecordBatch>) -> Self {
        Self {
            total_count: total,
            row_batches: rows,
            facet_batches: Some(facets),
        }
    }
}

impl Backend for MacroListMockBackend {
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

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, BackendError> {
        if sql.contains("DISTINCT") {
            return Ok(self.facet_batches.clone().unwrap_or_default());
        }
        Ok(self.row_batches.clone())
    }
}

fn make_list_state(backend: MacroListMockBackend) -> Arc<AppState> {
    let providers = Providers {
        backend: Arc::new(backend),
        ..Providers::default()
    };
    Arc::new(AppState {
        has_dbt_state: false,
        index_dir: PathBuf::from("/tmp"),
        providers,
    })
}

// ---------------------------------------------------------------------------
// Batch builders: list rows
// ---------------------------------------------------------------------------

fn macro_list_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("arguments", DataType::Utf8, true),
    ]))
}

fn macro_list_row(
    unique_id: &str,
    name: &str,
    package_name: Option<&str>,
    description: Option<&str>,
    arguments_json: Option<&str>,
) -> RecordBatch {
    RecordBatch::try_new(
        macro_list_schema(),
        vec![
            Arc::new(StringArray::from(vec![unique_id])),
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec![package_name])),
            Arc::new(StringArray::from(vec![description])),
            Arc::new(StringArray::from(vec![arguments_json])),
        ],
    )
    .expect("valid macro list row batch")
}

fn facet_value_batch(values: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let arr: Vec<Option<&str>> = values.iter().map(|v| Some(*v)).collect();
    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(arr))])
        .expect("valid facet value batch")
}

// ---------------------------------------------------------------------------
// Unit tests: SQL builder
// ---------------------------------------------------------------------------

#[test]
fn sort_default_is_name_asc() {
    let params = MacroListParams::default();
    let (_, rows) = build_macro_list_sql(&params, 10, None).unwrap();
    assert!(rows.contains("ORDER BY m.name ASC NULLS LAST"));
}

#[test]
fn sort_name_desc_accepted() {
    let params = MacroListParams {
        sort: Some("name:desc".into()),
        ..Default::default()
    };
    let (_, rows) = build_macro_list_sql(&params, 10, None).unwrap();
    assert!(rows.contains("ORDER BY m.name DESC NULLS LAST"));
}

#[test]
fn sort_unknown_column_returns_err() {
    let params = MacroListParams {
        sort: Some("package_name:asc".into()),
        ..Default::default()
    };
    assert!(build_macro_list_sql(&params, 10, None).is_err());
}

#[test]
fn sort_unknown_direction_returns_err() {
    let params = MacroListParams {
        sort: Some("name:random".into()),
        ..Default::default()
    };
    assert!(build_macro_list_sql(&params, 10, None).is_err());
}

#[test]
fn package_filter_applied() {
    let params = MacroListParams {
        package: Some("dbt_utils".into()),
        ..Default::default()
    };
    let (count, rows) = build_macro_list_sql(&params, 10, None).unwrap();
    assert!(count.contains("package_name = 'dbt_utils'"));
    assert!(rows.contains("package_name = 'dbt_utils'"));
}

#[test]
fn empty_package_filter_not_applied() {
    let params = MacroListParams {
        package: Some("".into()),
        ..Default::default()
    };
    let (count, rows) = build_macro_list_sql(&params, 10, None).unwrap();
    // The WHERE clause must not filter on package_name when the param is empty.
    assert!(
        !count.contains("package_name ="),
        "count must not filter on package_name when empty"
    );
    assert!(
        !rows.contains("package_name ="),
        "rows must not filter on package_name when empty"
    );
}

#[test]
fn count_sql_excludes_cursor_rows_sql_includes_cursor() {
    let c = Cursor {
        sort_value: Some("cents_to_dollars".into()),
        unique_id: "macro.jaffle_shop.cents_to_dollars".into(),
    };
    let params = MacroListParams::default();
    let (count, rows) = build_macro_list_sql(&params, 10, Some(&c)).unwrap();
    assert!(
        !count.contains("cents_to_dollars"),
        "count must exclude cursor predicate"
    );
    assert!(
        rows.contains("cents_to_dollars"),
        "rows must include cursor predicate"
    );
}

// ---------------------------------------------------------------------------
// Integration tests: list_macros
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_macros_empty_catalog() {
    let state = make_list_state(MacroListMockBackend::new(0, vec![]));
    let r = list_macros(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"], serde_json::json!([]));
    assert_eq!(body["page_info"]["total_count"], 0);
    assert_eq!(body["page_info"]["has_next_page"], false);
    assert_eq!(body["page_info"]["start_cursor"], serde_json::Value::Null);
    assert_eq!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_macros_all_fields_hydrated() {
    let row = macro_list_row(
        "macro.jaffle_shop.cents_to_dollars",
        "cents_to_dollars",
        Some("jaffle_shop"),
        Some("Convert cents to dollars."),
        Some(r#"[{"name":"column_name","type":"string","description":"cents col"}]"#),
    );
    let state = make_list_state(MacroListMockBackend::new(1, vec![row]));
    let r = list_macros(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(
        body["data"][0]["unique_id"],
        "macro.jaffle_shop.cents_to_dollars"
    );
    assert_eq!(body["data"][0]["name"], "cents_to_dollars");
    assert_eq!(body["data"][0]["package_name"], "jaffle_shop");
    assert_eq!(body["data"][0]["description"], "Convert cents to dollars.");
    assert_eq!(
        body["data"][0]["arguments"],
        serde_json::json!([{"name":"column_name","type":"string","description":"cents col"}])
    );
    assert_eq!(body["page_info"]["total_count"], 1);
    // Single page: end_cursor must be null.
    assert_eq!(
        body["page_info"]["end_cursor"],
        serde_json::Value::Null,
        "end_cursor must be null on last page"
    );
    assert_ne!(body["page_info"]["start_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_macros_nullable_fields() {
    let row = macro_list_row(
        "macro.dbt.generate_alias_name",
        "generate_alias_name",
        None,
        None,
        None, // arguments absent
    );
    let state = make_list_state(MacroListMockBackend::new(1, vec![row]));
    let r = list_macros(State(state), Query(Default::default())).await;
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["package_name"], serde_json::Value::Null);
    assert_eq!(body["data"][0]["description"], serde_json::Value::Null);
    assert_eq!(body["data"][0]["arguments"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_macros_arguments_null_when_malformed() {
    let row = macro_list_row("macro.pkg.m", "m", None, None, Some("[not valid json"));
    let state = make_list_state(MacroListMockBackend::new(1, vec![row]));
    let r = list_macros(State(state), Query(Default::default())).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(
        body["data"][0]["arguments"],
        serde_json::Value::Null,
        "malformed arguments JSON must serialize as null"
    );
}

#[tokio::test]
async fn list_macros_sort_unknown_column_returns_400() {
    let state = make_list_state(MacroListMockBackend::new(0, vec![]));
    let params = MacroListParams {
        sort: Some("description:asc".into()),
        ..Default::default()
    };
    let r = list_macros(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_macros_invalid_cursor_returns_400() {
    let state = make_list_state(MacroListMockBackend::new(0, vec![]));
    let params = MacroListParams {
        after: Some("not-valid-base64!!!".into()),
        ..Default::default()
    };
    let r = list_macros(State(state), Query(params)).await;
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn list_macros_multi_page_has_next_page_true() {
    let row_a = macro_list_row("macro.pkg.alpha", "alpha", None, None, None);
    let row_b = macro_list_row("macro.pkg.beta", "beta", None, None, None);
    // Return 2 rows with first=1: mock returns both, handler peeks and truncates.
    let state = make_list_state(MacroListMockBackend::new(2, vec![row_a, row_b]));
    let params = MacroListParams {
        first: Some(1),
        ..Default::default()
    };
    let r = list_macros(State(state), Query(params)).await;
    let body = response_body(r).await;
    assert_eq!(body["page_info"]["has_next_page"], true);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_ne!(body["page_info"]["end_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_macros_last_page_end_cursor_null() {
    let row = macro_list_row("macro.pkg.z", "z", None, None, None);
    let state = make_list_state(MacroListMockBackend::new(1, vec![row]));
    let r = list_macros(State(state), Query(Default::default())).await;
    let body = response_body(r).await;
    assert_eq!(body["page_info"]["has_next_page"], false);
    assert_eq!(
        body["page_info"]["end_cursor"],
        serde_json::Value::Null,
        "end_cursor must be null on last page"
    );
}

#[tokio::test]
async fn list_macros_cursor_advances_page() {
    let after = Cursor {
        sort_value: Some("alpha".into()),
        unique_id: "macro.pkg.alpha".into(),
    }
    .encode();
    let row = macro_list_row("macro.pkg.beta", "beta", None, None, None);
    let state = make_list_state(MacroListMockBackend::new(1, vec![row]));
    let params = MacroListParams {
        after: Some(after),
        ..Default::default()
    };
    let r = list_macros(State(state), Query(params)).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["data"][0]["name"], "beta");
}

// ---------------------------------------------------------------------------
// Integration tests: list_macro_facets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_macro_facets_empty_catalog() {
    let state = make_list_state(MacroListMockBackend::new(0, vec![]));
    let r = list_macro_facets(State(state)).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    assert_eq!(body["packages"], serde_json::json!([]));
}

#[tokio::test]
async fn list_macro_facets_returns_packages() {
    let facets = facet_value_batch(&["dbt", "dbt_utils", "jaffle_shop"]);
    let state = make_list_state(MacroListMockBackend::with_facets(0, vec![], vec![facets]));
    let r = list_macro_facets(State(state)).await;
    assert_eq!(r.status(), 200);
    let body = response_body(r).await;
    let pkgs = body["packages"].as_array().expect("packages array");
    assert_eq!(pkgs.len(), 3);
    assert_eq!(pkgs[0]["value"], "dbt");
    assert_eq!(pkgs[1]["value"], "dbt_utils");
    assert_eq!(pkgs[2]["value"], "jaffle_shop");
    // count must be null in v0.
    assert_eq!(pkgs[0]["count"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_macro_facets_has_packages_key_even_when_empty() {
    // Even with zero rows, the response shape must be `{"packages": []}`
    // not `{}` — the packages facet key is always present.
    let state = make_list_state(MacroListMockBackend::with_facets(0, vec![], vec![]));
    let r = list_macro_facets(State(state)).await;
    let body = response_body(r).await;
    assert!(
        body.get("packages").is_some(),
        "packages key must be present even when empty"
    );
    assert_eq!(body["packages"], serde_json::json!([]));
}
