use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use axum::extract::State;
use axum::response::Response;

use super::*;
use crate::providers::{Backend, BackendError, Providers};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Unit tests: projection_for SQL generation
// ---------------------------------------------------------------------------

#[test]
fn projection_for_nodes_uses_inline_resource_type_and_real_patch_path() {
    let sql = projection_for("dbt.nodes", None);
    assert!(
        sql.contains(", resource_type,"),
        "dbt.nodes must project its own resource_type column, got: {sql}"
    );
    assert!(
        !sql.contains("AS resource_type"),
        "dbt.nodes must not synthesize a literal resource_type"
    );
    assert!(
        sql.contains("patch_path") && !sql.contains("CAST(NULL"),
        "dbt.nodes must project its real patch_path column"
    );
    assert!(
        !sql.contains("WHERE"),
        "dbt.nodes has no filter clause, got: {sql}"
    );
}

#[test]
fn projection_for_macros_uses_literal_resource_type_and_real_patch_path() {
    let sql = projection_for("dbt.macros", Some("macro"));
    assert!(
        sql.contains("'macro' AS resource_type"),
        "macros must synthesize a literal resource_type, got: {sql}"
    );
    assert!(
        sql.contains("patch_path") && !sql.contains("CAST(NULL"),
        "dbt.macros must project its real patch_path column"
    );
}

#[test]
fn projection_for_generic_table_casts_null_patch_path() {
    let sql = projection_for("dbt.exposures", Some("exposure"));
    assert!(sql.contains("'exposure' AS resource_type"));
    assert!(
        sql.contains("CAST(NULL AS VARCHAR) AS patch_path"),
        "non-node/macro tables must cast NULL patch_path, got: {sql}"
    );
    assert!(!sql.contains("WHERE"));
}

#[test]
fn projection_for_semantic_models_filters_null_paths() {
    let sql = projection_for("dbt.semantic_models", Some("semantic_model"));
    assert!(
        sql.contains("WHERE original_file_path IS NOT NULL"),
        "semantic_models must filter null paths, got: {sql}"
    );
}

#[test]
fn sources_have_unique_tables_and_literals() {
    // Tables must be unique — a duplicate would double-count rows in UNION ALL.
    let mut seen = HashSet::new();
    for (table, _) in SOURCES {
        assert!(seen.insert(*table), "duplicate source table: {table}");
    }
    // Only dbt.nodes uses inline resource_type (literal = None).
    let inline = SOURCES
        .iter()
        .filter(|(_, lit)| lit.is_none())
        .map(|(t, _)| *t)
        .collect::<Vec<_>>();
    assert_eq!(
        inline,
        vec!["dbt.nodes"],
        "only dbt.nodes should rely on its own resource_type column"
    );
}

// ---------------------------------------------------------------------------
// Integration tests: list_files handler via direct invocation
// ---------------------------------------------------------------------------

/// Mock backend that reports a configurable set of tables as populated and
/// returns canned scalar/arrow results for any SQL the handler runs.
struct MockBackend {
    populated_tables: HashSet<&'static str>,
    scalar_result: Option<String>,
    arrow_result: Result<Vec<RecordBatch>, BackendError>,
}

impl MockBackend {
    fn with_no_tables() -> Self {
        Self {
            populated_tables: HashSet::new(),
            scalar_result: Some("0".into()),
            arrow_result: Ok(vec![]),
        }
    }

    fn with_tables(tables: &[&'static str], count: u64, rows: Vec<RecordBatch>) -> Self {
        Self {
            populated_tables: tables.iter().copied().collect(),
            scalar_result: Some(count.to_string()),
            arrow_result: Ok(rows),
        }
    }

    fn with_query_error(tables: &[&'static str]) -> Self {
        Self {
            populated_tables: tables.iter().copied().collect(),
            scalar_result: Some("1".into()),
            arrow_result: Err(BackendError::Query("boom".into())),
        }
    }
}

impl Backend for MockBackend {
    fn is_available(&self) -> bool {
        true
    }

    fn table_has_rows(&self, table: &str) -> bool {
        self.populated_tables.contains(table)
    }

    fn query_scalar(&self, _sql: &str) -> Option<String> {
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

fn files_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("resource_type", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, true),
        Field::new("patch_path", DataType::Utf8, true),
    ]))
}

fn one_row_batch() -> RecordBatch {
    RecordBatch::try_new(
        files_schema(),
        vec![
            Arc::new(StringArray::from(vec!["model.pkg.customers"])),
            Arc::new(StringArray::from(vec!["customers"])),
            Arc::new(StringArray::from(vec!["model"])),
            Arc::new(StringArray::from(vec![Some("pkg")])),
            Arc::new(StringArray::from(vec![Some("models/customers.sql")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
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
async fn no_tables_returns_empty_list_without_querying() {
    // Every source reports zero rows → handler must short-circuit and return
    // {files: [], total: 0} instead of running an empty UNION.
    let state = make_state(MockBackend::with_no_tables());
    let response = list_files(State(state)).await;
    assert_eq!(response.status(), 200);

    let body = response_body(response).await;
    assert_eq!(body["total"], 0);
    assert_eq!(body["files"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn populated_table_returns_rows_with_total() {
    let state = make_state(MockBackend::with_tables(
        &["dbt.nodes"],
        1,
        vec![one_row_batch()],
    ));
    let body = response_body(list_files(State(state)).await).await;

    assert_eq!(body["total"], 1);
    let files = body["files"].as_array().unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0]["unique_id"], "model.pkg.customers");
    assert_eq!(files[0]["name"], "customers");
    assert_eq!(files[0]["resource_type"], "model");
    assert_eq!(files[0]["original_file_path"], "models/customers.sql");
    assert_eq!(files[0]["patch_path"], serde_json::Value::Null);
}

#[tokio::test]
async fn backend_query_error_returns_500() {
    let state = make_state(MockBackend::with_query_error(&["dbt.nodes"]));
    let response = list_files(State(state)).await;
    assert_eq!(response.status(), 500);
}
