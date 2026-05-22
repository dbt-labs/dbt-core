//! `GET /api/v1/tests/:id` — discriminated union over data tests and unit
//! tests.
//!
//! Both `test.*` and `unit_test.*` `unique_id`s land on this endpoint. The
//! handler routes by `resource_type`: data tests are stored in `dbt.nodes`
//! with `resource_type = 'test'` and joined against `dbt.test_metadata` for
//! generic-test fields (`column_name`, `severity`, `test_metadata.name`,
//! `kwargs`); unit tests live in `dbt.unit_tests` (not in `dbt.nodes`) and
//! carry inline fixture blobs (`given`, `expect`) serialized as JSON strings.
//!
//! `execution_info` is `null` when `dbt_rt.run_results` has no row for the
//! resource. Tests are leaf nodes — no `referenced_by`.
//!
//! JSON-string parquet columns (`meta`, `test_metadata.kwargs`, `given`,
//! `expect`) are deserialised via
//! [`crate::handlers::json::json_parse_or_null`] so the response carries
//! nested objects, not escaped strings.

use arrow_array::{Array, Float64Array, RecordBatch, StringArray};
use axum::Json;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::handlers::json::{bad_request, internal_error, json_parse_or_null, not_found};
use crate::handlers::node_base::{EdgeRef, NodeBase, extract_edge_refs, extract_str_list, opt_str};
use crate::handlers::sql::escape_str;
use crate::state::SharedState;

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Response body for `GET /api/v1/tests/:id`. Tagged at the JSON level by the
/// `resource_type` field carried inside each variant.
#[derive(Serialize)]
#[serde(untagged)]
pub enum TestDetail {
    Data(Box<DataTestDetail>),
    Unit(Box<UnitTestDetail>),
}

/// Fields common to both `DataTestDetail` and `UnitTestDetail`.
#[derive(Serialize)]
pub struct TestCommon {
    #[serde(flatten)]
    pub base: NodeBase,
    pub tags: Vec<String>,
    pub fqn: Vec<String>,
    pub file_path: Option<String>,
    pub patch_path: Option<String>,
    /// Parsed JSON object, or `null` when absent / unparseable.
    pub meta: serde_json::Value,
    /// 1-hop upstream parents. Tests have no `referenced_by` (leaf nodes).
    pub depends_on: Vec<EdgeRef>,
    /// `null` when `dbt_rt.run_results` has no row.
    pub execution_info: Option<TestExecutionInfo>,
}

/// Per-test last-run snapshot. Adds `error` over the standard
/// [`crate::handlers::node_base::ExecutionInfo`] — populated from
/// `dbt_rt.run_results.message` when status indicates failure.
#[derive(Serialize)]
pub struct TestExecutionInfo {
    pub status: Option<String>,
    pub error: Option<String>,
    pub completed_at: Option<String>,
    pub execution_time: Option<f64>,
}

/// Data test variant — `resource_type: "test"`.
#[derive(Serialize)]
pub struct DataTestDetail {
    #[serde(flatten)]
    pub common: TestCommon,
    /// Column under test (e.g. `"order_id"` for `not_null` / `unique` tests).
    pub column_name: Option<String>,
    /// `"generic"` when `dbt.test_metadata` has a row, `"singular"` otherwise.
    pub test_type: Option<String>,
    /// `"ERROR"` / `"WARN"`. From `dbt.test_metadata.severity`.
    pub severity: Option<String>,
    /// `null` for singular tests (no row in `dbt.test_metadata`).
    pub test_metadata: Option<TestMetadata>,
    pub raw_code: Option<String>,
    pub compiled_code: Option<String>,
}

#[derive(Serialize)]
pub struct TestMetadata {
    pub name: String,
    /// Parsed JSON object; `null` if `kwargs` parquet column is absent /
    /// unparseable.
    pub kwargs: serde_json::Value,
}

/// Unit test variant — `resource_type: "unit_test"`.
#[derive(Serialize)]
pub struct UnitTestDetail {
    #[serde(flatten)]
    pub common: TestCommon,
    /// Identifier of the model under test (e.g. `"order_items"`).
    pub model: Option<String>,
    pub given: Vec<UnitTestFixture>,
    pub expect: Option<UnitTestExpect>,
    pub num_given: Option<i64>,
    pub num_given_rows: Option<i64>,
    pub num_expect_rows: Option<i64>,
}

#[derive(Serialize)]
pub struct UnitTestFixture {
    /// `ref(...)` / `source(...)` expression naming the upstream input.
    pub input: String,
    /// Row data as parsed JSON objects.
    pub rows: Vec<serde_json::Value>,
}

#[derive(Serialize)]
pub struct UnitTestExpect {
    pub rows: Vec<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

const DATA_TEST_NODE_SQL: &str = "\
SELECT n.unique_id, n.name, n.resource_type, n.package_name, n.description, \
       n.original_file_path, n.file_path, n.patch_path, n.meta, \
       n.raw_code, n.compiled_code, n.tags, n.fqn \
FROM dbt.nodes n \
WHERE n.unique_id = '{id}' AND n.resource_type = 'test' \
LIMIT 1";

const TEST_METADATA_SQL: &str = "\
SELECT test_name, kwargs, column_name, severity \
FROM dbt.test_metadata \
WHERE unique_id = '{id}' \
LIMIT 1";

const UNIT_TEST_SQL: &str = "\
SELECT unique_id, name, package_name, description, \
       original_file_path, file_path, fqn, \
       model, given, expect, depends_on_nodes \
FROM dbt.unit_tests \
WHERE unique_id = '{id}' \
LIMIT 1";

const TEST_RUN_RESULT_SQL: &str = "\
SELECT status, execution_time, message, \
       CAST(created_at AS VARCHAR) AS completed_at \
FROM dbt_rt.run_results \
WHERE unique_id = '{id}' \
ORDER BY created_at DESC \
LIMIT 1";

// ---------------------------------------------------------------------------
// Extractors
// ---------------------------------------------------------------------------

/// Helper: read an Utf8 cell at row 0 by name, returning `None` for missing
/// columns or null cells.
fn s0(batch: &RecordBatch, name: &str) -> Option<String> {
    let col = batch
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<StringArray>()?;
    opt_str(col, 0)
}

fn extract_data_test_node(batch: &RecordBatch) -> (NodeBase, DataTestNodeFields) {
    let base = NodeBase {
        unique_id: s0(batch, "unique_id").unwrap_or_default(),
        name: s0(batch, "name").unwrap_or_default(),
        resource_type: s0(batch, "resource_type").unwrap_or_default(),
        package_name: s0(batch, "package_name"),
        description: s0(batch, "description"),
        original_file_path: s0(batch, "original_file_path"),
    };
    let fields = DataTestNodeFields {
        tags: extract_str_list(batch, "tags"),
        fqn: extract_str_list(batch, "fqn"),
        file_path: s0(batch, "file_path"),
        patch_path: s0(batch, "patch_path"),
        meta: json_parse_or_null(s0(batch, "meta").as_deref()),
        raw_code: s0(batch, "raw_code"),
        compiled_code: s0(batch, "compiled_code"),
    };
    (base, fields)
}

struct DataTestNodeFields {
    tags: Vec<String>,
    fqn: Vec<String>,
    file_path: Option<String>,
    patch_path: Option<String>,
    meta: serde_json::Value,
    raw_code: Option<String>,
    compiled_code: Option<String>,
}

/// Returns `(metadata, column_name, severity)`. Presence of the row also
/// indicates the test is generic (vs singular) — propagated by the caller.
fn extract_test_metadata(
    batches: &[RecordBatch],
) -> Option<(TestMetadata, Option<String>, Option<String>)> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;
    let test_name = s0(batch, "test_name").unwrap_or_default();
    let kwargs = json_parse_or_null(s0(batch, "kwargs").as_deref());
    let column_name = s0(batch, "column_name");
    let severity = s0(batch, "severity");
    Some((
        TestMetadata {
            name: test_name,
            kwargs,
        },
        column_name,
        severity,
    ))
}

fn extract_unit_test(batch: &RecordBatch) -> UnitTestExtracted {
    let base = NodeBase {
        unique_id: s0(batch, "unique_id").unwrap_or_default(),
        name: s0(batch, "name").unwrap_or_default(),
        // Unit tests don't carry a `resource_type` column in
        // `dbt.unit_tests` — synthesised from the table identity.
        resource_type: "unit_test".to_owned(),
        package_name: s0(batch, "package_name"),
        description: s0(batch, "description"),
        original_file_path: s0(batch, "original_file_path"),
    };
    let given_raw = s0(batch, "given");
    let expect_raw = s0(batch, "expect");
    let (given, num_given, num_given_rows) = parse_given(given_raw.as_deref());
    let (expect, num_expect_rows) = parse_expect(expect_raw.as_deref());
    UnitTestExtracted {
        base,
        fqn: extract_str_list(batch, "fqn"),
        file_path: s0(batch, "file_path"),
        model: s0(batch, "model"),
        depends_on_nodes: extract_str_list(batch, "depends_on_nodes"),
        given,
        expect,
        num_given,
        num_given_rows,
        num_expect_rows,
    }
}

struct UnitTestExtracted {
    base: NodeBase,
    fqn: Vec<String>,
    file_path: Option<String>,
    model: Option<String>,
    depends_on_nodes: Vec<String>,
    given: Vec<UnitTestFixture>,
    expect: Option<UnitTestExpect>,
    num_given: Option<i64>,
    num_given_rows: Option<i64>,
    num_expect_rows: Option<i64>,
}

/// Parse the `given` JSON-string column from `dbt.unit_tests`. The on-disk
/// shape is `[{"input": "...", "rows": [...], "format": ..., "fixture": ...}]`;
/// only `input` and `rows` are surfaced. Returns `(fixtures, num_given,
/// num_given_rows)`; counts are derived from the parsed array.
fn parse_given(raw: Option<&str>) -> (Vec<UnitTestFixture>, Option<i64>, Option<i64>) {
    let parsed = json_parse_or_null(raw);
    let Some(arr) = parsed.as_array() else {
        return (vec![], None, None);
    };
    let num_given = arr.len() as i64;
    let mut total_rows: i64 = 0;
    let mut fixtures = Vec::with_capacity(arr.len());
    for item in arr {
        let input = item
            .get("input")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned();
        let rows: Vec<serde_json::Value> = item
            .get("rows")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        total_rows += rows.len() as i64;
        fixtures.push(UnitTestFixture { input, rows });
    }
    (fixtures, Some(num_given), Some(total_rows))
}

/// Parse the `expect` JSON-string column. On-disk shape is `{"rows": [...],
/// "format": ..., "fixture": ...}`. Returns the expect block (with `rows`
/// only) and the row count.
fn parse_expect(raw: Option<&str>) -> (Option<UnitTestExpect>, Option<i64>) {
    let parsed = json_parse_or_null(raw);
    if parsed.is_null() {
        return (None, None);
    }
    let rows: Vec<serde_json::Value> = parsed
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let n = rows.len() as i64;
    (Some(UnitTestExpect { rows }), Some(n))
}

fn extract_test_execution_info(batches: &[RecordBatch]) -> Option<TestExecutionInfo> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;
    let status = s0(batch, "status");
    let message = s0(batch, "message");
    let completed_at = s0(batch, "completed_at");
    let execution_time = batch
        .column_by_name("execution_time")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .and_then(|c| if c.is_null(0) { None } else { Some(c.value(0)) });
    // Surface message only when the run actually failed; success rows often
    // carry an empty/diagnostic string that isn't an error.
    let error = match status.as_deref() {
        Some("error") | Some("fail") => message,
        _ => None,
    };
    Some(TestExecutionInfo {
        status,
        error,
        completed_at,
        execution_time,
    })
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `GET /api/v1/tests/:id` — full test detail. Returns a discriminated union
/// keyed on `resource_type`:
/// - `"test"` → [`DataTestDetail`]
/// - `"unit_test"` → [`UnitTestDetail`]
///
/// 404 when no row is found in either `dbt.nodes` (filtered to
/// `resource_type = 'test'`) or `dbt.unit_tests`.
pub async fn get_test(State(state): State<SharedState>, Path(unique_id): Path<String>) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }
    let id = escape_str(&unique_id);

    let data_node_sql = DATA_TEST_NODE_SQL.replace("{id}", &id);
    let test_metadata_sql = TEST_METADATA_SQL.replace("{id}", &id);
    let unit_test_sql = UNIT_TEST_SQL.replace("{id}", &id);
    let upstream_sql = format!(
        "SELECT parent_unique_id AS unique_id, edge_type \
         FROM dbt.edges WHERE child_unique_id = '{id}' \
         ORDER BY parent_unique_id"
    );
    let run_result_sql = TEST_RUN_RESULT_SQL.replace("{id}", &id);

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let data_node_batches = backend
            .query_arrow(&data_node_sql)
            .map_err(|e| e.to_string())?;
        let has_data_test = data_node_batches.iter().any(|b| b.num_rows() > 0);
        // Branch query selection: fetch the metadata + edges for data tests,
        // or the unit_tests row otherwise. Skipping irrelevant queries keeps
        // 404 paths cheap and avoids errors against absent views.
        let test_metadata_batches = if has_data_test {
            backend.query_arrow(&test_metadata_sql).ok()
        } else {
            None
        };
        let unit_test_batches = if has_data_test {
            None
        } else {
            backend.query_arrow(&unit_test_sql).ok()
        };
        let upstream_batches = backend.query_arrow(&upstream_sql).ok();
        let run_result_batches = backend.query_arrow(&run_result_sql).ok();
        Ok((
            data_node_batches,
            test_metadata_batches,
            unit_test_batches,
            upstream_batches,
            run_result_batches,
        ))
    })
    .await;

    let (
        data_node_batches,
        test_metadata_batches,
        unit_test_batches,
        upstream_batches,
        run_result_batches,
    ) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let depends_on = upstream_batches
        .as_deref()
        .map(extract_edge_refs)
        .unwrap_or_default();
    let execution_info = run_result_batches
        .as_deref()
        .and_then(extract_test_execution_info);

    // Data test path first — `dbt.nodes` is authoritative when present.
    if let Some(batch) = data_node_batches.iter().find(|b| b.num_rows() > 0) {
        let (base, fields) = extract_data_test_node(batch);
        let metadata_triple = test_metadata_batches
            .as_deref()
            .and_then(extract_test_metadata);
        let (test_metadata, column_name, severity) = match metadata_triple {
            Some((m, c, s)) => (Some(m), c, s),
            None => (None, None, None),
        };
        let test_type = Some(if test_metadata.is_some() {
            "generic".to_owned()
        } else {
            "singular".to_owned()
        });
        let detail = DataTestDetail {
            common: TestCommon {
                base,
                tags: fields.tags,
                fqn: fields.fqn,
                file_path: fields.file_path,
                patch_path: fields.patch_path,
                meta: fields.meta,
                depends_on,
                execution_info,
            },
            column_name,
            test_type,
            severity,
            test_metadata,
            raw_code: fields.raw_code,
            compiled_code: fields.compiled_code,
        };
        return Json(TestDetail::Data(Box::new(detail))).into_response();
    }

    // Unit test fallback — `dbt.unit_tests` is the only source.
    if let Some(batch) = unit_test_batches
        .as_deref()
        .and_then(|b| b.iter().find(|b| b.num_rows() > 0))
    {
        let extracted = extract_unit_test(batch);
        // Unit tests aren't in `dbt.edges`; synthesise EdgeRefs from
        // `depends_on_nodes` with a generic `"ref"` edge_type when the
        // edges query produced nothing.
        let depends_on = if depends_on.is_empty() {
            extracted
                .depends_on_nodes
                .into_iter()
                .map(|uid| EdgeRef {
                    unique_id: uid,
                    edge_type: "ref".to_owned(),
                })
                .collect()
        } else {
            depends_on
        };
        let detail = UnitTestDetail {
            common: TestCommon {
                base: extracted.base,
                // `dbt.unit_tests` has no `tags`, `patch_path`, or `meta`
                // columns — these surface as empty / null.
                tags: vec![],
                fqn: extracted.fqn,
                file_path: extracted.file_path,
                patch_path: None,
                meta: serde_json::Value::Null,
                depends_on,
                execution_info,
            },
            model: extracted.model,
            given: extracted.given,
            expect: extracted.expect,
            num_given: extracted.num_given,
            num_given_rows: extracted.num_given_rows,
            num_expect_rows: extracted.num_expect_rows,
        };
        return Json(TestDetail::Unit(Box::new(detail))).into_response();
    }

    not_found(format!("test {unique_id} not found"))
}

#[cfg(test)]
#[path = "tests_tests.rs"]
#[allow(clippy::module_inception)]
mod tests;
