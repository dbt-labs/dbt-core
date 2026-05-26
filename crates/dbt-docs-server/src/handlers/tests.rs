//! `GET /api/v1/tests/:id`, `GET /api/v1/tests`, and
//! `GET /api/v1/tests/facets`.
//!
//! Both `test.*` and `unit_test.*` `unique_id`s land on these endpoints. The
//! detail handler routes by `resource_type`: data tests are stored in
//! `dbt.nodes` with `resource_type = 'test'` and joined against
//! `dbt.test_metadata` for generic-test fields (`column_name`, `severity`,
//! `test_metadata.name`, `kwargs`); unit tests live in `dbt.unit_tests` (not
//! in `dbt.nodes`) and carry inline fixture blobs (`given`, `expect`)
//! serialized as JSON strings.
//!
//! The list handler issues a single UNION ALL query joining `dbt.nodes`
//! (filtered to `resource_type = 'test'`) and `dbt.unit_tests`, then
//! LEFT JOINs `dbt.test_metadata` and `dbt_rt.run_results`.
//!
//! `execution_info` is `null` when `dbt_rt.run_results` has no row for the
//! resource. Tests are leaf nodes — no `referenced_by`.
//!
//! JSON-string parquet columns (`meta`, `test_metadata.kwargs`, `given`,
//! `expect`) are deserialised via
//! [`crate::handlers::json::json_parse_or_null`] so the response carries
//! nested objects, not escaped strings.

use std::fmt::Write as _;

use arrow_array::{Array, Float64Array, RecordBatch, StringArray};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::handlers::json::{bad_request, internal_error, json_parse_or_null, not_found};
use crate::handlers::node_base::{EdgeRef, NodeBase, extract_edge_refs, extract_str_list, opt_str};
use crate::handlers::pagination::{Cursor, PageInfo, SortDir, clamp_first, cursor_where_fragment};
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

// ---------------------------------------------------------------------------
// Response types: GET /api/v1/tests and GET /api/v1/tests/facets
// ---------------------------------------------------------------------------

/// Execution snapshot for the list row — mirrors the detail `TestExecutionInfo`
/// but omits `execution_time` (not rendered in the list view).
#[derive(Serialize)]
pub struct TestListExecutionInfo {
    pub status: Option<String>,
    pub error: Option<String>,
    pub completed_at: Option<String>,
}

/// Shared metadata summary for data-test list rows.
#[derive(Serialize)]
pub struct TestMetadataSummary {
    pub namespace: Option<String>,
    /// Parsed JSON object; `null` if `kwargs` column absent / unparseable.
    pub kwargs: serde_json::Value,
}

/// List row for a `resource_type = 'test'` data test.
#[derive(Serialize)]
pub struct DataTestSummary {
    pub unique_id: String,
    pub name: String,
    pub resource_type: String,
    pub package_name: Option<String>,
    /// `dbt.test_metadata.test_name` — e.g. `"not_null"`, `"unique"`.
    pub test_type: Option<String>,
    /// `dbt.test_metadata.attached_node`.
    pub tested_node_unique_id: Option<String>,
    /// `dbt.test_metadata.column_name`.
    pub tested_column: Option<String>,
    /// `dbt.test_metadata.severity` — `"ERROR"` or `"WARN"`, often `null`.
    pub severity: Option<String>,
    /// `null` when `dbt_rt.run_results` has no row or the view is absent.
    pub execution_info: Option<TestListExecutionInfo>,
    /// `null` when no `dbt.test_metadata` row (singular test).
    pub test_metadata: Option<TestMetadataSummary>,
}

/// List row for a `resource_type = 'unit_test'` unit test.
#[derive(Serialize)]
pub struct UnitTestSummary {
    pub unique_id: String,
    pub name: String,
    pub resource_type: String,
    pub package_name: Option<String>,
    /// Always `"unit"` for unit tests.
    pub test_type: Option<String>,
    /// `depends_on_nodes[0]` — the primary model under test.
    pub tested_node_unique_id: Option<String>,
    /// Always `null` for unit tests.
    pub tested_column: Option<String>,
    /// Always `null` for unit tests.
    pub severity: Option<String>,
    /// `null` when `dbt_rt.run_results` has no row or the view is absent.
    pub execution_info: Option<TestListExecutionInfo>,
    /// Count of `given` fixtures. Derived from `dbt.unit_tests.given` JSON.
    pub num_given: Option<i64>,
    /// Total input rows across all fixtures.
    pub num_given_rows: Option<i64>,
    /// Expected output row count from `dbt.unit_tests.expect` JSON.
    pub num_expect_rows: Option<i64>,
}

/// ADR-3 discriminated union on `resource_type`.
#[derive(Serialize)]
#[serde(untagged)]
pub enum TestSummary {
    Data(DataTestSummary),
    Unit(UnitTestSummary),
}

/// ADR-6 cursor-paginated response for `GET /api/v1/tests`.
#[derive(Serialize)]
pub struct TestListResponse {
    pub data: Vec<TestSummary>,
    pub page_info: PageInfo,
}

/// One facet option for the tests endpoints; `count` is always `null` today.
#[derive(Serialize)]
pub struct TestFacetValue {
    pub value: String,
    pub count: Option<u64>,
}

impl TestFacetValue {
    fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            count: None,
        }
    }
}

/// Response body for `GET /api/v1/tests/facets`.
///
/// All three facet arrays are server constants — no parquet query required.
/// The value sets are closed enums defined by dbt-dag `resourceConstants.ts`.
#[derive(Serialize)]
pub struct TestFacetsResponse {
    pub results: Vec<TestFacetValue>,
    pub run_statuses: Vec<TestFacetValue>,
    pub test_types: Vec<TestFacetValue>,
}

/// Query parameters for `GET /api/v1/tests`.
#[derive(Debug, Default, Deserialize)]
pub struct TestListParams {
    /// Sort spec `<column>:<asc|desc>`. Allowlisted: `name` only.
    pub sort: Option<String>,
    pub first: Option<u32>,
    pub after: Option<String>,
    /// CSV-OR filter on `dbt_rt.run_results.status`; test-outcome values.
    pub result: Option<String>,
    /// CSV-OR filter on `dbt_rt.run_results.status`; run-engine values.
    pub run_status: Option<String>,
    /// `unit` → `resource_type = 'unit_test'`; `data` → `resource_type = 'test'`.
    pub test_type: Option<String>,
}

// ---------------------------------------------------------------------------
// SQL: GET /api/v1/tests
// ---------------------------------------------------------------------------

/// Allowlisted sort columns for tests.
const SORTABLE_COLUMNS: &[(&str, &str)] = &[("name", "t.name")];

struct SortSelection {
    sql_expr: &'static str,
    dir: SortDir,
}

fn parse_sort(s: &str) -> Result<SortSelection, &'static str> {
    let (col, dir_str) = match s.split_once(':') {
        Some(pair) => pair,
        None => return Err("sort must be in format <column>:<asc|desc>"),
    };
    let dir = match dir_str {
        "asc" => SortDir::Asc,
        "desc" => SortDir::Desc,
        _ => return Err("sort direction must be 'asc' or 'desc'"),
    };
    for &(name, expr) in SORTABLE_COLUMNS {
        if name == col {
            return Ok(SortSelection {
                sql_expr: expr,
                dir,
            });
        }
    }
    Err("unknown sort column")
}

/// Build `(count_sql, rows_sql)` for `GET /api/v1/tests`.
///
/// `with_run_results` controls whether `dbt_rt.run_results` is LEFT JOINed.
/// When `false`, execution-info columns are NULL and result/run_status filters
/// produce zero matches (degenerate filter).
///
/// The base query is a UNION ALL of the `test` rows from `dbt.nodes` (LEFT
/// JOINed with `dbt.test_metadata`) and the `unit_test` rows from
/// `dbt.unit_tests`. The union is wrapped in a CTE `t` so filters and cursors
/// apply uniformly.
pub(crate) fn build_test_list_sql(
    params: &TestListParams,
    with_run_results: bool,
    first: u32,
    cursor: Option<&Cursor>,
) -> Result<(String, String), &'static str> {
    let sort = match params.sort.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => parse_sort(s)?,
        None => SortSelection {
            sql_expr: "t.name",
            dir: SortDir::Asc,
        },
    };

    // Build the resource_type filter from ?test_type= param.
    let mut resource_type_filter = String::new();
    if let Some(raw) = params.test_type.as_deref().filter(|s| !s.is_empty()) {
        let types: Vec<&str> = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        let mut resource_types: Vec<&str> = vec![];
        for t in &types {
            match *t {
                "unit" => resource_types.push("'unit_test'"),
                "data" => resource_types.push("'test'"),
                _ => return Err("invalid test_type value; use 'unit' or 'data'"),
            }
        }
        if !resource_types.is_empty() {
            let list = resource_types.join(", ");
            resource_type_filter = format!(" AND t.resource_type IN ({list})");
        }
    }

    // Build status filter (both ?result and ?run_status map to the same column).
    let mut status_filter = String::new();
    if with_run_results {
        let mut status_values: Vec<String> = vec![];
        if let Some(raw) = params.result.as_deref().filter(|s| !s.is_empty()) {
            for v in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                status_values.push(format!("'{}'", escape_str(v)));
            }
        }
        if let Some(raw) = params.run_status.as_deref().filter(|s| !s.is_empty()) {
            for v in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                status_values.push(format!("'{}'", escape_str(v)));
            }
        }
        if !status_values.is_empty() {
            let list = status_values.join(", ");
            status_filter = format!(" AND rr.status IN ({list})");
        }
    } else if params.result.as_deref().filter(|s| !s.is_empty()).is_some()
        || params
            .run_status
            .as_deref()
            .filter(|s| !s.is_empty())
            .is_some()
    {
        // run_results view absent but filter requested — return nothing.
        status_filter = " AND 1=0".to_owned();
    }

    let filter_where = format!("WHERE 1=1{resource_type_filter}{status_filter}");

    let mut page_where = filter_where.clone();
    if let Some(c) = cursor {
        let frag = cursor_where_fragment(
            sort.sql_expr,
            "t.unique_id",
            sort.dir,
            c.sort_value.as_deref(),
            &c.unique_id,
        );
        let _ = write!(page_where, " AND {frag}");
    }

    // The UNION ALL CTE joins data tests (dbt.nodes + dbt.test_metadata) and
    // unit tests (dbt.unit_tests). Columns not present in one side are NULL.
    let union_cte = "\
WITH tests_union AS (\
  SELECT n.unique_id, n.name, n.package_name, 'test' AS resource_type, \
         tm.test_name AS test_type, tm.attached_node AS tested_node_unique_id, \
         tm.column_name AS tested_column, tm.severity, \
         tm.test_namespace AS test_namespace, tm.kwargs AS kwargs_raw, \
         NULL::VARCHAR AS given_raw, NULL::VARCHAR AS expect_raw, \
         NULL::VARCHAR[] AS depends_on_nodes \
  FROM dbt.nodes n \
  LEFT JOIN dbt.test_metadata tm ON tm.unique_id = n.unique_id \
  WHERE n.resource_type = 'test' \
  UNION ALL \
  SELECT ut.unique_id, ut.name, ut.package_name, 'unit_test' AS resource_type, \
         'unit' AS test_type, ut.depends_on_nodes[1] AS tested_node_unique_id, \
         NULL::VARCHAR AS tested_column, NULL::VARCHAR AS severity, \
         NULL::VARCHAR AS test_namespace, NULL::VARCHAR AS kwargs_raw, \
         ut.given AS given_raw, ut.expect AS expect_raw, \
         ut.depends_on_nodes \
  FROM dbt.unit_tests ut\
)";

    let (rr_cte, rr_join, rr_cols) = if with_run_results {
        (
            ", last_run AS (\
               SELECT unique_id, status, message, \
                      CAST(MAX(created_at) AS VARCHAR) AS completed_at \
               FROM dbt_rt.run_results \
               GROUP BY unique_id, status, message\
             )",
            "LEFT JOIN last_run rr ON rr.unique_id = t.unique_id",
            "rr.status AS rr_status, rr.message AS rr_message, rr.completed_at AS rr_completed_at",
        )
    } else {
        (
            "",
            "",
            "NULL::VARCHAR AS rr_status, NULL::VARCHAR AS rr_message, NULL::VARCHAR AS rr_completed_at",
        )
    };

    let full_cte = format!("{union_cte}{rr_cte}");

    let count_sql = format!(
        "{full_cte} \
         SELECT count(*) FROM tests_union t {rr_join} {filter_where}"
    );

    let peek = first + 1;
    let order_expr = sort.sql_expr;
    let order_dir = sort.dir.as_sql();

    let rows_sql = format!(
        "{full_cte} \
         SELECT t.unique_id, t.name, t.resource_type, t.package_name, \
                t.test_type, t.tested_node_unique_id, t.tested_column, \
                t.severity, t.test_namespace, t.kwargs_raw, \
                t.given_raw, t.expect_raw, \
                {rr_cols} \
         FROM tests_union t {rr_join} \
         {page_where} \
         ORDER BY {order_expr} {order_dir} NULLS LAST, t.unique_id ASC \
         LIMIT {peek}"
    );

    Ok((count_sql, rows_sql))
}

// ---------------------------------------------------------------------------
// Extraction helpers: GET /api/v1/tests
// ---------------------------------------------------------------------------

fn parse_test_list_execution_info(
    status: Option<&str>,
    message: Option<&str>,
    completed_at: Option<&str>,
) -> Option<TestListExecutionInfo> {
    let status_val = status?;
    let error = match status_val {
        "error" | "fail" => message.map(|m| m.to_owned()),
        _ => None,
    };
    Some(TestListExecutionInfo {
        status: Some(status_val.to_owned()),
        error,
        completed_at: completed_at.map(|s| s.to_owned()),
    })
}

fn batches_to_test_summary_rows(batches: &[RecordBatch]) -> Vec<TestSummary> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let unique_id_col = batch
            .column_by_name("unique_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("unique_id column missing");
        let name_col = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("name column missing");
        let resource_type_col = batch
            .column_by_name("resource_type")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("resource_type column missing");
        let s = |col_name: &str| -> &StringArray {
            batch
                .column_by_name(col_name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .expect("column missing")
        };
        let package_name = s("package_name");
        let test_type = s("test_type");
        let tested_node = s("tested_node_unique_id");
        let tested_column = s("tested_column");
        let severity = s("severity");
        let test_namespace = s("test_namespace");
        let kwargs_raw = s("kwargs_raw");
        let given_raw = s("given_raw");
        let expect_raw = s("expect_raw");
        let rr_status = s("rr_status");
        let rr_message = s("rr_message");
        let rr_completed_at = s("rr_completed_at");

        for i in 0..batch.num_rows() {
            let get = |col: &StringArray| -> Option<String> {
                if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i).to_owned())
                }
            };

            let resource_type = resource_type_col.value(i);
            let execution_info = parse_test_list_execution_info(
                get(rr_status).as_deref(),
                get(rr_message).as_deref(),
                get(rr_completed_at).as_deref(),
            );

            match resource_type {
                "test" => {
                    // Build test_metadata only if test_namespace or kwargs present.
                    let namespace = get(test_namespace);
                    let kwargs = json_parse_or_null(get(kwargs_raw).as_deref());
                    let test_metadata = if namespace.is_some() || !kwargs.is_null() {
                        Some(TestMetadataSummary { namespace, kwargs })
                    } else {
                        None
                    };
                    rows.push(TestSummary::Data(DataTestSummary {
                        unique_id: unique_id_col.value(i).to_owned(),
                        name: name_col.value(i).to_owned(),
                        resource_type: "test".to_owned(),
                        package_name: get(package_name),
                        test_type: get(test_type),
                        tested_node_unique_id: get(tested_node),
                        tested_column: get(tested_column),
                        severity: get(severity),
                        execution_info,
                        test_metadata,
                    }));
                }
                "unit_test" => {
                    let given_str = get(given_raw);
                    let expect_str = get(expect_raw);
                    let (num_given, num_given_rows) = count_given(given_str.as_deref());
                    let num_expect_rows = count_expect(expect_str.as_deref());
                    let tested_node_uid =
                        // The UNION ALL already computed depends_on_nodes[1] as
                        // tested_node_unique_id in the SQL projection.
                        get(tested_node);
                    rows.push(TestSummary::Unit(UnitTestSummary {
                        unique_id: unique_id_col.value(i).to_owned(),
                        name: name_col.value(i).to_owned(),
                        resource_type: "unit_test".to_owned(),
                        package_name: get(package_name),
                        test_type: Some("unit".to_owned()),
                        tested_node_unique_id: tested_node_uid,
                        tested_column: None,
                        severity: None,
                        execution_info,
                        num_given,
                        num_given_rows,
                        num_expect_rows,
                    }));
                }
                _ => {}
            }
        }
    }
    rows
}

/// Count `given` fixtures from the JSON string: returns `(num_given,
/// num_given_rows)`. Falls back to `(None, None)` on parse failure.
fn count_given(raw: Option<&str>) -> (Option<i64>, Option<i64>) {
    let parsed = json_parse_or_null(raw);
    let Some(arr) = parsed.as_array() else {
        return (None, None);
    };
    let num_given = arr.len() as i64;
    let mut total_rows: i64 = 0;
    for item in arr {
        if let Some(rows) = item.get("rows").and_then(|v| v.as_array()) {
            total_rows += rows.len() as i64;
        }
    }
    (Some(num_given), Some(total_rows))
}

/// Count `expect` rows from the JSON string. Falls back to `None` on parse
/// failure.
fn count_expect(raw: Option<&str>) -> Option<i64> {
    let parsed = json_parse_or_null(raw);
    if parsed.is_null() {
        return None;
    }
    parsed
        .get("rows")
        .and_then(|v| v.as_array())
        .map(|r| r.len() as i64)
}

// ---------------------------------------------------------------------------
// Handlers: GET /api/v1/tests and GET /api/v1/tests/facets
// ---------------------------------------------------------------------------

/// `GET /api/v1/tests` — cursor-paginated list of test nodes (both
/// `resource_type = 'test'` and `resource_type = 'unit_test'`).
///
/// Default sort is `name:asc`; only `name` is allowlisted.
/// Filters: `result`, `run_status` (both map to run_results.status),
/// `test_type` (`unit` or `data`).
/// `execution_info` is `null` when `dbt_rt.run_results` is absent or has no
/// row for this test.
pub async fn list_tests(
    State(state): State<SharedState>,
    Query(params): Query<TestListParams>,
) -> Response {
    let first = clamp_first(params.first);

    let cursor = match params.after.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => match Cursor::decode(s) {
            Ok(c) => Some(c),
            Err(msg) => return bad_request(msg),
        },
        None => None,
    };

    // Build with run_results first; validates sort and other params.
    let (count_sql, rows_sql) = match build_test_list_sql(&params, true, first, cursor.as_ref()) {
        Ok(pair) => pair,
        Err(msg) => return bad_request(msg),
    };
    // Fallback: run_results absent.
    let (count_sql_no_rr, rows_sql_no_rr) =
        build_test_list_sql(&params, false, first, cursor.as_ref())
            .expect("params already validated");

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let (total, batches) = match backend.query_scalar(&count_sql) {
            Some(count_str) => {
                let total = count_str
                    .parse::<u64>()
                    .map_err(|e| format!("could not parse test count: {e}"))?;
                let batches = backend.query_arrow(&rows_sql).map_err(|e| e.to_string())?;
                (total, batches)
            }
            None => {
                // run_results view absent; retry without it.
                let total = backend
                    .query_scalar(&count_sql_no_rr)
                    .ok_or_else(|| "count query returned no rows".to_string())?
                    .parse::<u64>()
                    .map_err(|e| format!("could not parse test count: {e}"))?;
                let batches = backend
                    .query_arrow(&rows_sql_no_rr)
                    .map_err(|e| e.to_string())?;
                (total, batches)
            }
        };
        Ok((total, batches))
    })
    .await;

    let (total_count, batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let mut rows = batches_to_test_summary_rows(&batches);

    let has_next_page = rows.len() as u32 > first;
    if has_next_page {
        rows.truncate(first as usize);
    }

    fn make_cursor(row: &TestSummary) -> String {
        let (name, uid) = match row {
            TestSummary::Data(d) => (d.name.as_str(), d.unique_id.as_str()),
            TestSummary::Unit(u) => (u.name.as_str(), u.unique_id.as_str()),
        };
        Cursor {
            sort_value: Some(name.to_owned()),
            unique_id: uid.to_owned(),
        }
        .encode()
    }

    let start_cursor = rows.first().map(make_cursor);
    let end_cursor = if has_next_page {
        rows.last().map(make_cursor)
    } else {
        None
    };

    Json(TestListResponse {
        data: rows,
        page_info: PageInfo {
            total_count,
            start_cursor,
            end_cursor,
            has_next_page,
        },
    })
    .into_response()
}

/// `GET /api/v1/tests/facets` — filter facet values for the tests list.
///
/// All three arrays are server constants — no parquet query required. The
/// value sets match dbt-dag `resourceConstants.ts`:
/// - `results`: `testStatusesPlusUnknown`
/// - `run_statuses`: `runStatuses`
/// - `test_types`: `testTypesDisplay`
pub async fn list_test_facets() -> Response {
    Json(TestFacetsResponse {
        results: ["pass", "fail", "warn", "error", "skipped", "unknown"]
            .iter()
            .map(|&v| TestFacetValue::new(v))
            .collect(),
        run_statuses: ["success", "error", "skipped", "reused"]
            .iter()
            .map(|&v| TestFacetValue::new(v))
            .collect(),
        test_types: ["unit", "data"]
            .iter()
            .map(|&v| TestFacetValue::new(v))
            .collect(),
    })
    .into_response()
}

#[cfg(test)]
#[path = "tests_tests.rs"]
#[allow(clippy::module_inception)]
mod tests;
