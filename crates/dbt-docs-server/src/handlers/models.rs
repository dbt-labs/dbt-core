use std::fmt::Write as _;

use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
};
use axum::Json;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use axum::extract::Path;

use crate::handlers::json::{bad_request, internal_error, not_found};
use crate::handlers::sql::escape_str;
use crate::state::SharedState;

const DEFAULT_LIMIT: u32 = 1000;
const HARD_MAX_LIMIT: u32 = 5000;

/// Per-layer SQL conditions: `(layer_name, OR'd LIKE clause)`.
///
/// Single source of truth for modeling-layer classification. Both the SELECT
/// CASE expression ([`modeling_layer_case_sql`]) and the WHERE filter
/// ([`modeling_layer_where`]) are generated from this table, so they can
/// never drift.
const LAYER_CONDITIONS: &[(&str, &str)] = &[
    (
        "Staging",
        "lower(n.original_file_path) LIKE '%/staging/%' \
         OR lower(n.original_file_path) LIKE '%/stg_%' \
         OR lower(n.original_file_path) LIKE 'staging/%'",
    ),
    (
        "Intermediate",
        "lower(n.original_file_path) LIKE '%/intermediate/%' \
         OR lower(n.original_file_path) LIKE '%/int_%' \
         OR lower(n.original_file_path) LIKE 'intermediate/%'",
    ),
    (
        "Marts",
        "lower(n.original_file_path) LIKE '%/marts/%' \
         OR lower(n.original_file_path) LIKE '%/dim_%' \
         OR lower(n.original_file_path) LIKE '%/fct_%' \
         OR lower(n.original_file_path) LIKE 'marts/%'",
    ),
];

/// Build the SQL CASE expression that projects `modeling_layer` in SELECT.
fn modeling_layer_case_sql() -> String {
    let mut sql = String::from("CASE");
    for (layer, cond) in LAYER_CONDITIONS {
        let _ = write!(sql, " WHEN {cond} THEN '{layer}'");
    }
    sql.push_str(" ELSE NULL END");
    sql
}

/// SQL for the run-results CTE (executed_at per model).
const RUN_RESULTS_CTE: &str = "\
WITH last_run AS (\
  SELECT unique_id, MAX(created_at) AS executed_at \
  FROM dbt_rt.run_results \
  GROUP BY unique_id\
)\n";

const OWNERS_FACET_SQL: &str = "\
SELECT DISTINCT name AS owner \
FROM dbt.groups \
ORDER BY owner";

/// Allowlisted sort columns: `(query param value, SQL expression)`.
const SORTABLE_COLUMNS: &[(&str, &str)] = &[
    ("name", "n.name"),
    ("modeling_layer", "modeling_layer"),
    ("access_level", "n.access_level"),
    ("contract_enforced", "n.contract_enforced"),
    ("owner", "owner"),
    ("executed_at", "executed_at"),
];

const VALID_ACCESS_LEVELS: &[&str] = &["private", "protected", "public"];

/// A single row in the `/api/v1/models` response.
///
/// All fields are always present in the JSON output. Optional fields serialize
/// as `null` (not absent) because serde serializes `Option::None` as `null`
/// by default — no post-processing needed to enforce the contract.
#[derive(Serialize)]
pub struct ModelSummary {
    pub unique_id: String,
    pub name: String,
    pub package_name: Option<String>,
    pub original_file_path: Option<String>,
    /// Server-computed modeling layer (`Staging`, `Intermediate`, `Marts`),
    /// or `null` when the path matches no convention.
    pub modeling_layer: Option<String>,
    pub access_level: Option<String>,
    pub contract_enforced: bool,
    /// Owner from the model's dbt group; `null` when ungrouped.
    pub owner: Option<String>,
    /// ISO-8601 timestamp of the last dbt run; `null` when never run.
    pub executed_at: Option<String>,
}

/// Response body for `GET /api/v1/models`.
#[derive(Serialize)]
pub struct ModelListResponse {
    pub models: Vec<ModelSummary>,
    pub total: u64,
    pub offset: u32,
    pub limit: u32,
}

/// A single facet option with an optional model count.
///
/// `count` is `null` today — reserved for a future enhancement that will
/// return the number of models matching each filter value without a full
/// query.
#[derive(Serialize)]
pub struct FacetValue {
    pub value: String,
    pub count: Option<u64>,
}

impl FacetValue {
    fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            count: None,
        }
    }
}

/// Response body for `GET /api/v1/models/facets`.
#[derive(Serialize)]
pub struct ModelFacetsResponse {
    /// Modeling layer options in convention order (Staging → Intermediate → Marts).
    pub modeling_layers: Vec<FacetValue>,
    /// Access level options in alphabetical order.
    pub accesses: Vec<FacetValue>,
    /// Owner (group) options; project-specific, sourced from `dbt.groups`.
    pub owners: Vec<FacetValue>,
}

/// Extract a `StringArray` column from a batch by name.
/// Panics on schema mismatch — indicates a bug in the SQL/struct alignment.
fn str_col<'a>(batch: &'a RecordBatch, name: &'static str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' missing from batch"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("column '{name}' is not a StringArray"))
}

/// Extract a `BooleanArray` column from a batch by name.
fn bool_col<'a>(batch: &'a RecordBatch, name: &'static str) -> &'a BooleanArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' missing from batch"))
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap_or_else(|| panic!("column '{name}' is not a BooleanArray"))
}

/// Extract the `owner` string column from the facets query result.
fn batches_to_owner_names(batches: &[RecordBatch]) -> Vec<String> {
    let mut owners = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = str_col(batch, "owner");
        for i in 0..batch.num_rows() {
            if !col.is_null(i) {
                owners.push(col.value(i).to_owned());
            }
        }
    }
    owners
}

/// Convert Arrow record batches from the models SQL query into typed rows.
///
/// Null handling is structural: `Option::None` for nullable columns, which
/// serde serializes as JSON `null` without any post-processing.
fn batches_to_model_rows(batches: &[RecordBatch]) -> Vec<ModelSummary> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let unique_id = str_col(batch, "unique_id");
        let name = str_col(batch, "name");
        let package_name = str_col(batch, "package_name");
        let original_file_path = str_col(batch, "original_file_path");
        let modeling_layer = str_col(batch, "modeling_layer");
        let access_level = str_col(batch, "access_level");
        let contract_enforced = bool_col(batch, "contract_enforced");
        let owner = str_col(batch, "owner");
        let executed_at = str_col(batch, "executed_at");

        let opt = |col: &StringArray, i: usize| -> Option<String> {
            if col.is_null(i) {
                None
            } else {
                Some(col.value(i).to_owned())
            }
        };

        for i in 0..batch.num_rows() {
            rows.push(ModelSummary {
                unique_id: unique_id.value(i).to_owned(),
                name: name.value(i).to_owned(),
                package_name: opt(package_name, i),
                original_file_path: opt(original_file_path, i),
                modeling_layer: opt(modeling_layer, i),
                access_level: opt(access_level, i),
                contract_enforced: contract_enforced.value(i),
                owner: opt(owner, i),
                executed_at: opt(executed_at, i),
            });
        }
    }
    rows
}

/// Query parameters for `GET /api/v1/models`.
#[derive(Debug, Default, Deserialize)]
pub struct ModelListParams {
    /// Comma-separated modeling layer filter: `Staging`, `Intermediate`, `Marts`.
    /// Multiple values are OR'd: `?modeling_layer=Staging,Marts`
    pub modeling_layer: Option<String>,
    /// Comma-separated access level filter: `public`, `protected`, `private`.
    /// Multiple values are OR'd: `?access=public,protected`
    pub access: Option<String>,
    /// Owner name (exact match): `?owner=Data+Team+%28EPD%29`
    pub owner: Option<String>,
    /// Sort spec `<column>:<asc|desc>`: `?sort=executed_at:desc`.
    /// Column must be one of: name, modeling_layer, access_level,
    /// contract_enforced, owner, executed_at. Default: `name:asc`.
    pub sort: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

/// Parse `"field:dir"` into a validated `(sql_expr, direction)` pair.
/// Returns `Err(&'static str)` so callers can keep the Err variant small.
fn parse_sort(s: &str) -> Result<(String, &'static str), &'static str> {
    let (col, dir) = match s.split_once(':') {
        Some((c, d)) => (c, d),
        None => (s, "asc"),
    };
    let sql_expr = SORTABLE_COLUMNS
        .iter()
        .find(|(k, _)| *k == col)
        .map(|(_, expr)| (*expr).to_string())
        .ok_or("invalid sort column")?;
    let dir = match dir.to_ascii_lowercase().as_str() {
        "asc" => "ASC",
        "desc" => "DESC",
        _ => return Err("sort direction must be asc or desc"),
    };
    Ok((sql_expr, dir))
}

/// Validate a comma-separated list of modeling layer values and return them.
fn parse_modeling_layers(raw: &str) -> Result<Vec<&str>, &'static str> {
    raw.split(',')
        .map(|v| {
            let v = v.trim();
            if LAYER_CONDITIONS.iter().any(|(name, _)| *name == v) {
                Ok(v)
            } else {
                Err("invalid modeling_layer value")
            }
        })
        .collect()
}

/// Validate a comma-separated list of access level values and return them.
fn parse_access_levels(raw: &str) -> Result<Vec<&str>, &'static str> {
    raw.split(',')
        .map(|v| {
            let v = v.trim();
            if VALID_ACCESS_LEVELS.contains(&v) {
                Ok(v)
            } else {
                Err("invalid access filter value")
            }
        })
        .collect()
}

/// Build the WHERE OR fragment for a modeling_layer filter.
///
/// Each requested layer maps to its LIKE conditions from [`LAYER_CONDITIONS`],
/// the same data that drives the SELECT CASE expression, so the two can
/// never drift.
fn modeling_layer_where(layers: &[&str]) -> String {
    layers
        .iter()
        .map(|layer| {
            let cond = LAYER_CONDITIONS
                .iter()
                .find(|(name, _)| name == layer)
                .map(|(_, cond)| *cond)
                .expect("layer already validated against LAYER_CONDITIONS");
            format!("({cond})")
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// Build and return `(count_sql, rows_sql)` for the models list query.
///
/// `with_run_results` controls whether the `last_run` CTE referencing
/// `dbt_rt.run_results` is included. Pass `false` when that view is absent.
///
/// Returns `Err(&'static str)` so the Err variant stays small; callers
/// convert to a `Response` via [`bad_request`].
fn build_list_sql(
    params: &ModelListParams,
    with_run_results: bool,
    limit: u32,
    offset: u32,
) -> Result<(String, String), &'static str> {
    // --- validate / parse params ---
    let layers: Vec<&str> = match params.modeling_layer.as_deref().filter(|s| !s.is_empty()) {
        Some(raw) => parse_modeling_layers(raw)?,
        None => vec![],
    };
    let accesses: Vec<&str> = match params.access.as_deref().filter(|s| !s.is_empty()) {
        Some(raw) => parse_access_levels(raw)?,
        None => vec![],
    };
    let (order_expr, order_dir) = match params.sort.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => parse_sort(s)?,
        None => ("n.name".to_string(), "ASC"),
    };

    // --- WHERE clause ---
    let mut where_clause = String::from("WHERE n.resource_type = 'model'");
    if !layers.is_empty() {
        let cond = modeling_layer_where(&layers);
        let _ = write!(where_clause, " AND ({cond})");
    }
    if !accesses.is_empty() {
        let list = accesses
            .iter()
            .map(|a| format!("'{}'", escape_str(a)))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = write!(where_clause, " AND n.access_level IN ({list})");
    }
    if let Some(owner) = params.owner.as_deref().filter(|s| !s.is_empty()) {
        let escaped = escape_str(owner);
        let _ = write!(where_clause, " AND n.group_name = '{escaped}'");
    }

    // --- CTE + executed_at column ---
    // Cast to VARCHAR so the Arrow column type is always StringArray regardless
    // of whether the CTE is present. batches_to_model_rows expects StringArray.
    let (cte, lr_join, executed_at_col) = if with_run_results {
        (
            RUN_RESULTS_CTE,
            "LEFT JOIN last_run lr ON lr.unique_id = n.unique_id",
            "CAST(lr.executed_at AS VARCHAR) AS executed_at",
        )
    } else {
        ("", "", "NULL::VARCHAR AS executed_at")
    };

    let count_sql = format!(
        "{cte}SELECT count(*) \
         FROM dbt.nodes n \
         {lr_join} \
         {where_clause}"
    );
    let ml_case = modeling_layer_case_sql();
    let rows_sql = format!(
        "{cte}SELECT \
           n.unique_id, n.name, n.package_name, n.original_file_path, \
           {ml_case} AS modeling_layer, \
           n.access_level, n.contract_enforced, \
           n.group_name AS owner, \
           {executed_at_col} \
         FROM dbt.nodes n \
         {lr_join} \
         {where_clause} \
         ORDER BY {order_expr} {order_dir} NULLS LAST \
         LIMIT {limit} OFFSET {offset}"
    );

    Ok((count_sql, rows_sql))
}

/// `GET /api/v1/models` — paginated, filterable, sortable list of model nodes.
///
/// Response shape:
/// ```json
/// {
///   "models": [...],
///   "total": 42,
///   "offset": 0,
///   "limit": 100
/// }
/// ```
///
/// If `dbt_rt.run_results` parquet is absent (project never run with
/// `dbt --use-index`), `executed_at` is `null` for every row rather than
/// returning an error.
pub async fn list_models(
    State(state): State<SharedState>,
    Query(params): Query<ModelListParams>,
) -> Response {
    let limit = params
        .limit
        .unwrap_or(DEFAULT_LIMIT)
        .clamp(1, HARD_MAX_LIMIT);
    let offset = params.offset.unwrap_or(0);

    let (count_sql, rows_sql) = match build_list_sql(&params, true, limit, offset) {
        Ok(pair) => pair,
        Err(msg) => return bad_request(msg),
    };
    // build_list_sql only varies on `with_run_results`; since params already
    // validated above, this second call cannot fail.
    let (count_sql_no_rr, rows_sql_no_rr) =
        build_list_sql(&params, false, limit, offset).expect("params already validated");

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        // Try with run_results CTE first. query_scalar returns None when the
        // underlying SQL query fails (e.g. dbt_rt.run_results view absent).
        // COUNT(*) always returns a row, so None unambiguously means a query error.
        let (total, batches) = match backend.query_scalar(&count_sql) {
            Some(count_str) => {
                let total = count_str
                    .parse::<u64>()
                    .map_err(|e| format!("could not parse model count: {e}"))?;
                let batches = backend.query_arrow(&rows_sql).map_err(|e| e.to_string())?;
                (total, batches)
            }
            None => {
                // run_results view absent; retry without the CTE.
                let total = backend
                    .query_scalar(&count_sql_no_rr)
                    .ok_or_else(|| "count query returned no rows".to_string())?
                    .parse::<u64>()
                    .map_err(|e| format!("could not parse model count: {e}"))?;
                let batches = backend
                    .query_arrow(&rows_sql_no_rr)
                    .map_err(|e| e.to_string())?;
                (total, batches)
            }
        };
        Ok((total, batches))
    })
    .await;

    let (total, batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let models = batches_to_model_rows(&batches);

    Json(ModelListResponse {
        models,
        total,
        offset,
        limit,
    })
    .into_response()
}

/// `GET /api/v1/models/facets` — all filter facet values for the models list.
///
/// Returns three keys so the client never hardcodes dbt concepts:
/// - `modeling_layers`: ordered labels from [`LAYER_CONDITIONS`] (server constant)
/// - `accesses`: ordered labels from [`VALID_ACCESS_LEVELS`] (server constant)
/// - `owners`: distinct `owner_name` values from `dbt.groups` (project-specific)
///
/// Response:
/// ```json
/// {
///   "modeling_layers": ["Staging", "Intermediate", "Marts"],
///   "accesses": ["private", "protected", "public"],
///   "owners": ["Data Team (EPD)", "Field Engineering"]
/// }
/// ```
pub async fn list_model_facets(State(state): State<SharedState>) -> Response {
    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || backend.query_arrow(OWNERS_FACET_SQL)).await;

    let batches = match result {
        Ok(Ok(b)) => b,
        Ok(Err(err)) => return internal_error(err.to_string()),
        Err(err) => return internal_error(err.to_string()),
    };

    let owners = batches_to_owner_names(&batches);

    Json(ModelFacetsResponse {
        modeling_layers: LAYER_CONDITIONS
            .iter()
            .map(|(n, _)| FacetValue::new(*n))
            .collect(),
        accesses: VALID_ACCESS_LEVELS
            .iter()
            .map(|v| FacetValue::new(*v))
            .collect(),
        owners: owners.into_iter().map(FacetValue::new).collect(),
    })
    .into_response()
}

// ---------------------------------------------------------------------------
// Response types: GET /api/v1/models/:id
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct ModelDetail {
    pub unique_id: String,
    pub name: String,
    pub resource_type: String,
    pub package_name: Option<String>,
    pub materialized: Option<String>,
    pub description: Option<String>,
    pub database_name: Option<String>,
    pub schema_name: Option<String>,
    pub relation_name: Option<String>,
    pub identifier: Option<String>,
    pub original_file_path: Option<String>,
    pub access_level: Option<String>,
    pub group_name: Option<String>,
    pub raw_code: Option<String>,
    pub compiled_code: Option<String>,
    pub contract_enforced: Option<bool>,
    pub tags: Vec<String>,
    pub fqn: Vec<String>,
    pub columns: Vec<ModelColumn>,
    pub depends_on: Vec<ModelEdgeRef>,
    pub referenced_by: Vec<ModelEdgeRef>,
    pub execution_info: Option<ExecutionInfo>,
    pub catalog: Option<ModelCatalogInfo>,
}

#[derive(Serialize)]
pub struct ModelColumn {
    pub name: String,
    pub index: Option<i64>,
    pub data_type: Option<String>,
    pub declared_type: Option<String>,
    pub inferred_type: Option<String>,
    pub catalog_type: Option<String>,
    pub description: Option<String>,
    pub label: Option<String>,
    pub granularity: Option<String>,
}

#[derive(Serialize)]
pub struct ModelEdgeRef {
    pub unique_id: String,
    pub edge_type: String,
}

#[derive(Serialize)]
pub struct ExecutionInfo {
    pub status: Option<String>,
    pub execution_time: Option<f64>,
    pub completed_at: Option<String>,
}

#[derive(Serialize)]
pub struct ModelCatalogInfo {
    // TODO: bytes_stat and row_count_stat live in dbt.catalog_stats keyed by
    // adapter-specific stat_id values. Stub NULL until a populated catalog
    // index from a live warehouse is available to confirm those keys.
    #[serde(rename = "type")]
    pub table_type: Option<String>,
    pub owner: Option<String>,
    pub bytes_stat: Option<i64>,
    pub row_count_stat: Option<i64>,
}

// ---------------------------------------------------------------------------
// Extraction helpers
// ---------------------------------------------------------------------------

fn extract_str_list(batch: &RecordBatch, col_name: &'static str) -> Vec<String> {
    let Some(col) = batch.column_by_name(col_name) else {
        return vec![];
    };
    if batch.num_rows() == 0 || col.is_null(0) {
        return vec![];
    }
    let Some(list) = col.as_any().downcast_ref::<ListArray>() else {
        return vec![];
    };
    let inner = list.value(0);
    let Some(strings) = inner.as_any().downcast_ref::<StringArray>() else {
        return vec![];
    };
    (0..strings.len())
        .filter(|&i| !strings.is_null(i))
        .map(|i| strings.value(i).to_owned())
        .collect()
}

fn extract_node_detail(batches: &[RecordBatch]) -> Option<ModelDetail> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;

    let str_opt = |name: &'static str| -> Option<String> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<StringArray>()?;
        if col.is_null(0) {
            None
        } else {
            Some(col.value(0).to_owned())
        }
    };
    let bool_opt = |name: &'static str| -> Option<bool> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<BooleanArray>()?;
        if col.is_null(0) {
            None
        } else {
            Some(col.value(0))
        }
    };

    Some(ModelDetail {
        unique_id: str_opt("unique_id").unwrap_or_default(),
        name: str_opt("name").unwrap_or_default(),
        resource_type: str_opt("resource_type").unwrap_or_default(),
        package_name: str_opt("package_name"),
        materialized: str_opt("materialized"),
        description: str_opt("description"),
        database_name: str_opt("database_name"),
        schema_name: str_opt("schema_name"),
        relation_name: str_opt("relation_name"),
        identifier: str_opt("identifier"),
        original_file_path: str_opt("original_file_path"),
        access_level: str_opt("access_level"),
        group_name: str_opt("group_name"),
        raw_code: str_opt("raw_code"),
        compiled_code: str_opt("compiled_code"),
        contract_enforced: bool_opt("contract_enforced"),
        tags: extract_str_list(batch, "tags"),
        fqn: extract_str_list(batch, "fqn"),
        // Sub-resources populated after extraction.
        columns: vec![],
        depends_on: vec![],
        referenced_by: vec![],
        execution_info: None,
        catalog: None,
    })
}

fn extract_model_columns(batches: &[RecordBatch]) -> Vec<ModelColumn> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let name_col = str_col(batch, "name");
        let data_type = str_col(batch, "data_type");
        let declared_type = str_col(batch, "declared_type");
        let inferred_type = str_col(batch, "inferred_type");
        let catalog_type = str_col(batch, "catalog_type");
        let description = str_col(batch, "description");
        let label = str_col(batch, "label");
        let granularity = str_col(batch, "granularity");
        let index_col = batch
            .column_by_name("index")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

        let opt = |col: &StringArray, i: usize| -> Option<String> {
            if col.is_null(i) {
                None
            } else {
                Some(col.value(i).to_owned())
            }
        };

        for i in 0..batch.num_rows() {
            rows.push(ModelColumn {
                name: name_col.value(i).to_owned(),
                index: index_col.and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) }),
                data_type: opt(data_type, i),
                declared_type: opt(declared_type, i),
                inferred_type: opt(inferred_type, i),
                catalog_type: opt(catalog_type, i),
                description: opt(description, i),
                label: opt(label, i),
                granularity: opt(granularity, i),
            });
        }
    }
    rows
}

fn extract_edge_refs(batches: &[RecordBatch]) -> Vec<ModelEdgeRef> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let uid = str_col(batch, "unique_id");
        let etype = str_col(batch, "edge_type");
        for i in 0..batch.num_rows() {
            rows.push(ModelEdgeRef {
                unique_id: uid.value(i).to_owned(),
                edge_type: etype.value(i).to_owned(),
            });
        }
    }
    rows
}

fn extract_execution_info(batches: &[RecordBatch]) -> Option<ExecutionInfo> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;
    let status_col = batch
        .column_by_name("status")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let exec_time_col = batch
        .column_by_name("execution_time")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
    let completed_at_col = batch
        .column_by_name("completed_at")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    Some(ExecutionInfo {
        status: status_col.and_then(|c| {
            if c.is_null(0) {
                None
            } else {
                Some(c.value(0).to_owned())
            }
        }),
        execution_time: exec_time_col
            .and_then(|c| if c.is_null(0) { None } else { Some(c.value(0)) }),
        completed_at: completed_at_col.and_then(|c| {
            if c.is_null(0) {
                None
            } else {
                Some(c.value(0).to_owned())
            }
        }),
    })
}

fn extract_catalog_info(batches: &[RecordBatch]) -> Option<ModelCatalogInfo> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;
    let str_opt = |name: &'static str| -> Option<String> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<StringArray>()?;
        if col.is_null(0) {
            None
        } else {
            Some(col.value(0).to_owned())
        }
    };
    let i64_opt = |name: &'static str| -> Option<i64> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<Int64Array>()?;
        if col.is_null(0) {
            None
        } else {
            Some(col.value(0))
        }
    };
    Some(ModelCatalogInfo {
        table_type: str_opt("type"),
        owner: str_opt("owner"),
        bytes_stat: i64_opt("bytes_stat"),
        row_count_stat: i64_opt("row_count_stat"),
    })
}

// ---------------------------------------------------------------------------
// SQL: GET /api/v1/models/:id
// ---------------------------------------------------------------------------

const MODEL_DETAIL_NODE_SQL: &str = "\
SELECT n.unique_id, n.name, n.resource_type, n.package_name, \
       n.materialized, n.description, n.database_name, n.schema_name, \
       n.relation_name, n.identifier, n.original_file_path, \
       n.access_level, n.group_name, n.raw_code, n.compiled_code, \
       n.contract_enforced, n.tags, n.fqn \
FROM dbt.nodes n \
WHERE n.unique_id = '{id}' AND n.resource_type = 'model' \
LIMIT 1";

const MODEL_DETAIL_RUN_RESULT_SQL: &str = "\
SELECT status, \
       execution_time, \
       CAST(created_at AS VARCHAR) AS completed_at \
FROM dbt_rt.run_results \
WHERE unique_id = '{id}' \
ORDER BY created_at DESC \
LIMIT 1";

// `table_type` and `table_owner` are the actual column names in dbt.catalog_tables.
// bytes_stat/row_count_stat live in dbt.catalog_stats (adapter-specific stat_id).
const MODEL_DETAIL_CATALOG_SQL: &str = "\
SELECT table_type AS type, \
       table_owner AS owner, \
       NULL::BIGINT AS bytes_stat, \
       NULL::BIGINT AS row_count_stat \
FROM dbt.catalog_tables \
WHERE unique_id = '{id}' \
LIMIT 1";

// ---------------------------------------------------------------------------
// Handler: GET /api/v1/models/:id
// ---------------------------------------------------------------------------

/// `GET /api/v1/models/:id` — full model detail.
///
/// `execution_info` and `catalog` are `null` when the corresponding parquet
/// tables are absent or empty.
///
/// `depends_on` and `referenced_by` are unbounded. Truncating would be
/// backwards-incompatible by output (not schema); if pagination is ever needed,
/// add a lineage sub-resource rather than capping this field.
pub async fn get_model(
    State(state): State<SharedState>,
    Path(unique_id): Path<String>,
) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }
    let id = escape_str(&unique_id);

    let node_sql = MODEL_DETAIL_NODE_SQL.replace("{id}", &id);
    let columns_sql = format!(
        "SELECT column_name AS name, column_index AS index, \
                data_type, declared_type, inferred_type, catalog_type, \
                description, label, granularity \
         FROM dbt.node_columns WHERE unique_id = '{id}' \
         ORDER BY column_index NULLS LAST, column_name"
    );
    let upstream_sql = format!(
        "SELECT parent_unique_id AS unique_id, edge_type \
         FROM dbt.edges WHERE child_unique_id = '{id}' \
         ORDER BY parent_unique_id"
    );
    let downstream_sql = format!(
        "SELECT child_unique_id AS unique_id, edge_type \
         FROM dbt.edges WHERE parent_unique_id = '{id}' \
         ORDER BY child_unique_id"
    );
    let run_result_sql = MODEL_DETAIL_RUN_RESULT_SQL.replace("{id}", &id);
    let catalog_sql = MODEL_DETAIL_CATALOG_SQL.replace("{id}", &id);

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let node_batches = backend.query_arrow(&node_sql).map_err(|e| e.to_string())?;
        let column_batches = backend
            .query_arrow(&columns_sql)
            .map_err(|e| e.to_string())?;
        let upstream_batches = backend
            .query_arrow(&upstream_sql)
            .map_err(|e| e.to_string())?;
        let downstream_batches = backend
            .query_arrow(&downstream_sql)
            .map_err(|e| e.to_string())?;
        let run_result_batches = backend.query_arrow(&run_result_sql).ok();
        let catalog_batches = backend.query_arrow(&catalog_sql).ok();
        Ok((
            node_batches,
            column_batches,
            upstream_batches,
            downstream_batches,
            run_result_batches,
            catalog_batches,
        ))
    })
    .await;

    let (
        node_batches,
        column_batches,
        upstream_batches,
        downstream_batches,
        run_result_batches,
        catalog_batches,
    ) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let Some(mut detail) = extract_node_detail(&node_batches) else {
        return not_found(format!("model {unique_id} not found"));
    };

    detail.columns = extract_model_columns(&column_batches);
    detail.depends_on = extract_edge_refs(&upstream_batches);
    detail.referenced_by = extract_edge_refs(&downstream_batches);
    detail.execution_info = run_result_batches
        .as_deref()
        .and_then(extract_execution_info);
    detail.catalog = catalog_batches.as_deref().and_then(extract_catalog_info);

    Json(detail).into_response()
}

#[cfg(test)]
#[path = "models_tests.rs"]
mod tests;
