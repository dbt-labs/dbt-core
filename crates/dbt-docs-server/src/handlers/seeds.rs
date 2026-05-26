//! `GET /api/v1/seeds/:id`, `GET /api/v1/seeds`, and `GET /api/v1/seeds/facets`.
//!
//! Seed-shaped vs. model-shaped: no `depends_on` (seeds have no upstream
//! references), no `raw_code` / `compiled_code` / `materialized` (seeds are
//! CSVs loaded as tables). `execution_info` and `catalog` are the optional
//! inline sub-objects, emitted as JSON `null` when no row exists in the
//! corresponding parquet view. `identifier` projects from `dbt.nodes.alias`
//! — the field that overrides the CSV filename to set the warehouse table
//! name.
//!
//! `meta` is a JSON-string parquet column; it's deserialized handler-side
//! via [`crate::handlers::json::json_parse_or_null`] so the response carries
//! a real JSON object, not an escaped string.
//!
//! Data sources:
//! - `dbt.nodes` — node row (filtered to `resource_type = 'seed'`)
//! - `dbt.node_columns` — `columns[]`
//! - `dbt.edges` — `referenced_by` (downstream only)
//! - `dbt_rt.run_results` — `execution_info` (optional)
//! - `dbt.catalog_tables` + `dbt.catalog_stats` — `catalog` (optional)

use std::fmt::Write as _;

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::handlers::json::{bad_request, internal_error, json_parse_or_null, not_found};
use crate::handlers::node_base::{
    EdgeRef, ExecutionInfo, NodeBase, extract_edge_refs, extract_execution_info, extract_str_list,
    opt_str, str_col,
};
use crate::handlers::pagination::{Cursor, PageInfo, SortDir, clamp_first, cursor_where_fragment};
use crate::handlers::sql::escape_str;
use crate::state::SharedState;

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Response body for `GET /api/v1/seeds/:id`.
#[derive(Serialize)]
pub struct SeedDetail {
    #[serde(flatten)]
    pub base: NodeBase,
    /// Project-relative path of the CSV file.
    pub file_path: Option<String>,
    /// Project-relative path of the YAML patch file.
    pub patch_path: Option<String>,
    pub tags: Vec<String>,
    pub fqn: Vec<String>,
    pub database_name: Option<String>,
    pub schema_name: Option<String>,
    /// Warehouse table name; projected from `dbt.nodes.alias`.
    pub identifier: Option<String>,
    /// Parsed JSON object, or `null` when absent / unparseable.
    pub meta: serde_json::Value,
    pub columns: Vec<SeedColumn>,
    /// Downstream consumers. Seeds have no `depends_on` (omitted entirely,
    /// not returned as `[]`).
    pub referenced_by: Vec<EdgeRef>,
    /// `null` when `dbt_rt.run_results` has no row for this seed.
    pub execution_info: Option<ExecutionInfo>,
    /// `null` when `dbt.catalog_tables` has no row for this seed.
    pub catalog: Option<SeedCatalogInfo>,
}

#[derive(Serialize)]
pub struct SeedColumn {
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

/// Seed catalog: base catalog shape plus `stats[]`. No `comment` or
/// `primary_key` — those are source-only.
#[derive(Serialize)]
pub struct SeedCatalogInfo {
    #[serde(rename = "type")]
    pub table_type: Option<String>,
    pub owner: Option<String>,
    pub row_count_stat: Option<i64>,
    pub bytes_stat: Option<i64>,
    pub stats: Vec<CatalogStat>,
}

#[derive(Serialize)]
pub struct CatalogStat {
    pub id: String,
    pub label: String,
    pub value: String,
    pub description: String,
    pub include: bool,
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

const SEED_DETAIL_NODE_SQL: &str = "\
SELECT n.unique_id, n.name, n.resource_type, n.package_name, n.description, \
       n.original_file_path, n.file_path, n.patch_path, \
       n.database_name, n.schema_name, n.alias AS identifier, n.meta, \
       n.tags, n.fqn \
FROM dbt.nodes n \
WHERE n.unique_id = '{id}' AND n.resource_type = 'seed' \
LIMIT 1";

const SEED_DETAIL_RUN_RESULT_SQL: &str = "\
SELECT status, \
       execution_time, \
       CAST(created_at AS VARCHAR) AS completed_at \
FROM dbt_rt.run_results \
WHERE unique_id = '{id}' \
ORDER BY created_at DESC \
LIMIT 1";

const SEED_DETAIL_CATALOG_SQL: &str = "\
SELECT table_type AS type, \
       table_owner AS owner, \
       NULL::BIGINT AS bytes_stat, \
       NULL::BIGINT AS row_count_stat \
FROM dbt.catalog_tables \
WHERE unique_id = '{id}' \
LIMIT 1";

const SEED_DETAIL_CATALOG_STATS_SQL: &str = "\
SELECT stat_id AS id, stat_label AS label, stat_value AS value, \
       description, include_in_stats AS include \
FROM dbt.catalog_stats \
WHERE unique_id = '{id}' \
ORDER BY stat_id";

// ---------------------------------------------------------------------------
// Extractors
// ---------------------------------------------------------------------------

fn extract_seed_detail(batches: &[RecordBatch]) -> Option<SeedDetail> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;

    let s = |name: &'static str| -> Option<String> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<StringArray>()?;
        opt_str(col, 0)
    };

    let meta_raw = s("meta");
    let meta = json_parse_or_null(meta_raw.as_deref());

    Some(SeedDetail {
        base: NodeBase {
            unique_id: s("unique_id").unwrap_or_default(),
            name: s("name").unwrap_or_default(),
            resource_type: s("resource_type").unwrap_or_default(),
            package_name: s("package_name"),
            description: s("description"),
            original_file_path: s("original_file_path"),
        },
        file_path: s("file_path"),
        patch_path: s("patch_path"),
        tags: extract_str_list(batch, "tags"),
        fqn: extract_str_list(batch, "fqn"),
        database_name: s("database_name"),
        schema_name: s("schema_name"),
        identifier: s("identifier"),
        meta,
        // Sub-resources populated after extraction.
        columns: vec![],
        referenced_by: vec![],
        execution_info: None,
        catalog: None,
    })
}

fn extract_seed_columns(batches: &[RecordBatch]) -> Vec<SeedColumn> {
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

        for i in 0..batch.num_rows() {
            rows.push(SeedColumn {
                name: name_col.value(i).to_owned(),
                index: index_col.and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) }),
                data_type: opt_str(data_type, i),
                declared_type: opt_str(declared_type, i),
                inferred_type: opt_str(inferred_type, i),
                catalog_type: opt_str(catalog_type, i),
                description: opt_str(description, i),
                label: opt_str(label, i),
                granularity: opt_str(granularity, i),
            });
        }
    }
    rows
}

fn extract_catalog_stats(batches: &[RecordBatch]) -> Vec<CatalogStat> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let id_col = str_col(batch, "id");
        let label_col = str_col(batch, "label");
        let value_col = str_col(batch, "value");
        let desc_col = str_col(batch, "description");
        let include_col = batch
            .column_by_name("include")
            .and_then(|c| c.as_any().downcast_ref::<BooleanArray>());

        for i in 0..batch.num_rows() {
            rows.push(CatalogStat {
                id: id_col.value(i).to_owned(),
                label: opt_str(label_col, i).unwrap_or_default(),
                value: opt_str(value_col, i).unwrap_or_default(),
                description: opt_str(desc_col, i).unwrap_or_default(),
                include: include_col
                    .map(|c| !c.is_null(i) && c.value(i))
                    .unwrap_or(false),
            });
        }
    }
    rows
}

fn extract_seed_catalog(
    table_batches: &[RecordBatch],
    stats_batches: &[RecordBatch],
) -> Option<SeedCatalogInfo> {
    let batch = table_batches.iter().find(|b| b.num_rows() > 0)?;

    let s = |name: &'static str| -> Option<String> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<StringArray>()?;
        opt_str(col, 0)
    };
    let i = |name: &'static str| -> Option<i64> {
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

    Some(SeedCatalogInfo {
        table_type: s("type"),
        owner: s("owner"),
        bytes_stat: i("bytes_stat"),
        row_count_stat: i("row_count_stat"),
        stats: extract_catalog_stats(stats_batches),
    })
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `GET /api/v1/seeds/:id` — full seed detail.
///
/// `execution_info` is `null` when `dbt_rt.run_results` has no row for this
/// seed; `catalog` is `null` when `dbt.catalog_tables` has no row. Seeds
/// never carry `depends_on` — the field is omitted, not returned as `[]`.
/// `referenced_by` is unbounded.
pub async fn get_seed(State(state): State<SharedState>, Path(unique_id): Path<String>) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }
    let id = escape_str(&unique_id);

    let node_sql = SEED_DETAIL_NODE_SQL.replace("{id}", &id);
    let columns_sql = format!(
        "SELECT column_name AS name, column_index AS index, \
                data_type, declared_type, inferred_type, catalog_type, \
                description, label, granularity \
         FROM dbt.node_columns WHERE unique_id = '{id}' \
         ORDER BY column_index NULLS LAST, column_name"
    );
    // Seeds are upstream-only; only downstream edges are meaningful.
    let downstream_sql = format!(
        "SELECT child_unique_id AS unique_id, edge_type \
         FROM dbt.edges WHERE parent_unique_id = '{id}' \
         ORDER BY child_unique_id"
    );
    let run_result_sql = SEED_DETAIL_RUN_RESULT_SQL.replace("{id}", &id);
    let catalog_sql = SEED_DETAIL_CATALOG_SQL.replace("{id}", &id);
    let catalog_stats_sql = SEED_DETAIL_CATALOG_STATS_SQL.replace("{id}", &id);

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let node_batches = backend.query_arrow(&node_sql).map_err(|e| e.to_string())?;
        let column_batches = backend
            .query_arrow(&columns_sql)
            .map_err(|e| e.to_string())?;
        let downstream_batches = backend
            .query_arrow(&downstream_sql)
            .map_err(|e| e.to_string())?;
        // Optional surfaces: missing parquet view → None → JSON `null` field.
        let run_result_batches = backend.query_arrow(&run_result_sql).ok();
        let catalog_batches = backend.query_arrow(&catalog_sql).ok();
        let catalog_stats_batches = backend.query_arrow(&catalog_stats_sql).ok();
        Ok((
            node_batches,
            column_batches,
            downstream_batches,
            run_result_batches,
            catalog_batches,
            catalog_stats_batches,
        ))
    })
    .await;

    let (
        node_batches,
        column_batches,
        downstream_batches,
        run_result_batches,
        catalog_batches,
        catalog_stats_batches,
    ) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let Some(mut detail) = extract_seed_detail(&node_batches) else {
        return not_found(format!("seed {unique_id} not found"));
    };

    detail.columns = extract_seed_columns(&column_batches);
    detail.referenced_by = extract_edge_refs(&downstream_batches);
    detail.execution_info = run_result_batches
        .as_deref()
        .and_then(extract_execution_info);
    detail.catalog = match (catalog_batches.as_deref(), catalog_stats_batches.as_deref()) {
        (Some(t), stats_opt) => extract_seed_catalog(t, stats_opt.unwrap_or(&[])),
        // No catalog_tables view at all — no catalog block.
        (None, _) => None,
    };

    Json(detail).into_response()
}

// ---------------------------------------------------------------------------
// Response types: GET /api/v1/seeds and GET /api/v1/seeds/facets
// ---------------------------------------------------------------------------

/// One row in `GET /api/v1/seeds`.
#[derive(Serialize)]
pub struct SeedSummary {
    pub unique_id: String,
    pub name: String,
    pub resource_type: String,
    pub package_name: Option<String>,
    pub description: Option<String>,
    pub original_file_path: Option<String>,
    /// Approximate row count from `dbt.catalog_stats`; `null` when
    /// `dbt docs generate` has not run or the adapter did not emit a
    /// row-count stat for this seed.
    pub row_count: Option<i64>,
    /// ISO 8601 completion timestamp from `dbt_rt.run_results`; `null` when
    /// `dbt seed` / `dbt build` has not run for this seed.
    pub executed_at: Option<String>,
}

/// ADR-6 cursor-paginated response for `GET /api/v1/seeds`.
#[derive(Serialize)]
pub struct SeedListResponse {
    pub data: Vec<SeedSummary>,
    pub page_info: PageInfo,
}

/// Response body for `GET /api/v1/seeds/facets`.
///
/// Seeds expose no filter dropdowns in dbt-ui (`SeedFilterView.tsx`), so
/// this struct is empty. The endpoint exists for API uniformity across
/// resource types.
#[derive(Serialize)]
pub struct SeedFacetsResponse {}

/// Query parameters for `GET /api/v1/seeds`.
#[derive(Debug, Default, Deserialize)]
pub struct SeedListParams {
    /// Sort spec `<column>:<asc|desc>`. Allowlisted: `name`, `row_count`,
    /// `executed_at`. Default: `name:asc`. Unknown column → 400.
    pub sort: Option<String>,
    pub first: Option<u32>,
    pub after: Option<String>,
}

// ---------------------------------------------------------------------------
// SQL: GET /api/v1/seeds
// ---------------------------------------------------------------------------

/// Allowlisted sort columns for seeds.
///
/// Each entry is `(query_param_name, sql_expression)`. The expression must be
/// usable in both `ORDER BY` and the cursor `WHERE` predicate.
const SORTABLE_COLUMNS: &[(&str, &str)] = &[
    ("name", "n.name"),
    ("row_count", "rc.row_count"),
    ("executed_at", "CAST(lr.executed_at AS VARCHAR)"),
];

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

/// Build `(count_sql, rows_sql)` for `GET /api/v1/seeds`.
///
/// `with_run_results` controls whether `dbt_rt.run_results` is joined.
/// `with_catalog_stats` controls whether `dbt.catalog_stats` is joined.
/// When `false`, the respective column is `NULL::VARCHAR` / `NULL::BIGINT`
/// so the Arrow column type is stable across fallback paths.
pub(crate) fn build_seed_list_sql(
    params: &SeedListParams,
    with_run_results: bool,
    with_catalog_stats: bool,
    first: u32,
    cursor: Option<&Cursor>,
) -> Result<(String, String), &'static str> {
    let sort = match params.sort.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => parse_sort(s)?,
        None => SortSelection {
            sql_expr: "n.name",
            dir: SortDir::Asc,
        },
    };

    let filter_where = "WHERE n.resource_type = 'seed'";

    let mut page_where = filter_where.to_owned();
    if let Some(c) = cursor {
        let frag = cursor_where_fragment(
            sort.sql_expr,
            "n.unique_id",
            sort.dir,
            c.sort_value.as_deref(),
            &c.unique_id,
        );
        let _ = write!(page_where, " AND {frag}");
    }

    // CTE header and JOIN fragments depend on which optional tables are present.
    let cte_prefix = match (with_run_results, with_catalog_stats) {
        (true, true) => "WITH last_run AS (\
              SELECT unique_id, MAX(created_at) AS executed_at \
              FROM dbt_rt.run_results GROUP BY unique_id\
            ), row_count_cte AS (\
              SELECT unique_id, \
                     TRY_CAST(stat_value AS BIGINT) AS row_count \
              FROM dbt.catalog_stats \
              WHERE stat_id IN ('row_count', 'num_rows')\
            )\n"
        .to_owned(),
        (true, false) => "WITH last_run AS (\
              SELECT unique_id, MAX(created_at) AS executed_at \
              FROM dbt_rt.run_results GROUP BY unique_id\
            )\n"
        .to_owned(),
        (false, true) => "WITH row_count_cte AS (\
              SELECT unique_id, \
                     TRY_CAST(stat_value AS BIGINT) AS row_count \
              FROM dbt.catalog_stats \
              WHERE stat_id IN ('row_count', 'num_rows')\
            )\n"
        .to_owned(),
        (false, false) => String::new(),
    };

    let lr_join = if with_run_results {
        "LEFT JOIN last_run lr ON lr.unique_id = n.unique_id"
    } else {
        ""
    };
    let executed_at_col = if with_run_results {
        "CAST(lr.executed_at AS VARCHAR) AS executed_at"
    } else {
        "NULL::VARCHAR AS executed_at"
    };
    let rc_join = if with_catalog_stats {
        "LEFT JOIN row_count_cte rc ON rc.unique_id = n.unique_id"
    } else {
        ""
    };
    let row_count_col = if with_catalog_stats {
        "rc.row_count"
    } else {
        "NULL::BIGINT AS row_count"
    };

    let count_sql = format!(
        "{cte_prefix}SELECT count(*) \
         FROM dbt.nodes n {lr_join} {rc_join} \
         {filter_where}"
    );
    let peek = first + 1;
    let order_expr = sort.sql_expr;
    let order_dir = sort.dir.as_sql();
    let rows_sql = format!(
        "{cte_prefix}SELECT n.unique_id, n.name, n.resource_type, n.package_name, \
                n.description, n.original_file_path, \
                {row_count_col}, \
                {executed_at_col} \
         FROM dbt.nodes n {lr_join} {rc_join} \
         {page_where} \
         ORDER BY {order_expr} {order_dir} NULLS LAST, n.unique_id ASC \
         LIMIT {peek}"
    );

    Ok((count_sql, rows_sql))
}

// ---------------------------------------------------------------------------
// Extraction helpers: GET /api/v1/seeds
// ---------------------------------------------------------------------------

fn batches_to_seed_summary_rows(batches: &[RecordBatch]) -> Vec<SeedSummary> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let unique_id = str_col(batch, "unique_id");
        let name = str_col(batch, "name");
        let resource_type = str_col(batch, "resource_type");
        let package_name = str_col(batch, "package_name");
        let description = str_col(batch, "description");
        let original_file_path = str_col(batch, "original_file_path");
        let executed_at = str_col(batch, "executed_at");
        let row_count_col = batch
            .column_by_name("row_count")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

        for i in 0..batch.num_rows() {
            let row_count = row_count_col.and_then(|arr| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            });
            rows.push(SeedSummary {
                unique_id: unique_id.value(i).to_owned(),
                name: name.value(i).to_owned(),
                resource_type: resource_type.value(i).to_owned(),
                package_name: opt_str(package_name, i),
                description: opt_str(description, i),
                original_file_path: opt_str(original_file_path, i),
                row_count,
                executed_at: opt_str(executed_at, i),
            });
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// Handlers: GET /api/v1/seeds and GET /api/v1/seeds/facets
// ---------------------------------------------------------------------------

/// `GET /api/v1/seeds` — cursor-paginated list of seed nodes.
///
/// Default sort is `name:asc`; allowlisted sort columns are `name`,
/// `row_count`, and `executed_at`. Unknown sort column → 400.
/// No filter parameters.
/// `executed_at` is `null` when `dbt_rt.run_results` is absent or has no
/// row for the seed. `row_count` is `null` when `dbt.catalog_stats` is
/// absent or has no matching stat row for the seed.
pub async fn list_seeds(
    State(state): State<SharedState>,
    Query(params): Query<SeedListParams>,
) -> Response {
    let first = clamp_first(params.first);

    let cursor = match params.after.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => match Cursor::decode(s) {
            Ok(c) => Some(c),
            Err(msg) => return bad_request(msg),
        },
        None => None,
    };

    // Build with both optional tables; validates sort/params.
    let (count_sql, rows_sql) =
        match build_seed_list_sql(&params, true, true, first, cursor.as_ref()) {
            Ok(pair) => pair,
            Err(msg) => return bad_request(msg),
        };
    // Fallback variants (params already validated above).
    let (count_sql_no_rr, rows_sql_no_rr) =
        build_seed_list_sql(&params, false, true, first, cursor.as_ref())
            .expect("params already validated");
    let (count_sql_bare, rows_sql_bare) =
        build_seed_list_sql(&params, false, false, first, cursor.as_ref())
            .expect("params already validated");

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        // Try with run_results CTE first. query_scalar returns None when the
        // underlying SQL fails (e.g. dbt_rt.run_results view absent).
        // COUNT(*) always returns a row, so None unambiguously means a query error.
        let (total, batches) = match backend.query_scalar(&count_sql) {
            Some(count_str) => {
                let total = count_str
                    .parse::<u64>()
                    .map_err(|e| format!("could not parse seed count: {e}"))?;
                let batches = backend.query_arrow(&rows_sql).map_err(|e| e.to_string())?;
                (total, batches)
            }
            None => {
                // run_results view absent; retry without it but keep catalog_stats.
                match backend.query_scalar(&count_sql_no_rr) {
                    Some(count_str) => {
                        let total = count_str
                            .parse::<u64>()
                            .map_err(|e| format!("could not parse seed count: {e}"))?;
                        let batches = backend
                            .query_arrow(&rows_sql_no_rr)
                            .map_err(|e| e.to_string())?;
                        (total, batches)
                    }
                    None => {
                        // Both optional tables absent; bare query.
                        let total = backend
                            .query_scalar(&count_sql_bare)
                            .ok_or_else(|| "count query returned no rows".to_string())?
                            .parse::<u64>()
                            .map_err(|e| format!("could not parse seed count: {e}"))?;
                        let batches = backend
                            .query_arrow(&rows_sql_bare)
                            .map_err(|e| e.to_string())?;
                        (total, batches)
                    }
                }
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

    let mut rows = batches_to_seed_summary_rows(&batches);

    let has_next_page = rows.len() as u32 > first;
    if has_next_page {
        rows.truncate(first as usize);
    }

    let sort_column = params
        .sort
        .as_deref()
        .filter(|s| !s.is_empty())
        .and_then(|s| s.split_once(':').map(|(col, _)| col))
        .unwrap_or("name");

    let sort_value_for = |row: &SeedSummary| -> Option<String> {
        match sort_column {
            "name" => Some(row.name.clone()),
            "row_count" => row.row_count.map(|n| n.to_string()),
            "executed_at" => row.executed_at.clone(),
            _ => Some(row.name.clone()),
        }
    };

    let start_cursor = rows.first().map(|row| {
        Cursor {
            sort_value: sort_value_for(row),
            unique_id: row.unique_id.clone(),
        }
        .encode()
    });
    let end_cursor = if has_next_page {
        rows.last().map(|row| {
            Cursor {
                sort_value: sort_value_for(row),
                unique_id: row.unique_id.clone(),
            }
            .encode()
        })
    } else {
        None
    };

    Json(SeedListResponse {
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

/// `GET /api/v1/seeds/facets` — filter facet values for the seeds list.
///
/// Seeds expose no filter dropdowns in dbt-ui, so this returns `{}`. The
/// endpoint exists for API uniformity so clients can hit
/// `GET /api/v1/<resource>/facets` for every resource without special-casing seeds.
pub async fn list_seed_facets() -> Response {
    Json(SeedFacetsResponse {}).into_response()
}

#[cfg(test)]
#[path = "seeds_tests.rs"]
mod tests;
