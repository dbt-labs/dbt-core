//! `GET /api/v1/snapshots/:id` — typed snapshot detail.
//!
//! Snapshots share the model execution surface (`execution_info`,
//! `catalog`, `columns`, `depends_on`, `referenced_by`, `raw_code`,
//! `compiled_code`) and add `patch_path` (the `.yml` patch file, separate
//! from `original_file_path`). `materialized` is always `"snapshot"`.
//! `SnapshotCatalogInfo` adds `primary_key` and `stats[]` over the model
//! catalog shape — same as `SourceCatalogInfo` minus `comment`.
//!
//! `meta` is a JSON-string parquet column; it's deserialized handler-side
//! via [`crate::handlers::json::json_parse_or_null`] so the response carries
//! a real JSON object, not an escaped string.
//!
//! Data sources:
//! - `dbt.nodes` — node row (filtered to `resource_type = 'snapshot'`)
//! - `dbt.node_columns` — `columns[]`
//! - `dbt.edges` — `depends_on` (upstream) and `referenced_by` (downstream)
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

/// Response body for `GET /api/v1/snapshots/:id`.
#[derive(Serialize)]
pub struct SnapshotDetail {
    #[serde(flatten)]
    pub base: NodeBase,
    /// Project-relative path of the `.yml` patch file (separate from the
    /// `.sql` `original_file_path`).
    pub patch_path: Option<String>,
    pub tags: Vec<String>,
    pub fqn: Vec<String>,
    pub database_name: Option<String>,
    pub schema_name: Option<String>,
    pub identifier: Option<String>,
    pub relation_name: Option<String>,
    /// Always `"snapshot"` for this endpoint.
    pub materialized: String,
    pub raw_code: Option<String>,
    pub compiled_code: Option<String>,
    /// Parsed JSON object, or `null` when absent / unparseable.
    pub meta: serde_json::Value,
    pub depends_on: Vec<EdgeRef>,
    pub referenced_by: Vec<EdgeRef>,
    pub columns: Vec<SnapshotColumn>,
    /// `null` when `dbt_rt.run_results` has no row for this snapshot.
    pub execution_info: Option<ExecutionInfo>,
    /// `null` when `dbt.catalog_tables` has no row for this snapshot.
    pub catalog: Option<SnapshotCatalogInfo>,
}

#[derive(Serialize)]
pub struct SnapshotColumn {
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

/// Snapshot-specific catalog: adds `primary_key` and `stats[]` over the
/// model catalog. `primary_key` is sourced from `dbt.nodes.primary_key`
/// (a `List<String>` column) — `dbt.catalog_tables` has no `primary_key`
/// column.
#[derive(Serialize)]
pub struct SnapshotCatalogInfo {
    #[serde(rename = "type")]
    pub table_type: Option<String>,
    pub owner: Option<String>,
    pub primary_key: Vec<String>,
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

const SNAPSHOT_DETAIL_NODE_SQL: &str = "\
SELECT n.unique_id, n.name, n.resource_type, n.package_name, n.description, \
       n.original_file_path, n.patch_path, \
       n.database_name, n.schema_name, n.identifier, n.relation_name, \
       n.materialized, n.raw_code, n.compiled_code, n.meta, \
       n.tags, n.fqn, n.primary_key \
FROM dbt.nodes n \
WHERE n.unique_id = '{id}' AND n.resource_type = 'snapshot' \
LIMIT 1";

const SNAPSHOT_DETAIL_RUN_RESULT_SQL: &str = "\
SELECT status, \
       execution_time, \
       CAST(created_at AS VARCHAR) AS completed_at \
FROM dbt_rt.run_results \
WHERE unique_id = '{id}' \
ORDER BY created_at DESC \
LIMIT 1";

const SNAPSHOT_DETAIL_CATALOG_SQL: &str = "\
SELECT table_type AS type, \
       table_owner AS owner, \
       NULL::BIGINT AS bytes_stat, \
       NULL::BIGINT AS row_count_stat \
FROM dbt.catalog_tables \
WHERE unique_id = '{id}' \
LIMIT 1";

// Catalog stats are independently keyed — adapter-specific stat_id values.
// Always queried alongside catalog_tables; empty result → `stats: []`.
const SNAPSHOT_DETAIL_CATALOG_STATS_SQL: &str = "\
SELECT stat_id AS id, stat_label AS label, stat_value AS value, \
       description, include_in_stats AS include \
FROM dbt.catalog_stats \
WHERE unique_id = '{id}' \
ORDER BY stat_id";

// ---------------------------------------------------------------------------
// Extractors
// ---------------------------------------------------------------------------

fn extract_snapshot_detail(batches: &[RecordBatch]) -> Option<(SnapshotDetail, Vec<String>)> {
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

    let primary_key = extract_str_list(batch, "primary_key");

    let detail = SnapshotDetail {
        base: NodeBase {
            unique_id: s("unique_id").unwrap_or_default(),
            name: s("name").unwrap_or_default(),
            resource_type: s("resource_type").unwrap_or_default(),
            package_name: s("package_name"),
            description: s("description"),
            original_file_path: s("original_file_path"),
        },
        patch_path: s("patch_path"),
        tags: extract_str_list(batch, "tags"),
        fqn: extract_str_list(batch, "fqn"),
        database_name: s("database_name"),
        schema_name: s("schema_name"),
        identifier: s("identifier"),
        relation_name: s("relation_name"),
        materialized: s("materialized").unwrap_or_else(|| "snapshot".to_owned()),
        raw_code: s("raw_code"),
        compiled_code: s("compiled_code"),
        meta,
        // Sub-resources populated after extraction.
        depends_on: vec![],
        referenced_by: vec![],
        columns: vec![],
        execution_info: None,
        catalog: None,
    };
    Some((detail, primary_key))
}

fn extract_snapshot_columns(batches: &[RecordBatch]) -> Vec<SnapshotColumn> {
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
            rows.push(SnapshotColumn {
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

fn extract_snapshot_catalog(
    table_batches: &[RecordBatch],
    stats_batches: &[RecordBatch],
    primary_key: Vec<String>,
) -> Option<SnapshotCatalogInfo> {
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

    Some(SnapshotCatalogInfo {
        table_type: s("type"),
        owner: s("owner"),
        primary_key,
        bytes_stat: i("bytes_stat"),
        row_count_stat: i("row_count_stat"),
        stats: extract_catalog_stats(stats_batches),
    })
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `GET /api/v1/snapshots/:id` — full snapshot detail.
///
/// `execution_info` is `null` when `dbt_rt.run_results` has no row for this
/// snapshot; `catalog` is `null` when `dbt.catalog_tables` has no row.
/// `depends_on` and `referenced_by` are both unbounded.
pub async fn get_snapshot(
    State(state): State<SharedState>,
    Path(unique_id): Path<String>,
) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }
    let id = escape_str(&unique_id);

    let node_sql = SNAPSHOT_DETAIL_NODE_SQL.replace("{id}", &id);
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
    let run_result_sql = SNAPSHOT_DETAIL_RUN_RESULT_SQL.replace("{id}", &id);
    let catalog_sql = SNAPSHOT_DETAIL_CATALOG_SQL.replace("{id}", &id);
    let catalog_stats_sql = SNAPSHOT_DETAIL_CATALOG_STATS_SQL.replace("{id}", &id);

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
        // Optional surfaces: missing parquet view → None → JSON `null` field.
        let run_result_batches = backend.query_arrow(&run_result_sql).ok();
        let catalog_batches = backend.query_arrow(&catalog_sql).ok();
        let catalog_stats_batches = backend.query_arrow(&catalog_stats_sql).ok();
        Ok((
            node_batches,
            column_batches,
            upstream_batches,
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
        upstream_batches,
        downstream_batches,
        run_result_batches,
        catalog_batches,
        catalog_stats_batches,
    ) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let Some((mut detail, primary_key)) = extract_snapshot_detail(&node_batches) else {
        return not_found(format!("snapshot {unique_id} not found"));
    };

    detail.columns = extract_snapshot_columns(&column_batches);
    detail.depends_on = extract_edge_refs(&upstream_batches);
    detail.referenced_by = extract_edge_refs(&downstream_batches);
    detail.execution_info = run_result_batches
        .as_deref()
        .and_then(extract_execution_info);
    detail.catalog = match (catalog_batches.as_deref(), catalog_stats_batches.as_deref()) {
        (Some(t), stats_opt) => extract_snapshot_catalog(t, stats_opt.unwrap_or(&[]), primary_key),
        // No catalog_tables view at all — no catalog block.
        (None, _) => None,
    };

    Json(detail).into_response()
}

// ---------------------------------------------------------------------------
// Response types: GET /api/v1/snapshots and GET /api/v1/snapshots/facets
// ---------------------------------------------------------------------------

/// One row in `GET /api/v1/snapshots`.
#[derive(Serialize)]
pub struct SnapshotSummary {
    pub unique_id: String,
    pub name: String,
    pub resource_type: String,
    pub package_name: Option<String>,
    /// Always `"snapshot"` for this endpoint. From `dbt.nodes.materialized`.
    pub materialized: Option<String>,
    /// Snapshot strategy (`"timestamp"` or `"check"`). Parsed from the
    /// `config` JSON-string column handler-side per CC-7. `null` when
    /// absent or unparseable.
    pub strategy: Option<String>,
    /// Name of the timestamp column watched by the `timestamp` strategy
    /// (e.g., `"updated_at"`). `null` for `check`-strategy snapshots and
    /// when absent or unparseable from config. This is the column *name*,
    /// not a timestamp value.
    pub updated_at: Option<String>,
    /// `null` when `dbt_rt.run_results` has no row for this snapshot.
    pub execution_info: Option<SnapshotListExecutionInfo>,
    /// `null` when `dbt.catalog_tables` has no row for this snapshot.
    pub catalog: Option<SnapshotListCatalogInfo>,
}

/// Execution info as surfaced on the snapshots list endpoint.
///
/// Shape parallels `ModelDetail.ExecutionInfo` + ADR-4's `error` bare name.
#[derive(Serialize)]
pub struct SnapshotListExecutionInfo {
    pub status: Option<String>,
    pub completed_at: Option<String>,
    pub error: Option<String>,
}

/// Catalog info as surfaced on the snapshots list endpoint.
///
/// Narrower than `SnapshotDetail.catalog`: only the per-stat fields rendered
/// by `SnapshotFilterView`. The full catalog remains on
/// `GET /api/v1/snapshots/:id`.
#[derive(Serialize)]
pub struct SnapshotListCatalogInfo {
    /// Approximate row count from `dbt.catalog_stats`. `null` when absent
    /// or when the adapter did not emit a row-count stat for this snapshot.
    pub row_count_stat: Option<i64>,
    /// Size in bytes from `dbt.catalog_stats`. `null` same as `row_count_stat`.
    pub bytes_stat: Option<i64>,
    /// Last-modified timestamp from `dbt.catalog_stats` where
    /// `stat_id = 'last_modified'`. Snowflake-only — always `null` on
    /// other adapters.
    pub last_modified_stat: Option<String>,
}

/// ADR-6 cursor-paginated response for `GET /api/v1/snapshots`.
#[derive(Serialize)]
pub struct SnapshotListResponse {
    pub data: Vec<SnapshotSummary>,
    pub page_info: PageInfo,
}

/// Response body for `GET /api/v1/snapshots/facets`.
///
/// Snapshots expose no filter dropdowns in dbt-ui (`SnapshotFilterView.tsx`),
/// so this struct is empty. The endpoint exists for API uniformity.
#[derive(Serialize)]
pub struct SnapshotFacetsResponse {}

/// Query parameters for `GET /api/v1/snapshots`.
#[derive(Debug, Default, Deserialize)]
pub struct SnapshotListParams {
    /// Sort spec `<column>:<asc|desc>`. Allowlisted: `name`, `package_name`,
    /// `updated_at`. Default: `name:asc`. Unknown column → 400.
    pub sort: Option<String>,
    pub first: Option<u32>,
    pub after: Option<String>,
}

// ---------------------------------------------------------------------------
// SQL: GET /api/v1/snapshots
// ---------------------------------------------------------------------------

/// Allowlisted sort columns for snapshots.
///
/// Each entry is `(query_param_name, sql_expression)`. The expression must be
/// usable in both `ORDER BY` and the cursor `WHERE` predicate.
const SORTABLE_COLUMNS: &[(&str, &str)] = &[
    ("name", "n.name"),
    ("package_name", "n.package_name"),
    (
        "updated_at",
        "json_extract_string(n.config, '$.updated_at')",
    ),
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

/// Build `(count_sql, rows_sql)` for `GET /api/v1/snapshots`.
///
/// `with_run_results` controls whether `dbt_rt.run_results` is joined.
/// `with_catalog` controls whether `dbt.catalog_tables` / `dbt.catalog_stats`
/// are joined. When `false`, the respective columns are `NULL`-typed so the
/// Arrow column type is stable across fallback paths.
pub(crate) fn build_snapshot_list_sql(
    params: &SnapshotListParams,
    with_run_results: bool,
    with_catalog: bool,
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

    let filter_where = "WHERE n.resource_type = 'snapshot'";

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

    // CTE header depends on which optional tables are available.
    let cte_prefix = match (with_run_results, with_catalog) {
        (true, true) => "WITH last_run AS (\
              SELECT unique_id, \
                     MAX(created_at) AS completed_at, \
                     FIRST(status ORDER BY created_at DESC) AS status, \
                     FIRST(message ORDER BY created_at DESC) AS error \
              FROM dbt_rt.run_results GROUP BY unique_id\
            ), cat_tables AS (\
              SELECT unique_id FROM dbt.catalog_tables\
            ), row_count_cte AS (\
              SELECT unique_id, \
                     TRY_CAST(stat_value AS BIGINT) AS row_count_stat \
              FROM dbt.catalog_stats WHERE stat_id IN ('row_count', 'num_rows')\
            ), bytes_cte AS (\
              SELECT unique_id, \
                     TRY_CAST(stat_value AS BIGINT) AS bytes_stat \
              FROM dbt.catalog_stats WHERE stat_id IN ('bytes', 'num_bytes')\
            ), last_modified_cte AS (\
              SELECT unique_id, stat_value AS last_modified_stat \
              FROM dbt.catalog_stats WHERE stat_id = 'last_modified'\
            )\n"
        .to_owned(),
        (true, false) => "WITH last_run AS (\
              SELECT unique_id, \
                     MAX(created_at) AS completed_at, \
                     FIRST(status ORDER BY created_at DESC) AS status, \
                     FIRST(message ORDER BY created_at DESC) AS error \
              FROM dbt_rt.run_results GROUP BY unique_id\
            )\n"
        .to_owned(),
        (false, true) => "WITH cat_tables AS (\
              SELECT unique_id FROM dbt.catalog_tables\
            ), row_count_cte AS (\
              SELECT unique_id, \
                     TRY_CAST(stat_value AS BIGINT) AS row_count_stat \
              FROM dbt.catalog_stats WHERE stat_id IN ('row_count', 'num_rows')\
            ), bytes_cte AS (\
              SELECT unique_id, \
                     TRY_CAST(stat_value AS BIGINT) AS bytes_stat \
              FROM dbt.catalog_stats WHERE stat_id IN ('bytes', 'num_bytes')\
            ), last_modified_cte AS (\
              SELECT unique_id, stat_value AS last_modified_stat \
              FROM dbt.catalog_stats WHERE stat_id = 'last_modified'\
            )\n"
        .to_owned(),
        (false, false) => String::new(),
    };

    let lr_join = if with_run_results {
        "LEFT JOIN last_run lr ON lr.unique_id = n.unique_id"
    } else {
        ""
    };
    let execution_info_cols = if with_run_results {
        "CAST(lr.completed_at AS VARCHAR) AS ei_completed_at, \
         lr.status AS ei_status, \
         lr.error AS ei_error, \
         CASE WHEN lr.unique_id IS NOT NULL THEN 1 ELSE 0 END AS has_run_result"
    } else {
        "NULL::VARCHAR AS ei_completed_at, \
         NULL::VARCHAR AS ei_status, \
         NULL::VARCHAR AS ei_error, \
         0 AS has_run_result"
    };

    let (cat_joins, catalog_cols) = if with_catalog {
        (
            "LEFT JOIN cat_tables ct ON ct.unique_id = n.unique_id \
             LEFT JOIN row_count_cte rc ON rc.unique_id = n.unique_id \
             LEFT JOIN bytes_cte bc ON bc.unique_id = n.unique_id \
             LEFT JOIN last_modified_cte lm ON lm.unique_id = n.unique_id",
            "CASE WHEN ct.unique_id IS NOT NULL THEN 1 ELSE 0 END AS has_catalog, \
             rc.row_count_stat, \
             bc.bytes_stat, \
             lm.last_modified_stat",
        )
    } else {
        (
            "",
            "0 AS has_catalog, \
             NULL::BIGINT AS row_count_stat, \
             NULL::BIGINT AS bytes_stat, \
             NULL::VARCHAR AS last_modified_stat",
        )
    };

    let count_sql = format!(
        "{cte_prefix}SELECT count(*) \
         FROM dbt.nodes n {lr_join} {cat_joins} \
         {filter_where}"
    );
    let peek = first + 1;
    let order_expr = sort.sql_expr;
    let order_dir = sort.dir.as_sql();
    let rows_sql = format!(
        "{cte_prefix}SELECT n.unique_id, n.name, n.resource_type, n.package_name, \
                n.materialized, \
                json_extract_string(n.config, '$.strategy') AS strategy, \
                json_extract_string(n.config, '$.updated_at') AS config_updated_at, \
                {execution_info_cols}, \
                {catalog_cols} \
         FROM dbt.nodes n {lr_join} {cat_joins} \
         {page_where} \
         ORDER BY {order_expr} {order_dir} NULLS LAST, n.unique_id ASC \
         LIMIT {peek}"
    );

    Ok((count_sql, rows_sql))
}

// ---------------------------------------------------------------------------
// Extraction helpers: GET /api/v1/snapshots
// ---------------------------------------------------------------------------

fn batches_to_snapshot_summary_rows(batches: &[RecordBatch]) -> Vec<SnapshotSummary> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let unique_id = str_col(batch, "unique_id");
        let name = str_col(batch, "name");
        let resource_type = str_col(batch, "resource_type");
        let package_name = str_col(batch, "package_name");
        let materialized = str_col(batch, "materialized");
        let strategy = str_col(batch, "strategy");
        let config_updated_at = str_col(batch, "config_updated_at");
        let ei_status = str_col(batch, "ei_status");
        let ei_completed_at = str_col(batch, "ei_completed_at");
        let ei_error = str_col(batch, "ei_error");
        let row_count_col = batch
            .column_by_name("row_count_stat")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let bytes_col = batch
            .column_by_name("bytes_stat")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let last_modified = str_col(batch, "last_modified_stat");

        // has_run_result / has_catalog are Int32 (CASE WHEN ... THEN 1 ELSE 0)
        let has_run_result_col = batch
            .column_by_name("has_run_result")
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::Int32Array>());
        let has_catalog_col = batch
            .column_by_name("has_catalog")
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::Int32Array>());

        for i in 0..batch.num_rows() {
            let has_run = has_run_result_col
                .map(|c| !c.is_null(i) && c.value(i) != 0)
                .unwrap_or(false);
            let has_cat = has_catalog_col
                .map(|c| !c.is_null(i) && c.value(i) != 0)
                .unwrap_or(false);

            let execution_info = if has_run {
                Some(SnapshotListExecutionInfo {
                    status: opt_str(ei_status, i),
                    completed_at: opt_str(ei_completed_at, i),
                    error: opt_str(ei_error, i),
                })
            } else {
                None
            };

            let catalog = if has_cat {
                Some(SnapshotListCatalogInfo {
                    row_count_stat: row_count_col
                        .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) }),
                    bytes_stat: bytes_col
                        .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) }),
                    last_modified_stat: opt_str(last_modified, i),
                })
            } else {
                None
            };

            rows.push(SnapshotSummary {
                unique_id: unique_id.value(i).to_owned(),
                name: name.value(i).to_owned(),
                resource_type: resource_type.value(i).to_owned(),
                package_name: opt_str(package_name, i),
                materialized: opt_str(materialized, i),
                strategy: opt_str(strategy, i),
                updated_at: opt_str(config_updated_at, i),
                execution_info,
                catalog,
            });
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// Handlers: GET /api/v1/snapshots and GET /api/v1/snapshots/facets
// ---------------------------------------------------------------------------

/// `GET /api/v1/snapshots` — cursor-paginated list of snapshot nodes.
///
/// Default sort is `name:asc`; allowlisted sort columns are `name`,
/// `package_name`, and `updated_at`. Unknown sort column → 400.
/// No filter parameters.
/// `execution_info` is `null` when `dbt_rt.run_results` is absent or has
/// no row for the snapshot. `catalog` is `null` when `dbt.catalog_tables`
/// is absent or has no row for the snapshot.
pub async fn list_snapshots(
    State(state): State<SharedState>,
    Query(params): Query<SnapshotListParams>,
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
        match build_snapshot_list_sql(&params, true, true, first, cursor.as_ref()) {
            Ok(pair) => pair,
            Err(msg) => return bad_request(msg),
        };
    // Fallback variants (params already validated above).
    let (count_sql_no_rr, rows_sql_no_rr) =
        build_snapshot_list_sql(&params, false, true, first, cursor.as_ref())
            .expect("params already validated");
    let (count_sql_bare, rows_sql_bare) =
        build_snapshot_list_sql(&params, false, false, first, cursor.as_ref())
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
                    .map_err(|e| format!("could not parse snapshot count: {e}"))?;
                let batches = backend.query_arrow(&rows_sql).map_err(|e| e.to_string())?;
                (total, batches)
            }
            None => {
                // run_results view absent; retry without it but keep catalog.
                match backend.query_scalar(&count_sql_no_rr) {
                    Some(count_str) => {
                        let total = count_str
                            .parse::<u64>()
                            .map_err(|e| format!("could not parse snapshot count: {e}"))?;
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
                            .map_err(|e| format!("could not parse snapshot count: {e}"))?;
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

    let mut rows = batches_to_snapshot_summary_rows(&batches);

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

    let sort_value_for = |row: &SnapshotSummary| -> Option<String> {
        match sort_column {
            "name" => Some(row.name.clone()),
            "package_name" => row.package_name.clone(),
            "updated_at" => row.updated_at.clone(),
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

    Json(SnapshotListResponse {
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

/// `GET /api/v1/snapshots/facets` — filter facet values for the snapshots list.
///
/// Snapshots expose no filter dropdowns in dbt-ui, so this returns `{}`. The
/// endpoint exists for API uniformity so clients can hit
/// `GET /api/v1/<resource>/facets` for every resource without special-casing snapshots.
pub async fn list_snapshot_facets() -> Response {
    Json(SnapshotFacetsResponse {}).into_response()
}

#[cfg(test)]
#[path = "snapshots_tests.rs"]
mod tests;
