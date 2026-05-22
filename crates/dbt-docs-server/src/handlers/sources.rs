//! `GET /api/v1/sources/:id` — typed source detail.
//!
//! Source-shaped vs. model-shaped: no `depends_on` (sources are leaf
//! upstream nodes), no `execution_info` (sources aren't executed by
//! `dbt build`); `freshness` and `catalog` are the optional inline
//! sub-objects, emitted as JSON `null` when no row exists in the
//! corresponding parquet view. `SourceCatalogInfo` adds `comment`,
//! `primary_key`, and `stats[]` over the model catalog shape.
//!
//! `meta` is a JSON-string parquet column; it's deserialized handler-side
//! via [`crate::handlers::json::json_parse_or_null`] so the response carries
//! a real JSON object, not an escaped string.
//!
//! Data sources:
//! - `dbt.nodes` — node row (filtered to `resource_type = 'source'`)
//! - `dbt.node_columns` — `columns[]`
//! - `dbt.edges` — `referenced_by` (downstream only)
//! - `dbt.source_freshness` — `freshness` (optional)
//! - `dbt.catalog_tables` + `dbt.catalog_stats` — `catalog` (optional)

use std::fmt::Write as _;

use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::handlers::json::{bad_request, internal_error, json_parse_or_null, not_found};
use crate::handlers::node_base::{
    EdgeRef, NodeBase, extract_edge_refs, extract_str_list, opt_str, str_col,
};
use crate::handlers::pagination::{Cursor, PageInfo, SortDir, clamp_first, cursor_where_fragment};
use crate::handlers::sql::escape_str;
use crate::state::SharedState;

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Response body for `GET /api/v1/sources/:id`.
#[derive(Serialize)]
pub struct SourceDetail {
    #[serde(flatten)]
    pub base: NodeBase,
    /// Project-relative path of the `.yml` containing the source block —
    /// equal to `original_file_path` for sources (YAML-only resources).
    pub file_path: Option<String>,
    pub tags: Vec<String>,
    pub fqn: Vec<String>,
    pub database_name: Option<String>,
    pub schema_name: Option<String>,
    pub identifier: Option<String>,
    /// dbt source block name (e.g., `"raw_jaffle"`).
    pub source_name: Option<String>,
    /// Block-level description from YAML.
    pub source_description: Option<String>,
    pub loader: Option<String>,
    /// Parsed JSON object, or `null` when absent / unparseable.
    pub meta: serde_json::Value,
    /// Downstream consumers. Sources have no `depends_on` (omitted entirely,
    /// not returned as `[]`).
    pub referenced_by: Vec<EdgeRef>,
    pub columns: Vec<SourceColumn>,
    /// `null` when `dbt.source_freshness` has no row for this source.
    pub freshness: Option<FreshnessInfo>,
    /// `null` when `dbt.catalog_tables` has no row for this source.
    pub catalog: Option<SourceCatalogInfo>,
}

#[derive(Serialize)]
pub struct SourceColumn {
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
pub struct FreshnessInfo {
    pub status: String,
    pub snapshotted_at: Option<String>,
    pub max_loaded_at: Option<String>,
    pub max_loaded_at_time_ago: Option<f64>,
    pub criteria: Option<FreshnessCriteria>,
}

#[derive(Serialize)]
pub struct FreshnessCriteria {
    pub error_after: Option<FreshnessThreshold>,
    pub warn_after: Option<FreshnessThreshold>,
}

#[derive(Serialize)]
pub struct FreshnessThreshold {
    pub count: Option<i64>,
    pub period: Option<String>,
}

/// Source-specific catalog: adds `comment`, `primary_key`, and `stats[]`
/// over the model catalog. `primary_key` is sourced from
/// `dbt.nodes.primary_key` (a `List<String>` column) — `dbt.catalog_tables`
/// has no `primary_key` column.
#[derive(Serialize)]
pub struct SourceCatalogInfo {
    #[serde(rename = "type")]
    pub table_type: Option<String>,
    pub owner: Option<String>,
    pub comment: Option<String>,
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
//
// Each query is a single `SELECT`; the handler dispatches them inside one
// `spawn_blocking`. JSON-string columns (`meta`) are returned as raw strings
// and parsed handler-side.

const SOURCE_DETAIL_NODE_SQL: &str = "\
SELECT n.unique_id, n.name, n.resource_type, n.package_name, n.description, \
       n.original_file_path, n.file_path, \
       n.database_name, n.schema_name, n.identifier, \
       n.source_name, n.source_description, n.loader, n.meta, \
       n.tags, n.fqn, n.primary_key \
FROM dbt.nodes n \
WHERE n.unique_id = '{id}' AND n.resource_type = 'source' \
LIMIT 1";

const SOURCE_DETAIL_FRESHNESS_SQL: &str = "\
SELECT status, \
       CAST(snapshotted_at AS VARCHAR) AS snapshotted_at, \
       CAST(max_loaded_at AS VARCHAR) AS max_loaded_at, \
       max_loaded_at_time_ago, \
       warn_after_count, warn_after_period, \
       error_after_count, error_after_period \
FROM dbt.source_freshness \
WHERE unique_id = '{id}' \
ORDER BY created_at DESC \
LIMIT 1";

const SOURCE_DETAIL_CATALOG_SQL: &str = "\
SELECT table_type AS type, \
       table_owner AS owner, \
       table_comment AS comment, \
       NULL::BIGINT AS bytes_stat, \
       NULL::BIGINT AS row_count_stat \
FROM dbt.catalog_tables \
WHERE unique_id = '{id}' \
LIMIT 1";

// Catalog stats are independently keyed — adapter-specific stat_id values.
// Always queried alongside catalog_tables; empty result → `stats: []`.
const SOURCE_DETAIL_CATALOG_STATS_SQL: &str = "\
SELECT stat_id AS id, stat_label AS label, stat_value AS value, \
       description, include_in_stats AS include \
FROM dbt.catalog_stats \
WHERE unique_id = '{id}' \
ORDER BY stat_id";

// ---------------------------------------------------------------------------
// Extractors
// ---------------------------------------------------------------------------

fn extract_source_detail(batches: &[RecordBatch]) -> Option<(SourceDetail, Vec<String>)> {
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

    let detail = SourceDetail {
        base: NodeBase {
            unique_id: s("unique_id").unwrap_or_default(),
            name: s("name").unwrap_or_default(),
            resource_type: s("resource_type").unwrap_or_default(),
            package_name: s("package_name"),
            description: s("description"),
            original_file_path: s("original_file_path"),
        },
        file_path: s("file_path"),
        tags: extract_str_list(batch, "tags"),
        fqn: extract_str_list(batch, "fqn"),
        database_name: s("database_name"),
        schema_name: s("schema_name"),
        identifier: s("identifier"),
        source_name: s("source_name"),
        source_description: s("source_description"),
        loader: s("loader"),
        meta,
        // Sub-resources populated after extraction.
        referenced_by: vec![],
        columns: vec![],
        freshness: None,
        catalog: None,
    };
    Some((detail, primary_key))
}

fn extract_source_columns(batches: &[RecordBatch]) -> Vec<SourceColumn> {
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
            rows.push(SourceColumn {
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

fn extract_freshness_info(batches: &[RecordBatch]) -> Option<FreshnessInfo> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;

    let status_col = batch
        .column_by_name("status")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())?;
    let snapshotted_at_col = batch
        .column_by_name("snapshotted_at")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let max_loaded_at_col = batch
        .column_by_name("max_loaded_at")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let max_loaded_at_time_ago_col = batch
        .column_by_name("max_loaded_at_time_ago")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>());

    let int_col = |name: &'static str| -> Option<i64> {
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
    let str_col_opt = |name: &'static str| -> Option<String> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<StringArray>()?;
        opt_str(col, 0)
    };

    let warn_count = int_col("warn_after_count");
    let warn_period = str_col_opt("warn_after_period");
    let error_count = int_col("error_after_count");
    let error_period = str_col_opt("error_after_period");

    let warn_after = if warn_count.is_some() || warn_period.is_some() {
        Some(FreshnessThreshold {
            count: warn_count,
            period: warn_period,
        })
    } else {
        None
    };
    let error_after = if error_count.is_some() || error_period.is_some() {
        Some(FreshnessThreshold {
            count: error_count,
            period: error_period,
        })
    } else {
        None
    };
    let criteria = if warn_after.is_some() || error_after.is_some() {
        Some(FreshnessCriteria {
            error_after,
            warn_after,
        })
    } else {
        None
    };

    Some(FreshnessInfo {
        // Defaults to empty string rather than panicking if the column is
        // unexpectedly null — surfaces the bug to the FE without 500ing.
        status: opt_str(status_col, 0).unwrap_or_default(),
        snapshotted_at: snapshotted_at_col.and_then(|c| opt_str(c, 0)),
        max_loaded_at: max_loaded_at_col.and_then(|c| opt_str(c, 0)),
        max_loaded_at_time_ago: max_loaded_at_time_ago_col
            .and_then(|c| if c.is_null(0) { None } else { Some(c.value(0)) }),
        criteria,
    })
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

fn extract_source_catalog(
    table_batches: &[RecordBatch],
    stats_batches: &[RecordBatch],
    primary_key: Vec<String>,
) -> Option<SourceCatalogInfo> {
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

    Some(SourceCatalogInfo {
        table_type: s("type"),
        owner: s("owner"),
        comment: s("comment"),
        primary_key,
        bytes_stat: i("bytes_stat"),
        row_count_stat: i("row_count_stat"),
        stats: extract_catalog_stats(stats_batches),
    })
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `GET /api/v1/sources/:id` — full source detail.
///
/// `freshness` is `null` when `dbt.source_freshness` has no row for this
/// source; `catalog` is `null` when `dbt.catalog_tables` has no row.
/// Sources never carry `depends_on` — the field is omitted, not returned
/// as `[]`. `referenced_by` is unbounded.
pub async fn get_source(
    State(state): State<SharedState>,
    Path(unique_id): Path<String>,
) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }
    let id = escape_str(&unique_id);

    let node_sql = SOURCE_DETAIL_NODE_SQL.replace("{id}", &id);
    let columns_sql = format!(
        "SELECT column_name AS name, column_index AS index, \
                data_type, declared_type, inferred_type, catalog_type, \
                description, label, granularity \
         FROM dbt.node_columns WHERE unique_id = '{id}' \
         ORDER BY column_index NULLS LAST, column_name"
    );
    // Sources are upstream-only; only downstream edges are meaningful.
    let downstream_sql = format!(
        "SELECT child_unique_id AS unique_id, edge_type \
         FROM dbt.edges WHERE parent_unique_id = '{id}' \
         ORDER BY child_unique_id"
    );
    let freshness_sql = SOURCE_DETAIL_FRESHNESS_SQL.replace("{id}", &id);
    let catalog_sql = SOURCE_DETAIL_CATALOG_SQL.replace("{id}", &id);
    let catalog_stats_sql = SOURCE_DETAIL_CATALOG_STATS_SQL.replace("{id}", &id);

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
        let freshness_batches = backend.query_arrow(&freshness_sql).ok();
        let catalog_batches = backend.query_arrow(&catalog_sql).ok();
        let catalog_stats_batches = backend.query_arrow(&catalog_stats_sql).ok();
        Ok((
            node_batches,
            column_batches,
            downstream_batches,
            freshness_batches,
            catalog_batches,
            catalog_stats_batches,
        ))
    })
    .await;

    let (
        node_batches,
        column_batches,
        downstream_batches,
        freshness_batches,
        catalog_batches,
        catalog_stats_batches,
    ) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let Some((mut detail, primary_key)) = extract_source_detail(&node_batches) else {
        return not_found(format!("source {unique_id} not found"));
    };

    detail.columns = extract_source_columns(&column_batches);
    detail.referenced_by = extract_edge_refs(&downstream_batches);
    detail.freshness = freshness_batches
        .as_deref()
        .and_then(extract_freshness_info);
    detail.catalog = match (catalog_batches.as_deref(), catalog_stats_batches.as_deref()) {
        (Some(t), stats_opt) => extract_source_catalog(t, stats_opt.unwrap_or(&[]), primary_key),
        // No catalog_tables view at all — no catalog block.
        (None, _) => None,
    };

    Json(detail).into_response()
}

// ---------------------------------------------------------------------------
// Response types: GET /api/v1/sources and GET /api/v1/sources/facets
// ---------------------------------------------------------------------------

/// Simplified freshness for list rows — omits `criteria` and
/// `max_loaded_at_time_ago` which are detail-only.
#[derive(Serialize)]
pub struct SourceListFreshness {
    pub status: String,
    pub snapshotted_at: Option<String>,
    pub max_loaded_at: Option<String>,
}

/// One row in `GET /api/v1/sources`.
#[derive(Serialize)]
pub struct SourceSummary {
    pub unique_id: String,
    pub name: String,
    pub resource_type: String,
    pub package_name: Option<String>,
    pub source_name: Option<String>,
    pub source_description: Option<String>,
    pub database_name: Option<String>,
    pub schema_name: Option<String>,
    pub identifier: Option<String>,
    pub loader: Option<String>,
    pub tags: Vec<String>,
    /// `null` when no `dbt.source_freshness` row exists for this `unique_id`,
    /// or when the parquet is absent from the index.
    pub freshness: Option<SourceListFreshness>,
}

/// ADR-6 cursor-paginated response for `GET /api/v1/sources`.
#[derive(Serialize)]
pub struct SourceListResponse {
    pub data: Vec<SourceSummary>,
    pub page_info: PageInfo,
}

/// One facet option; `count` is always `null` today (reserved).
#[derive(Serialize)]
pub struct SourceFacetValue {
    pub value: String,
    pub count: Option<u64>,
}

impl SourceFacetValue {
    fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            count: None,
        }
    }
}

/// Response body for `GET /api/v1/sources/facets`.
#[derive(Serialize)]
pub struct SourceFacetsResponse {
    pub freshness_status: Vec<SourceFacetValue>,
    pub databases: Vec<SourceFacetValue>,
    pub schemas: Vec<SourceFacetValue>,
}

/// Query parameters for `GET /api/v1/sources`.
#[derive(Debug, Default, Deserialize)]
pub struct SourceListParams {
    pub freshness_status: Option<String>,
    pub databases: Option<String>,
    pub schemas: Option<String>,
    /// Rejected with 400 — sources expose no sort UI.
    pub sort: Option<String>,
    pub first: Option<u32>,
    pub after: Option<String>,
}

// ---------------------------------------------------------------------------
// SQL: GET /api/v1/sources
// ---------------------------------------------------------------------------

/// `freshness_status` values in dbt-ui dropdown order (no_data sentinel first).
const FRESHNESS_STATUS_FACET_VALUES: &[&str] =
    &["no_data", "pass", "warn", "error", "runtime_error"];

const VALID_FRESHNESS_STATUSES: &[&str] = &["pass", "warn", "error", "runtime_error", "no_data"];

const DATABASES_FACET_SQL: &str = "\
SELECT DISTINCT database_name AS value \
FROM dbt.nodes \
WHERE resource_type = 'source' AND database_name IS NOT NULL \
ORDER BY value";

const SCHEMAS_FACET_SQL: &str = "\
SELECT DISTINCT schema_name AS value \
FROM dbt.nodes \
WHERE resource_type = 'source' AND schema_name IS NOT NULL \
ORDER BY value";

/// Build `(count_sql, rows_sql)` for `GET /api/v1/sources`.
///
/// `with_freshness` controls whether `dbt.source_freshness` is LEFT-JOINed.
/// When `false`, freshness columns are `NULL::VARCHAR` and the freshness filter
/// degenerates: `no_data` matches all rows, specific statuses match none.
///
/// The count query excludes the cursor predicate so `total_count` reflects the
/// full filter-matching set per ADR-6.
pub(crate) fn build_source_list_sql(
    params: &SourceListParams,
    with_freshness: bool,
    first: u32,
    cursor: Option<&Cursor>,
) -> Result<(String, String), &'static str> {
    if params.sort.as_deref().filter(|s| !s.is_empty()).is_some() {
        return Err("sort is not supported on this endpoint");
    }

    let mut filter_where = String::from("WHERE n.resource_type = 'source'");

    if let Some(raw) = params.freshness_status.as_deref().filter(|s| !s.is_empty()) {
        if let Some(pred) = build_freshness_predicate(raw, with_freshness)? {
            let _ = write!(filter_where, " AND {pred}");
        }
    }
    if let Some(raw) = params.databases.as_deref().filter(|s| !s.is_empty()) {
        let list = csv_sql_list(raw);
        let _ = write!(filter_where, " AND n.database_name IN ({list})");
    }
    if let Some(raw) = params.schemas.as_deref().filter(|s| !s.is_empty()) {
        let list = csv_sql_list(raw);
        let _ = write!(filter_where, " AND n.schema_name IN ({list})");
    }

    let mut page_where = filter_where.clone();
    if let Some(c) = cursor {
        let frag = cursor_where_fragment(
            "n.name",
            "n.unique_id",
            SortDir::Asc,
            c.sort_value.as_deref(),
            &c.unique_id,
        );
        let _ = write!(page_where, " AND {frag}");
    }

    let (freshness_join, freshness_cols) = if with_freshness {
        (
            "LEFT JOIN dbt.source_freshness sf ON sf.unique_id = n.unique_id",
            "CASE sf.status WHEN 'runtime error' THEN 'runtime_error' \
             ELSE sf.status END AS freshness_status, \
             CAST(sf.snapshotted_at AS VARCHAR) AS freshness_snapshotted_at, \
             CAST(sf.max_loaded_at AS VARCHAR) AS freshness_max_loaded_at",
        )
    } else {
        (
            "",
            "NULL::VARCHAR AS freshness_status, \
             NULL::VARCHAR AS freshness_snapshotted_at, \
             NULL::VARCHAR AS freshness_max_loaded_at",
        )
    };

    let count_sql = format!("SELECT count(*) FROM dbt.nodes n {freshness_join} {filter_where}");
    let peek = first + 1;
    let rows_sql = format!(
        "SELECT n.unique_id, n.name, n.resource_type, n.package_name, \
                n.source_name, n.source_description, n.database_name, n.schema_name, \
                n.identifier, n.loader, n.tags, \
                {freshness_cols} \
         FROM dbt.nodes n {freshness_join} {page_where} \
         ORDER BY n.name ASC NULLS LAST, n.unique_id ASC \
         LIMIT {peek}"
    );

    Ok((count_sql, rows_sql))
}

/// Build the SQL freshness_status predicate fragment, or `None` when the
/// predicate is vacuously true (view absent + `no_data` only).
pub(crate) fn build_freshness_predicate(
    raw: &str,
    with_freshness: bool,
) -> Result<Option<String>, &'static str> {
    let values: Vec<&str> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if values.is_empty() {
        return Err("freshness_status must not be empty");
    }
    for v in &values {
        if !VALID_FRESHNESS_STATUSES.contains(v) {
            return Err("invalid freshness_status value");
        }
    }

    if !with_freshness {
        return if values.contains(&"no_data") {
            Ok(None)
        } else {
            Ok(Some("1=0".to_owned()))
        };
    }

    let mut status_sql: Vec<String> = vec![];
    let mut has_no_data = false;
    for v in &values {
        match *v {
            "no_data" => has_no_data = true,
            "runtime_error" => status_sql.push("'runtime error'".to_owned()),
            s => status_sql.push(format!("'{}'", escape_str(s))),
        }
    }

    let mut clauses: Vec<String> = vec![];
    if !status_sql.is_empty() {
        clauses.push(format!("sf.status IN ({})", status_sql.join(", ")));
    }
    if has_no_data {
        clauses.push("sf.unique_id IS NULL".to_owned());
    }

    Ok(Some(format!("({})", clauses.join(" OR "))))
}

fn csv_sql_list(raw: &str) -> String {
    raw.split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| format!("'{}'", escape_str(v)))
        .collect::<Vec<_>>()
        .join(", ")
}

// ---------------------------------------------------------------------------
// Extraction helpers
// ---------------------------------------------------------------------------

fn extract_str_list_at(batch: &RecordBatch, col_name: &'static str, row: usize) -> Vec<String> {
    let Some(col) = batch.column_by_name(col_name) else {
        return vec![];
    };
    if col.is_null(row) {
        return vec![];
    }
    let Some(list) = col.as_any().downcast_ref::<ListArray>() else {
        return vec![];
    };
    let inner = list.value(row);
    let Some(strings) = inner.as_any().downcast_ref::<StringArray>() else {
        return vec![];
    };
    (0..strings.len())
        .filter(|&i| !strings.is_null(i))
        .map(|i| strings.value(i).to_owned())
        .collect()
}

fn batches_to_source_summary_rows(batches: &[RecordBatch]) -> Vec<SourceSummary> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let unique_id = str_col(batch, "unique_id");
        let name = str_col(batch, "name");
        let resource_type = str_col(batch, "resource_type");
        let package_name = str_col(batch, "package_name");
        let source_name = str_col(batch, "source_name");
        let source_description = str_col(batch, "source_description");
        let database_name = str_col(batch, "database_name");
        let schema_name = str_col(batch, "schema_name");
        let identifier = str_col(batch, "identifier");
        let loader = str_col(batch, "loader");
        let freshness_status = str_col(batch, "freshness_status");
        let freshness_snapshotted_at = str_col(batch, "freshness_snapshotted_at");
        let freshness_max_loaded_at = str_col(batch, "freshness_max_loaded_at");

        for i in 0..batch.num_rows() {
            let freshness = if freshness_status.is_null(i) {
                None
            } else {
                Some(SourceListFreshness {
                    status: freshness_status.value(i).to_owned(),
                    snapshotted_at: opt_str(freshness_snapshotted_at, i),
                    max_loaded_at: opt_str(freshness_max_loaded_at, i),
                })
            };
            rows.push(SourceSummary {
                unique_id: unique_id.value(i).to_owned(),
                name: name.value(i).to_owned(),
                resource_type: resource_type.value(i).to_owned(),
                package_name: opt_str(package_name, i),
                source_name: opt_str(source_name, i),
                source_description: opt_str(source_description, i),
                database_name: opt_str(database_name, i),
                schema_name: opt_str(schema_name, i),
                identifier: opt_str(identifier, i),
                loader: opt_str(loader, i),
                tags: extract_str_list_at(batch, "tags", i),
                freshness,
            });
        }
    }
    rows
}

fn batches_to_source_facet_values(batches: &[RecordBatch]) -> Vec<SourceFacetValue> {
    let mut values = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = str_col(batch, "value");
        for i in 0..batch.num_rows() {
            if !col.is_null(i) {
                values.push(SourceFacetValue::new(col.value(i)));
            }
        }
    }
    values
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /api/v1/sources` — cursor-paginated, filterable list of source nodes.
///
/// Sort is fixed at `name:asc`; `?sort` returns 400.
/// Filters: `freshness_status`, `databases`, `schemas`.
/// `freshness` per row is `null` when `dbt.source_freshness` has no match or
/// the parquet is absent — no capability gate.
pub async fn list_sources(
    State(state): State<SharedState>,
    Query(params): Query<SourceListParams>,
) -> Response {
    let first = clamp_first(params.first);

    let cursor = match params.after.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => match Cursor::decode(s) {
            Ok(c) => Some(c),
            Err(msg) => return bad_request(msg),
        },
        None => None,
    };

    let (count_sql, rows_sql) = match build_source_list_sql(&params, true, first, cursor.as_ref()) {
        Ok(pair) => pair,
        Err(msg) => return bad_request(msg),
    };
    let (count_sql_no_sf, rows_sql_no_sf) =
        build_source_list_sql(&params, false, first, cursor.as_ref())
            .expect("params already validated");

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let (total, batches) = match backend.query_scalar(&count_sql) {
            Some(count_str) => {
                let total = count_str
                    .parse::<u64>()
                    .map_err(|e| format!("could not parse source count: {e}"))?;
                let batches = backend.query_arrow(&rows_sql).map_err(|e| e.to_string())?;
                (total, batches)
            }
            None => {
                let total = backend
                    .query_scalar(&count_sql_no_sf)
                    .ok_or_else(|| "count query returned no rows".to_string())?
                    .parse::<u64>()
                    .map_err(|e| format!("could not parse source count: {e}"))?;
                let batches = backend
                    .query_arrow(&rows_sql_no_sf)
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

    let mut rows = batches_to_source_summary_rows(&batches);

    let has_next_page = rows.len() as u32 > first;
    if has_next_page {
        rows.truncate(first as usize);
    }

    let start_cursor = rows.first().map(|row| {
        Cursor {
            sort_value: Some(row.name.clone()),
            unique_id: row.unique_id.clone(),
        }
        .encode()
    });
    let end_cursor = if has_next_page {
        rows.last().map(|row| {
            Cursor {
                sort_value: Some(row.name.clone()),
                unique_id: row.unique_id.clone(),
            }
            .encode()
        })
    } else {
        None
    };

    Json(SourceListResponse {
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

/// `GET /api/v1/sources/facets` — filter facet values for the sources list.
///
/// `freshness_status` is static; `databases` and `schemas` come from `dbt.nodes`.
/// All `count` fields are `null` today.
pub async fn list_source_facets(State(state): State<SharedState>) -> Response {
    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let db_batches = backend
            .query_arrow(DATABASES_FACET_SQL)
            .map_err(|e| e.to_string())?;
        let schema_batches = backend
            .query_arrow(SCHEMAS_FACET_SQL)
            .map_err(|e| e.to_string())?;
        Ok((db_batches, schema_batches))
    })
    .await;

    let (db_batches, schema_batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    Json(SourceFacetsResponse {
        freshness_status: FRESHNESS_STATUS_FACET_VALUES
            .iter()
            .map(|v| SourceFacetValue::new(*v))
            .collect(),
        databases: batches_to_source_facet_values(&db_batches),
        schemas: batches_to_source_facet_values(&schema_batches),
    })
    .into_response()
}

#[cfg(test)]
#[path = "sources_tests.rs"]
mod tests;
