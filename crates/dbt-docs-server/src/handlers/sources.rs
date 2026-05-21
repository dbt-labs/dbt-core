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

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use axum::Json;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::handlers::json::{bad_request, internal_error, json_parse_or_null, not_found};
use crate::handlers::node_base::{
    EdgeRef, NodeBase, extract_edge_refs, extract_str_list, opt_str, str_col,
};
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

#[cfg(test)]
#[path = "sources_tests.rs"]
mod tests;
