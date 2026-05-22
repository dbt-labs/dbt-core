//! `GET /api/v1/seeds/:id` — typed seed detail.
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

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
use axum::Json;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::handlers::json::{bad_request, internal_error, json_parse_or_null, not_found};
use crate::handlers::node_base::{
    EdgeRef, ExecutionInfo, NodeBase, extract_edge_refs, extract_execution_info, extract_str_list,
    opt_str, str_col,
};
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

#[cfg(test)]
#[path = "seeds_tests.rs"]
mod tests;
