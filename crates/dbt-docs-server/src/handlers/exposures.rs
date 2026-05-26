//! `GET /api/v1/exposures/:id`, `GET /api/v1/exposures`, and
//! `GET /api/v1/exposures/facets`.
//!
//! Exposures are definition-only leaf consumers: no `execution_info`, no
//! `catalog`, no `columns`, no SQL body. The contract has `depends_on` but
//! no `referenced_by` (nothing refs an exposure).
//!
//! `meta` is a JSON-string parquet column; it's deserialized handler-side
//! via [`crate::handlers::json::json_parse_or_null`] so the response carries
//! a real JSON object, not an escaped string.
//!
//! `depends_on` is synthesized directly from `dbt.exposures.depends_on_nodes`
//! (a `List<Utf8>` of upstream unique_ids); `edge_type` is derived from the
//! `unique_id` prefix (`model.` → `"model"`, `source.` → `"source"`, etc.).
//!
//! Data source:
//! - `dbt.exposures` — single row per exposure (NOT `dbt.nodes`).

use std::fmt::Write as _;

use arrow_array::{Array, Float64Array, ListArray, RecordBatch, StringArray};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::handlers::json::{bad_request, internal_error, json_parse_or_null, not_found};
use crate::handlers::node_base::{EdgeRef, NodeBase, extract_str_list, opt_str};
use crate::handlers::pagination::{Cursor, PageInfo, SortDir, clamp_first, cursor_where_fragment};
use crate::handlers::sql::escape_str;
use crate::state::SharedState;

// ---------------------------------------------------------------------------
// Response types: GET /api/v1/exposures/:id
// ---------------------------------------------------------------------------

/// Response body for `GET /api/v1/exposures/:id`.
#[derive(Serialize)]
pub struct ExposureDetail {
    #[serde(flatten)]
    pub base: NodeBase,
    /// Project-relative path of the `.yml` containing the exposure block.
    pub file_path: Option<String>,
    pub tags: Vec<String>,
    pub fqn: Vec<String>,
    pub label: Option<String>,
    /// One of `"dashboard"` · `"notebook"` · `"analysis"` · `"ml"` ·
    /// `"application"`. Plain `Utf8` in parquet — not enum-validated.
    pub exposure_type: Option<String>,
    pub maturity: Option<String>,
    pub url: Option<String>,
    pub owner_name: Option<String>,
    pub owner_email: Option<String>,
    /// Parsed JSON object, or `null` when absent / unparseable.
    pub meta: serde_json::Value,
    /// 1-hop upstream refs (models, sources, metrics, seeds, …). `edge_type`
    /// is derived from the `unique_id` prefix.
    pub depends_on: Vec<EdgeRef>,
    /// Epoch seconds (float) — definition-update timestamp.
    pub created_at: Option<f64>,
}

// ---------------------------------------------------------------------------
// Response types: GET /api/v1/exposures and GET /api/v1/exposures/facets
// ---------------------------------------------------------------------------

/// One row in `GET /api/v1/exposures`.
#[derive(Serialize)]
pub struct ExposureSummary {
    pub unique_id: String,
    pub name: String,
    pub exposure_type: Option<String>,
    pub maturity: Option<String>,
    pub owner_name: Option<String>,
    pub owner_email: Option<String>,
    pub tags: Vec<String>,
    /// Epoch seconds (float); from `dbt.exposures.created_at`. Per ADR-5 the
    /// definition-time surrogate for "last updated" on definition-only resources.
    pub created_at: Option<f64>,
    /// 1-hop upstream refs; derived from `dbt.exposures.depends_on_nodes`.
    /// Capped at 500 per CC-6.
    pub depends_on: Vec<EdgeRef>,
    /// CC-6 truncation signal — `true` when the underlying upstream list
    /// exceeded 500 entries and was capped.
    pub depends_on_truncated: bool,
}

/// ADR-6 cursor-paginated response for `GET /api/v1/exposures`.
#[derive(Serialize)]
pub struct ExposureListResponse {
    pub data: Vec<ExposureSummary>,
    pub page_info: PageInfo,
}

/// A single facet option with an optional count (always `null` today).
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

/// Response body for `GET /api/v1/exposures/facets`.
///
/// Returns distinct owner names from `dbt.exposures.owner_name`. The
/// Cloud-only `auto_bi_providers` and `exposure_modes` filter facets are
/// absent — the underlying `auto_bi_provider` / `definition_type` columns do
/// not exist in `dbt.exposures.parquet` (Design note #3 in API-CONTRACTS.md).
#[derive(Serialize)]
pub struct ExposureFacetsResponse {
    pub owners: Vec<FacetValue>,
}

/// Query parameters for `GET /api/v1/exposures`.
#[derive(Debug, Default, Deserialize)]
pub struct ExposureListParams {
    /// Sort is **not accepted** on this endpoint — any value is rejected with
    /// 400. The default order is `name:asc` (internal; clients must not pass
    /// `?sort`). See Design note #1 in API-CONTRACTS.md.
    pub sort: Option<String>,
    pub first: Option<u32>,
    pub after: Option<String>,
    /// Exact-match filter against `dbt.exposures.owner_name`. Single value
    /// only — no comma-separated list. See Design note #2.
    pub owner: Option<String>,
}

// ---------------------------------------------------------------------------
// SQL: GET /api/v1/exposures/:id
// ---------------------------------------------------------------------------

const EXPOSURE_DETAIL_SQL: &str = "\
SELECT unique_id, name, package_name, description, \
       original_file_path, file_path, \
       label, exposure_type, maturity, url, \
       owner_name, owner_email, meta, \
       tags, fqn, depends_on_nodes, created_at \
FROM dbt.exposures \
WHERE unique_id = '{id}' \
LIMIT 1";

// ---------------------------------------------------------------------------
// SQL: GET /api/v1/exposures
// ---------------------------------------------------------------------------

/// CC-6 cap on `depends_on` inline edges per list row.
const DEPENDS_ON_CAP: usize = 500;

/// The only sort applied: `name ASC` (default; `?sort` is rejected entirely).
const SORT_EXPR: &str = "e.name";
const SORT_DIR: SortDir = SortDir::Asc;

/// Build `(count_sql, rows_sql)` for `GET /api/v1/exposures`.
///
/// `owner` is an optional exact-match filter on `dbt.exposures.owner_name`.
fn build_exposure_list_sql(
    owner: Option<&str>,
    first: u32,
    cursor: Option<&Cursor>,
) -> (String, String) {
    let mut filter_where = "WHERE 1=1".to_owned();
    if let Some(o) = owner {
        let _ = write!(filter_where, " AND e.owner_name = '{}'", escape_str(o));
    }

    let mut page_where = filter_where.clone();
    if let Some(c) = cursor {
        let frag = cursor_where_fragment(
            SORT_EXPR,
            "e.unique_id",
            SORT_DIR,
            c.sort_value.as_deref(),
            &c.unique_id,
        );
        let _ = write!(page_where, " AND {frag}");
    }

    let count_sql = format!("SELECT count(*) FROM dbt.exposures e {filter_where}");
    let peek = first + 1;
    // `depends_on_nodes` is a `varchar[]` column — `len(depends_on_nodes)` for
    // the truncation flag, `list_slice(depends_on_nodes, 1, 500)` for the cap.
    let rows_sql = format!(
        "SELECT e.unique_id, e.name, e.exposure_type, e.maturity, \
                e.owner_name, e.owner_email, e.tags, e.created_at, \
                list_slice(e.depends_on_nodes, 1, {DEPENDS_ON_CAP}) AS depends_on_nodes_capped, \
                len(e.depends_on_nodes) > {DEPENDS_ON_CAP} AS depends_on_truncated \
         FROM dbt.exposures e \
         {page_where} \
         ORDER BY {SORT_EXPR} ASC NULLS LAST, e.unique_id ASC \
         LIMIT {peek}"
    );
    (count_sql, rows_sql)
}

// ---------------------------------------------------------------------------
// SQL: GET /api/v1/exposures/facets
// ---------------------------------------------------------------------------

const EXPOSURE_OWNERS_FACET_SQL: &str = "\
SELECT DISTINCT owner_name AS value \
FROM dbt.exposures \
WHERE owner_name IS NOT NULL \
ORDER BY owner_name";

// ---------------------------------------------------------------------------
// Extractors: GET /api/v1/exposures/:id
// ---------------------------------------------------------------------------

/// Derive an `EdgeRef`'s `edge_type` from a `unique_id`'s prefix:
/// `model.x.y` → `"model"`, `source.x.y.z` → `"source"`, etc.
fn edge_type_from_prefix(unique_id: &str) -> String {
    unique_id
        .split_once('.')
        .map(|(prefix, _)| prefix.to_owned())
        .unwrap_or_default()
}

fn extract_exposure_detail(batches: &[RecordBatch]) -> Option<ExposureDetail> {
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

    let depends_on = extract_str_list(batch, "depends_on_nodes")
        .into_iter()
        .map(|unique_id| EdgeRef {
            edge_type: edge_type_from_prefix(&unique_id),
            unique_id,
        })
        .collect();

    let created_at = batch
        .column_by_name("created_at")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .and_then(|c| if c.is_null(0) { None } else { Some(c.value(0)) });

    Some(ExposureDetail {
        base: NodeBase {
            unique_id: s("unique_id").unwrap_or_default(),
            name: s("name").unwrap_or_default(),
            // `dbt.exposures` has no `resource_type` column — hardcoded.
            resource_type: "exposure".to_owned(),
            package_name: s("package_name"),
            description: s("description"),
            original_file_path: s("original_file_path"),
        },
        file_path: s("file_path"),
        tags: extract_str_list(batch, "tags"),
        fqn: extract_str_list(batch, "fqn"),
        label: s("label"),
        exposure_type: s("exposure_type"),
        maturity: s("maturity"),
        url: s("url"),
        owner_name: s("owner_name"),
        owner_email: s("owner_email"),
        meta,
        depends_on,
        created_at,
    })
}

// ---------------------------------------------------------------------------
// Extractors: GET /api/v1/exposures
// ---------------------------------------------------------------------------

/// Extract `depends_on_nodes_capped` (a `List<Utf8>` column) from a single
/// row at position `i` in a batch.
fn extract_capped_depends_on(batch: &RecordBatch, i: usize) -> Vec<EdgeRef> {
    let Some(col) = batch.column_by_name("depends_on_nodes_capped") else {
        return vec![];
    };
    if col.is_null(i) {
        return vec![];
    }
    let Some(list) = col.as_any().downcast_ref::<ListArray>() else {
        return vec![];
    };
    let inner = list.value(i);
    let Some(strings) = inner.as_any().downcast_ref::<StringArray>() else {
        return vec![];
    };
    (0..strings.len())
        .filter(|&j| !strings.is_null(j))
        .map(|j| {
            let uid = strings.value(j).to_owned();
            EdgeRef {
                edge_type: edge_type_from_prefix(&uid),
                unique_id: uid,
            }
        })
        .collect()
}

/// Extract a `List<Utf8>` column's row `i` into a `Vec<String>`.
fn extract_list_row(batch: &RecordBatch, col_name: &str, i: usize) -> Vec<String> {
    let Some(col) = batch.column_by_name(col_name) else {
        return vec![];
    };
    if col.is_null(i) {
        return vec![];
    }
    let Some(list) = col.as_any().downcast_ref::<ListArray>() else {
        return vec![];
    };
    let inner = list.value(i);
    let Some(strings) = inner.as_any().downcast_ref::<StringArray>() else {
        return vec![];
    };
    (0..strings.len())
        .filter(|&j| !strings.is_null(j))
        .map(|j| strings.value(j).to_owned())
        .collect()
}

fn batches_to_exposure_summary_rows(batches: &[RecordBatch]) -> Vec<ExposureSummary> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let unique_id_col = batch
            .column_by_name("unique_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let name_col = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let exposure_type_col = batch
            .column_by_name("exposure_type")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let maturity_col = batch
            .column_by_name("maturity")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let owner_name_col = batch
            .column_by_name("owner_name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let owner_email_col = batch
            .column_by_name("owner_email")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let created_at_col = batch
            .column_by_name("created_at")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let truncated_col = batch
            .column_by_name("depends_on_truncated")
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>());

        let Some(unique_id_col) = unique_id_col else {
            continue;
        };
        let Some(name_col) = name_col else {
            continue;
        };

        for i in 0..batch.num_rows() {
            let unique_id = unique_id_col.value(i).to_owned();
            let name = name_col.value(i).to_owned();
            let exposure_type = exposure_type_col.and_then(|c| opt_str(c, i));
            let maturity = maturity_col.and_then(|c| opt_str(c, i));
            let owner_name = owner_name_col.and_then(|c| opt_str(c, i));
            let owner_email = owner_email_col.and_then(|c| opt_str(c, i));
            let created_at =
                created_at_col.and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) });
            let depends_on_truncated = truncated_col
                .map(|c| !c.is_null(i) && c.value(i))
                .unwrap_or(false);

            // Extract tags from List<Utf8> at row i.
            let tags = extract_list_row(batch, "tags", i);

            let depends_on = extract_capped_depends_on(batch, i);

            rows.push(ExposureSummary {
                unique_id,
                name,
                exposure_type,
                maturity,
                owner_name,
                owner_email,
                tags,
                created_at,
                depends_on,
                depends_on_truncated,
            });
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// Handler: GET /api/v1/exposures/:id
// ---------------------------------------------------------------------------

/// `GET /api/v1/exposures/:id` — full exposure detail.
///
/// Exposures are leaf consumers — no `referenced_by` field (omitted, not
/// `[]`). No `execution_info`, `catalog`, `columns`, or SQL body.
pub async fn get_exposure(
    State(state): State<SharedState>,
    Path(unique_id): Path<String>,
) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }
    let id = escape_str(&unique_id);
    let sql = EXPOSURE_DETAIL_SQL.replace("{id}", &id);

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        backend.query_arrow(&sql).map_err(|e| e.to_string())
    })
    .await;

    let batches = match result {
        Ok(Ok(b)) => b,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let Some(detail) = extract_exposure_detail(&batches) else {
        return not_found(format!("exposure {unique_id} not found"));
    };

    Json(detail).into_response()
}

// ---------------------------------------------------------------------------
// Handler: GET /api/v1/exposures
// ---------------------------------------------------------------------------

/// `GET /api/v1/exposures` — cursor-paginated list of exposure nodes.
///
/// Default sort is `name:asc` (internal). `?sort` is **not accepted** —
/// any `?sort` value returns 400. `?owner` is an exact-match filter on
/// `dbt.exposures.owner_name`; a NULL `owner_name` is excluded (matches
/// `ExposureFilterView` dropdown behavior which only lists non-null owners).
pub async fn list_exposures(
    State(state): State<SharedState>,
    Query(params): Query<ExposureListParams>,
) -> Response {
    // Reject any ?sort value — ExposureFilterView exposes no sort UI.
    if params.sort.as_deref().filter(|s| !s.is_empty()).is_some() {
        return bad_request("sort is not supported on this endpoint");
    }

    let first = clamp_first(params.first);

    let cursor = match params.after.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => match Cursor::decode(s) {
            Ok(c) => Some(c),
            Err(msg) => return bad_request(msg),
        },
        None => None,
    };

    let owner = params
        .owner
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(str::to_owned);

    let (count_sql, rows_sql) = build_exposure_list_sql(owner.as_deref(), first, cursor.as_ref());

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let total_str = backend
            .query_scalar(&count_sql)
            .ok_or_else(|| "count query returned no rows".to_string())?;
        let total = total_str
            .parse::<u64>()
            .map_err(|e| format!("could not parse exposure count: {e}"))?;
        let batches = backend.query_arrow(&rows_sql).map_err(|e| e.to_string())?;
        Ok((total, batches))
    })
    .await;

    let (total_count, batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let mut rows = batches_to_exposure_summary_rows(&batches);

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

    Json(ExposureListResponse {
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

// ---------------------------------------------------------------------------
// Handler: GET /api/v1/exposures/facets
// ---------------------------------------------------------------------------

/// `GET /api/v1/exposures/facets` — distinct owner names for the filter
/// dropdown in `ExposureFilterView`.
///
/// Sourced from `dbt.exposures.owner_name` (NOT `dbt.groups`) because
/// exposures declare their owner directly in YAML (`owner.name`). Returns
/// `{ "owners": [] }` when no exposures have a non-null `owner_name`.
pub async fn list_exposure_facets(State(state): State<SharedState>) -> Response {
    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        backend
            .query_arrow(EXPOSURE_OWNERS_FACET_SQL)
            .map_err(|e| e.to_string())
    })
    .await;

    let batches = match result {
        Ok(Ok(b)) => b,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let mut owners: Vec<FacetValue> = Vec::new();
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let Some(col) = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        else {
            continue;
        };
        for i in 0..batch.num_rows() {
            if !col.is_null(i) {
                owners.push(FacetValue::new(col.value(i)));
            }
        }
    }

    Json(ExposureFacetsResponse { owners }).into_response()
}

#[cfg(test)]
#[path = "exposures_tests.rs"]
mod tests;
