//! `GET /api/v1/groups/:id` — typed group detail.
//! `GET /api/v1/groups` — cursor-paginated group list.
//! `GET /api/v1/groups/facets` — filter facet metadata (returns `{}`).
//!
//! Groups are definition-only: no SQL, no columns, no warehouse relation,
//! no run results, no catalog, no freshness, no lineage edges. The endpoint
//! renders the GroupView details panel plus an inline list of member models.
//!
//! `owner` is a nested object (`{name, email, slack, github}`); `slack` and
//! `github` are sourced from the `config` JSON column (top-level
//! `dbt.groups` schema only carries `owner_name`/`owner_email`). `tags` and
//! `meta` are also lifted from `config` when present.
//!
//! `dbt.groups` has no `created_at` column; `ingested_at` (ISO 8601) is the
//! fallback "definition updated as of" surface.
//!
//! Member models join through `(group_name, package_name)` (group names are
//! local to a package). Returned inline with a `?first=` cap and a top-level
//! `truncated` flag; `model_count` reports the unbounded total. Cursor
//! pagination is deferred.
//!
//! `model_count` on list rows is aggregated at query time via a correlated
//! subquery against `dbt.nodes` scoped by `(group_name, package_name)`.
//!
//! Data sources:
//! - `dbt.groups` — group row + config JSON
//! - `dbt.nodes` — `models[]` member list + `model_count` (filtered to
//!   `resource_type = 'model'`, joined on `(group_name, package_name)`)

use std::fmt::Write as _;

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::handlers::json::{bad_request, internal_error, json_parse_or_null, not_found};
use crate::handlers::node_base::{NodeBase, opt_str, str_col};
use crate::handlers::pagination::{Cursor, PageInfo, SortDir, clamp_first, cursor_where_fragment};
use crate::handlers::sql::escape_str;
use crate::state::SharedState;

/// Default cap on `models[]` when `?first=` is unset.
const DEFAULT_MODEL_LIMIT: u32 = 500;
/// Hard ceiling on `?first=` regardless of caller input.
const HARD_MAX_MODEL_LIMIT: u32 = 500;

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Response body for `GET /api/v1/groups/:id`.
#[derive(Serialize)]
pub struct GroupDetail {
    #[serde(flatten)]
    pub base: NodeBase,
    /// Lifted from `config.tags` when present; `[]` otherwise.
    pub tags: Vec<String>,
    /// `null` when no owner fields are set (name, email, slack, github all absent).
    pub owner: Option<Owner>,
    /// Parsed from `config.meta` when present; `null` otherwise.
    pub meta: serde_json::Value,
    /// Inline list of member models, capped by `?first=`.
    pub models: Vec<GroupModelMember>,
    /// Total member-model count, unaffected by `?first=` truncation.
    pub model_count: u64,
    /// `true` when `model_count > models.len()`.
    pub truncated: bool,
    /// ISO 8601 timestamp from `dbt.groups.ingested_at` (groups have no
    /// dedicated `created_at` parquet column).
    pub ingested_at: Option<String>,
}

#[derive(Serialize)]
pub struct Owner {
    pub name: Option<String>,
    pub email: Option<String>,
    pub slack: Option<String>,
    pub github: Option<String>,
}

#[derive(Serialize)]
pub struct GroupModelMember {
    pub unique_id: String,
    pub name: String,
    pub database_name: Option<String>,
    pub schema_name: Option<String>,
    pub contract_enforced: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct GroupDetailParams {
    pub first: Option<u32>,
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

const GROUP_DETAIL_NODE_SQL: &str = "\
SELECT g.unique_id, g.name, g.package_name, g.description, \
       g.original_file_path, \
       g.owner_name, g.owner_email, g.config, \
       CAST(g.ingested_at AS VARCHAR) AS ingested_at \
FROM dbt.groups g \
WHERE g.unique_id = '{id}' \
LIMIT 1";

// ---------------------------------------------------------------------------
// Extractors
// ---------------------------------------------------------------------------

/// Returns the parsed group detail and the group's `(name, package_name)`
/// (needed to query member models via `dbt.nodes`, scoped by both name and
/// package to avoid cross-package collisions).
fn extract_group_detail(batches: &[RecordBatch]) -> Option<(GroupDetail, String, Option<String>)> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;

    let s = |name: &'static str| -> Option<String> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<StringArray>()?;
        opt_str(col, 0)
    };

    let unique_id = s("unique_id").unwrap_or_default();
    let group_name = s("name").unwrap_or_default();
    let package_name = s("package_name");
    let owner_name = s("owner_name");
    let owner_email = s("owner_email");
    let config_raw = s("config");
    let config = json_parse_or_null(config_raw.as_deref());

    // Pull tags, slack/github, and meta out of config when shaped as an object.
    let (tags, slack, github, meta) = extract_config_surface(&config);

    let owner =
        if owner_name.is_some() || owner_email.is_some() || slack.is_some() || github.is_some() {
            Some(Owner {
                name: owner_name,
                email: owner_email,
                slack,
                github,
            })
        } else {
            None
        };

    let detail = GroupDetail {
        base: NodeBase {
            unique_id,
            name: group_name.clone(),
            resource_type: "group".to_owned(),
            package_name: package_name.clone(),
            description: s("description"),
            original_file_path: s("original_file_path"),
        },
        tags,
        owner,
        meta,
        // Populated after the member-model queries.
        models: vec![],
        model_count: 0,
        truncated: false,
        ingested_at: s("ingested_at"),
    };
    Some((detail, group_name, package_name))
}

/// Lift `tags`, `slack`, `github`, and `meta` from the parsed `config` value.
/// `tags` is a top-level string array; `slack`/`github` live under
/// `owner.{slack,github}`; `meta` is a top-level key.
fn extract_config_surface(
    config: &serde_json::Value,
) -> (
    Vec<String>,
    Option<String>,
    Option<String>,
    serde_json::Value,
) {
    let Some(obj) = config.as_object() else {
        return (vec![], None, None, serde_json::Value::Null);
    };
    let tags = obj
        .get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    let owner = obj.get("owner").and_then(|v| v.as_object());
    let slack = owner
        .and_then(|o| o.get("slack"))
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let github = owner
        .and_then(|o| o.get("github"))
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let meta = obj.get("meta").cloned().unwrap_or(serde_json::Value::Null);
    (tags, slack, github, meta)
}

fn extract_group_models(batches: &[RecordBatch]) -> Vec<GroupModelMember> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let uid = str_col(batch, "unique_id");
        let name = str_col(batch, "name");
        let database_name = batch
            .column_by_name("database_name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let schema_name = batch
            .column_by_name("schema_name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let contract_enforced = batch
            .column_by_name("contract_enforced")
            .and_then(|c| c.as_any().downcast_ref::<BooleanArray>());

        for i in 0..batch.num_rows() {
            rows.push(GroupModelMember {
                unique_id: uid.value(i).to_owned(),
                name: name.value(i).to_owned(),
                database_name: database_name.and_then(|c| opt_str(c, i)),
                schema_name: schema_name.and_then(|c| opt_str(c, i)),
                contract_enforced: contract_enforced
                    .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) }),
            });
        }
    }
    rows
}

fn extract_model_count(batches: &[RecordBatch]) -> u64 {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        // Use the first numeric column; nextest fixtures and DuckDB both
        // project `count(*)` as Int64.
        if let Some(col) = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .filter(|c| !c.is_null(0))
        {
            return col.value(0).max(0) as u64;
        }
    }
    0
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `GET /api/v1/groups/:id?first=<n>` — full group detail with inline
/// member-model list.
///
/// `owner` is `null` when no owner-shaped fields are set on the group.
/// `tags` defaults to `[]`; `meta` defaults to `null`. `models[]` is capped
/// at `?first=` (default 500); `model_count` is the unbounded total and
/// `truncated` flips when `model_count > models.len()`. `ingested_at` is
/// the ISO 8601 fallback — groups have no dedicated `created_at` column.
pub async fn get_group(
    State(state): State<SharedState>,
    Path(unique_id): Path<String>,
    Query(params): Query<GroupDetailParams>,
) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }
    let id = escape_str(&unique_id);
    let limit = params
        .first
        .unwrap_or(DEFAULT_MODEL_LIMIT)
        .clamp(1, HARD_MAX_MODEL_LIMIT);

    let node_sql = GROUP_DETAIL_NODE_SQL.replace("{id}", &id);

    let backend = state.providers.backend.clone();
    let node_result = tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>, String> {
        backend.query_arrow(&node_sql).map_err(|e| e.to_string())
    })
    .await;

    let node_batches = match node_result {
        Ok(Ok(b)) => b,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let Some((mut detail, group_name, package_name)) = extract_group_detail(&node_batches) else {
        return not_found(format!("group {unique_id} not found"));
    };

    // Member models scope by `(group_name, package_name)` — group names are
    // local to a package, so a name match alone risks cross-package collisions.
    let escaped_name = escape_str(&group_name);
    let pkg_clause = match package_name.as_deref() {
        Some(pkg) => format!(" AND n.package_name = '{}'", escape_str(pkg)),
        None => String::new(),
    };
    let models_sql = format!(
        "SELECT n.unique_id, n.name, n.database_name, n.schema_name, n.contract_enforced \
         FROM dbt.nodes n \
         WHERE n.group_name = '{escaped_name}' AND n.resource_type = 'model'{pkg_clause} \
         ORDER BY n.name \
         LIMIT {limit}"
    );
    let count_sql = format!(
        "SELECT count(*) FROM dbt.nodes n \
         WHERE n.group_name = '{escaped_name}' AND n.resource_type = 'model'{pkg_clause}"
    );

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let models = backend
            .query_arrow(&models_sql)
            .map_err(|e| e.to_string())?;
        let count = backend.query_arrow(&count_sql).map_err(|e| e.to_string())?;
        Ok((models, count))
    })
    .await;

    let (models_batches, count_batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    detail.models = extract_group_models(&models_batches);
    detail.model_count = extract_model_count(&count_batches);
    detail.truncated = detail.model_count > detail.models.len() as u64;

    Json(detail).into_response()
}

// ---------------------------------------------------------------------------
// Response types: GET /api/v1/groups and GET /api/v1/groups/facets
// ---------------------------------------------------------------------------

/// One row in `GET /api/v1/groups`.
///
/// `owner_github` and `owner_slack` are sourced from `json_extract_string(config,
/// '$.owner.github')` / `'$.owner.slack'` — there are no top-level parquet
/// columns for these fields. Both are `null` when absent from the JSON.
///
/// `model_count` is aggregated at query time via a correlated subquery against
/// `dbt.nodes` scoped by `(group_name, package_name)`; it is not a column
/// in `dbt.groups`.
#[derive(Serialize)]
pub struct GroupSummary {
    pub unique_id: String,
    pub name: String,
    pub owner_name: Option<String>,
    pub owner_email: Option<String>,
    pub owner_github: Option<String>,
    pub owner_slack: Option<String>,
    pub model_count: u64,
}

/// Cursor-paginated response for `GET /api/v1/groups`.
#[derive(Serialize)]
pub struct GroupListResponse {
    pub data: Vec<GroupSummary>,
    pub page_info: PageInfo,
}

/// Response body for `GET /api/v1/groups/facets`.
///
/// No filter parameters exist on `GET /api/v1/groups` in v0; this endpoint
/// returns an empty object for API uniformity.
#[derive(Serialize)]
pub struct GroupFacetsResponse {}

/// Query parameters for `GET /api/v1/groups`.
#[derive(Debug, Default, Deserialize)]
pub struct GroupListParams {
    /// Sort: `name:asc` (default) or `name:desc`. Any other key returns 400.
    pub sort: Option<String>,
    pub first: Option<u32>,
    pub after: Option<String>,
}

// ---------------------------------------------------------------------------
// SQL: GET /api/v1/groups
// ---------------------------------------------------------------------------

/// Build `(count_sql, rows_sql)` for `GET /api/v1/groups`.
///
/// Supports sort on `name` only; any other key returns `Err`.
/// `model_count` is computed via a correlated subquery scoped by both
/// `name` and `package_name` to prevent cross-package collisions.
pub(crate) fn build_group_list_sql(
    params: &GroupListParams,
    first: u32,
    cursor: Option<&Cursor>,
) -> Result<(String, String), &'static str> {
    let (sort_col, dir) = parse_group_sort(params.sort.as_deref())?;

    let mut page_where = String::from("WHERE 1=1");
    if let Some(c) = cursor {
        let frag = cursor_where_fragment(
            &format!("g.{sort_col}"),
            "g.unique_id",
            dir,
            c.sort_value.as_deref(),
            &c.unique_id,
        );
        let _ = write!(page_where, " AND {frag}");
    }

    let dir_sql = dir.as_sql();
    let count_sql = "SELECT count(*) FROM dbt.groups g".to_owned();
    let peek = first + 1;
    let rows_sql = format!(
        "SELECT g.unique_id, g.name, g.owner_name, g.owner_email, \
                json_extract_string(g.config, '$.owner.github') AS owner_github, \
                json_extract_string(g.config, '$.owner.slack') AS owner_slack, \
                (SELECT COUNT(*) FROM dbt.nodes n \
                 WHERE n.group_name = g.name \
                   AND n.package_name = g.package_name \
                   AND n.resource_type = 'model') AS model_count \
         FROM dbt.groups g {page_where} \
         ORDER BY g.{sort_col} {dir_sql} NULLS LAST, g.unique_id ASC \
         LIMIT {peek}"
    );

    Ok((count_sql, rows_sql))
}

/// Parse the `?sort=` parameter.
///
/// Returns `("name", Asc)` by default. Returns `Err` on unknown column or
/// unknown direction.
fn parse_group_sort(sort: Option<&str>) -> Result<(&'static str, SortDir), &'static str> {
    let Some(raw) = sort.filter(|s| !s.is_empty()) else {
        return Ok(("name", SortDir::Asc));
    };
    let (col, dir_str) = raw
        .split_once(':')
        .ok_or("sort must be <column>:<asc|desc>")?;
    let dir = match dir_str {
        "asc" => SortDir::Asc,
        "desc" => SortDir::Desc,
        _ => return Err("sort direction must be asc or desc"),
    };
    match col {
        "name" => Ok(("name", dir)),
        _ => Err("unknown sort column; only 'name' is supported"),
    }
}

// ---------------------------------------------------------------------------
// Extraction helpers: GET /api/v1/groups
// ---------------------------------------------------------------------------

fn batches_to_group_summary_rows(batches: &[RecordBatch]) -> Vec<GroupSummary> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let unique_id = str_col(batch, "unique_id");
        let name = str_col(batch, "name");
        let owner_name = str_col(batch, "owner_name");
        let owner_email = str_col(batch, "owner_email");
        let owner_github = str_col(batch, "owner_github");
        let owner_slack = str_col(batch, "owner_slack");
        let model_count_col = batch
            .column_by_name("model_count")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

        for i in 0..batch.num_rows() {
            let model_count = model_count_col
                .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) })
                .unwrap_or(0)
                .max(0) as u64;

            rows.push(GroupSummary {
                unique_id: unique_id.value(i).to_owned(),
                name: name.value(i).to_owned(),
                owner_name: opt_str(owner_name, i),
                owner_email: opt_str(owner_email, i),
                owner_github: opt_str(owner_github, i),
                owner_slack: opt_str(owner_slack, i),
                model_count,
            });
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// Handlers: GET /api/v1/groups and GET /api/v1/groups/facets
// ---------------------------------------------------------------------------

/// `GET /api/v1/groups` — cursor-paginated list of groups.
///
/// Sort defaults to `name:asc`; only `name` is sortable.
/// No filter parameters in v0. `model_count` is aggregated from `dbt.nodes`.
pub async fn list_groups(
    State(state): State<SharedState>,
    Query(params): Query<GroupListParams>,
) -> Response {
    let first = clamp_first(params.first);

    let cursor = match params.after.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => match Cursor::decode(s) {
            Ok(c) => Some(c),
            Err(msg) => return bad_request(msg),
        },
        None => None,
    };

    let (count_sql, rows_sql) = match build_group_list_sql(&params, first, cursor.as_ref()) {
        Ok(pair) => pair,
        Err(msg) => return bad_request(msg),
    };

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let total = backend
            .query_scalar(&count_sql)
            .ok_or_else(|| "count query returned no rows".to_string())?
            .parse::<u64>()
            .map_err(|e| format!("could not parse group count: {e}"))?;
        let batches = backend.query_arrow(&rows_sql).map_err(|e| e.to_string())?;
        Ok((total, batches))
    })
    .await;

    let (total_count, batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let mut rows = batches_to_group_summary_rows(&batches);

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

    Json(GroupListResponse {
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

/// `GET /api/v1/groups/facets` — returns `{}`.
///
/// No filter parameters exist on the groups list in v0. The endpoint is
/// implemented for API uniformity — every list endpoint has a matching
/// facets endpoint so client codegen can treat all resource types
/// identically.
pub async fn list_group_facets(_state: State<SharedState>) -> Response {
    Json(GroupFacetsResponse {}).into_response()
}

#[cfg(test)]
#[path = "groups_tests.rs"]
mod tests;
