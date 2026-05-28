//! `GET /api/v1/semantic_models/:id` — typed semantic model detail.
//! `GET /api/v1/semantic_models` — cursor-paginated semantic model list.
//! `GET /api/v1/semantic_models/facets` — filter facet metadata (empty in v0).
//!
//! Semantic models are MetricFlow / Semantic Layer specs: YAML-only,
//! spec-only nodes that bind entities, dimensions, and measures onto an
//! underlying dbt model. They are never executed against the warehouse, so
//! the response carries no `execution_info`, `catalog`, `freshness`,
//! `columns`, or compiled/raw code surfaces.
//!
//! `entities`, `dimensions`, and `measures` are inlined as arrays sourced
//! from sibling parquet tables (`dbt.semantic_entities`,
//! `dbt.semantic_dimensions`, `dbt.semantic_measures`) joined on the
//! parent `unique_id`. Bounded by spec authorship — no pagination cap.
//!
//! JSON-string parquet columns (`node_relation`, `defaults`, measure
//! `agg_params` and `non_additive_dimension`, dimension `validity_params`)
//! are parsed handler-side via [`crate::handlers::json::json_parse_or_null`]
//! so the response carries real nested JSON, not escaped strings.
//!
//! Data sources:
//! - `dbt.semantic_models` — the spec row
//! - `dbt.nodes` — joined on `dbt.semantic_models.model` for the
//!   `UpstreamModelRef`'s `access_level` / `alias`
//! - `dbt.semantic_entities` / `_dimensions` / `_measures` — inline arrays
//! - `dbt.edges` — `depends_on` (upstream) and `referenced_by` (downstream)

use std::fmt::Write as _;

use arrow_array::{
    Array, BooleanArray, Float64Array, ListArray, RecordBatch, StringArray, StructArray,
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

/// Response body for `GET /api/v1/semantic_models/:id`.
#[derive(Serialize)]
pub struct SemanticModelDetail {
    #[serde(flatten)]
    pub base: NodeBase,
    /// Project-relative path of the YAML containing the spec.
    pub file_path: Option<String>,
    pub tags: Vec<String>,
    pub fqn: Vec<String>,
    /// Parsed JSON object from `dbt.semantic_models.config.meta`, or `null`.
    pub meta: serde_json::Value,
    pub group_name: Option<String>,
    pub label: Option<String>,
    /// The model this semantic model is built on; `null` when the joined
    /// `dbt.nodes` row is absent.
    pub model: Option<UpstreamModelRef>,
    /// Parsed `node_relation` JSON, or `null`.
    pub node_relation: serde_json::Value,
    pub primary_entity: Option<String>,
    /// Parsed `defaults` JSON, or `null`.
    pub defaults: serde_json::Value,
    pub entities: Vec<SemanticEntity>,
    pub dimensions: Vec<SemanticDimension>,
    pub measures: Vec<SemanticMeasure>,
    /// 1-hop upstream — typically one entry pointing at the underlying
    /// model. Array-shaped for uniformity with other `*Detail` responses.
    pub depends_on: Vec<EdgeRef>,
    /// 1-hop downstream — metrics and saved queries that consume this
    /// semantic model.
    pub referenced_by: Vec<EdgeRef>,
    /// Epoch seconds; the per-resource "Definition updated as of …"
    /// timestamp.
    pub created_at: Option<f64>,
}

#[derive(Serialize)]
pub struct UpstreamModelRef {
    pub unique_id: String,
    pub name: String,
    pub access_level: Option<String>,
    pub alias: Option<String>,
}

#[derive(Serialize)]
pub struct SemanticEntity {
    pub name: String,
    #[serde(rename = "type")]
    pub entity_type: Option<String>,
    pub description: Option<String>,
    pub label: Option<String>,
    pub expr: Option<String>,
    pub role: Option<String>,
}

#[derive(Serialize)]
pub struct SemanticDimension {
    pub name: String,
    #[serde(rename = "type")]
    pub dimension_type: Option<String>,
    pub description: Option<String>,
    pub label: Option<String>,
    pub expr: Option<String>,
    pub is_partition: Option<bool>,
    pub time_granularity: Option<String>,
    /// Parsed `validity_params` JSON, or `null`. Replaces the GraphQL
    /// `typeParams` field (not a parquet column).
    pub validity_params: serde_json::Value,
}

#[derive(Serialize)]
pub struct SemanticMeasure {
    pub name: String,
    pub agg: Option<String>,
    pub description: Option<String>,
    pub label: Option<String>,
    pub expr: Option<String>,
    pub create_metric: Option<bool>,
    pub agg_time_dimension: Option<String>,
    /// Parsed `agg_params` JSON, or `null`.
    pub agg_params: serde_json::Value,
    /// Parsed `non_additive_dimension` JSON, or `null`.
    pub non_additive_dimension: serde_json::Value,
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

const SEMANTIC_MODEL_NODE_SQL: &str = "\
SELECT sm.unique_id, sm.name, sm.package_name, sm.description, \
       sm.original_file_path, sm.file_path, sm.label, sm.fqn, \
       sm.node_relation, sm.primary_entity, sm.defaults, \
       sm.group_name, sm.config, sm.created_at, sm.model AS model_unique_id, \
       n.name AS model_name, n.access_level AS model_access_level, \
       n.alias AS model_alias \
FROM dbt.semantic_models sm \
LEFT JOIN dbt.nodes n ON n.unique_id = sm.model \
WHERE sm.unique_id = '{id}' \
LIMIT 1";

const SEMANTIC_ENTITIES_SQL: &str = "\
SELECT name, entity_type, description, label, expr, entity_role AS role \
FROM dbt.semantic_entities WHERE unique_id = '{id}' \
ORDER BY name";

const SEMANTIC_DIMENSIONS_SQL: &str = "\
SELECT name, dimension_type, description, label, expr, \
       is_partition, time_granularity, validity_params \
FROM dbt.semantic_dimensions WHERE unique_id = '{id}' \
ORDER BY name";

const SEMANTIC_MEASURES_SQL: &str = "\
SELECT name, agg, description, label, expr, \
       create_metric, agg_time_dimension, agg_params, non_additive_dimension \
FROM dbt.semantic_measures WHERE unique_id = '{id}' \
ORDER BY name";

// ---------------------------------------------------------------------------
// Extractors
// ---------------------------------------------------------------------------

fn extract_semantic_model_detail(batches: &[RecordBatch]) -> Option<SemanticModelDetail> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;

    let s = |name: &'static str| -> Option<String> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<StringArray>()?;
        opt_str(col, 0)
    };

    let node_relation = json_parse_or_null(s("node_relation").as_deref());
    let defaults = json_parse_or_null(s("defaults").as_deref());

    // `tags` and `meta` are nested inside the `config` JSON blob on
    // semantic_models — extract via parse rather than top-level columns.
    let config_value = json_parse_or_null(s("config").as_deref());
    let tags = config_value
        .get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    let meta = config_value
        .get("meta")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    let created_at = batch
        .column_by_name("created_at")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .and_then(|c| if c.is_null(0) { None } else { Some(c.value(0)) });

    let model = build_upstream_model_ref(batch);

    Some(SemanticModelDetail {
        base: NodeBase {
            unique_id: s("unique_id").unwrap_or_default(),
            name: s("name").unwrap_or_default(),
            resource_type: "semantic_model".to_owned(),
            package_name: s("package_name"),
            description: s("description"),
            original_file_path: s("original_file_path"),
        },
        file_path: s("file_path"),
        tags,
        fqn: extract_str_list(batch, "fqn"),
        meta,
        group_name: s("group_name"),
        label: s("label"),
        model,
        node_relation,
        primary_entity: s("primary_entity"),
        defaults,
        // Sub-resources populated after extraction.
        entities: vec![],
        dimensions: vec![],
        measures: vec![],
        depends_on: vec![],
        referenced_by: vec![],
        created_at,
    })
}

fn build_upstream_model_ref(batch: &RecordBatch) -> Option<UpstreamModelRef> {
    let s = |name: &'static str| -> Option<String> {
        let col = batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<StringArray>()?;
        opt_str(col, 0)
    };
    let unique_id = s("model_unique_id")?;
    // The JOIN may not match (e.g., the model row was filtered out or the
    // semantic_models.model points at a node that hasn't been indexed yet).
    // Surface what we have from semantic_models, leaving access_level/alias
    // as `null` and the model `name` defaulting to the bare suffix.
    let name = s("model_name").unwrap_or_else(|| {
        unique_id
            .rsplit_once('.')
            .map(|(_, suffix)| suffix.to_owned())
            .unwrap_or_else(|| unique_id.clone())
    });
    Some(UpstreamModelRef {
        unique_id,
        name,
        access_level: s("model_access_level"),
        alias: s("model_alias"),
    })
}

fn extract_entities(batches: &[RecordBatch]) -> Vec<SemanticEntity> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let name = str_col(batch, "name");
        let entity_type = str_col(batch, "entity_type");
        let description = str_col(batch, "description");
        let label = str_col(batch, "label");
        let expr = str_col(batch, "expr");
        let role = str_col(batch, "role");
        for i in 0..batch.num_rows() {
            rows.push(SemanticEntity {
                name: name.value(i).to_owned(),
                entity_type: opt_str(entity_type, i),
                description: opt_str(description, i),
                label: opt_str(label, i),
                expr: opt_str(expr, i),
                role: opt_str(role, i),
            });
        }
    }
    rows
}

fn extract_dimensions(batches: &[RecordBatch]) -> Vec<SemanticDimension> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let name = str_col(batch, "name");
        let dimension_type = str_col(batch, "dimension_type");
        let description = str_col(batch, "description");
        let label = str_col(batch, "label");
        let expr = str_col(batch, "expr");
        let time_granularity = str_col(batch, "time_granularity");
        let validity_params = str_col(batch, "validity_params");
        let is_partition = batch
            .column_by_name("is_partition")
            .and_then(|c| c.as_any().downcast_ref::<BooleanArray>());
        for i in 0..batch.num_rows() {
            rows.push(SemanticDimension {
                name: name.value(i).to_owned(),
                dimension_type: opt_str(dimension_type, i),
                description: opt_str(description, i),
                label: opt_str(label, i),
                expr: opt_str(expr, i),
                is_partition: is_partition
                    .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) }),
                time_granularity: opt_str(time_granularity, i),
                validity_params: json_parse_or_null(opt_str(validity_params, i).as_deref()),
            });
        }
    }
    rows
}

fn extract_measures(batches: &[RecordBatch]) -> Vec<SemanticMeasure> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let name = str_col(batch, "name");
        let agg = str_col(batch, "agg");
        let description = str_col(batch, "description");
        let label = str_col(batch, "label");
        let expr = str_col(batch, "expr");
        let agg_time_dimension = str_col(batch, "agg_time_dimension");
        let agg_params = str_col(batch, "agg_params");
        let non_additive_dimension = str_col(batch, "non_additive_dimension");
        let create_metric = batch
            .column_by_name("create_metric")
            .and_then(|c| c.as_any().downcast_ref::<BooleanArray>());
        for i in 0..batch.num_rows() {
            rows.push(SemanticMeasure {
                name: name.value(i).to_owned(),
                agg: opt_str(agg, i),
                description: opt_str(description, i),
                label: opt_str(label, i),
                expr: opt_str(expr, i),
                create_metric: create_metric
                    .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) }),
                agg_time_dimension: opt_str(agg_time_dimension, i),
                agg_params: json_parse_or_null(opt_str(agg_params, i).as_deref()),
                non_additive_dimension: json_parse_or_null(
                    opt_str(non_additive_dimension, i).as_deref(),
                ),
            });
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `GET /api/v1/semantic_models/:id` — full semantic model detail.
///
/// Semantic models are spec-only: no `execution_info`, `catalog`,
/// `freshness`, `columns`, or compiled code. `entities`, `dimensions`,
/// `measures` are inlined as arrays. `depends_on` is array-shaped despite
/// being typically length-1.
pub async fn get_semantic_model(
    State(state): State<SharedState>,
    Path(unique_id): Path<String>,
) -> Response {
    if unique_id.is_empty() || unique_id.contains('\'') {
        return bad_request("invalid unique_id");
    }
    let id = escape_str(&unique_id);

    let node_sql = SEMANTIC_MODEL_NODE_SQL.replace("{id}", &id);
    let entities_sql = SEMANTIC_ENTITIES_SQL.replace("{id}", &id);
    let dimensions_sql = SEMANTIC_DIMENSIONS_SQL.replace("{id}", &id);
    let measures_sql = SEMANTIC_MEASURES_SQL.replace("{id}", &id);
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

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let node_batches = backend.query_arrow(&node_sql).map_err(|e| e.to_string())?;
        // Sibling tables are independent — missing parquet view → `[]`
        // on that field, not a 500.
        let entity_batches = backend.query_arrow(&entities_sql).ok().unwrap_or_default();
        let dimension_batches = backend
            .query_arrow(&dimensions_sql)
            .ok()
            .unwrap_or_default();
        let measure_batches = backend.query_arrow(&measures_sql).ok().unwrap_or_default();
        let upstream_batches = backend
            .query_arrow(&upstream_sql)
            .map_err(|e| e.to_string())?;
        let downstream_batches = backend
            .query_arrow(&downstream_sql)
            .map_err(|e| e.to_string())?;
        Ok((
            node_batches,
            entity_batches,
            dimension_batches,
            measure_batches,
            upstream_batches,
            downstream_batches,
        ))
    })
    .await;

    let (
        node_batches,
        entity_batches,
        dimension_batches,
        measure_batches,
        upstream_batches,
        downstream_batches,
    ) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let Some(mut detail) = extract_semantic_model_detail(&node_batches) else {
        return not_found(format!("semantic model {unique_id} not found"));
    };

    detail.entities = extract_entities(&entity_batches);
    detail.dimensions = extract_dimensions(&dimension_batches);
    detail.measures = extract_measures(&measure_batches);
    detail.depends_on = extract_edge_refs(&upstream_batches);
    detail.referenced_by = extract_edge_refs(&downstream_batches);

    Json(detail).into_response()
}

// ---------------------------------------------------------------------------
// Response types: GET /api/v1/semantic_models and GET /api/v1/semantic_models/facets
// ---------------------------------------------------------------------------

/// One entity reference inlined per list row: just name + type.
#[derive(Serialize)]
pub struct SemanticEntityRef {
    pub name: String,
    #[serde(rename = "type")]
    pub entity_type: Option<String>,
}

/// One row in `GET /api/v1/semantic_models`.
#[derive(Serialize)]
pub struct SemanticModelSummary {
    pub unique_id: String,
    pub name: String,
    pub package_name: Option<String>,
    pub group_name: Option<String>,
    pub primary_entity: Option<String>,
    pub entities: Vec<SemanticEntityRef>,
    pub description: Option<String>,
    /// Epoch seconds (float); from `dbt.semantic_models.created_at`.
    pub created_at: Option<f64>,
    /// `true` when `entities[]` was capped by `?first=<n>` on this row.
    pub truncated: bool,
}

/// Cursor-paginated response for `GET /api/v1/semantic_models`.
#[derive(Serialize)]
pub struct SemanticModelListResponse {
    pub data: Vec<SemanticModelSummary>,
    pub page_info: PageInfo,
}

/// Response body for `GET /api/v1/semantic_models/facets`.
///
/// No filter dimensions in v0 — the semantic models list exposes no filter
/// params. Returns `{}` so adding a facet dimension later is wire-additive.
#[derive(Serialize)]
pub struct SemanticModelFacetsResponse {}

/// Query parameters for `GET /api/v1/semantic_models`.
#[derive(Debug, Default, Deserialize)]
pub struct SemanticModelListParams {
    /// Sort: `name:asc` (default) or `name:desc`. Any other key returns 400.
    pub sort: Option<String>,
    /// Per-row cap on `entities[]`. Rows with more entries set `truncated: true`.
    pub first: Option<u32>,
    pub after: Option<String>,
}

/// Maximum number of `entities[]` entries inlined per list row.
const ENTITIES_ROW_CAP: usize = 500;

// ---------------------------------------------------------------------------
// SQL: GET /api/v1/semantic_models
// ---------------------------------------------------------------------------

/// Build `(count_sql, rows_sql)` for `GET /api/v1/semantic_models`.
///
/// Only `name` is sortable; any other column returns `Err`.
/// `entities[]` is aggregated with a LEFT JOIN in a single query.
/// The count query excludes the cursor predicate so `total_count` reflects
/// the full set.
pub(crate) fn build_semantic_model_list_sql(
    params: &SemanticModelListParams,
    page_size: u32,
    cursor: Option<&Cursor>,
) -> Result<(String, String), &'static str> {
    let (sort_col, dir) = parse_semantic_model_sort(params.sort.as_deref())?;

    let filter_where = String::from("WHERE 1=1");

    let mut page_where = filter_where.clone();
    if let Some(c) = cursor {
        let frag = cursor_where_fragment(
            &format!("sm.{sort_col}"),
            "sm.unique_id",
            dir,
            c.sort_value.as_deref(),
            &c.unique_id,
        );
        let _ = write!(page_where, " AND {frag}");
    }

    let dir_sql = dir.as_sql();
    let count_sql = format!("SELECT count(*) FROM dbt.semantic_models sm {filter_where}");
    let peek = page_size + 1;
    // LEFT JOIN against semantic_entities to collect entity refs per row.
    // Entities are ordered by name for deterministic rendering.
    let rows_sql = format!(
        "SELECT sm.unique_id, sm.name, sm.package_name, sm.group_name, \
                sm.primary_entity, sm.description, sm.created_at, \
                LIST({{name: e.name, entity_type: e.entity_type}} \
                     ORDER BY e.name) \
                     FILTER (e.name IS NOT NULL) AS entities \
         FROM dbt.semantic_models sm \
         LEFT JOIN dbt.semantic_entities e ON e.unique_id = sm.unique_id \
         {page_where} \
         GROUP BY sm.unique_id, sm.name, sm.package_name, sm.group_name, \
                  sm.primary_entity, sm.description, sm.created_at \
         ORDER BY sm.{sort_col} {dir_sql} NULLS LAST, sm.unique_id ASC \
         LIMIT {peek}"
    );

    Ok((count_sql, rows_sql))
}

/// Parse the `?sort=` parameter for semantic models.
///
/// Returns `("name", Asc)` by default. Returns `Err` on unknown column or
/// unknown direction.
fn parse_semantic_model_sort(sort: Option<&str>) -> Result<(&'static str, SortDir), &'static str> {
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
// Extraction helpers: GET /api/v1/semantic_models list
// ---------------------------------------------------------------------------

/// Extract `entities[]` from the `LIST({name, entity_type})` struct array.
///
/// DuckDB emits a `ListArray` whose child is a `StructArray` with
/// `name: Utf8` and `entity_type: Utf8` fields. A missing or null column
/// yields an empty vec.
fn extract_entity_refs(batch: &RecordBatch, row: usize) -> Vec<SemanticEntityRef> {
    let Some(col) = batch.column_by_name("entities") else {
        return vec![];
    };
    if col.is_null(row) {
        return vec![];
    }
    let Some(list) = col.as_any().downcast_ref::<ListArray>() else {
        return vec![];
    };
    let inner = list.value(row);
    let Some(structs) = inner.as_any().downcast_ref::<StructArray>() else {
        return vec![];
    };
    let name_col = structs
        .column_by_name("name")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let type_col = structs
        .column_by_name("entity_type")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());

    let Some(names) = name_col else {
        return vec![];
    };
    (0..structs.len())
        .filter(|&i| !names.is_null(i))
        .map(|i| SemanticEntityRef {
            name: names.value(i).to_owned(),
            entity_type: type_col.and_then(|c| opt_str(c, i)),
        })
        .collect()
}

fn batches_to_semantic_model_summary_rows(batches: &[RecordBatch]) -> Vec<SemanticModelSummary> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let unique_id = str_col(batch, "unique_id");
        let name = str_col(batch, "name");
        let package_name = str_col(batch, "package_name");
        let group_name = str_col(batch, "group_name");
        let primary_entity = str_col(batch, "primary_entity");
        let description = str_col(batch, "description");
        let created_at_col = batch
            .column_by_name("created_at")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());

        for i in 0..batch.num_rows() {
            let created_at = created_at_col.and_then(|col| {
                if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i))
                }
            });
            let mut entity_refs = extract_entity_refs(batch, i);
            let truncated = entity_refs.len() > ENTITIES_ROW_CAP;
            if truncated {
                entity_refs.truncate(ENTITIES_ROW_CAP);
            }
            rows.push(SemanticModelSummary {
                unique_id: unique_id.value(i).to_owned(),
                name: name.value(i).to_owned(),
                package_name: opt_str(package_name, i),
                group_name: opt_str(group_name, i),
                primary_entity: opt_str(primary_entity, i),
                entities: entity_refs,
                description: opt_str(description, i),
                created_at,
                truncated,
            });
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// Handlers: GET /api/v1/semantic_models and GET /api/v1/semantic_models/facets
// ---------------------------------------------------------------------------

/// `GET /api/v1/semantic_models` — cursor-paginated list of semantic model definitions.
///
/// Sort defaults to `name:asc`; only `name` is sortable. No filter params in v0.
/// `entities[]` is inlined per row via a LEFT JOIN, capped at 500; `truncated`
/// signals whether the cap was hit.
pub async fn list_semantic_models(
    State(state): State<SharedState>,
    Query(params): Query<SemanticModelListParams>,
) -> Response {
    let page_size = clamp_first(params.first);

    let cursor = match params.after.as_deref().filter(|s| !s.is_empty()) {
        Some(s) => match Cursor::decode(s) {
            Ok(c) => Some(c),
            Err(msg) => return bad_request(msg),
        },
        None => None,
    };

    let (count_sql, rows_sql) =
        match build_semantic_model_list_sql(&params, page_size, cursor.as_ref()) {
            Ok(pair) => pair,
            Err(msg) => return bad_request(msg),
        };

    let backend = state.providers.backend.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<_, String> {
        let total = backend
            .query_scalar(&count_sql)
            .ok_or_else(|| "count query returned no rows".to_string())?
            .parse::<u64>()
            .map_err(|e| format!("could not parse semantic model count: {e}"))?;
        let batches = backend.query_arrow(&rows_sql).map_err(|e| e.to_string())?;
        Ok((total, batches))
    })
    .await;

    let (total_count, batches) = match result {
        Ok(Ok(t)) => t,
        Ok(Err(err)) => return internal_error(err),
        Err(err) => return internal_error(err.to_string()),
    };

    let mut rows = batches_to_semantic_model_summary_rows(&batches);

    let has_next_page = rows.len() as u32 > page_size;
    if has_next_page {
        rows.truncate(page_size as usize);
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

    Json(SemanticModelListResponse {
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

/// `GET /api/v1/semantic_models/facets` — filter facet values for the semantic
/// models list.
///
/// No filter dimensions in v0: returns `{}`. When a future revision adds
/// filter params to the list endpoint, the corresponding facet keys will be
/// added here at the same time.
pub async fn list_semantic_model_facets() -> Response {
    Json(SemanticModelFacetsResponse {}).into_response()
}

#[cfg(test)]
#[path = "semantic_models_tests.rs"]
mod tests;
