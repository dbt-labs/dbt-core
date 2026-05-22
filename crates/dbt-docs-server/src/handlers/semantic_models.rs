//! `GET /api/v1/semantic_models/:id` — typed semantic model detail.
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

use arrow_array::{Array, BooleanArray, Float64Array, RecordBatch, StringArray};
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

#[cfg(test)]
#[path = "semantic_models_tests.rs"]
mod tests;
