//! MetricFlow semantic query compiler — translates metric queries into executable SQL.
//!
//! This crate provides a storage-agnostic compiler: callers implement the
//! [`MetricStore`] trait to supply metric/model metadata, and the compiler
//! produces SQL for the requested dialect.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::str::FromStr;

// ═══════════════════════════════════════════════════════════════════════════
// Error type
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, thiserror::Error)]
pub enum MetricFlowError {
    #[error("{0}")]
    Other(String),
}

// ═══════════════════════════════════════════════════════════════════════════
// Metadata store trait
// ═══════════════════════════════════════════════════════════════════════════

/// Raw metric row from the metadata store.
#[derive(Debug, Clone)]
pub struct RawMetricRow {
    pub name: String,
    pub metric_type: String,
    pub description: String,
    pub type_params: String,
    pub metric_filter: String,
}

/// Raw semantic model row.
#[derive(Debug, Clone)]
pub struct RawModelRow {
    pub name: String,
    pub node_relation: String,
    pub primary_entity: String,
    pub unique_id: String,
}

/// Raw entity row.
#[derive(Debug, Clone)]
pub struct RawEntityRow {
    pub name: String,
    pub entity_type: String,
    pub expr: String,
}

/// Raw dimension row.
#[derive(Debug, Clone)]
pub struct RawDimensionRow {
    pub name: String,
    pub dimension_type: String,
    pub expr: String,
    pub time_granularity: String,
}

/// Raw join graph entry.
#[derive(Debug, Clone)]
pub struct RawJoinGraphRow {
    pub model_name: String,
    pub entity_name: String,
    pub entity_type: String,
    pub expr: String,
}

/// Raw time spine row.
#[derive(Debug, Clone)]
pub struct RawTimeSpineRow {
    pub node_relation: String,
    pub primary_column: String,
    pub primary_granularity: String,
}

/// Abstraction over the metadata store (dbt index, or any other source).
pub trait MetricStore {
    fn lookup_metric(&mut self, name: &str) -> Result<Option<RawMetricRow>, MetricFlowError>;
    fn list_metric_names(&mut self) -> Result<Vec<String>, MetricFlowError>;
    fn lookup_semantic_model(&mut self, name: &str)
    -> Result<Option<RawModelRow>, MetricFlowError>;
    fn lookup_model_entities(
        &mut self,
        unique_id: &str,
    ) -> Result<Vec<RawEntityRow>, MetricFlowError>;
    fn lookup_model_dimensions(
        &mut self,
        unique_id: &str,
    ) -> Result<Vec<RawDimensionRow>, MetricFlowError>;
    fn lookup_all_join_graph_entities(&mut self) -> Result<Vec<RawJoinGraphRow>, MetricFlowError>;
    fn find_model_for_entity(
        &mut self,
        entity_name: &str,
        primary_or_unique_only: bool,
    ) -> Result<Option<String>, MetricFlowError>;
    fn check_entity_in_model(
        &mut self,
        model_name: &str,
        entity_name: &str,
    ) -> Result<bool, MetricFlowError>;
    fn lookup_time_spine(&mut self) -> Result<Option<RawTimeSpineRow>, MetricFlowError>;
}

// ═══════════════════════════════════════════════════════════════════════════
// In-memory metric store (for testing and embedding)
// ═══════════════════════════════════════════════════════════════════════════

/// A [`MetricStore`] backed by in-memory data structures.
///
/// Build one from a semantic manifest JSON blob via [`InMemoryMetricStore::from_manifest`],
/// or populate it manually for tests.
#[derive(Debug, Default)]
pub struct InMemoryMetricStore {
    pub metrics: HashMap<String, RawMetricRow>,
    pub models: HashMap<String, RawModelRow>,
    /// Insertion-order model names (for deterministic iteration).
    pub model_order: Vec<String>,
    pub entities: HashMap<String, Vec<RawEntityRow>>,
    pub dimensions: HashMap<String, Vec<RawDimensionRow>>,
    pub time_spine: Option<RawTimeSpineRow>,
}

impl InMemoryMetricStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build from a semantic manifest JSON value (the format produced by dbt).
    pub fn from_manifest(manifest: &serde_json::Value) -> Self {
        let mut store = Self::new();

        if let Some(models) = manifest.get("semantic_models").and_then(|v| v.as_array()) {
            for m in models {
                let name = m.get("name").and_then(|v| v.as_str()).unwrap_or_default();
                let nr = m
                    .get("node_relation")
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                let pe = m
                    .get("primary_entity")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let unique_id = format!("semantic_model.test.{name}");

                store.model_order.push(name.to_string());
                store.models.insert(
                    name.to_string(),
                    RawModelRow {
                        name: name.to_string(),
                        node_relation: nr,
                        primary_entity: pe.to_string(),
                        unique_id: unique_id.clone(),
                    },
                );

                // Entities
                let mut ents = Vec::new();
                if let Some(arr) = m.get("entities").and_then(|v| v.as_array()) {
                    for e in arr {
                        ents.push(RawEntityRow {
                            name: e
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            entity_type: e
                                .get("type")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            expr: e
                                .get("expr")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    }
                }
                store.entities.insert(unique_id.clone(), ents);

                // Dimensions
                let mut dims = Vec::new();
                if let Some(arr) = m.get("dimensions").and_then(|v| v.as_array()) {
                    for d in arr {
                        let gran = d
                            .get("type_params")
                            .and_then(|tp| tp.get("time_granularity"))
                            .and_then(|v| v.as_str())
                            .unwrap_or_default();
                        dims.push(RawDimensionRow {
                            name: d
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            dimension_type: d
                                .get("type")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            expr: d
                                .get("expr")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            time_granularity: gran.to_string(),
                        });
                    }
                }
                store.dimensions.insert(unique_id, dims);
            }
        }

        if let Some(metrics) = manifest.get("metrics").and_then(|v| v.as_array()) {
            for m in metrics {
                let name = m.get("name").and_then(|v| v.as_str()).unwrap_or_default();
                store.metrics.insert(
                    name.to_string(),
                    RawMetricRow {
                        name: name.to_string(),
                        metric_type: m
                            .get("type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("simple")
                            .to_string(),
                        description: m
                            .get("description")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        type_params: m
                            .get("type_params")
                            .map(|v| v.to_string())
                            .unwrap_or_default(),
                        metric_filter: m.get("filter").map(|v| v.to_string()).unwrap_or_default(),
                    },
                );
            }
        }

        if let Some(spines) = manifest.get("project_configuration").and_then(|v| {
            v.get("time_spines")
                .or_else(|| v.get("time_spine_table_configurations"))
                .and_then(|v| v.as_array())
        }) {
            if let Some(s) = spines.first() {
                let nr = s
                    .get("node_relation")
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                let col = s
                    .get("primary_column")
                    .and_then(|v| v.get("name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("ds");
                let gran = s
                    .get("primary_column")
                    .and_then(|v| v.get("time_granularity"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("day");
                store.time_spine = Some(RawTimeSpineRow {
                    node_relation: nr,
                    primary_column: col.to_string(),
                    primary_granularity: gran.to_string(),
                });
            }
        }

        store
    }
}

impl MetricStore for InMemoryMetricStore {
    fn lookup_metric(&mut self, name: &str) -> Result<Option<RawMetricRow>, MetricFlowError> {
        Ok(self.metrics.get(name).cloned())
    }

    fn list_metric_names(&mut self) -> Result<Vec<String>, MetricFlowError> {
        let mut names: Vec<String> = self.metrics.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    fn lookup_semantic_model(
        &mut self,
        name: &str,
    ) -> Result<Option<RawModelRow>, MetricFlowError> {
        Ok(self.models.get(name).cloned())
    }

    fn lookup_model_entities(
        &mut self,
        unique_id: &str,
    ) -> Result<Vec<RawEntityRow>, MetricFlowError> {
        Ok(self.entities.get(unique_id).cloned().unwrap_or_default())
    }

    fn lookup_model_dimensions(
        &mut self,
        unique_id: &str,
    ) -> Result<Vec<RawDimensionRow>, MetricFlowError> {
        Ok(self.dimensions.get(unique_id).cloned().unwrap_or_default())
    }

    fn lookup_all_join_graph_entities(&mut self) -> Result<Vec<RawJoinGraphRow>, MetricFlowError> {
        let mut rows = Vec::new();
        for model_name in &self.model_order {
            if let Some(model) = self.models.get(model_name) {
                if let Some(ents) = self.entities.get(&model.unique_id) {
                    for e in ents {
                        rows.push(RawJoinGraphRow {
                            model_name: model_name.clone(),
                            entity_name: e.name.clone(),
                            entity_type: e.entity_type.clone(),
                            expr: e.expr.clone(),
                        });
                    }
                }
            }
        }
        Ok(rows)
    }

    fn find_model_for_entity(
        &mut self,
        entity_name: &str,
        primary_or_unique_only: bool,
    ) -> Result<Option<String>, MetricFlowError> {
        for model_name in &self.model_order {
            if let Some(model) = self.models.get(model_name) {
                if let Some(ents) = self.entities.get(&model.unique_id) {
                    for e in ents {
                        if e.name == entity_name {
                            if primary_or_unique_only
                                && !matches!(
                                    e.entity_type.as_str(),
                                    "primary" | "unique" | "natural"
                                )
                            {
                                continue;
                            }
                            return Ok(Some(model_name.clone()));
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    fn check_entity_in_model(
        &mut self,
        model_name: &str,
        entity_name: &str,
    ) -> Result<bool, MetricFlowError> {
        if let Some(model) = self.models.get(model_name) {
            if let Some(ents) = self.entities.get(&model.unique_id) {
                return Ok(ents.iter().any(|e| e.name == entity_name));
            }
        }
        Ok(false)
    }

    fn lookup_time_spine(&mut self) -> Result<Option<RawTimeSpineRow>, MetricFlowError> {
        Ok(self.time_spine.clone())
    }
}

/// SQL dialect for rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    DuckDB,
    Snowflake,
    Redshift,
    BigQuery,
    Databricks,
}

impl FromStr for Dialect {
    type Err = MetricFlowError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "duckdb" | "duck" => Ok(Dialect::DuckDB),
            "snowflake" | "sf" => Ok(Dialect::Snowflake),
            "redshift" | "rs" => Ok(Dialect::Redshift),
            "bigquery" | "bq" => Ok(Dialect::BigQuery),
            "databricks" | "spark" => Ok(Dialect::Databricks),
            other => Err(MetricFlowError::Other(format!(
                "unknown dialect: {other:?}. Use 'duckdb', 'snowflake', 'redshift', 'bigquery', or 'databricks'"
            ))),
        }
    }
}

/// A parsed semantic query specification.
#[derive(Debug, Clone)]
pub struct SemanticQuerySpec {
    pub metrics: Vec<String>,
    pub group_by: Vec<GroupBySpec>,
    pub where_filters: Vec<String>,
    pub order_by: Vec<OrderBySpec>,
    pub limit: Option<usize>,
    /// Optional time constraint: `[start_date, end_date]` applied as
    /// `WHERE metric_time >= start AND metric_time <= end`.
    pub time_constraint: Option<(String, String)>,
}

/// A group-by specification — either a dimension or time dimension.
#[derive(Debug, Clone)]
pub enum GroupBySpec {
    Dimension {
        entity: Option<String>,
        name: String,
    },
    TimeDimension {
        name: String,
        granularity: String,
        /// When set, extract a date part instead of truncating (e.g., EXTRACT(YEAR FROM ...)).
        date_part: Option<String>,
    },
    /// Group by an entity column (e.g., `Entity('listing')` → `listing_id`).
    Entity { name: String },
}

/// Compute the output column alias for each group-by spec.
///
/// When a single time dimension name appears at multiple granularities
/// (e.g. `metric_time__month` + `metric_time__week`), the alias is
/// disambiguated as `name__granularity`. Otherwise the bare `name` is used.
fn group_by_output_cols(group_by: &[GroupBySpec]) -> Vec<String> {
    let mut time_dim_counts: HashMap<&str, usize> = HashMap::new();
    for gb in group_by {
        if let GroupBySpec::TimeDimension { name, .. } = gb {
            *time_dim_counts.entry(name.as_str()).or_default() += 1;
        }
    }
    group_by
        .iter()
        .map(|gb| match gb {
            GroupBySpec::TimeDimension {
                name, granularity, ..
            } => {
                if time_dim_counts.get(name.as_str()).copied().unwrap_or(1) > 1 {
                    format!("{name}__{granularity}")
                } else {
                    name.clone()
                }
            }
            GroupBySpec::Dimension {
                entity: Some(e),
                name,
            } => format!("{e}__{name}"),
            GroupBySpec::Dimension { entity: None, name } => name.clone(),
            GroupBySpec::Entity { name } => name.clone(),
        })
        .collect()
}

/// An order-by specification.
#[derive(Debug, Clone)]
pub struct OrderBySpec {
    pub name: String,
    pub descending: bool,
}

/// Metric type as stored in dbt.metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricType {
    Simple,
    Derived,
    Ratio,
    Cumulative,
    Conversion,
}

/// A resolved metric with all information needed for compilation.
#[derive(Debug, Clone)]
pub struct ResolvedMetric {
    pub name: String,
    pub metric_type: MetricType,
    pub description: String,
    /// For simple metrics: the aggregation details.
    pub agg_params: Option<AggParams>,
    /// For simple metrics: where filters from the metric definition.
    pub metric_filters: Vec<String>,
    /// For derived metrics: the expression template.
    pub derived_expr: Option<String>,
    /// For derived/ratio metrics: input metric names.
    pub input_metrics: Vec<MetricInput>,
    /// For ratio metrics: numerator and denominator.
    pub numerator: Option<MetricInput>,
    pub denominator: Option<MetricInput>,
    /// For cumulative metrics.
    pub cumulative_params: Option<CumulativeParams>,
    /// For conversion metrics.
    pub conversion_params: Option<ConversionParams>,
    /// Whether to join to time spine for null-filling.
    pub join_to_timespine: bool,
    /// Value to fill nulls with.
    pub fill_nulls_with: Option<i64>,
}

/// Aggregation parameters for a simple metric's measure.
#[derive(Debug, Clone)]
pub struct AggParams {
    pub semantic_model: String,
    pub agg: String,
    pub expr: String,
    pub agg_time_dimension: Option<String>,
    pub non_additive_dimension: Option<serde_json::Value>,
}

/// A reference to an input metric (used in derived/ratio metrics).
#[derive(Debug, Clone)]
pub struct MetricInput {
    pub name: String,
    pub filter: Option<String>,
    pub alias: Option<String>,
    pub offset_window: Option<String>,
    pub offset_to_grain: Option<String>,
}

/// Cumulative metric parameters.
#[derive(Debug, Clone)]
pub struct CumulativeParams {
    pub window_count: Option<i64>,
    pub window_granularity: Option<String>,
    pub grain_to_date: Option<String>,
}

/// Conversion metric parameters.
#[derive(Debug, Clone)]
pub struct ConversionParams {
    pub entity: String,
    pub base_metric: String,
    pub conversion_metric: String,
    pub calculation: String,
    pub window_count: Option<i64>,
    pub window_granularity: Option<String>,
    pub constant_properties: Vec<(String, String)>,
}

/// A resolved semantic model with its physical table and join keys.
#[derive(Debug, Clone)]
pub struct ResolvedModel {
    pub name: String,
    pub relation_name: String,
    pub alias: String,
    pub schema_name: String,
    pub database: String,
    pub primary_entity: Option<String>,
    pub entities: Vec<EntityDef>,
    pub dimensions: Vec<DimensionDef>,
}

/// Entity definition within a semantic model.
#[derive(Debug, Clone)]
pub struct EntityDef {
    pub name: String,
    pub entity_type: String,
    pub expr: String,
}

/// Dimension definition within a semantic model.
#[derive(Debug, Clone)]
pub struct DimensionDef {
    pub name: String,
    pub dimension_type: String,
    pub expr: String,
    pub time_granularity: Option<String>,
}

/// Time spine information.
#[derive(Debug, Clone)]
pub struct TimeSpine {
    pub relation_name: String,
    pub primary_column: String,
    pub primary_granularity: String,
}

/// A join between two semantic models via a shared entity.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct JoinEdge {
    from_model: String,
    to_model: String,
    from_expr: String,
    to_expr: String,
    entity_name: String,
}

// ═══════════════════════════════════════════════════════════════════════════
// Input validation
// ═══════════════════════════════════════════════════════════════════════════

/// Check that a string is a valid SQL identifier (alphanumeric + underscore).
fn is_valid_identifier(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Check if `haystack` contains `word` as a whole word (surrounded by non-alphanumeric or boundaries).
fn contains_word(haystack: &str, word: &str) -> bool {
    let bytes = haystack.as_bytes();
    let wlen = word.len();
    for (i, _) in haystack.match_indices(word) {
        let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
        let after_ok = i + wlen >= bytes.len() || !bytes[i + wlen].is_ascii_alphanumeric();
        if before_ok && after_ok {
            return true;
        }
    }
    false
}

/// Reject WHERE filter strings that contain dangerous SQL patterns.
/// Allowed: `{{ Dimension/TimeDimension/Metric(...) }}` templates, comparison operators,
/// literals, AND/OR/NOT, IS NULL, IN (...), BETWEEN, LIKE.
/// Rejected: semicolons, comments, subqueries, DDL/DML keywords, UNION.
fn validate_where_filter(filter: &str) -> Result<(), MetricFlowError> {
    // Strip out recognized Jinja templates so they don't trigger false positives.
    let mut stripped = filter.to_string();
    while let Some(start) = stripped.find("{{") {
        if let Some(end) = stripped[start..].find("}}").map(|i| start + i + 2) {
            stripped.replace_range(start..end, "TMPL");
        } else {
            break;
        }
    }

    let upper = stripped.to_ascii_uppercase();

    // Reject semicolons (statement chaining).
    if stripped.contains(';') {
        return Err(MetricFlowError::Other(
            "WHERE filter must not contain ';'".into(),
        ));
    }

    // Reject SQL comments.
    if stripped.contains("--") || stripped.contains("/*") {
        return Err(MetricFlowError::Other(
            "WHERE filter must not contain SQL comments".into(),
        ));
    }

    // Reject dangerous keywords (word-boundary matched).
    static DANGEROUS: &[&str] = &[
        "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "EXEC", "EXECUTE", "UNION", "INTO",
    ];
    for kw in DANGEROUS {
        if contains_word(&upper, kw) {
            return Err(MetricFlowError::Other(format!(
                "WHERE filter must not contain '{kw}'"
            )));
        }
    }

    Ok(())
}

/// Valid time granularities supported by MetricFlow / DuckDB / Snowflake.
pub const VALID_GRANULARITIES: &[&str] = &[
    "day",
    "week",
    "month",
    "quarter",
    "year",
    "hour",
    "minute",
    "second",
    "millisecond",
];

/// Validate Dimension/TimeDimension references inside WHERE filter strings.
fn validate_where_dim_refs(
    filters: &[String],
    avail_dims: &[String],
    avail_time_dims: &[String],
) -> Result<(), MetricFlowError> {
    let all_dim_names: Vec<&str> = avail_dims
        .iter()
        .chain(avail_time_dims.iter())
        .map(|s| s.as_str())
        .collect();

    for filter in filters {
        // Extract Dimension('...') references (but not TimeDimension).
        let mut cursor = 0usize;
        while let Some(start) = filter[cursor..].find("Dimension('") {
            let abs_pos = cursor + start;
            if abs_pos > 0 && filter.as_bytes()[abs_pos - 1].is_ascii_alphanumeric() {
                cursor = abs_pos + 11;
                continue;
            }
            let abs_start = abs_pos + 11; // skip "Dimension('"
            if let Some(end) = filter[abs_start..].find("')") {
                let dim_ref = &filter[abs_start..abs_start + end];
                let base_name = dim_ref.split("__").last().unwrap_or(dim_ref);
                let found = all_dim_names
                    .iter()
                    .any(|d| *d == dim_ref || d.ends_with(&format!("__{base_name}")));
                if !found {
                    return Err(MetricFlowError::Other(format!(
                        "unknown dimension in where filter: {dim_ref:?}\n\
                         Available dimensions: {}\n\
                         Available time dimensions: {}",
                        avail_dims.join(", "),
                        avail_time_dims.join(", ")
                    )));
                }
                cursor = abs_start + end + 2;
            } else {
                break;
            }
        }
        // Extract TimeDimension('...', '...') references.
        cursor = 0;
        while let Some(start) = filter[cursor..].find("TimeDimension(") {
            let abs_start = cursor + start + 14; // skip "TimeDimension("
            if let Some(end) = filter[abs_start..].find(')') {
                let inner = &filter[abs_start..abs_start + end];
                let args: Vec<&str> = inner.split(',').collect();
                let dim_name = args
                    .first()
                    .map(|a| a.trim().trim_matches('\'').trim_matches('"'))
                    .unwrap_or("");
                let base_name = dim_name.split("__").last().unwrap_or(dim_name);
                let found = avail_time_dims
                    .iter()
                    .any(|d| d == dim_name || d.ends_with(&format!("__{base_name}")));
                if !found {
                    return Err(MetricFlowError::Other(format!(
                        "unknown time dimension in where filter: {dim_name:?}\n\
                         Available time dimensions: {}",
                        avail_time_dims.join(", ")
                    )));
                }
                if let Some(g_arg) = args.get(1) {
                    let granularity = g_arg.trim().trim_matches('\'').trim_matches('"');
                    if !granularity.is_empty() && !VALID_GRANULARITIES.contains(&granularity) {
                        return Err(MetricFlowError::Other(format!(
                            "unknown granularity in where filter: {granularity:?}\n\
                             Valid granularities: {}",
                            VALID_GRANULARITIES.join(", ")
                        )));
                    }
                }
                cursor = abs_start + end + 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}

/// Validate a query spec against the resolved semantic models.
///
/// Called from `compile()` after models are resolved — at that point we know
/// every dimension, entity, and metric available.  Returns a clear error
/// message listing the available options whenever a name doesn't match.
fn validate_spec(
    spec: &SemanticQuerySpec,
    all_metrics: &HashMap<String, ResolvedMetric>,
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
) -> Result<(), MetricFlowError> {
    // Collect all available dimension names (including entity-prefixed forms).
    let mut avail_dims: Vec<String> = Vec::new();
    let mut avail_time_dims: Vec<String> = Vec::new();
    let mut avail_entities: Vec<String> = Vec::new();

    for (_alias, model) in model_aliases.values() {
        // Collect all entity names for this model (explicit + primary).
        let mut model_entity_names: Vec<&str> =
            model.entities.iter().map(|e| e.name.as_str()).collect();
        if let Some(ref pe) = model.primary_entity {
            if !model_entity_names.contains(&pe.as_str()) {
                model_entity_names.push(pe.as_str());
            }
        }

        for dim in &model.dimensions {
            if dim.dimension_type == "time" {
                if !avail_time_dims.contains(&dim.name) {
                    avail_time_dims.push(dim.name.clone());
                }
            } else if !avail_dims.contains(&dim.name) {
                avail_dims.push(dim.name.clone());
            }
            // Also add entity-prefixed forms.
            for ent_name in &model_entity_names {
                let ref_form = format!("{ent_name}__{}", dim.name);
                if dim.dimension_type == "time" {
                    if !avail_time_dims.contains(&ref_form) {
                        avail_time_dims.push(ref_form);
                    }
                } else if !avail_dims.contains(&ref_form) {
                    avail_dims.push(ref_form);
                }
            }
        }
        for ent_name in &model_entity_names {
            let name_str = (*ent_name).to_string();
            if !avail_entities.contains(&name_str) {
                avail_entities.push(name_str);
            }
        }
    }
    // metric_time is always available as a synthetic time dimension.
    if !avail_time_dims.contains(&"metric_time".to_string()) {
        avail_time_dims.push("metric_time".to_string());
    }

    avail_dims.sort();
    avail_time_dims.sort();
    avail_entities.sort();

    // ── Validate group-by ──────────────────────────────────────────────
    for gb in &spec.group_by {
        match gb {
            GroupBySpec::TimeDimension {
                name, granularity, ..
            } => {
                // Check granularity.
                if !VALID_GRANULARITIES.contains(&granularity.as_str()) {
                    return Err(MetricFlowError::Other(format!(
                        "unknown granularity: {granularity:?}\n\
                         Valid granularities: {}",
                        VALID_GRANULARITIES.join(", ")
                    )));
                }
                // Check time dimension name.
                let base_name = name.split("__").last().unwrap_or(name);
                let found = avail_time_dims
                    .iter()
                    .any(|d| d == name || d.ends_with(&format!("__{base_name}")));
                if !found {
                    return Err(MetricFlowError::Other(format!(
                        "unknown time dimension: {name:?}\n\
                         Available time dimensions: {}",
                        avail_time_dims.join(", ")
                    )));
                }
            }
            GroupBySpec::Dimension { entity, name } => {
                let full_ref = match entity {
                    Some(e) => format!("{e}__{name}"),
                    None => name.clone(),
                };
                // Check that dimension exists in at least one resolved model.
                let found = model_aliases.values().any(|(_alias, model)| {
                    model.dimensions.iter().any(|d| {
                        if d.dimension_type == "time" {
                            return false; // time dims must use TimeDimension syntax
                        }
                        if d.name == *name {
                            // If entity is specified, verify the model has that entity
                            // (check both explicit entities and primary_entity).
                            match entity {
                                Some(e) => {
                                    model.entities.iter().any(|ent| ent.name == *e)
                                        || model.primary_entity.as_deref() == Some(e.as_str())
                                }
                                None => true,
                            }
                        } else {
                            false
                        }
                    })
                });
                if !found {
                    return Err(MetricFlowError::Other(format!(
                        "unknown dimension: {full_ref:?}\n\
                         Available dimensions: {}\n\
                         Available time dimensions (use metric_time:day syntax): {}",
                        avail_dims.join(", "),
                        avail_time_dims.join(", ")
                    )));
                }
            }
            GroupBySpec::Entity { name } => {
                let found = model_aliases.values().any(|(_alias, model)| {
                    model.entities.iter().any(|e| e.name == *name)
                        || model.primary_entity.as_deref() == Some(name.as_str())
                });
                if !found {
                    return Err(MetricFlowError::Other(format!(
                        "unknown entity: {name:?}\n\
                         Available entities: {}",
                        avail_entities.join(", ")
                    )));
                }
            }
        }
    }

    // ── Validate order-by ──────────────────────────────────────────────
    // Order-by names must refer to a metric or a group-by dimension/entity.
    let metric_names: Vec<&str> = all_metrics.keys().map(|s| s.as_str()).collect();
    let group_by_names: Vec<String> = spec
        .group_by
        .iter()
        .flat_map(|gb| match gb {
            GroupBySpec::TimeDimension {
                name, granularity, ..
            } => {
                vec![name.clone(), format!("{name}__{granularity}")]
            }
            GroupBySpec::Dimension { name, .. } => vec![name.clone()],
            GroupBySpec::Entity { name } => vec![name.clone()],
        })
        .collect();

    for ob in &spec.order_by {
        let found = metric_names.contains(&ob.name.as_str())
            || group_by_names.iter().any(|g| g == &ob.name);
        if !found {
            let mut available: Vec<String> =
                metric_names.iter().map(|s| (*s).to_string()).collect();
            available.extend(group_by_names.iter().cloned());
            available.sort();
            available.dedup();
            return Err(MetricFlowError::Other(format!(
                "unknown order-by: {:?}\n\
                 Order-by must reference a metric or group-by column.\n\
                 Available: {}",
                ob.name,
                available.join(", ")
            )));
        }
    }

    // ── Validate where-filter dimension references ─────────────────────
    validate_where_dim_refs(&spec.where_filters, &avail_dims, &avail_time_dims)?;

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// Query spec parsing
// ═══════════════════════════════════════════════════════════════════════════

/// Parse a JSON semantic query specification.
///
/// Expected format:
/// ```json
/// {
///   "metrics": ["revenue", "order_count"],
///   "group_by": ["TimeDimension('metric_time', 'day')", "Dimension('customer__segment')"],
///   "where": ["{{ Dimension('order_id__status') }} = 'completed'"],
///   "order_by": ["-revenue", "+metric_time"],
///   "limit": 100
/// }
/// ```
pub fn parse_query_spec(json_str: &str) -> Result<SemanticQuerySpec, MetricFlowError> {
    let v: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| MetricFlowError::Other(format!("invalid query JSON: {e}")))?;

    let metrics = v
        .get("metrics")
        .and_then(|m| m.as_array())
        .ok_or_else(|| MetricFlowError::Other("query must have a 'metrics' array".into()))?
        .iter()
        .filter_map(|m| m.as_str().map(String::from))
        .collect::<Vec<_>>();

    if metrics.is_empty() {
        return Err(MetricFlowError::Other(
            "at least one metric is required".into(),
        ));
    }

    let group_by = match v.get("group_by").and_then(|g| g.as_array()) {
        Some(arr) => {
            let mut parsed = Vec::with_capacity(arr.len());
            for item in arr {
                let s = item.as_str().ok_or_else(|| {
                    MetricFlowError::Other("group_by items must be strings".into())
                })?;
                match parse_group_by_str(s) {
                    Some(gb) => parsed.push(gb),
                    None => {
                        return Err(MetricFlowError::Other(format!(
                            "invalid group_by: {s:?}\n\
                             Expected formats:\n  \
                               metric_time:day          (time dimension with granularity)\n  \
                               customer__segment        (entity-prefixed dimension)\n  \
                               segment                  (plain dimension)\n  \
                               TimeDimension('metric_time', 'day')\n  \
                               Dimension('customer__segment')\n  \
                               Entity('listing')"
                        )));
                    }
                }
            }
            parsed
        }
        None => vec![],
    };

    let where_filters: Vec<String> = v
        .get("where")
        .and_then(|w| w.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|w| w.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    // Validate WHERE filters against SQL injection patterns.
    for filter in &where_filters {
        validate_where_filter(filter)?;
    }

    let order_by = match v.get("order_by").and_then(|o| o.as_array()) {
        Some(arr) => {
            let mut parsed = Vec::with_capacity(arr.len());
            for item in arr {
                let s = item.as_str().ok_or_else(|| {
                    MetricFlowError::Other("order_by items must be strings".into())
                })?;
                match parse_order_by_str(s) {
                    Some(ob) => parsed.push(ob),
                    None => {
                        return Err(MetricFlowError::Other(format!(
                            "invalid order_by: {s:?}\n\
                             Expected formats:\n  \
                               revenue           (ascending)\n  \
                               -revenue          (descending)\n  \
                               +metric_time      (ascending, explicit)"
                        )));
                    }
                }
            }
            parsed
        }
        None => vec![],
    };

    let limit = v.get("limit").and_then(|l| l.as_u64()).map(|l| l as usize);

    let time_constraint = v
        .get("time_constraint")
        .and_then(|tc| tc.as_array())
        .and_then(|arr| {
            let start = arr.first()?.as_str()?.to_string();
            let end = arr.get(1)?.as_str()?.to_string();
            Some((start, end))
        });

    Ok(SemanticQuerySpec {
        metrics,
        group_by,
        where_filters,
        order_by,
        limit,
        time_constraint,
    })
}

pub fn parse_group_by_str(s: &str) -> Option<GroupBySpec> {
    // TimeDimension('metric_time', 'day')
    if let Some(inner) = s
        .strip_prefix("TimeDimension(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let parts: Vec<&str> = inner.split(',').collect();
        let name = parts.first()?.trim().trim_matches('\'').trim_matches('"');
        let granularity = parts
            .get(1)
            .map(|g| g.trim().trim_matches('\'').trim_matches('"'))
            .unwrap_or("day");
        if !is_valid_identifier(name) || !is_valid_identifier(granularity) {
            return None;
        }
        // Optional date_part: TimeDimension('metric_time', 'day', date_part='year')
        let date_part = parts.iter().skip(2).find_map(|p| {
            let trimmed = p.trim();
            trimmed
                .strip_prefix("date_part=")
                .or_else(|| trimmed.strip_prefix("date_part ="))
                .map(|v| v.trim().trim_matches('\'').trim_matches('"').to_string())
        });
        return Some(GroupBySpec::TimeDimension {
            name: name.to_string(),
            granularity: granularity.to_string(),
            date_part,
        });
    }

    // Dimension('entity__name') or Dimension('name')
    if let Some(inner) = s
        .strip_prefix("Dimension(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let dim_ref = inner.trim().trim_matches('\'').trim_matches('"');
        if let Some((entity, name)) = dim_ref.split_once("__") {
            if !is_valid_identifier(entity) || !is_valid_identifier(name) {
                return None;
            }
            return Some(GroupBySpec::Dimension {
                entity: Some(entity.to_string()),
                name: name.to_string(),
            });
        }
        if !is_valid_identifier(dim_ref) {
            return None;
        }
        return Some(GroupBySpec::Dimension {
            entity: None,
            name: dim_ref.to_string(),
        });
    }

    // Entity('listing')
    if let Some(inner) = s.strip_prefix("Entity(").and_then(|s| s.strip_suffix(')')) {
        let entity_name = inner.trim().trim_matches('\'').trim_matches('"');
        if !is_valid_identifier(entity_name) {
            return None;
        }
        return Some(GroupBySpec::Entity {
            name: entity_name.to_string(),
        });
    }

    // Colon shorthand for time dimensions: metric_time:day, metric_time:week
    // Also supports entity prefix: order_id__metric_time:month
    if let Some((prefix, granularity)) = s.split_once(':') {
        if !is_valid_identifier(granularity) {
            return None;
        }
        let name = if let Some((_entity, dim_name)) = prefix.split_once("__") {
            if !is_valid_identifier(dim_name) {
                return None;
            }
            dim_name
        } else {
            if !is_valid_identifier(prefix) {
                return None;
            }
            prefix
        };
        return Some(GroupBySpec::TimeDimension {
            name: name.to_string(),
            granularity: granularity.to_string(),
            date_part: None,
        });
    }

    // Double-underscore with granularity: metric_time__day → TimeDimension
    // This is the standard MetricFlow JSON notation for time dimensions.
    if let Some((prefix, suffix)) = s.split_once("__") {
        if VALID_GRANULARITIES.contains(&suffix) {
            if !is_valid_identifier(prefix) {
                return None;
            }
            return Some(GroupBySpec::TimeDimension {
                name: prefix.to_string(),
                granularity: suffix.to_string(),
                date_part: None,
            });
        }
    }

    // Plain string: treat as dimension name
    if let Some((entity, name)) = s.split_once("__") {
        if !is_valid_identifier(entity) || !is_valid_identifier(name) {
            return None;
        }
        Some(GroupBySpec::Dimension {
            entity: Some(entity.to_string()),
            name: name.to_string(),
        })
    } else {
        if !is_valid_identifier(s) {
            return None;
        }
        Some(GroupBySpec::Dimension {
            entity: None,
            name: s.to_string(),
        })
    }
}

pub fn parse_order_by_str(s: &str) -> Option<OrderBySpec> {
    let (name, descending) = if let Some(name) = s.strip_prefix('-') {
        (name, true)
    } else if let Some(name) = s.strip_prefix('+') {
        (name, false)
    } else {
        (s, false)
    };
    // Validate identifier: only alphanumeric + underscore allowed.
    if !is_valid_identifier(name) {
        return None;
    }
    Some(OrderBySpec {
        name: name.to_string(),
        descending,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// Metric resolution — read from MetricStore
// ═══════════════════════════════════════════════════════════════════════════

fn resolve_metric(
    store: &mut impl MetricStore,
    name: &str,
) -> Result<ResolvedMetric, MetricFlowError> {
    let row = store.lookup_metric(name)?.ok_or_else(|| {
        let avail = store.list_metric_names().unwrap_or_default();
        let hint = if avail.is_empty() {
            String::new()
        } else {
            format!("\nAvailable metrics: {}", avail.join(", "))
        };
        MetricFlowError::Other(format!("metric not found: {name}{hint}"))
    })?;

    let metric_type_str = row.metric_type;
    let description = row.description;
    let type_params_json = row.type_params;
    let metric_filter_json = row.metric_filter;

    let metric_type = match metric_type_str.as_str() {
        "simple" => MetricType::Simple,
        "derived" => MetricType::Derived,
        "ratio" => MetricType::Ratio,
        "cumulative" => MetricType::Cumulative,
        "conversion" => MetricType::Conversion,
        other => {
            return Err(MetricFlowError::Other(format!(
                "unknown metric type for {name}: {other}"
            )));
        }
    };

    let tp: serde_json::Value = if type_params_json.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&type_params_json).unwrap_or(serde_json::Value::Null)
    };

    // Parse metric-level filters.
    let metric_filters = parse_metric_filters(&metric_filter_json);

    // Parse aggregation params for simple metrics.
    let agg_params = tp.get("metric_aggregation_params").and_then(|p| {
        if p.is_null() {
            return None;
        }
        Some(AggParams {
            semantic_model: p
                .get("semantic_model")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            agg: p
                .get("agg")
                .and_then(|v| v.as_str())
                .unwrap_or("sum")
                .to_string(),
            expr: p
                .get("expr")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                // Fallback: some manifest formats store the measure expr at
                // type_params.expr rather than metric_aggregation_params.expr.
                .or_else(|| {
                    tp.get("expr")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                })
                .unwrap_or("")
                .to_string(),
            agg_time_dimension: p
                .get("agg_time_dimension")
                .and_then(|v| v.as_str())
                .map(String::from),
            non_additive_dimension: p
                .get("non_additive_dimension")
                .filter(|v| !v.is_null())
                .cloned(),
        })
    });

    // Parse derived metric expression and input metrics.
    let derived_expr = tp.get("expr").and_then(|v| v.as_str()).map(String::from);

    let input_metrics = tp
        .get("metrics")
        .and_then(|m| m.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|m| {
                    let name = m.get("name")?.as_str()?.to_string();
                    let filter = m.get("filter").and_then(|f| f.as_str()).map(String::from);
                    let alias = m.get("alias").and_then(|a| a.as_str()).map(String::from);
                    let offset_window = m.get("offset_window").and_then(|o| {
                        if let Some(s) = o.as_str() {
                            Some(s.to_string())
                        } else {
                            let count = o.get("count").and_then(|c| c.as_u64())?;
                            let gran = o.get("granularity").and_then(|g| g.as_str())?;
                            Some(format!("{count} {gran}"))
                        }
                    });
                    let offset_to_grain = m
                        .get("offset_to_grain")
                        .and_then(|o| o.as_str())
                        .map(String::from);
                    Some(MetricInput {
                        name,
                        filter,
                        alias,
                        offset_window,
                        offset_to_grain,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    // Parse ratio metric numerator/denominator.
    let numerator = tp.get("numerator").and_then(|n| {
        if n.is_null() {
            return None;
        }
        Some(MetricInput {
            name: n.get("name")?.as_str()?.to_string(),
            filter: n.get("filter").and_then(|f| f.as_str()).map(String::from),
            alias: n.get("alias").and_then(|a| a.as_str()).map(String::from),
            offset_window: None,
            offset_to_grain: None,
        })
    });

    let denominator = tp.get("denominator").and_then(|d| {
        if d.is_null() {
            return None;
        }
        Some(MetricInput {
            name: d.get("name")?.as_str()?.to_string(),
            filter: d.get("filter").and_then(|f| f.as_str()).map(String::from),
            alias: d.get("alias").and_then(|a| a.as_str()).map(String::from),
            offset_window: None,
            offset_to_grain: None,
        })
    });

    // Cumulative params.
    let cumulative_params = tp.get("cumulative_type_params").and_then(|c| {
        if c.is_null() {
            return None;
        }
        let window = c.get("window");
        Some(CumulativeParams {
            window_count: window.and_then(|w| w.get("count")).and_then(|c| c.as_i64()),
            window_granularity: window
                .and_then(|w| w.get("granularity"))
                .and_then(|g| g.as_str())
                .map(String::from),
            grain_to_date: c
                .get("grain_to_date")
                .and_then(|g| g.as_str())
                .map(String::from),
        })
    });

    // Conversion params.
    let conversion_params = tp.get("conversion_type_params").and_then(|c| {
        if c.is_null() {
            return None;
        }
        let window = c.get("window");
        let const_props = c
            .get("constant_properties")
            .and_then(|cp| cp.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|p| {
                        let base = p.get("base_property")?.as_str()?.to_string();
                        let conv = p.get("conversion_property")?.as_str()?.to_string();
                        Some((base, conv))
                    })
                    .collect()
            })
            .unwrap_or_default();
        Some(ConversionParams {
            entity: c
                .get("entity")
                .and_then(|e| e.as_str())
                .unwrap_or("")
                .to_string(),
            base_metric: c
                .get("base_metric")
                .and_then(|b| b.get("name"))
                .and_then(|n| n.as_str())
                .or_else(|| {
                    c.get("base_measure")
                        .and_then(|b| b.get("name"))
                        .and_then(|n| n.as_str())
                })
                .unwrap_or("")
                .to_string(),
            conversion_metric: c
                .get("conversion_metric")
                .and_then(|b| b.get("name"))
                .and_then(|n| n.as_str())
                .or_else(|| {
                    c.get("conversion_measure")
                        .and_then(|b| b.get("name"))
                        .and_then(|n| n.as_str())
                })
                .unwrap_or("")
                .to_string(),
            calculation: c
                .get("calculation")
                .and_then(|v| v.as_str())
                .unwrap_or("conversion_rate")
                .to_string(),
            window_count: window.and_then(|w| w.get("count")).and_then(|c| c.as_i64()),
            window_granularity: window
                .and_then(|w| w.get("granularity"))
                .and_then(|g| g.as_str())
                .map(String::from),
            constant_properties: const_props,
        })
    });

    let join_to_timespine = tp
        .get("join_to_timespine")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let fill_nulls_with = tp.get("fill_nulls_with").and_then(|v| v.as_i64());

    Ok(ResolvedMetric {
        name: name.to_string(),
        metric_type,
        description,
        agg_params,
        metric_filters,
        derived_expr,
        input_metrics,
        numerator,
        denominator,
        cumulative_params,
        conversion_params,
        join_to_timespine,
        fill_nulls_with,
    })
}

fn parse_metric_filters(filter_json: &str) -> Vec<String> {
    if filter_json.is_empty() {
        return vec![];
    }
    let v: serde_json::Value = match serde_json::from_str(filter_json) {
        Ok(v) => v,
        Err(_) => return vec![],
    };
    if v.is_null() {
        return vec![];
    }
    // WhereFilterIntersection: { "where_filters": [{"where_sql_template": "..."}] }
    v.get("where_filters")
        .and_then(|wf| wf.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|f| {
                    f.get("where_sql_template")
                        .and_then(|t| t.as_str())
                        .map(|s| s.trim().to_string())
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Resolve a semantic model by name from the store.
fn resolve_model(
    store: &mut impl MetricStore,
    name: &str,
) -> Result<ResolvedModel, MetricFlowError> {
    let row = store
        .lookup_semantic_model(name)?
        .ok_or_else(|| MetricFlowError::Other(format!("semantic model not found: {name}")))?;

    let node_relation_json = row.node_relation;
    let primary_entity = row.primary_entity;
    let unique_id = row.unique_id;

    let nr: serde_json::Value =
        serde_json::from_str(&node_relation_json).unwrap_or(serde_json::Value::Null);

    let relation_name = nr
        .get("relation_name")
        .and_then(|v| v.as_str())
        .unwrap_or(name)
        .to_string();
    let alias = nr
        .get("alias")
        .and_then(|v| v.as_str())
        .unwrap_or(name)
        .to_string();
    let schema_name = nr
        .get("schema_name")
        .and_then(|v| v.as_str())
        .unwrap_or("public")
        .to_string();
    let database = nr
        .get("database")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let primary_entity = if primary_entity.is_empty() {
        None
    } else {
        Some(primary_entity)
    };

    // Get entities.
    let ent_rows = store.lookup_model_entities(&unique_id)?;
    let entities: Vec<EntityDef> = ent_rows
        .into_iter()
        .map(|r| {
            let expr = if r.expr.is_empty() {
                r.name.clone()
            } else {
                r.expr
            };
            EntityDef {
                name: r.name,
                entity_type: r.entity_type,
                expr,
            }
        })
        .collect();

    // Get dimensions.
    let dim_rows = store.lookup_model_dimensions(&unique_id)?;
    let dimensions: Vec<DimensionDef> = dim_rows
        .into_iter()
        .map(|r| {
            let expr = if r.expr.is_empty() {
                r.name.clone()
            } else {
                r.expr
            };
            DimensionDef {
                name: r.name,
                dimension_type: r.dimension_type,
                expr,
                time_granularity: if r.time_granularity.is_empty() {
                    None
                } else {
                    Some(r.time_granularity)
                },
            }
        })
        .collect();

    Ok(ResolvedModel {
        name: name.to_string(),
        relation_name,
        alias,
        schema_name,
        database,
        primary_entity,
        entities,
        dimensions,
    })
}

/// Find the semantic model that owns a given entity as primary/unique.
fn find_model_for_entity_pk(
    store: &mut impl MetricStore,
    entity_name: &str,
) -> Result<Option<String>, MetricFlowError> {
    store.find_model_for_entity(entity_name, true)
}

/// Like `find_model_for_entity_pk` but matches any entity type (including foreign).
fn find_model_for_entity_any(
    store: &mut impl MetricStore,
    entity_name: &str,
) -> Result<Option<String>, MetricFlowError> {
    store.find_model_for_entity(entity_name, false)
}

/// Load time spine from the store, if any.
fn load_time_spine(store: &mut impl MetricStore) -> Option<TimeSpine> {
    let row = store.lookup_time_spine().ok()??;

    let nr: serde_json::Value = serde_json::from_str(&row.node_relation).ok()?;
    let relation_name = nr.get("relation_name")?.as_str()?.to_string();

    Some(TimeSpine {
        relation_name,
        primary_column: row.primary_column,
        primary_granularity: row.primary_granularity,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// Entity join graph
// ═══════════════════════════════════════════════════════════════════════════

/// Build the join graph from entity relationships across all semantic models.
fn build_join_graph(store: &mut impl MetricStore) -> Result<Vec<JoinEdge>, MetricFlowError> {
    let rows = store.lookup_all_join_graph_entities()?;

    struct EntityInfo {
        model_name: String,
        entity_type: String,
        expr: String,
    }

    let mut entity_map: HashMap<String, Vec<EntityInfo>> = HashMap::new();
    for r in &rows {
        entity_map
            .entry(r.entity_name.clone())
            .or_default()
            .push(EntityInfo {
                model_name: r.model_name.clone(),
                entity_type: r.entity_type.clone(),
                expr: if r.expr.is_empty() {
                    r.entity_name.clone()
                } else {
                    r.expr.clone()
                },
            });
    }

    let mut edges = Vec::new();

    for (entity_name, infos) in &entity_map {
        // Find primary/unique side (PK) and foreign side (FK).
        let pk_models: Vec<&EntityInfo> = infos
            .iter()
            .filter(|i| {
                i.entity_type == "primary"
                    || i.entity_type == "unique"
                    || i.entity_type == "natural"
            })
            .collect();
        let fk_models: Vec<&EntityInfo> = infos
            .iter()
            .filter(|i| i.entity_type == "foreign")
            .collect();

        // Create bidirectional edges between FK and PK models so
        // find_join_path works regardless of which model is primary.
        for fk in &fk_models {
            for pk in &pk_models {
                if fk.model_name != pk.model_name {
                    edges.push(JoinEdge {
                        from_model: fk.model_name.clone(),
                        to_model: pk.model_name.clone(),
                        from_expr: fk.expr.clone(),
                        to_expr: pk.expr.clone(),
                        entity_name: entity_name.clone(),
                    });
                    edges.push(JoinEdge {
                        from_model: pk.model_name.clone(),
                        to_model: fk.model_name.clone(),
                        from_expr: pk.expr.clone(),
                        to_expr: fk.expr.clone(),
                        entity_name: entity_name.clone(),
                    });
                }
            }
        }

        // Also create edges between primary entities of different models
        // (for models that share a primary entity at the same grain).
        if pk_models.len() > 1 {
            for i in 0..pk_models.len() {
                for j in (i + 1)..pk_models.len() {
                    edges.push(JoinEdge {
                        from_model: pk_models[i].model_name.clone(),
                        to_model: pk_models[j].model_name.clone(),
                        from_expr: pk_models[i].expr.clone(),
                        to_expr: pk_models[j].expr.clone(),
                        entity_name: entity_name.clone(),
                    });
                    // Bidirectional.
                    edges.push(JoinEdge {
                        from_model: pk_models[j].model_name.clone(),
                        to_model: pk_models[i].model_name.clone(),
                        from_expr: pk_models[j].expr.clone(),
                        to_expr: pk_models[i].expr.clone(),
                        entity_name: entity_name.clone(),
                    });
                }
            }
        }
    }

    Ok(edges)
}

/// Find the shortest join path from one model to another using BFS.
fn find_join_path(edges: &[JoinEdge], from: &str, to: &str) -> Option<Vec<JoinEdge>> {
    if from == to {
        return Some(vec![]);
    }

    // Build adjacency list.
    let mut adj: HashMap<&str, Vec<&JoinEdge>> = HashMap::new();
    for edge in edges {
        adj.entry(edge.from_model.as_str()).or_default().push(edge);
    }

    // BFS.
    let mut visited: HashSet<&str> = HashSet::new();
    visited.insert(from);
    let mut queue: Vec<(String, Vec<JoinEdge>)> = vec![(from.to_string(), vec![])];

    while let Some((current, path)) = queue.first().cloned() {
        queue.remove(0);

        if let Some(neighbors) = adj.get(current.as_str()) {
            for edge in neighbors {
                if edge.to_model == to {
                    let mut full_path = path;
                    full_path.push((*edge).clone());
                    return Some(full_path);
                }
                if !visited.contains(edge.to_model.as_str()) {
                    visited.insert(
                        // Extend lifetime via the edge slice.
                        edges
                            .iter()
                            .find(|e| e.to_model == edge.to_model)
                            .map(|e| e.to_model.as_str())
                            .unwrap_or(""),
                    );
                    let mut new_path = path.clone();
                    new_path.push((*edge).clone());
                    queue.push((edge.to_model.clone(), new_path));
                }
            }
        }
    }

    None
}

// ═══════════════════════════════════════════════════════════════════════════
// Where-filter template resolution
// ═══════════════════════════════════════════════════════════════════════════

/// Resolve Jinja-style dimension references in where filters.
///
/// Converts `{{ Dimension('entity__name') }}` → `alias.column_expr`
/// Converts `{{ TimeDimension('metric_time', 'day') }}` → `DATE_TRUNC('day', alias.column)`
fn resolve_where_filter(
    template: &str,
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
    dialect: Dialect,
    primary_model_name: &str,
) -> String {
    let mut result = template.to_string();

    // Resolve {{ Dimension('entity__name') }} patterns.
    while let Some(start) = result.find("{{ Dimension(") {
        let Some(end) = result[start..].find("}}").map(|i| start + i + 2) else {
            break;
        };
        let inner = &result[start + 2..end - 2].trim();
        let dim_ref = inner
            .strip_prefix("Dimension(")
            .and_then(|s| s.strip_suffix(')'))
            .unwrap_or(inner)
            .trim()
            .trim_matches('\'')
            .trim_matches('"');

        let resolved = resolve_dimension_ref(dim_ref, model_aliases, dialect, primary_model_name);
        result.replace_range(start..end, &resolved);
    }

    // Resolve {{ TimeDimension('name', 'grain') }} patterns.
    while let Some(start) = result.find("{{ TimeDimension(") {
        let Some(end) = result[start..].find("}}").map(|i| start + i + 2) else {
            break;
        };
        let inner = &result[start + 2..end - 2].trim();
        let td_ref = inner
            .strip_prefix("TimeDimension(")
            .and_then(|s| s.strip_suffix(')'))
            .unwrap_or(inner);
        let parts: Vec<&str> = td_ref.split(',').collect();
        let name = parts
            .first()
            .unwrap_or(&"metric_time")
            .trim()
            .trim_matches('\'')
            .trim_matches('"');
        let granularity = parts
            .get(1)
            .unwrap_or(&"day")
            .trim()
            .trim_matches('\'')
            .trim_matches('"');

        let resolved = resolve_time_dimension_ref(
            name,
            granularity,
            model_aliases,
            dialect,
            primary_model_name,
        );
        result.replace_range(start..end, &resolved);
    }

    // Resolve {{ Entity('name') }} patterns → entity column reference.
    while let Some(start) = result.find("{{ Entity(") {
        let Some(end) = result[start..].find("}}").map(|i| start + i + 2) else {
            break;
        };
        let inner = &result[start + 2..end - 2].trim();
        let entity_ref = inner
            .strip_prefix("Entity(")
            .and_then(|s| s.strip_suffix(')'))
            .unwrap_or(inner)
            .trim()
            .trim_matches('\'')
            .trim_matches('"');

        let resolved = resolve_entity_ref(entity_ref, model_aliases, primary_model_name);
        result.replace_range(start..end, &resolved);
    }

    // Resolve {{ Metric('name', ['entity']) }} — replace with the CTE column reference.
    // The CTE is pre-compiled by compile_metric_filter_ctes() and joined by the caller.
    while let Some(start) = result.find("{{ Metric(") {
        let Some(end) = result[start..].find("}}").map(|i| start + i + 2) else {
            break;
        };
        let inner = &result[start + 2..end - 2].trim();
        let metric_ref = inner
            .strip_prefix("Metric(")
            .and_then(|s| s.strip_suffix(')'))
            .unwrap_or(inner)
            .trim();

        // Parse metric name from 'metric_name', ['entity1']
        let metric_name = metric_ref
            .split(',')
            .next()
            .unwrap_or("")
            .trim()
            .trim_matches('\'')
            .trim_matches('"');

        // Parse entity list to build the qualified column name.
        let entity_prefix: String = metric_ref
            .split('[')
            .nth(1)
            .and_then(|s| s.split(']').next())
            .map(|s| {
                s.split(',')
                    .map(|e| e.trim().trim_matches('\'').trim_matches('"'))
                    .filter(|e| !e.is_empty())
                    .collect::<Vec<_>>()
                    .join("__")
            })
            .unwrap_or_default();

        // The CTE alias is __mf_{metric_name}. The column is {entity_prefix}__{metric_name}.
        let col_name = if entity_prefix.is_empty() {
            metric_name.to_string()
        } else {
            format!("{entity_prefix}__{metric_name}")
        };
        let resolved = format!("__mf_{metric_name}.{col_name}");
        result.replace_range(start..end, &resolved);
    }

    result
}

/// Parsed metric filter reference: metric name + entity list.
struct MetricFilterRef {
    metric_name: String,
    entities: Vec<String>,
}

/// Scan WHERE filters for `{{ Metric('name', ['entity']) }}` and return parsed references.
fn extract_metric_filter_refs(filters: &[String]) -> Vec<MetricFilterRef> {
    let mut refs = Vec::new();
    for filter in filters {
        let mut cursor = 0;
        while let Some(start) = filter[cursor..].find("{{ Metric(") {
            let abs_start = cursor + start;
            let Some(end) = filter[abs_start..].find("}}").map(|i| abs_start + i + 2) else {
                break;
            };
            let inner = &filter[abs_start + 2..end - 2].trim();
            let metric_ref = inner
                .strip_prefix("Metric(")
                .and_then(|s| s.strip_suffix(')'))
                .unwrap_or(inner)
                .trim();

            let metric_name = metric_ref
                .split(',')
                .next()
                .unwrap_or("")
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();

            let entities: Vec<String> = metric_ref
                .split('[')
                .nth(1)
                .and_then(|s| s.split(']').next())
                .map(|s| {
                    s.split(',')
                        .map(|e| e.trim().trim_matches('\'').trim_matches('"').to_string())
                        .filter(|e| !e.is_empty())
                        .collect()
                })
                .unwrap_or_default();

            if !metric_name.is_empty()
                && !refs
                    .iter()
                    .any(|r: &MetricFilterRef| r.metric_name == metric_name)
            {
                refs.push(MetricFilterRef {
                    metric_name,
                    entities,
                });
            }
            cursor = end;
        }
    }
    refs
}

/// Compile metric filter CTEs and return LEFT JOIN clauses to add to the main query.
///
/// For each `{{ Metric('booking_value', ['guest']) }}` reference, produces a CTE like:
///   __mf_booking_value AS (SELECT guest_id AS guest, SUM(booking_value) AS guest__booking_value FROM ... GROUP BY 1)
/// and a JOIN clause like:
///   LEFT JOIN __mf_booking_value ON outer.guest_id = __mf_booking_value.guest
/// Compile a simple metric filter CTE: `SELECT entity_expr AS entity, AGG(expr) AS col FROM src GROUP BY 1`
fn compile_simple_metric_filter_cte(
    metric_name: &str,
    ap: &AggParams,
    entities: &[String],
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
    dialect: Dialect,
    ctes: &mut Vec<(String, String)>,
) -> Option<String> {
    let (_, source_model) = model_aliases.get(&ap.semantic_model)?;
    let cte_name = format!("__mf_{metric_name}");
    let source_relation = render_full_relation(source_model, dialect);
    let agg_expr = render_agg(&ap.agg, &ap.expr, dialect);

    let mut select_parts: Vec<String> = Vec::new();
    for entity_name in entities {
        let bare_entity = entity_name
            .rsplit("__")
            .next()
            .unwrap_or(entity_name.as_str());
        let source_entity = source_model.entities.iter().find(|e| e.name == bare_entity);
        let source_expr = source_entity
            .map(|e| e.expr.as_str())
            .unwrap_or(entity_name.as_str());
        select_parts.push(format!("{source_expr} AS {entity_name}"));
    }

    let col_name = if entities.is_empty() {
        metric_name.to_string()
    } else {
        format!("{}__{metric_name}", entities.join("__"))
    };
    select_parts.push(format!("{agg_expr} AS {col_name}"));

    let group_indices: Vec<String> = (1..=entities.len()).map(|i| i.to_string()).collect();
    let group_clause = if group_indices.is_empty() {
        String::new()
    } else {
        format!(" GROUP BY {}", group_indices.join(", "))
    };

    let cte_sql = format!(
        "SELECT {} FROM {source_relation}{group_clause}",
        select_parts.join(", "),
    );
    ctes.push((cte_name, cte_sql));
    Some(col_name)
}

#[allow(clippy::cognitive_complexity)]
fn compile_metric_filter_ctes(
    refs: &[MetricFilterRef],
    all_metrics: &HashMap<String, ResolvedMetric>,
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
    primary_model_name: &str,
    primary_alias: &str,
    dialect: Dialect,
    ctes: &mut Vec<(String, String)>,
) -> Vec<String> {
    let mut joins = Vec::new();

    for mfr in refs {
        let metric = match all_metrics.get(&mfr.metric_name) {
            Some(m) => m,
            None => continue,
        };

        let cte_name = format!("__mf_{}", mfr.metric_name);

        match metric.metric_type {
            MetricType::Simple | MetricType::Cumulative => {
                let ap = match &metric.agg_params {
                    Some(ap) => ap,
                    None => continue,
                };
                compile_simple_metric_filter_cte(
                    &mfr.metric_name,
                    ap,
                    &mfr.entities,
                    model_aliases,
                    dialect,
                    ctes,
                );
            }
            MetricType::Derived => {
                // Compile each input metric as a sub-CTE, then build a composite CTE.
                let derived_expr = match &metric.derived_expr {
                    Some(e) => e.clone(),
                    None => continue,
                };
                let mut sub_cols: Vec<(String, String)> = Vec::new(); // (alias/name, cte_col_name)
                for input in &metric.input_metrics {
                    let sub_metric = match all_metrics.get(&input.name) {
                        Some(m) => m,
                        None => continue,
                    };
                    if let Some(sub_ap) = &sub_metric.agg_params {
                        let sub_name = input.alias.as_deref().unwrap_or(&input.name);
                        if let Some(col) = compile_simple_metric_filter_cte(
                            sub_name,
                            sub_ap,
                            &mfr.entities,
                            model_aliases,
                            dialect,
                            ctes,
                        ) {
                            sub_cols.push((sub_name.to_string(), col));
                        }
                    }
                }
                if sub_cols.is_empty() {
                    continue;
                }
                // Build composite CTE joining all sub-CTEs.
                let first_sub = format!("__mf_{}", sub_cols[0].0);
                let mut entity_selects: Vec<String> = Vec::new();
                for entity_name in &mfr.entities {
                    entity_selects.push(format!("{first_sub}.{entity_name}"));
                }
                // Resolve the derived expression: replace alias names with CTE column references.
                // Sort by length (longest first) to prevent substring collisions.
                let mut sorted_subs = sub_cols.clone();
                sorted_subs.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
                let mut resolved_expr = derived_expr.clone();
                for (alias, col) in &sorted_subs {
                    let cte_ref = format!("__mf_{alias}.{col}");
                    resolved_expr = replace_word(&resolved_expr, alias, &cte_ref);
                }
                let out_col = if mfr.entities.is_empty() {
                    mfr.metric_name.clone()
                } else {
                    format!("{}__{}", mfr.entities.join("__"), mfr.metric_name)
                };
                entity_selects.push(format!("{resolved_expr} AS {out_col}"));
                let mut from_clause = first_sub.clone();
                for (alias, _) in sub_cols.iter().skip(1) {
                    let sub_cte = format!("__mf_{alias}");
                    if mfr.entities.is_empty() {
                        from_clause.push_str(&format!(" CROSS JOIN {sub_cte}"));
                    } else {
                        let join_conds: Vec<String> = mfr
                            .entities
                            .iter()
                            .map(|e| format!("{first_sub}.{e} = {sub_cte}.{e}"))
                            .collect();
                        from_clause.push_str(&format!(
                            " LEFT JOIN {sub_cte} ON {}",
                            join_conds.join(" AND ")
                        ));
                    }
                }
                let cte_sql = format!("SELECT {} FROM {from_clause}", entity_selects.join(", "),);
                ctes.push((cte_name.clone(), cte_sql));
            }
            MetricType::Ratio => {
                // Compile numerator and denominator as sub-CTEs.
                let num = match &metric.numerator {
                    Some(n) => n,
                    None => continue,
                };
                let den = match &metric.denominator {
                    Some(d) => d,
                    None => continue,
                };
                let num_metric = match all_metrics.get(&num.name) {
                    Some(m) => m,
                    None => continue,
                };
                let den_metric = match all_metrics.get(&den.name) {
                    Some(m) => m,
                    None => continue,
                };
                let num_ap = match &num_metric.agg_params {
                    Some(ap) => ap,
                    None => continue,
                };
                let den_ap = match &den_metric.agg_params {
                    Some(ap) => ap,
                    None => continue,
                };
                let num_label = format!("{}_num", mfr.metric_name);
                let den_label = format!("{}_den", mfr.metric_name);
                let num_col = compile_simple_metric_filter_cte(
                    &num_label,
                    num_ap,
                    &mfr.entities,
                    model_aliases,
                    dialect,
                    ctes,
                );
                let den_col = compile_simple_metric_filter_cte(
                    &den_label,
                    den_ap,
                    &mfr.entities,
                    model_aliases,
                    dialect,
                    ctes,
                );
                if let (Some(nc), Some(dc)) = (num_col, den_col) {
                    let num_cte = format!("__mf_{num_label}");
                    let den_cte = format!("__mf_{den_label}");
                    let mut entity_selects: Vec<String> = Vec::new();
                    for entity_name in &mfr.entities {
                        entity_selects.push(format!("{num_cte}.{entity_name}"));
                    }
                    let out_col = if mfr.entities.is_empty() {
                        mfr.metric_name.clone()
                    } else {
                        format!("{}__{}", mfr.entities.join("__"), mfr.metric_name)
                    };
                    let ratio_expr = render_cast_double(&format!("{num_cte}.{nc}"), dialect);
                    let den_expr = format!("NULLIF({den_cte}.{dc}, 0)");
                    entity_selects.push(format!("{ratio_expr} / {den_expr} AS {out_col}"));
                    let join_clause = if mfr.entities.is_empty() {
                        format!("{num_cte} CROSS JOIN {den_cte}")
                    } else {
                        let join_conds: Vec<String> = mfr
                            .entities
                            .iter()
                            .map(|e| format!("{num_cte}.{e} = {den_cte}.{e}"))
                            .collect();
                        format!(
                            "{num_cte} LEFT JOIN {den_cte} ON {}",
                            join_conds.join(" AND ")
                        )
                    };
                    let cte_sql =
                        format!("SELECT {} FROM {join_clause}", entity_selects.join(", "),);
                    ctes.push((cte_name.clone(), cte_sql));
                }
            }
            MetricType::Conversion => continue,
        }

        // Build join condition from the outer CTE to __mf_{metric_name}.
        let mut join_conds: Vec<String> = Vec::new();
        if let Some((_, primary_model)) = model_aliases.get(primary_model_name) {
            for entity_name in &mfr.entities {
                let bare_entity = entity_name
                    .rsplit("__")
                    .next()
                    .unwrap_or(entity_name.as_str());
                let outer_expr = primary_model
                    .entities
                    .iter()
                    .find(|e| e.name == bare_entity)
                    .map(|e| format!("{primary_alias}.{}", e.expr))
                    .unwrap_or_else(|| format!("{primary_alias}.{entity_name}"));
                join_conds.push(format!("{outer_expr} = {cte_name}.{entity_name}"));
            }
        }

        if join_conds.is_empty() {
            joins.push(format!("LEFT JOIN {cte_name} ON TRUE"));
        } else {
            joins.push(format!(
                "LEFT JOIN {cte_name} ON {}",
                join_conds.join(" AND ")
            ));
        }
    }

    joins
}

fn resolve_dimension_ref(
    dim_ref: &str,
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
    _dialect: Dialect,
    primary_model_name: &str,
) -> String {
    let (entity, dim_name) = if let Some((e, d)) = dim_ref.split_once("__") {
        (Some(e), d)
    } else {
        (None, dim_ref)
    };

    let check_model = |alias: &str, model: &ResolvedModel| -> Option<String> {
        if let Some(entity_name) = entity {
            let has_entity = model.entities.iter().any(|e| e.name == entity_name)
                || model.primary_entity.as_deref() == Some(entity_name);
            if !has_entity {
                return None;
            }
        }
        model
            .dimensions
            .iter()
            .find(|d| d.name == dim_name)
            .map(|dim| format!("{}.{}", alias, dim.expr))
    };

    // Check primary model first for deterministic resolution.
    if let Some((alias, model)) = model_aliases.get(primary_model_name) {
        if let Some(result) = check_model(alias, model) {
            return result;
        }
    }
    for (alias, model) in model_aliases.values() {
        if let Some(result) = check_model(alias, model) {
            return result;
        }
    }

    // Fallback: use the dimension name as-is.
    dim_name.to_string()
}

/// Resolve `Entity('name')` to `alias.expr` by finding the entity in any resolved model.
/// Checks the primary model first for deterministic resolution.
fn resolve_entity_ref(
    entity_name: &str,
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
    primary_model_name: &str,
) -> String {
    // Check the primary model first.
    if let Some((alias, model)) = model_aliases.get(primary_model_name) {
        if let Some(ent) = model.entities.iter().find(|e| e.name == entity_name) {
            return format!("{}.{}", alias, ent.expr);
        }
    }
    // Check remaining models.
    for (alias, model) in model_aliases.values() {
        if let Some(ent) = model.entities.iter().find(|e| e.name == entity_name) {
            return format!("{}.{}", alias, ent.expr);
        }
    }
    // Fallback: use entity name as column name.
    entity_name.to_string()
}

fn resolve_time_dimension_ref(
    name: &str,
    granularity: &str,
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
    dialect: Dialect,
    primary_model_name: &str,
) -> String {
    // Strip an optional entity prefix: `user_account_activity__date_day` → `date_day`.
    // Also extract the entity name for scoped matching (mirrors resolve_dimension_ref).
    let (entity_prefix, dim_name) = match name.split_once("__") {
        Some((e, d)) => (Some(e), d),
        None => (None, name),
    };

    let check_model = |alias: &str, model: &ResolvedModel| -> Option<String> {
        // If an entity prefix was supplied, require the model to own that entity
        // (check both explicit entities and the primary_entity).
        if let Some(entity) = entity_prefix {
            let has_entity = model.entities.iter().any(|e| e.name == entity)
                || model.primary_entity.as_deref() == Some(entity);
            if !has_entity {
                return None;
            }
        }
        for dim in &model.dimensions {
            if dim.dimension_type == "time"
                && (name == "metric_time" || dim.name == dim_name || dim.name == name)
            {
                return Some(render_date_trunc(
                    granularity,
                    &format!("{}.{}", alias, dim.expr),
                    dialect,
                ));
            }
        }
        None
    };
    // Check primary model first for deterministic resolution.
    if let Some((alias, model)) = model_aliases.get(primary_model_name) {
        if let Some(result) = check_model(alias, model) {
            return result;
        }
    }
    for (alias, model) in model_aliases.values() {
        if let Some(result) = check_model(alias, model) {
            return result;
        }
    }

    // Fallback.
    render_date_trunc(granularity, name, dialect)
}

// ═══════════════════════════════════════════════════════════════════════════
// Dialect-specific SQL rendering helpers
// ═══════════════════════════════════════════════════════════════════════════

fn render_type_cast(expr: &str, sql_type: &str, dialect: Dialect) -> String {
    match dialect {
        Dialect::DuckDB | Dialect::Snowflake | Dialect::Redshift => {
            format!("{expr}::{sql_type}")
        }
        Dialect::BigQuery | Dialect::Databricks => {
            format!("CAST({expr} AS {sql_type})")
        }
    }
}

fn render_date_trunc(granularity: &str, expr: &str, dialect: Dialect) -> String {
    let subdaily = matches!(granularity, "hour" | "minute" | "second" | "millisecond");
    let target_type = if subdaily { "TIMESTAMP" } else { "DATE" };
    let gran_upper = granularity.to_uppercase();
    let raw = match dialect {
        Dialect::BigQuery => format!("DATE_TRUNC({expr}, {gran_upper})"),
        _ => format!("DATE_TRUNC('{granularity}', {expr})"),
    };
    render_type_cast(&raw, target_type, dialect)
}

fn render_extract(part: &str, expr: &str, dialect: Dialect) -> String {
    let p = part.to_uppercase();
    if p == "DOW" {
        return match dialect {
            Dialect::DuckDB => format!("EXTRACT(ISODOW FROM {expr})"),
            Dialect::Snowflake => format!("EXTRACT(DAYOFWEEKISO FROM {expr})"),
            Dialect::Databricks => format!("EXTRACT(DAYOFWEEK_ISO FROM {expr})"),
            Dialect::Redshift => {
                format!("(EXTRACT(DOW FROM {expr}) + 6) % 7 + 1")
            }
            Dialect::BigQuery => {
                let base = format!("EXTRACT(DAYOFWEEK FROM {expr})");
                format!("IF({base} = 1, 7, {base} - 1)")
            }
        };
    }
    let mapped = match p.as_str() {
        "DOY" if matches!(dialect, Dialect::BigQuery) => "DAYOFYEAR",
        other => other,
    };
    format!("EXTRACT({mapped} FROM {expr})")
}

fn render_cast_double(expr: &str, dialect: Dialect) -> String {
    let float_type = match dialect {
        Dialect::DuckDB | Dialect::Databricks => "DOUBLE",
        Dialect::Snowflake => "FLOAT",
        Dialect::Redshift => "DOUBLE PRECISION",
        Dialect::BigQuery => "FLOAT64",
    };
    format!("CAST({expr} AS {float_type})")
}

fn render_interval(count: i64, granularity: &str, dialect: Dialect) -> String {
    let gran_upper = granularity.to_uppercase();
    match dialect {
        Dialect::Redshift => {
            let (days, gran) = match granularity {
                "month" | "months" => (count * 30, "day"),
                "year" | "years" => (count * 365, "day"),
                "week" | "weeks" => (count * 7, "day"),
                _ => (count, granularity),
            };
            format!("INTERVAL '{days} {gran}'")
        }
        Dialect::DuckDB | Dialect::Snowflake => {
            format!("INTERVAL '{count} {granularity}'")
        }
        Dialect::BigQuery | Dialect::Databricks => {
            format!("INTERVAL {count} {gran_upper}")
        }
    }
}

fn render_interval_str(raw: &str, dialect: Dialect) -> String {
    if matches!(dialect, Dialect::Redshift) {
        if let Some((num_str, gran)) = raw.split_once(' ') {
            if let Ok(count) = num_str.trim().parse::<i64>() {
                return render_interval(count, gran.trim(), dialect);
            }
        }
    }
    format!("INTERVAL '{raw}'")
}

/// Resolve an order-by name to the canonical SQL output column name.
///
/// When multiple granularities of the same time dimension are present, the
/// output columns are qualified (`metric_time__month`), so we return the
/// qualified form.  Otherwise the bare name is used.
fn resolve_order_by_col(name: &str, group_by: &[GroupBySpec]) -> String {
    let out_cols = group_by_output_cols(group_by);
    // Direct match on output column names.
    if out_cols.iter().any(|c| c == name) {
        return name.to_string();
    }
    // Match `metric_time__month` against a TimeDimension with that qualified name.
    for (gb, out_col) in group_by.iter().zip(out_cols.iter()) {
        if let GroupBySpec::TimeDimension {
            name: td_name,
            granularity,
            ..
        } = gb
        {
            if name == format!("{td_name}__{granularity}") {
                return out_col.clone();
            }
        }
    }
    name.to_string()
}

fn render_agg(agg: &str, expr: &str, dialect: Dialect) -> String {
    match agg {
        "sum" => format!("SUM({expr})"),
        "count" => format!("COUNT({expr})"),
        "count_distinct" => format!("COUNT(DISTINCT {expr})"),
        "average" | "avg" => format!("AVG({expr})"),
        "min" => format!("MIN({expr})"),
        "max" => format!("MAX({expr})"),
        "sum_boolean" => match dialect {
            Dialect::DuckDB => format!("SUM(CAST({expr} AS INTEGER))"),
            _ => format!("SUM(CASE WHEN {expr} THEN 1 ELSE 0 END)"),
        },
        "median" => match dialect {
            Dialect::BigQuery => format!("APPROX_QUANTILES({expr}, 2)[OFFSET(1)]"),
            _ => format!("MEDIAN({expr})"),
        },
        "percentile" => match dialect {
            Dialect::BigQuery => format!("APPROX_QUANTILES({expr}, 100)[OFFSET(50)]"),
            _ => format!("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {expr})"),
        },
        other => format!("{other}({expr})"),
    }
}

/// Qualify a measure expression with a table alias.
/// Literals (numbers, `*`, strings) and expressions containing operators or
/// function calls are left as-is; simple column names get `alias.col`.
/// Word-boundary-aware string replacement.  Only replaces occurrences of
/// `find` that are not immediately preceded or followed by an identifier
/// character (alphanumeric or underscore).
fn replace_word(text: &str, find: &str, replace: &str) -> String {
    if find.is_empty() {
        return text.to_string();
    }
    let mut result = String::with_capacity(text.len());
    let text_bytes = text.as_bytes();
    let find_bytes = find.as_bytes();
    let mut i = 0;
    while i <= text_bytes.len().saturating_sub(find_bytes.len()) {
        if text_bytes[i..].starts_with(find_bytes) {
            // Check character before match.
            let before_ok = i == 0 || {
                let c = text_bytes[i - 1];
                !c.is_ascii_alphanumeric() && c != b'_'
            };
            // Check character after match.
            let after_pos = i + find_bytes.len();
            let after_ok = after_pos >= text_bytes.len() || {
                let c = text_bytes[after_pos];
                !c.is_ascii_alphanumeric() && c != b'_'
            };
            if before_ok && after_ok {
                result.push_str(replace);
                i += find_bytes.len();
                continue;
            }
        }
        result.push(text_bytes[i] as char);
        i += 1;
    }
    // Append remaining characters that couldn't start a match.
    while i < text_bytes.len() {
        result.push(text_bytes[i] as char);
        i += 1;
    }
    result
}

/// Returns `true` if `expr` is a compound expression — one that cannot be
/// trivially qualified by prepending a table alias — because it contains
/// operators, function calls, or SQL keywords (e.g. a CASE expression).
///
/// Pure literals (`*`, numeric constants, string literals) return `false`
/// because they contain no column references and are never ambiguous.
fn is_complex_measure_expr(expr: &str) -> bool {
    let trimmed = expr.trim();
    if trimmed == "*" || trimmed.parse::<f64>().is_ok() || trimmed.starts_with('\'') {
        return false;
    }
    trimmed.contains('(') || trimmed.contains(' ')
}

fn qualify_measure_expr(alias: &str, expr: &str) -> String {
    let trimmed = expr.trim();
    if trimmed == "*"
        || trimmed.parse::<f64>().is_ok()
        || trimmed.starts_with('\'')
        || is_complex_measure_expr(trimmed)
    {
        trimmed.to_string()
    } else {
        format!("{alias}.{trimmed}")
    }
}

fn render_full_relation(model: &ResolvedModel, dialect: Dialect) -> String {
    match dialect {
        Dialect::Databricks => model.relation_name.replace('"', "`"),
        _ => model.relation_name.clone(),
    }
}

/// Generate an inline time spine CTE that produces a DATE column named `out_col`
/// spanning `[MIN(src_col) FROM src_cte .. MAX(src_col) FROM src_cte]` at the given granularity.
///
/// `src_col` is the column in the source CTE to derive the range from.
/// `out_col` is the output column name in the spine CTE.
///
/// DuckDB:    `SELECT ds::DATE AS out_col FROM generate_series(MIN, MAX, INTERVAL '1 gran') AS t(ds)`
/// Snowflake: `SELECT DATEADD(gran, ROW_NUMBER() OVER (ORDER BY 1) - 1, MIN)::DATE AS out_col
///             FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF(gran, MIN, MAX) + 1))`
fn inline_time_spine_sql(
    out_col: &str,
    src_cte: &str,
    src_col: &str,
    granularity: &str,
    dialect: Dialect,
) -> String {
    let min_expr = format!("(SELECT MIN({src_col}) FROM {src_cte})");
    let max_expr = format!("(SELECT MAX({src_col}) FROM {src_cte})");
    let subdaily = matches!(granularity, "hour" | "minute" | "second" | "millisecond");
    let target_type = if subdaily { "TIMESTAMP" } else { "DATE" };
    let gran_upper = granularity.to_uppercase();
    match dialect {
        Dialect::DuckDB => {
            let cast = render_type_cast("ds", target_type, dialect);
            format!(
                "SELECT {cast} AS {out_col} \
                 FROM generate_series({min_expr}, {max_expr}, INTERVAL '1 {granularity}') AS t(ds)"
            )
        }
        Dialect::Snowflake => {
            let dateadd =
                format!("DATEADD('{granularity}', ROW_NUMBER() OVER (ORDER BY 1) - 1, {min_expr})");
            let cast = render_type_cast(&dateadd, target_type, dialect);
            format!(
                "SELECT {cast} AS {out_col} \
                 FROM TABLE(GENERATOR(ROWCOUNT => 100000)) \
                 QUALIFY {out_col} <= {max_expr}"
            )
        }
        Dialect::Redshift => {
            let digits = "(SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 \
                           UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 \
                           UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)";
            let dateadd = format!("DATEADD('{granularity}', n, {min_expr})");
            let cast = render_type_cast(&dateadd, target_type, dialect);
            format!(
                "SELECT {cast} AS {out_col} \
                 FROM (\
                 SELECT (p0.n + p1.n * 10 + p2.n * 100 + p3.n * 1000 + p4.n * 10000) AS n \
                 FROM {digits} p0 \
                 CROSS JOIN {digits} p1 \
                 CROSS JOIN {digits} p2 \
                 CROSS JOIN {digits} p3 \
                 CROSS JOIN {digits} p4\
                 ) \
                 WHERE {out_col} <= {max_expr}"
            )
        }
        Dialect::BigQuery => {
            let array_fn = if subdaily {
                format!("GENERATE_TIMESTAMP_ARRAY({min_expr}, {max_expr}, INTERVAL 1 {gran_upper})")
            } else {
                format!("GENERATE_DATE_ARRAY({min_expr}, {max_expr}, INTERVAL 1 {gran_upper})")
            };
            format!("SELECT {out_col} FROM UNNEST({array_fn}) AS {out_col}")
        }
        Dialect::Databricks => {
            format!(
                "SELECT EXPLODE(SEQUENCE({min_expr}, {max_expr}, INTERVAL 1 {gran_upper})) AS {out_col}"
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Main compilation pipeline
// ═══════════════════════════════════════════════════════════════════════════

/// Compile a semantic query spec into SQL.
#[allow(clippy::cognitive_complexity)]
pub fn compile(
    store: &mut impl MetricStore,
    spec: &SemanticQuerySpec,
    dialect: Dialect,
) -> Result<String, MetricFlowError> {
    // 1. Resolve all metrics (recursively for derived/ratio).
    let mut all_metrics: HashMap<String, ResolvedMetric> = HashMap::new();
    for metric_name in &spec.metrics {
        resolve_metrics_recursive(store, metric_name, &mut all_metrics)?;
    }

    // 1b. Also resolve metrics referenced in {{ Metric() }} WHERE filters.
    let all_filters: Vec<String> = all_metrics
        .values()
        .flat_map(|m| m.metric_filters.iter().cloned())
        .chain(spec.where_filters.iter().cloned())
        .collect();
    for mfr in extract_metric_filter_refs(&all_filters) {
        resolve_metrics_recursive(store, &mfr.metric_name, &mut all_metrics)?;
    }

    // 2. Identify which metric types we're dealing with.
    let top_level: Vec<&ResolvedMetric> = spec
        .metrics
        .iter()
        .filter_map(|name| all_metrics.get(name))
        .collect();

    // 3. Compile based on the metric types present.
    // If all top-level metrics are simple, we can often combine into one query.
    // Derived and ratio metrics require subquery composition.

    let mut ctes: Vec<(String, String)> = Vec::new();
    let mut final_select_columns: Vec<String> = Vec::new();
    let mut final_from = String::new();
    let mut final_joins: Vec<String> = Vec::new();

    // Collect all needed semantic models and build the join graph.
    let join_edges = build_join_graph(store)?;

    // Determine which models are needed from metrics.
    let mut needed_models = collect_needed_models(&all_metrics, &spec.metrics)?;

    // Also resolve models needed for dimension entity references in group_by.
    for gb in &spec.group_by {
        if let GroupBySpec::Dimension {
            entity: Some(entity_name),
            ..
        } = gb
        {
            // Find the semantic model that owns this entity as primary/unique.
            if let Some(model_name) = find_model_for_entity_pk(store, entity_name)? {
                if !needed_models.contains(&model_name) {
                    needed_models.push(model_name);
                }
            }
        }
    }

    // Also resolve models needed for Entity group-by specs — but only if
    // no already-needed model provides the entity.
    for gb in &spec.group_by {
        if let GroupBySpec::Entity { name: entity_name } = gb {
            let already_covered = needed_models.iter().any(|mn| {
                store
                    .check_entity_in_model(mn, entity_name)
                    .unwrap_or(false)
            });
            if !already_covered {
                if let Some(model_name) = find_model_for_entity_any(store, entity_name)? {
                    if !needed_models.contains(&model_name) {
                        needed_models.push(model_name);
                    }
                }
            }
        }
    }

    // Also resolve models needed for entity references in where filters.
    // e.g. {{ Dimension('listing__country_latest') }} needs the listing model.
    for filter in spec
        .where_filters
        .iter()
        .chain(all_metrics.values().flat_map(|m| m.metric_filters.iter()))
    {
        let mut cursor = 0usize;
        while let Some(dim_start) = filter[cursor..].find("Dimension(") {
            let abs_pos = cursor + dim_start;
            // Skip if this is actually "TimeDimension(".
            if abs_pos > 0 && filter.as_bytes()[abs_pos - 1].is_ascii_alphanumeric() {
                cursor = abs_pos + 10;
                continue;
            }
            let abs_start = abs_pos + 10;
            if let Some(paren_end) = filter[abs_start..].find(')') {
                let dim_ref = filter[abs_start..abs_start + paren_end]
                    .trim()
                    .trim_matches('\'')
                    .trim_matches('"');
                if let Some((entity_name, _)) = dim_ref.split_once("__") {
                    if let Some(model_name) = find_model_for_entity_pk(store, entity_name)? {
                        if !needed_models.contains(&model_name) {
                            needed_models.push(model_name);
                        }
                    }
                }
                cursor = abs_start + paren_end + 1;
            } else {
                break;
            }
        }
    }

    // Also resolve models needed for {{ Metric(...) }} filter references.
    // The metric filter may reference metrics from models not otherwise needed.
    // For derived/ratio metrics, we need models from their input metrics too.
    for mfr in &extract_metric_filter_refs(&all_filters) {
        if let Some(m) = all_metrics.get(&mfr.metric_name) {
            if let Some(ref ap) = m.agg_params {
                if !ap.semantic_model.is_empty() && !needed_models.contains(&ap.semantic_model) {
                    needed_models.push(ap.semantic_model.clone());
                }
            }
            // Derived: resolve models for each input metric.
            for input in &m.input_metrics {
                if let Some(sub) = all_metrics.get(&input.name) {
                    if let Some(ref ap) = sub.agg_params {
                        if !ap.semantic_model.is_empty()
                            && !needed_models.contains(&ap.semantic_model)
                        {
                            needed_models.push(ap.semantic_model.clone());
                        }
                    }
                }
            }
            // Ratio: resolve models for numerator and denominator.
            for mi in m.numerator.iter().chain(m.denominator.iter()) {
                if let Some(sub) = all_metrics.get(&mi.name) {
                    if let Some(ref ap) = sub.agg_params {
                        if !ap.semantic_model.is_empty()
                            && !needed_models.contains(&ap.semantic_model)
                        {
                            needed_models.push(ap.semantic_model.clone());
                        }
                    }
                }
            }
        }
    }

    // Discover intermediate models needed for multi-hop join paths.
    // For each target model, find the BFS path from the primary model and
    // add any intermediate models that aren't yet in needed_models.
    let primary_model_name = needed_models.first().cloned().unwrap_or_default();
    let mut extra_models: Vec<String> = Vec::new();
    for target in &needed_models {
        if *target == primary_model_name {
            continue;
        }
        if let Some(path) = find_join_path(&join_edges, &primary_model_name, target) {
            for edge in &path {
                if !needed_models.contains(&edge.to_model) && !extra_models.contains(&edge.to_model)
                {
                    extra_models.push(edge.to_model.clone());
                }
            }
        }
    }
    needed_models.extend(extra_models);

    let mut resolved_models: HashMap<String, ResolvedModel> = HashMap::new();
    for model_name in &needed_models {
        resolved_models.insert(model_name.clone(), resolve_model(store, model_name)?);
    }

    // Assign table aliases.  Use the deterministic `needed_models` order
    // so the primary model always gets the short alias (i == 0).
    let mut model_alias_map: HashMap<String, (String, &ResolvedModel)> = HashMap::new();
    for (i, name) in needed_models.iter().enumerate() {
        if let Some(model) = resolved_models.get(name) {
            let alias = if i == 0 {
                model.alias.chars().next().unwrap_or('t').to_string()
            } else {
                format!("{}{}", model.alias.chars().next().unwrap_or('t'), i)
            };
            model_alias_map.insert(name.clone(), (alias, model));
        }
    }

    // Validate the query spec against the resolved models.
    validate_spec(spec, &all_metrics, &model_alias_map)?;

    // Check if we can compile everything into a single query or need CTEs.
    //
    // The single-query path is only semantically correct when all metrics share
    // the same semantic model (base table).  Mixing models in one FROM/JOIN
    // introduces fanout and leaks one metric's filters into the other.
    // When models differ, each metric must compile to its own CTE and the
    // results are stitched together with FULL OUTER JOIN.
    let all_simple = top_level
        .iter()
        .all(|m| m.metric_type == MetricType::Simple);
    let all_same_model = top_level
        .iter()
        .filter_map(|m| m.agg_params.as_ref().map(|ap| ap.semantic_model.as_str()))
        .collect::<HashSet<_>>()
        .len()
        <= 1;

    if all_simple && all_same_model && !top_level.is_empty() {
        compile_simple_metrics(
            spec,
            &top_level,
            &all_metrics,
            &model_alias_map,
            &join_edges,
            dialect,
        )
    } else {
        // Mixed or complex metrics: use CTE approach.
        compile_complex_metrics(
            store,
            spec,
            &all_metrics,
            &model_alias_map,
            &resolved_models,
            &join_edges,
            dialect,
            &mut ctes,
            &mut final_select_columns,
            &mut final_from,
            &mut final_joins,
        )
    }
}

/// Recursively resolve a metric and all its dependencies.
fn resolve_metrics_recursive(
    store: &mut impl MetricStore,
    name: &str,
    resolved: &mut HashMap<String, ResolvedMetric>,
) -> Result<(), MetricFlowError> {
    if resolved.contains_key(name) {
        return Ok(());
    }

    let metric = resolve_metric(store, name)?;

    // Resolve input metrics for derived metrics.
    for input in &metric.input_metrics {
        resolve_metrics_recursive(store, &input.name, resolved)?;
    }

    // Resolve numerator/denominator for ratio metrics.
    if let Some(ref num) = metric.numerator {
        resolve_metrics_recursive(store, &num.name, resolved)?;
    }
    if let Some(ref den) = metric.denominator {
        resolve_metrics_recursive(store, &den.name, resolved)?;
    }

    // Resolve base/conversion metrics for conversion metrics.
    if let Some(ref cp) = metric.conversion_params {
        if !cp.base_metric.is_empty() {
            resolve_metrics_recursive(store, &cp.base_metric, resolved)?;
        }
        if !cp.conversion_metric.is_empty() {
            resolve_metrics_recursive(store, &cp.conversion_metric, resolved)?;
        }
    }

    // Resolve cumulative metric's input metric.
    if metric.metric_type == MetricType::Cumulative {
        // Cumulative metrics typically have one input metric in the metrics array.
        for input in &metric.input_metrics {
            resolve_metrics_recursive(store, &input.name, resolved)?;
        }
    }

    resolved.insert(name.to_string(), metric);
    Ok(())
}

/// Collect all semantic model names needed by the requested metrics.
fn collect_needed_models(
    all_metrics: &HashMap<String, ResolvedMetric>,
    requested: &[String],
) -> Result<Vec<String>, MetricFlowError> {
    let mut models: HashSet<String> = HashSet::new();

    fn collect_from_metric(
        metric: &ResolvedMetric,
        all: &HashMap<String, ResolvedMetric>,
        models: &mut HashSet<String>,
    ) {
        if let Some(ref ap) = metric.agg_params {
            if !ap.semantic_model.is_empty() {
                models.insert(ap.semantic_model.clone());
            }
        }
        for input in &metric.input_metrics {
            if let Some(m) = all.get(&input.name) {
                collect_from_metric(m, all, models);
            }
        }
        if let Some(ref num) = metric.numerator {
            if let Some(m) = all.get(&num.name) {
                collect_from_metric(m, all, models);
            }
        }
        if let Some(ref den) = metric.denominator {
            if let Some(m) = all.get(&den.name) {
                collect_from_metric(m, all, models);
            }
        }
        if let Some(ref cp) = metric.conversion_params {
            if let Some(m) = all.get(&cp.base_metric) {
                collect_from_metric(m, all, models);
            }
            if let Some(m) = all.get(&cp.conversion_metric) {
                collect_from_metric(m, all, models);
            }
        }
    }

    for name in requested {
        if let Some(metric) = all_metrics.get(name) {
            collect_from_metric(metric, all_metrics, &mut models);
        }
    }

    let mut result: Vec<String> = models.into_iter().collect();
    result.sort();
    Ok(result)
}

// ═══════════════════════════════════════════════════════════════════════════
// Simple metric compilation (single-query path)
// ═══════════════════════════════════════════════════════════════════════════

#[allow(clippy::cognitive_complexity)]
fn compile_simple_metrics(
    spec: &SemanticQuerySpec,
    metrics: &[&ResolvedMetric],
    all_metrics: &HashMap<String, ResolvedMetric>,
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
    join_edges: &[JoinEdge],
    dialect: Dialect,
) -> Result<String, MetricFlowError> {
    let mut sql = String::new();

    // Determine the primary model (first metric's model).
    let primary_model_name = metrics
        .first()
        .and_then(|m| m.agg_params.as_ref())
        .map(|ap| ap.semantic_model.clone())
        .ok_or_else(|| {
            MetricFlowError::Other("no semantic model found for simple metrics".into())
        })?;

    let (primary_alias, primary_model) =
        model_aliases.get(&primary_model_name).ok_or_else(|| {
            MetricFlowError::Other(format!("semantic model not resolved: {primary_model_name}"))
        })?;

    // Collect target models that need to be reachable from the primary model.
    // We first gather the *target* model names, then resolve full join paths
    // (which may include intermediate hops).
    let mut target_models: Vec<String> = Vec::new();
    let mut target_set: HashSet<String> = HashSet::new();

    // 1. Metrics from other models.
    for metric in metrics {
        if let Some(ref ap) = metric.agg_params {
            if !ap.semantic_model.is_empty()
                && ap.semantic_model != primary_model_name
                && !target_set.contains(&ap.semantic_model)
            {
                target_models.push(ap.semantic_model.clone());
                target_set.insert(ap.semantic_model.clone());
            }
        }
    }

    // 2. Dimensions from other models (via entity references like Dimension('listing__country')).
    for gb in &spec.group_by {
        if let GroupBySpec::Dimension {
            entity: Some(entity_name),
            ..
        } = gb
        {
            // Find the model that owns this entity as primary/unique (the join target).
            for (model_name, (_alias, model)) in model_aliases {
                if model_name == &primary_model_name || target_set.contains(model_name) {
                    continue;
                }
                let has_entity = model.entities.iter().any(|e| e.name == *entity_name);
                if has_entity {
                    target_models.push(model_name.clone());
                    target_set.insert(model_name.clone());
                }
            }
        }
    }

    // 3. Entity group-by specs (e.g. Entity('lux_listing')) — find models with that entity.
    for gb in &spec.group_by {
        if let GroupBySpec::Entity { name: entity_name } = gb {
            // Check if the primary model already has this entity.
            let primary_has = primary_model
                .entities
                .iter()
                .any(|e| e.name == *entity_name);
            if !primary_has {
                for (model_name, (_alias, model)) in model_aliases {
                    if model_name == &primary_model_name || target_set.contains(model_name) {
                        continue;
                    }
                    let has_entity = model.entities.iter().any(|e| e.name == *entity_name);
                    if has_entity {
                        target_models.push(model_name.clone());
                        target_set.insert(model_name.clone());
                    }
                }
            }
        }
    }

    // 4. Where filters with entity-prefixed dimension references.
    for filter in spec
        .where_filters
        .iter()
        .chain(metrics.iter().flat_map(|m| m.metric_filters.iter()))
    {
        let mut cursor = 0usize;
        while let Some(dim_start) = filter[cursor..].find("Dimension(") {
            let abs_start = cursor + dim_start + 10;
            if let Some(paren_end) = filter[abs_start..].find(')') {
                let dim_ref = filter[abs_start..abs_start + paren_end]
                    .trim()
                    .trim_matches('\'')
                    .trim_matches('"');
                if let Some((entity_name, _)) = dim_ref.split_once("__") {
                    for (model_name, (_alias, model)) in model_aliases.iter() {
                        if model_name == &primary_model_name || target_set.contains(model_name) {
                            continue;
                        }
                        let has_entity = model.entities.iter().any(|e| e.name == entity_name);
                        if has_entity {
                            target_models.push(model_name.clone());
                            target_set.insert(model_name.clone());
                        }
                    }
                }
                cursor = abs_start + paren_end + 1;
            } else {
                break;
            }
        }
    }

    // Resolve full join paths for each target model, collecting all intermediate
    // models along the way.  Each entry is (edge, target_model_alias, target_model).
    // We track which models are already in the join set to avoid duplicates.
    // Models that can't be reached via join edges get a CROSS JOIN fallback.
    let mut join_sequence: Vec<JoinEdge> = Vec::new();
    let mut cross_join_models: Vec<String> = Vec::new();
    let mut joined_set: HashSet<String> = HashSet::new();
    joined_set.insert(primary_model_name.clone());

    for target in &target_models {
        if joined_set.contains(target) {
            continue;
        }
        if let Some(path) = find_join_path(join_edges, &primary_model_name, target) {
            for edge in &path {
                if !joined_set.contains(&edge.to_model) {
                    joined_set.insert(edge.to_model.clone());
                    join_sequence.push(edge.clone());
                }
            }
        } else {
            // No join path found — mark for CROSS JOIN.
            joined_set.insert(target.clone());
            cross_join_models.push(target.clone());
        }
    }

    // Build SELECT columns.
    let _ = writeln!(sql, "SELECT");

    let mut select_parts: Vec<String> = Vec::new();
    let out_cols = group_by_output_cols(&spec.group_by);

    // Group-by columns.
    for (gb, out_col) in spec.group_by.iter().zip(out_cols.iter()) {
        match gb {
            GroupBySpec::TimeDimension {
                name,
                granularity,
                date_part,
            } => {
                let col_expr = resolve_time_dimension_ref(
                    name,
                    granularity,
                    model_aliases,
                    dialect,
                    &primary_model_name,
                );
                if let Some(part) = date_part {
                    let extract_expr = render_extract(part, &col_expr, dialect);
                    select_parts.push(format!("  {extract_expr} AS {out_col}"));
                } else {
                    select_parts.push(format!("  {col_expr} AS {out_col}"));
                }
            }
            GroupBySpec::Dimension { entity, name } => {
                let dim_ref = match entity {
                    Some(e) => format!("{e}__{name}"),
                    None => name.clone(),
                };
                let col_expr =
                    resolve_dimension_ref(&dim_ref, model_aliases, dialect, &primary_model_name);
                select_parts.push(format!("  {col_expr} AS {out_col}"));
            }
            GroupBySpec::Entity { name } => {
                let col_expr = resolve_entity_ref(name, model_aliases, &primary_model_name);
                select_parts.push(format!("  {col_expr} AS {out_col}"));
            }
        }
    }

    // Metric columns.
    for metric in metrics {
        if let Some(ref ap) = metric.agg_params {
            let model_alias = model_aliases
                .get(&ap.semantic_model)
                .map(|(a, _)| a.as_str())
                .unwrap_or("t");
            let col_expr =
                if ap.semantic_model != primary_model_name && is_complex_measure_expr(&ap.expr) {
                    // Expression is pre-computed in the derived-table JOIN; reference it by name.
                    format!("{model_alias}.__mf_{}_expr", metric.name)
                } else {
                    qualify_measure_expr(model_alias, &ap.expr)
                };
            let agg_expr = render_agg(&ap.agg, &col_expr, dialect);
            select_parts.push(format!("  {agg_expr} AS {}", metric.name));
        }
    }

    sql.push_str(&select_parts.join(",\n"));
    let _ = writeln!(sql);

    // FROM clause.
    let from_relation = render_full_relation(primary_model, dialect);
    let _ = writeln!(sql, "FROM {from_relation} AS {primary_alias}");

    // For secondary models whose measure expression is complex (e.g. a CASE expression),
    // we wrap the JOIN target in a derived table that pre-computes the expression as a
    // named column.  This makes all column references unambiguous by construction —
    // inside the subquery only one table is in scope, so no alias qualification is needed.
    let mut complex_exprs_by_model: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for metric in metrics.iter() {
        if let Some(ref ap) = metric.agg_params {
            if ap.semantic_model != primary_model_name && is_complex_measure_expr(&ap.expr) {
                complex_exprs_by_model
                    .entry(ap.semantic_model.clone())
                    .or_default()
                    .push((metric.name.clone(), ap.expr.clone()));
            }
        }
    }

    // JOIN clauses — emit one LEFT JOIN per edge in the resolved join sequence.
    for edge in &join_sequence {
        let (join_alias, join_model) = match model_aliases.get(&edge.to_model) {
            Some((a, m)) => (a.as_str(), *m),
            None => continue,
        };
        let left_alias = if edge.from_model == primary_model_name {
            primary_alias.as_str()
        } else {
            model_aliases
                .get(&edge.from_model)
                .map(|(a, _)| a.as_str())
                .unwrap_or(primary_alias.as_str())
        };
        let join_relation = render_full_relation(join_model, dialect);
        if let Some(complex_exprs) = complex_exprs_by_model.get(&edge.to_model) {
            // Derived-table JOIN: pre-compute complex measure exprs so that bare column
            // references inside them are resolved against the secondary table only.
            let derived_cols: Vec<String> = complex_exprs
                .iter()
                .map(|(name, expr)| format!("  {expr} AS __mf_{name}_expr"))
                .collect();
            let _ = writeln!(
                sql,
                "LEFT JOIN (\n  SELECT *,\n{}\n  FROM {join_relation}\n) AS {join_alias} ON {left_alias}.{} = {join_alias}.{}",
                derived_cols.join(",\n"),
                edge.from_expr,
                edge.to_expr,
            );
        } else {
            let _ = writeln!(
                sql,
                "LEFT JOIN {join_relation} AS {join_alias} ON {left_alias}.{} = {join_alias}.{}",
                edge.from_expr, edge.to_expr,
            );
        }
    }

    // CROSS JOIN for models that have no entity-based join path.
    for model_name in &cross_join_models {
        if let Some((join_alias, join_model)) = model_aliases.get(model_name) {
            let join_relation = render_full_relation(join_model, dialect);
            let _ = writeln!(sql, "CROSS JOIN {join_relation} AS {join_alias}");
        }
    }

    // Semi-additive measure handling: INNER JOIN to subquery with MIN/MAX on
    // the non-additive time dimension.
    for metric in metrics {
        if let Some(ref ap) = metric.agg_params {
            if let Some(ref nad) = ap.non_additive_dimension {
                let nad_name = nad.get("name").and_then(|v| v.as_str()).unwrap_or("ds");
                let window_choice = nad
                    .get("window_choice")
                    .and_then(|v| v.as_str())
                    .unwrap_or("max");
                let window_groupings: Vec<&str> = nad
                    .get("window_groupings")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
                    .unwrap_or_default();

                let nad_dim = primary_model.dimensions.iter().find(|d| d.name == nad_name);
                let nad_expr = nad_dim.map(|d| d.expr.as_str()).unwrap_or(nad_name);
                let nad_gran = nad_dim
                    .and_then(|d| d.time_granularity.as_deref())
                    .unwrap_or("day");
                let agg_fn = if window_choice == "min" { "MIN" } else { "MAX" };

                let grouping_exprs: Vec<(String, String)> = window_groupings
                    .iter()
                    .filter_map(|&g| {
                        primary_model
                            .entities
                            .iter()
                            .find(|e| e.name == g)
                            .map(|e| (g.to_string(), e.expr.clone()))
                    })
                    .collect();

                let mut nad_select = Vec::new();
                let mut nad_join_conds = Vec::new();
                for (name, expr) in &grouping_exprs {
                    nad_select.push(format!("{expr} AS {name}"));
                    nad_join_conds.push(format!("{primary_alias}.{expr} = __nad.{name}"));
                }
                let trunc = format!("DATE_TRUNC('{nad_gran}', {nad_expr})");
                nad_select.push(format!("{agg_fn}({trunc}) AS __nad_time"));
                nad_join_conds.push(format!(
                    "DATE_TRUNC('{nad_gran}', {primary_alias}.{nad_expr}) = __nad.__nad_time"
                ));

                let nad_from = render_full_relation(primary_model, dialect);
                let group_by = if grouping_exprs.is_empty() {
                    String::new()
                } else {
                    let indices: Vec<String> =
                        (1..=grouping_exprs.len()).map(|i| i.to_string()).collect();
                    format!(" GROUP BY {}", indices.join(", "))
                };

                let _ = writeln!(
                    sql,
                    "INNER JOIN (SELECT {} FROM {nad_from}{group_by}) AS __nad ON {}",
                    nad_select.join(", "),
                    nad_join_conds.join(" AND "),
                );
            }
        }
    }

    // Metric filter CTEs: detect {{ Metric(...) }} in WHERE filters, compile as CTEs,
    // and LEFT JOIN them to the main query.
    let all_filters: Vec<String> = metrics
        .iter()
        .flat_map(|m| m.metric_filters.iter().cloned())
        .chain(spec.where_filters.iter().cloned())
        .collect();
    let metric_filter_refs = extract_metric_filter_refs(&all_filters);
    let mut metric_filter_ctes: Vec<(String, String)> = Vec::new();
    let metric_filter_joins = compile_metric_filter_ctes(
        &metric_filter_refs,
        all_metrics,
        model_aliases,
        &primary_model_name,
        primary_alias,
        dialect,
        &mut metric_filter_ctes,
    );
    for join_clause in &metric_filter_joins {
        let _ = writeln!(sql, "{join_clause}");
    }

    // WHERE clause: metric filters + user filters.
    let mut where_parts: Vec<String> = Vec::new();

    for metric in metrics {
        for filter in &metric.metric_filters {
            let resolved =
                resolve_where_filter(filter, model_aliases, dialect, &primary_model_name);
            where_parts.push(resolved);
        }
    }

    for filter in &spec.where_filters {
        let resolved = resolve_where_filter(filter, model_aliases, dialect, &primary_model_name);
        where_parts.push(resolved);
    }

    // Time constraint: metric_time >= start AND metric_time <= end.
    if let Some((start, end)) = &spec.time_constraint {
        // Find the metric_time column expression (use the primary agg_time_dim).
        let time_col = resolve_time_dimension_ref(
            "metric_time",
            "day",
            model_aliases,
            dialect,
            &primary_model_name,
        );
        where_parts.push(format!("{time_col} >= '{start}'"));
        where_parts.push(format!("{time_col} <= '{end}'"));
    }

    if !where_parts.is_empty() {
        let _ = writeln!(sql, "WHERE {}", where_parts.join("\n  AND "));
    }

    // GROUP BY.
    if !spec.group_by.is_empty() {
        let group_indices: Vec<String> = (1..=spec.group_by.len()).map(|i| i.to_string()).collect();
        let _ = writeln!(sql, "GROUP BY {}", group_indices.join(", "));
    }

    // ORDER BY.
    if !spec.order_by.is_empty() {
        let order_parts: Vec<String> = spec
            .order_by
            .iter()
            .map(|o| {
                let col = resolve_order_by_col(&o.name, &spec.group_by);
                if o.descending {
                    format!("{col} DESC")
                } else {
                    format!("{col} ASC")
                }
            })
            .collect();
        let _ = writeln!(sql, "ORDER BY {}", order_parts.join(", "));
    }

    // LIMIT.
    if let Some(limit) = spec.limit {
        let _ = writeln!(sql, "LIMIT {limit}");
    }

    // Prepend WITH clause if metric filter CTEs were generated.
    if !metric_filter_ctes.is_empty() {
        let mut with_sql = String::from("WITH\n");
        for (i, (name, cte_sql)) in metric_filter_ctes.iter().enumerate() {
            if i > 0 {
                with_sql.push_str(",\n");
            }
            let _ = write!(with_sql, "  {name} AS (\n    {cte_sql}\n  )");
        }
        with_sql.push('\n');
        with_sql.push_str(&sql);
        return Ok(with_sql.trim_end().to_string());
    }

    Ok(sql.trim_end().to_string())
}

// ═══════════════════════════════════════════════════════════════════════════
// Complex metric compilation (CTE-based path)
// ═══════════════════════════════════════════════════════════════════════════

#[allow(clippy::too_many_arguments)]
fn compile_complex_metrics(
    store: &mut impl MetricStore,
    spec: &SemanticQuerySpec,
    all_metrics: &HashMap<String, ResolvedMetric>,
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
    resolved_models: &HashMap<String, ResolvedModel>,
    join_edges: &[JoinEdge],
    dialect: Dialect,
    ctes: &mut Vec<(String, String)>,
    _final_select_columns: &mut Vec<String>,
    _final_from: &mut String,
    _final_joins: &mut Vec<String>,
) -> Result<String, MetricFlowError> {
    // Strategy: compile each top-level metric into a CTE, then combine.
    // Simple metrics: one CTE with aggregation.
    // Derived metrics: CTEs for each input, then a final CTE with the expression.
    // Ratio metrics: CTEs for numerator and denominator.
    // Cumulative metrics: CTE with window function.
    // Conversion metrics: CTE with self-join.

    let time_spine = load_time_spine(store);

    for metric_name in &spec.metrics {
        let metric = all_metrics
            .get(metric_name)
            .ok_or_else(|| MetricFlowError::Other(format!("metric not found: {metric_name}")))?;

        match metric.metric_type {
            MetricType::Simple => {
                compile_simple_metric_cte(
                    metric,
                    spec,
                    all_metrics,
                    model_aliases,
                    join_edges,
                    dialect,
                    "",
                    ctes,
                )?;
            }
            MetricType::Derived => {
                compile_derived_metric_cte(
                    metric,
                    spec,
                    all_metrics,
                    model_aliases,
                    join_edges,
                    dialect,
                    "",
                    ctes,
                )?;
            }
            MetricType::Ratio => {
                compile_ratio_metric_cte(
                    metric,
                    spec,
                    all_metrics,
                    model_aliases,
                    join_edges,
                    dialect,
                    "",
                    ctes,
                )?;
            }
            MetricType::Cumulative => {
                compile_cumulative_metric_cte(
                    metric,
                    spec,
                    all_metrics,
                    model_aliases,
                    join_edges,
                    dialect,
                    time_spine.as_ref(),
                    ctes,
                )?;
            }
            MetricType::Conversion => {
                compile_conversion_metric_cte(
                    metric,
                    spec,
                    all_metrics,
                    model_aliases,
                    resolved_models,
                    join_edges,
                    dialect,
                    ctes,
                )?;
            }
        }
    }

    // Build the final SQL from CTEs.
    build_final_sql(spec, ctes, dialect)
}

#[allow(clippy::too_many_arguments)]
fn compile_simple_metric_cte(
    metric: &ResolvedMetric,
    spec: &SemanticQuerySpec,
    all_metrics: &HashMap<String, ResolvedMetric>,
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
    join_edges: &[JoinEdge],
    dialect: Dialect,
    cte_scope: &str,
    ctes: &mut Vec<(String, String)>,
) -> Result<(), MetricFlowError> {
    let cte_name = if cte_scope.is_empty() {
        metric.name.clone()
    } else {
        format!("{cte_scope}__{}", metric.name)
    };
    // Check if this CTE already exists (might be shared by multiple derived metrics).
    if ctes.iter().any(|(name, _)| *name == cte_name) {
        return Ok(());
    }

    let ap = metric.agg_params.as_ref().ok_or_else(|| {
        MetricFlowError::Other(format!(
            "simple metric {} has no aggregation params",
            metric.name
        ))
    })?;

    let (primary_alias, primary_model) =
        model_aliases.get(&ap.semantic_model).ok_or_else(|| {
            MetricFlowError::Other(format!(
                "semantic model not resolved: {}",
                ap.semantic_model
            ))
        })?;

    let mut cte_sql = String::new();
    let mut select_parts: Vec<String> = Vec::new();
    let out_cols = group_by_output_cols(&spec.group_by);

    // Group-by columns.
    for (gb, out_col) in spec.group_by.iter().zip(out_cols.iter()) {
        match gb {
            GroupBySpec::TimeDimension {
                name,
                granularity,
                date_part,
            } => {
                let col_expr = resolve_time_dimension_ref(
                    name,
                    granularity,
                    model_aliases,
                    dialect,
                    &ap.semantic_model,
                );
                if let Some(part) = date_part {
                    let extract_expr = render_extract(part, &col_expr, dialect);
                    select_parts.push(format!("{extract_expr} AS {out_col}"));
                } else {
                    select_parts.push(format!("{col_expr} AS {out_col}"));
                }
            }
            GroupBySpec::Dimension { entity, name } => {
                let dim_ref = match entity {
                    Some(e) => format!("{e}__{name}"),
                    None => name.clone(),
                };
                let col_expr =
                    resolve_dimension_ref(&dim_ref, model_aliases, dialect, &ap.semantic_model);
                select_parts.push(format!("{col_expr} AS {out_col}"));
            }
            GroupBySpec::Entity { name } => {
                let col_expr = resolve_entity_ref(name, model_aliases, &ap.semantic_model);
                select_parts.push(format!("{col_expr} AS {out_col}"));
            }
        }
    }

    // Aggregation.
    let col_expr = qualify_measure_expr(primary_alias, &ap.expr);
    let agg_expr = render_agg(&ap.agg, &col_expr, dialect);
    select_parts.push(format!("{agg_expr} AS {}", metric.name));

    let _ = write!(cte_sql, "SELECT {}", select_parts.join(", "));

    let from_relation = render_full_relation(primary_model, dialect);
    let _ = write!(cte_sql, " FROM {from_relation} AS {primary_alias}");

    // Add joins for dimensions from other models.
    add_dimension_joins(
        spec,
        &metric.metric_filters,
        &ap.semantic_model,
        primary_alias,
        model_aliases,
        join_edges,
        dialect,
        &mut cte_sql,
    );

    // Semi-additive measure handling: INNER JOIN to a subquery that filters on
    // MIN/MAX of the non-additive time dimension.
    if let Some(ref nad) = ap.non_additive_dimension {
        let nad_name = nad.get("name").and_then(|v| v.as_str()).unwrap_or("ds");
        let window_choice = nad
            .get("window_choice")
            .and_then(|v| v.as_str())
            .unwrap_or("max");
        let window_groupings: Vec<&str> = nad
            .get("window_groupings")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
            .unwrap_or_default();

        // Resolve the non-additive dimension's expression in the source table.
        let nad_dim = primary_model.dimensions.iter().find(|d| d.name == nad_name);
        let nad_expr = nad_dim.map(|d| d.expr.as_str()).unwrap_or(nad_name);
        let nad_granularity = nad_dim
            .and_then(|d| d.time_granularity.as_deref())
            .unwrap_or("day");

        let agg_fn = if window_choice == "min" { "MIN" } else { "MAX" };

        // Resolve window_grouping entity expressions.
        let grouping_exprs: Vec<(String, String)> = window_groupings
            .iter()
            .filter_map(|&g| {
                primary_model
                    .entities
                    .iter()
                    .find(|e| e.name == g)
                    .map(|e| (g.to_string(), e.expr.clone()))
            })
            .collect();

        // Build the subquery.
        let mut nad_select = Vec::new();
        let mut nad_join_conds = Vec::new();
        for (name, expr) in &grouping_exprs {
            nad_select.push(format!("{expr} AS {name}"));
            nad_join_conds.push(format!("{primary_alias}.{expr} = __nad.{name}"));
        }
        let trunc = format!("DATE_TRUNC('{nad_granularity}', {nad_expr})");
        nad_select.push(format!("{agg_fn}({trunc}) AS __nad_time"));
        nad_join_conds.push(format!(
            "DATE_TRUNC('{nad_granularity}', {primary_alias}.{nad_expr}) = __nad.__nad_time"
        ));

        let nad_from = render_full_relation(primary_model, dialect);
        let group_by = if grouping_exprs.is_empty() {
            String::new()
        } else {
            let indices: Vec<String> = (1..=grouping_exprs.len()).map(|i| i.to_string()).collect();
            format!(" GROUP BY {}", indices.join(", "))
        };

        let _ = write!(
            cte_sql,
            " INNER JOIN (SELECT {} FROM {nad_from}{group_by}) AS __nad ON {}",
            nad_select.join(", "),
            nad_join_conds.join(" AND "),
        );
    }

    // Metric filter CTEs: if any metric_filter references {{ Metric(...) }}, emit the
    // corresponding __mf_* CTE before this one and JOIN it into the CTE body so that
    // the WHERE reference resolves correctly.
    let mf_refs = extract_metric_filter_refs(&metric.metric_filters);
    if !mf_refs.is_empty() {
        let mf_joins = compile_metric_filter_ctes(
            &mf_refs,
            all_metrics,
            model_aliases,
            &ap.semantic_model,
            primary_alias,
            dialect,
            ctes,
        );
        for join in &mf_joins {
            let _ = write!(cte_sql, " {join}");
        }
    }

    // WHERE: metric-level filters + user-supplied spec.where_filters.
    let mut where_parts: Vec<String> = Vec::new();
    for filter in &metric.metric_filters {
        where_parts.push(resolve_where_filter(
            filter,
            model_aliases,
            dialect,
            &ap.semantic_model,
        ));
    }
    for filter in &spec.where_filters {
        where_parts.push(resolve_where_filter(
            filter,
            model_aliases,
            dialect,
            &ap.semantic_model,
        ));
    }
    if !where_parts.is_empty() {
        let _ = write!(cte_sql, " WHERE {}", where_parts.join(" AND "));
    }

    // GROUP BY.
    if !spec.group_by.is_empty() {
        let group_indices: Vec<String> = (1..=spec.group_by.len()).map(|i| i.to_string()).collect();
        let _ = write!(cte_sql, " GROUP BY {}", group_indices.join(", "));
    }

    ctes.push((cte_name, cte_sql));
    Ok(())
}

#[allow(clippy::cognitive_complexity)]
#[allow(clippy::too_many_arguments)]
fn compile_derived_metric_cte(
    metric: &ResolvedMetric,
    spec: &SemanticQuerySpec,
    all_metrics: &HashMap<String, ResolvedMetric>,
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
    join_edges: &[JoinEdge],
    dialect: Dialect,
    cte_scope: &str,
    ctes: &mut Vec<(String, String)>,
) -> Result<(), MetricFlowError> {
    // Check if this derived metric CTE already exists (shared by multiple inputs).
    let scoped_name = if cte_scope.is_empty() {
        metric.name.clone()
    } else {
        format!("{cte_scope}__{}", metric.name)
    };
    if ctes.iter().any(|(name, _)| *name == scoped_name) {
        return Ok(());
    }

    // Propagate any filters defined on this derived metric down to its inputs.
    // When filters are present we assign a unique CTE-name scope so that two
    // outer derived metrics with different filters each get their own copies of
    // shared base CTEs rather than incorrectly reusing the first one's filtered CTE.
    let (child_spec, child_scope) = if metric.metric_filters.is_empty() {
        (spec.clone(), cte_scope.to_string())
    } else {
        let mut s = spec.clone();
        s.where_filters
            .extend(metric.metric_filters.iter().cloned());
        (s, metric.name.clone())
    };
    let child_scope = child_scope.as_str();

    // When a derived metric has offset inputs AND the spec requests a non-day
    // granularity, base CTEs must be compiled at day granularity so the offset
    // join (spine.time - INTERVAL = base.time) lines up correctly.  A separate
    // "day-level spec" is used for base CTE compilation when any input has an offset.
    let has_offsets = metric
        .input_metrics
        .iter()
        .any(|i| i.offset_window.is_some() || i.offset_to_grain.is_some());
    let day_spec;
    let base_spec = if has_offsets {
        let mut seen_time_dims: HashSet<String> = HashSet::new();
        let day_group_by: Vec<GroupBySpec> = child_spec
            .group_by
            .iter()
            .filter_map(|gb| match gb {
                GroupBySpec::TimeDimension { name, .. } => {
                    if seen_time_dims.insert(name.clone()) {
                        Some(GroupBySpec::TimeDimension {
                            name: name.clone(),
                            granularity: "day".into(),
                            date_part: None,
                        })
                    } else {
                        None
                    }
                }
                other => Some(other.clone()),
            })
            .collect();
        day_spec = SemanticQuerySpec {
            metrics: child_spec.metrics.clone(),
            group_by: day_group_by,
            where_filters: Vec::new(),
            order_by: child_spec.order_by.clone(),
            limit: child_spec.limit,
            time_constraint: child_spec.time_constraint.clone(),
        };
        &day_spec
    } else {
        &child_spec
    };

    // First, compile all input metrics as CTEs (using base_spec for day granularity).
    for input in &metric.input_metrics {
        if let Some(input_metric) = all_metrics.get(&input.name) {
            match input_metric.metric_type {
                MetricType::Simple => {
                    compile_simple_metric_cte(
                        input_metric,
                        base_spec,
                        all_metrics,
                        model_aliases,
                        join_edges,
                        dialect,
                        child_scope,
                        ctes,
                    )?;
                }
                MetricType::Derived => {
                    compile_derived_metric_cte(
                        input_metric,
                        base_spec,
                        all_metrics,
                        model_aliases,
                        join_edges,
                        dialect,
                        child_scope,
                        ctes,
                    )?;
                }
                MetricType::Ratio => {
                    compile_ratio_metric_cte(
                        input_metric,
                        base_spec,
                        all_metrics,
                        model_aliases,
                        join_edges,
                        dialect,
                        child_scope,
                        ctes,
                    )?;
                }
                _ => {
                    // Fallback: try to compile as simple if possible.
                    if input_metric.agg_params.is_some() {
                        compile_simple_metric_cte(
                            input_metric,
                            base_spec,
                            all_metrics,
                            model_aliases,
                            join_edges,
                            dialect,
                            child_scope,
                            ctes,
                        )?;
                    }
                }
            }
        }
    }

    // ── Offset wrapper CTEs ────────────────────────────────────────────
    // For inputs with offset_window or offset_to_grain, create wrapper CTEs
    // that join the base metric CTE to an inline time spine with a shifted condition.
    let has_time_dim = spec
        .group_by
        .iter()
        .any(|gb| matches!(gb, GroupBySpec::TimeDimension { .. }));
    for input in &metric.input_metrics {
        if input.offset_window.is_none() && input.offset_to_grain.is_none() {
            continue;
        }
        let alias = input.alias.as_deref().unwrap_or(&input.name);
        if ctes.iter().any(|(name, _)| name == alias) {
            continue;
        }
        if !has_time_dim {
            return Err(MetricFlowError::Other(format!(
                "offset metric input '{}' requires a time dimension group-by",
                input.name
            )));
        }

        // Find the time dimension column name from spec.
        let time_col = spec
            .group_by
            .iter()
            .find_map(|gb| {
                if let GroupBySpec::TimeDimension { name, .. } = gb {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "metric_time__day".to_string());

        // Create inline time spine CTE over the base CTE's time range.
        let spine_name = format!("{alias}_spine");
        if !ctes.iter().any(|(name, _)| name == &spine_name) {
            let spine_sql =
                inline_time_spine_sql(&time_col, &input.name, &time_col, "day", dialect);
            ctes.push((spine_name.clone(), spine_sql));
        }

        // Create offset wrapper CTE.
        let join_condition = if let Some(ref offset_window) = input.offset_window {
            let interval = render_interval_str(offset_window, dialect);
            format!("spine.{time_col} - {interval} = base.{time_col}")
        } else if let Some(ref grain) = input.offset_to_grain {
            // DATE_TRUNC('grain', spine.time) = base.time
            format!("DATE_TRUNC('{grain}', spine.{time_col}) = base.{time_col}")
        } else {
            unreachable!()
        };

        // Carry non-time group-by columns through the offset wrapper.
        let mut offset_select = format!("spine.{time_col}");
        for gb in &spec.group_by {
            let col = match gb {
                GroupBySpec::TimeDimension { .. } => continue,
                GroupBySpec::Dimension {
                    entity: Some(e),
                    name,
                } => format!("{e}__{name}"),
                GroupBySpec::Dimension { entity: None, name } => name.clone(),
                GroupBySpec::Entity { name } => name.clone(),
            };
            offset_select.push_str(&format!(", base.{col}"));
        }
        if alias == metric.name.as_str() {
            offset_select.push_str(&format!(", base.{} AS {}", input.name, metric.name));
        } else {
            offset_select.push_str(&format!(", base.{}", input.name));
        }

        let offset_sql = format!(
            "SELECT {offset_select} \
             FROM {spine_name} AS spine \
             INNER JOIN {base} AS base ON {join_condition}",
            base = input.name,
        );
        ctes.push((alias.to_string(), offset_sql));
    }

    // Now build the derived metric CTE that references the input CTEs.
    let expr = metric.derived_expr.as_deref().ok_or_else(|| {
        MetricFlowError::Other(format!("derived metric {} has no expression", metric.name))
    })?;

    // The expression references input metrics by name.
    // We need to join the input CTEs and substitute metric names with CTE column references.

    let group_by_cols = group_by_output_cols(&spec.group_by);
    // For JOIN conditions we need the base CTE column names (from base_spec), which
    // use deduplicated day-level names when offsets are present.
    let join_cols = if has_offsets {
        group_by_output_cols(&base_spec.group_by)
    } else {
        group_by_cols.clone()
    };

    if metric.input_metrics.is_empty() {
        return Err(MetricFlowError::Other(format!(
            "derived metric {} has no input metrics",
            metric.name
        )));
    }

    // Determine the effective CTE name for each input:
    // - If the input has an offset, the wrapper CTE is named after the alias.
    // - Otherwise, the CTE is the base metric name, scoped when child_scope is set.
    let effective_cte_name = |input: &MetricInput| -> String {
        let base = if input.offset_window.is_some() || input.offset_to_grain.is_some() {
            input.alias.as_deref().unwrap_or(&input.name).to_string()
        } else {
            input.name.clone()
        };
        if child_scope.is_empty() {
            base
        } else {
            format!("{child_scope}__{base}")
        }
    };

    let first_input = &metric.input_metrics[0];
    let first_cte_alias = format!(
        "{}_cte",
        first_input.alias.as_deref().unwrap_or(&first_input.name)
    );
    let first_effective_cte = effective_cte_name(first_input);

    let mut select_parts: Vec<String> = Vec::new();
    for (gb, out_col) in spec.group_by.iter().zip(group_by_cols.iter()) {
        match gb {
            GroupBySpec::TimeDimension {
                name,
                granularity,
                date_part,
            } if has_offsets && (granularity != "day" || date_part.is_some()) => {
                // Base CTEs are at day granularity; apply truncation/extract in the outer CTE.
                let col = format!("{first_cte_alias}.{name}");
                if let Some(part) = date_part {
                    select_parts.push(format!(
                        "{} AS {out_col}",
                        render_extract(part, &col, dialect)
                    ));
                } else {
                    let trunc = render_date_trunc(granularity, &col, dialect);
                    select_parts.push(format!("{trunc} AS {out_col}"));
                }
            }
            _ => {
                // Placeholder — will be replaced after all joins are known
                // so we can COALESCE across all CTE aliases for 3+ inputs.
                select_parts.push(format!("__GROUP_BY_PLACEHOLDER_{out_col}__"));
            }
        }
    }

    // Build expression with CTE references.
    // The expression uses the alias (or metric name if no alias) to refer to
    // each input metric.  The CTE's output column is the metric's actual name.
    // Two-pass replacement to avoid cascading matches (e.g. replacing "bookings"
    // inside an already-substituted "bookings_1_day_ago_cte.bookings").
    let mut resolved_expr = expr.to_string();
    let mut replacements: Vec<(String, String, String)> = metric
        .input_metrics
        .iter()
        .enumerate()
        .map(|(i, input)| {
            let expr_ref = input.alias.as_deref().unwrap_or(&input.name);
            let cte_alias = format!("{}_cte", expr_ref);
            let placeholder = format!("\x00PH{i}\x00");
            let col_ref = format!("{cte_alias}.{}", input.name);
            // If the input metric has fill_nulls_with, wrap in COALESCE so that
            // NULL values from FULL OUTER JOIN gaps get filled.
            let final_ref =
                if let Some(fill) = all_metrics.get(&input.name).and_then(|m| m.fill_nulls_with) {
                    format!("COALESCE({col_ref}, {fill})")
                } else {
                    col_ref
                };
            (expr_ref.to_string(), placeholder, final_ref)
        })
        .collect();
    // Sort longest-first to prefer longer matches.
    replacements.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
    // Pass 1: replace word matches with unique placeholders.
    for (find, placeholder, _) in &replacements {
        resolved_expr = replace_word(&resolved_expr, find, placeholder);
    }
    // Pass 2: replace placeholders with final values.
    for (_, placeholder, final_val) in &replacements {
        resolved_expr = resolved_expr.replace(placeholder, final_val);
    }
    select_parts.push(format!("{resolved_expr} AS {}", metric.name));

    let mut cte_sql = format!("SELECT {}", select_parts.join(", "));

    // FROM first input CTE.
    let _ = write!(
        cte_sql,
        " FROM {} AS {first_cte_alias}",
        first_effective_cte
    );

    // JOIN remaining input CTEs, accumulating aliases for COALESCE.
    let mut joined_aliases: Vec<String> = vec![first_cte_alias.clone()];
    for input in metric.input_metrics.iter().skip(1) {
        let alias = input.alias.as_deref().unwrap_or(&input.name);
        let cte_alias = format!("{alias}_cte");
        let eff_cte = effective_cte_name(input);

        let join_conditions: Vec<String> = join_cols
            .iter()
            .map(|col| {
                if joined_aliases.len() == 1 {
                    format!(
                        "{}.{col} IS NOT DISTINCT FROM {cte_alias}.{col}",
                        joined_aliases[0]
                    )
                } else {
                    let coalesce = joined_aliases
                        .iter()
                        .map(|a| format!("{a}.{col}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("COALESCE({coalesce}) IS NOT DISTINCT FROM {cte_alias}.{col}")
                }
            })
            .collect();

        if join_conditions.is_empty() {
            let _ = write!(cte_sql, " CROSS JOIN {} AS {cte_alias}", eff_cte);
        } else {
            let _ = write!(
                cte_sql,
                " FULL OUTER JOIN {} AS {cte_alias} ON {}",
                eff_cte,
                join_conditions.join(" AND ")
            );
        }
        joined_aliases.push(cte_alias);
    }

    // Replace group-by placeholders with COALESCE across all joined aliases.
    // Use join_cols (base CTE names) for column references, group_by_cols for output alias.
    for (jcol, out_col) in join_cols.iter().zip(group_by_cols.iter()) {
        let placeholder = format!("__GROUP_BY_PLACEHOLDER_{out_col}__");
        let replacement = if joined_aliases.len() == 1 {
            format!("{}.{jcol}", joined_aliases[0])
        } else {
            let parts: Vec<String> = joined_aliases
                .iter()
                .map(|a| format!("{a}.{jcol}"))
                .collect();
            format!("COALESCE({}) AS {out_col}", parts.join(", "))
        };
        cte_sql = cte_sql.replace(&placeholder, &replacement);
    }

    // Apply deferred WHERE filters for offset metrics.
    // When offsets are present, WHERE filters are excluded from base CTEs and applied here
    // on the derived CTE output. Only dimension references that match group_by columns
    // can be resolved (they're carried through the CTE chain).
    if has_offsets && !spec.where_filters.is_empty() {
        let mut where_parts: Vec<String> = Vec::new();
        for filter in &spec.where_filters {
            // Resolve Dimension('entity__name') → first_cte_alias.entity__name
            let mut resolved = filter.clone();
            let mut cursor = 0usize;
            while let Some(start) = resolved[cursor..].find("Dimension('") {
                let abs_pos = cursor + start;
                if abs_pos > 0 && resolved.as_bytes()[abs_pos - 1].is_ascii_alphanumeric() {
                    cursor = abs_pos + 11;
                    continue;
                }
                let abs_start = abs_pos + 11;
                if let Some(end) = resolved[abs_start..].find("')") {
                    let dim_ref = resolved[abs_start..abs_start + end].to_string();
                    let col = group_by_cols
                        .iter()
                        .find(|c| {
                            **c == dim_ref
                                || c.ends_with(&format!(
                                    "__{}",
                                    dim_ref.split("__").last().unwrap_or(&dim_ref)
                                ))
                        })
                        .cloned()
                        .unwrap_or(dim_ref);
                    let replacement = format!("{first_cte_alias}.{col}");
                    resolved.replace_range(abs_pos..abs_start + end + 2, &replacement);
                    cursor = abs_pos + replacement.len();
                } else {
                    break;
                }
            }
            // Strip remaining {{ }} Jinja wrappers.
            resolved = resolved.replace("{{ ", "").replace(" }}", "");
            where_parts.push(resolved);
        }
        if !where_parts.is_empty() {
            let _ = write!(cte_sql, " WHERE {}", where_parts.join(" AND "));
        }
    }

    // Skip if an offset wrapper CTE already produced a CTE with the same name
    // (happens when the derived metric is a simple passthrough of an offset alias).
    if !ctes.iter().any(|(name, _)| *name == scoped_name) {
        // When offsets forced day-level base CTEs but the spec wants coarser granularity,
        // wrap the derived CTE in an outer aggregation that truncates and re-aggregates.
        let needs_reagg = has_offsets
            && spec.group_by.iter().any(|gb| {
                matches!(gb, GroupBySpec::TimeDimension { granularity, date_part, .. }
                     if granularity != "day" || date_part.is_some())
            });
        if needs_reagg {
            // Emit the day-level derived CTE with a temporary name.
            let inner_name = format!("{scoped_name}_day");
            ctes.push((inner_name.clone(), cte_sql));
            // Build outer aggregation CTE.
            let mut outer_select: Vec<String> = Vec::new();
            let mut outer_group: Vec<String> = Vec::new();
            let mut idx = 1;
            for (gb, out_col) in spec.group_by.iter().zip(group_by_cols.iter()) {
                match gb {
                    GroupBySpec::TimeDimension {
                        granularity,
                        date_part,
                        ..
                    } if granularity != "day" || date_part.is_some() => {
                        if date_part.is_some() {
                            outer_select.push(out_col.clone());
                        } else {
                            let trunc = render_date_trunc(granularity, out_col, dialect);
                            outer_select.push(format!("{trunc} AS {out_col}"));
                        }
                        outer_group.push(idx.to_string());
                        idx += 1;
                    }
                    _ => {
                        outer_select.push(out_col.clone());
                        outer_group.push(idx.to_string());
                        idx += 1;
                    }
                }
            }
            outer_select.push(format!("SUM({}) AS {}", metric.name, metric.name));
            let outer_sql = format!(
                "SELECT {} FROM {inner_name} GROUP BY {}",
                outer_select.join(", "),
                outer_group.join(", "),
            );
            ctes.push((scoped_name, outer_sql));
        } else {
            ctes.push((scoped_name, cte_sql));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn compile_ratio_metric_cte(
    metric: &ResolvedMetric,
    spec: &SemanticQuerySpec,
    all_metrics: &HashMap<String, ResolvedMetric>,
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
    join_edges: &[JoinEdge],
    dialect: Dialect,
    cte_scope: &str,
    ctes: &mut Vec<(String, String)>,
) -> Result<(), MetricFlowError> {
    let numerator = metric.numerator.as_ref().ok_or_else(|| {
        MetricFlowError::Other(format!("ratio metric {} has no numerator", metric.name))
    })?;
    let denominator = metric.denominator.as_ref().ok_or_else(|| {
        MetricFlowError::Other(format!("ratio metric {} has no denominator", metric.name))
    })?;

    // Compile numerator and denominator as CTEs.
    if let Some(num_metric) = all_metrics.get(&numerator.name) {
        match num_metric.metric_type {
            MetricType::Simple => {
                compile_simple_metric_cte(
                    num_metric,
                    spec,
                    all_metrics,
                    model_aliases,
                    join_edges,
                    dialect,
                    cte_scope,
                    ctes,
                )?;
            }
            MetricType::Derived => {
                compile_derived_metric_cte(
                    num_metric,
                    spec,
                    all_metrics,
                    model_aliases,
                    join_edges,
                    dialect,
                    cte_scope,
                    ctes,
                )?;
            }
            _ => {}
        }
    }
    if let Some(den_metric) = all_metrics.get(&denominator.name) {
        match den_metric.metric_type {
            MetricType::Simple => {
                compile_simple_metric_cte(
                    den_metric,
                    spec,
                    all_metrics,
                    model_aliases,
                    join_edges,
                    dialect,
                    cte_scope,
                    ctes,
                )?;
            }
            MetricType::Derived => {
                compile_derived_metric_cte(
                    den_metric,
                    spec,
                    all_metrics,
                    model_aliases,
                    join_edges,
                    dialect,
                    cte_scope,
                    ctes,
                )?;
            }
            _ => {}
        }
    }

    // Build the ratio CTE.
    let group_by_cols = group_by_output_cols(&spec.group_by);

    let num_alias = "num";
    let den_alias = "den";

    let mut select_parts: Vec<String> = Vec::new();
    for col in &group_by_cols {
        select_parts.push(format!(
            "COALESCE({num_alias}.{col}, {den_alias}.{col}) AS {col}"
        ));
    }

    let cast_num = render_cast_double(&format!("{num_alias}.{}", numerator.name), dialect);
    let cast_den = render_cast_double(&format!("{den_alias}.{}", denominator.name), dialect);
    select_parts.push(format!(
        "{cast_num} / NULLIF({cast_den}, 0) AS {}",
        metric.name
    ));

    let mut cte_sql = format!("SELECT {}", select_parts.join(", "));
    let _ = write!(cte_sql, " FROM {} AS {num_alias}", numerator.name);

    let join_conditions: Vec<String> = group_by_cols
        .iter()
        .map(|col| format!("{num_alias}.{col} IS NOT DISTINCT FROM {den_alias}.{col}"))
        .collect();

    if join_conditions.is_empty() {
        let _ = write!(cte_sql, " CROSS JOIN {} AS {den_alias}", denominator.name);
    } else {
        let _ = write!(
            cte_sql,
            " FULL OUTER JOIN {} AS {den_alias} ON {}",
            denominator.name,
            join_conditions.join(" AND ")
        );
    }

    ctes.push((metric.name.clone(), cte_sql));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn compile_cumulative_metric_cte(
    metric: &ResolvedMetric,
    spec: &SemanticQuerySpec,
    all_metrics: &HashMap<String, ResolvedMetric>,
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
    join_edges: &[JoinEdge],
    dialect: Dialect,
    _time_spine: Option<&TimeSpine>,
    ctes: &mut Vec<(String, String)>,
) -> Result<(), MetricFlowError> {
    // Cumulative metrics use an inline time spine (generate_series) joined to the
    // source data.  Three patterns:
    //   - All-time:       src.time <= spine.time
    //   - Rolling window: src.time <= spine.time AND src.time > spine.time - interval
    //   - Grain-to-date:  src.time <= spine.time AND src.time >= date_trunc(grain, spine.time)

    if ctes.iter().any(|(name, _)| name == &metric.name) {
        return Ok(());
    }

    let cp = metric.cumulative_params.as_ref().ok_or_else(|| {
        MetricFlowError::Other(format!(
            "cumulative metric {} has no cumulative params",
            metric.name
        ))
    })?;

    // Resolve aggregation params — either directly on this metric or via an input metric.
    let ap = metric
        .agg_params
        .as_ref()
        .or_else(|| {
            metric
                .input_metrics
                .first()
                .and_then(|inp| all_metrics.get(&inp.name))
                .and_then(|m| m.agg_params.as_ref())
        })
        .ok_or_else(|| {
            MetricFlowError::Other(format!(
                "cumulative metric {} has no aggregation params",
                metric.name
            ))
        })?;

    let (primary_alias, primary_model) =
        model_aliases.get(&ap.semantic_model).ok_or_else(|| {
            MetricFlowError::Other(format!(
                "semantic model not resolved: {}",
                ap.semantic_model
            ))
        })?;

    let from_relation = render_full_relation(primary_model, dialect);

    // Resolve the raw time column on the source table (e.g., "created_at" or "ds").
    let time_col_raw = ap
        .agg_time_dimension
        .as_deref()
        .map(|dim_name| {
            primary_model
                .dimensions
                .iter()
                .find(|d| d.name == dim_name)
                .map(|d| d.expr.as_str())
                .unwrap_or(dim_name)
        })
        .unwrap_or("ds");

    let has_time_dim = spec
        .group_by
        .iter()
        .any(|gb| matches!(gb, GroupBySpec::TimeDimension { .. }));

    let granularity = spec
        .group_by
        .iter()
        .find_map(|gb| {
            if let GroupBySpec::TimeDimension { granularity, .. } = gb {
                Some(granularity.as_str())
            } else {
                None
            }
        })
        .unwrap_or("day");

    // Measure expression in the source CTE.
    let measure_col = qualify_measure_expr("f", &ap.expr);
    let agg_src = render_agg(&ap.agg, "src.measure_value", dialect);

    // Collect non-time dimension output names for GROUP BY pass-through.
    let dim_col_names: Vec<String> = spec
        .group_by
        .iter()
        .filter_map(|gb| match gb {
            GroupBySpec::Dimension {
                entity: Some(e),
                name,
            } => Some(format!("{e}__{name}")),
            GroupBySpec::Dimension { entity: None, name } => Some(name.clone()),
            GroupBySpec::Entity { name } => Some(name.clone()),
            _ => None,
        })
        .collect();

    if !has_time_dim {
        // No time dimension — falls back to a simple aggregate (no spine needed).
        let col_expr = qualify_measure_expr(primary_alias, &ap.expr);
        let agg_expr = render_agg(&ap.agg, &col_expr, dialect);
        let mut select_parts: Vec<String> = Vec::new();
        // Add dimension group-by columns.
        for gb in &spec.group_by {
            match gb {
                GroupBySpec::Dimension { entity, name } => {
                    let dim_ref = match entity {
                        Some(e) => format!("{e}__{name}"),
                        None => name.clone(),
                    };
                    let scoped = scoped_aliases_for(ap, primary_alias, primary_model);
                    let col = resolve_dimension_ref(&dim_ref, &scoped, dialect, "");
                    let output = match entity {
                        Some(e) => format!("{e}__{name}"),
                        None => name.clone(),
                    };
                    select_parts.push(format!("{col} AS {output}"));
                }
                GroupBySpec::Entity { name } => {
                    let scoped = scoped_aliases_for(ap, primary_alias, primary_model);
                    let col = resolve_entity_ref(name, &scoped, "");
                    select_parts.push(format!("{col} AS {name}"));
                }
                _ => {}
            }
        }
        select_parts.push(format!("{agg_expr} AS {}", metric.name));
        let mut cte_sql = format!(
            "SELECT {} FROM {from_relation} AS {primary_alias}",
            select_parts.join(", ")
        );
        add_dimension_joins(
            spec,
            &metric.metric_filters,
            &ap.semantic_model,
            primary_alias,
            model_aliases,
            join_edges,
            dialect,
            &mut cte_sql,
        );
        {
            let mut where_parts: Vec<String> = Vec::new();
            for filter in &metric.metric_filters {
                where_parts.push(resolve_where_filter(
                    filter,
                    model_aliases,
                    dialect,
                    &ap.semantic_model,
                ));
            }
            for filter in &spec.where_filters {
                where_parts.push(resolve_where_filter(
                    filter,
                    model_aliases,
                    dialect,
                    &ap.semantic_model,
                ));
            }
            if !where_parts.is_empty() {
                let _ = write!(cte_sql, " WHERE {}", where_parts.join(" AND "));
            }
        }
        if !spec.group_by.is_empty() {
            let indices: Vec<String> = (1..=spec.group_by.len()).map(|i| i.to_string()).collect();
            let _ = write!(cte_sql, " GROUP BY {}", indices.join(", "));
        }
        ctes.push((metric.name.clone(), cte_sql));
        return Ok(());
    }

    // ── Step 1: Source data CTE ─────────────────────────────────────────
    let src_cte = format!("{}_src", metric.name);
    let mut src_select = vec![format!("f.{time_col_raw} AS src_time")];
    src_select.push(format!("{measure_col} AS measure_value"));
    // Include dimension columns from joins for pass-through.
    {
        let scoped = scoped_aliases_for(ap, primary_alias, primary_model);
        for gb in &spec.group_by {
            match gb {
                GroupBySpec::Dimension { entity, name } => {
                    let dim_ref = match entity {
                        Some(e) => format!("{e}__{name}"),
                        None => name.clone(),
                    };
                    let col = resolve_dimension_ref(&dim_ref, &scoped, dialect, "");
                    let output = match entity {
                        Some(e) => format!("{e}__{name}"),
                        None => name.clone(),
                    };
                    src_select.push(format!("{col} AS {output}"));
                }
                GroupBySpec::Entity { name } => {
                    let col = resolve_entity_ref(name, &scoped, "");
                    src_select.push(format!("{col} AS {name}"));
                }
                _ => {}
            }
        }
    }
    let mut src_sql = format!("SELECT {} FROM {from_relation} AS f", src_select.join(", "));
    // Use "f" as the primary alias for joins in the source CTE.
    add_dimension_joins(
        spec,
        &metric.metric_filters,
        &ap.semantic_model,
        "f",
        model_aliases,
        join_edges,
        dialect,
        &mut src_sql,
    );
    // Apply metric-level + user-supplied WHERE filters.
    {
        let mut where_parts: Vec<String> = Vec::new();
        for filter in &metric.metric_filters {
            where_parts.push(resolve_where_filter(
                filter,
                model_aliases,
                dialect,
                &ap.semantic_model,
            ));
        }
        for filter in &spec.where_filters {
            where_parts.push(resolve_where_filter(
                filter,
                model_aliases,
                dialect,
                &ap.semantic_model,
            ));
        }
        if !where_parts.is_empty() {
            let _ = write!(src_sql, " WHERE {}", where_parts.join(" AND "));
        }
    }
    ctes.push((src_cte.clone(), src_sql));

    // ── Step 2: Inline time spine ─────────────────────────────────────────
    let spine_cte = format!("{}_spine", metric.name);
    let spine_sql = inline_time_spine_sql("spine_time", &src_cte, "src_time", granularity, dialect);
    ctes.push((spine_cte.clone(), spine_sql));

    // ── Step 3: Join spine → source with time range predicate ────────────
    let join_cond = if let (Some(count), Some(gran)) = (&cp.window_count, &cp.window_granularity) {
        let interval = render_interval(*count, gran, dialect);
        format!(
            "src.src_time <= spine.spine_time \
             AND src.src_time > spine.spine_time - {interval}"
        )
    } else if let Some(ref grain) = cp.grain_to_date {
        format!(
            "src.src_time <= spine.spine_time \
             AND src.src_time >= DATE_TRUNC('{grain}', spine.spine_time)"
        )
    } else {
        "src.src_time <= spine.spine_time".to_string()
    };

    let trunc = format!("DATE_TRUNC('{granularity}', spine.spine_time)");
    let mut cum_select = vec![format!("{trunc} AS metric_time")];
    for dcn in &dim_col_names {
        cum_select.push(format!("src.{dcn}"));
    }
    cum_select.push(format!("{agg_src} AS {}", metric.name));

    let cum_group: Vec<String> = (1..=1 + dim_col_names.len())
        .map(|i| i.to_string())
        .collect();

    let cum_sql = format!(
        "SELECT {} FROM {spine_cte} AS spine \
         INNER JOIN {src_cte} AS src ON {join_cond} \
         GROUP BY {}",
        cum_select.join(", "),
        cum_group.join(", ")
    );

    ctes.push((metric.name.clone(), cum_sql));
    Ok(())
}

/// Build a scoped model-alias map containing only one model (for CTE compilation).
fn scoped_aliases_for<'a>(
    ap: &AggParams,
    alias: &str,
    model: &'a ResolvedModel,
) -> HashMap<String, (String, &'a ResolvedModel)> {
    let mut m = HashMap::new();
    m.insert(ap.semantic_model.clone(), (alias.to_string(), model));
    m
}

/// Compile a conversion metric into CTEs.
///
/// Pattern:
///   {metric}_base  — raw rows from the base measure's model (entity key + time)
///   {metric}_conv  — raw rows from the conversion measure's model (entity key + time)
///   {metric}       — matched conversions joined by entity within the time window
///
/// For `conversion_rate`: matched / total_base
/// For `conversions`: COUNT of matched conversion events
#[allow(clippy::too_many_arguments)]
fn compile_conversion_metric_cte(
    metric: &ResolvedMetric,
    _spec: &SemanticQuerySpec,
    all_metrics: &HashMap<String, ResolvedMetric>,
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
    _resolved_models: &HashMap<String, ResolvedModel>,
    _join_edges: &[JoinEdge],
    dialect: Dialect,
    ctes: &mut Vec<(String, String)>,
) -> Result<(), MetricFlowError> {
    let cp = metric.conversion_params.as_ref().ok_or_else(|| {
        MetricFlowError::Other(format!(
            "conversion metric {} has no conversion params",
            metric.name
        ))
    })?;

    // Resolve the base and conversion measures to get their models and columns.
    let base_metric = all_metrics.get(&cp.base_metric).ok_or_else(|| {
        MetricFlowError::Other(format!("base metric not resolved: {}", cp.base_metric))
    })?;
    let conv_metric = all_metrics.get(&cp.conversion_metric).ok_or_else(|| {
        MetricFlowError::Other(format!(
            "conversion metric not resolved: {}",
            cp.conversion_metric
        ))
    })?;

    let base_ap = base_metric
        .agg_params
        .as_ref()
        .ok_or_else(|| MetricFlowError::Other("base metric has no agg_params".into()))?;
    let conv_ap = conv_metric
        .agg_params
        .as_ref()
        .ok_or_else(|| MetricFlowError::Other("conversion metric has no agg_params".into()))?;

    let base_model = model_aliases.get(&base_ap.semantic_model).ok_or_else(|| {
        MetricFlowError::Other(format!("model not resolved: {}", base_ap.semantic_model))
    })?;
    let conv_model = model_aliases.get(&conv_ap.semantic_model).ok_or_else(|| {
        MetricFlowError::Other(format!("model not resolved: {}", conv_ap.semantic_model))
    })?;

    // Find the entity expression in each model.
    let base_entity_expr = base_model
        .1
        .entities
        .iter()
        .find(|e| e.name == cp.entity)
        .map(|e| e.expr.as_str())
        .unwrap_or(&cp.entity);
    let conv_entity_expr = conv_model
        .1
        .entities
        .iter()
        .find(|e| e.name == cp.entity)
        .map(|e| e.expr.as_str())
        .unwrap_or(&cp.entity);

    // Find the time dimension expression in each model.
    let base_time_dim = base_model
        .1
        .dimensions
        .iter()
        .find(|d| d.dimension_type == "time")
        .map(|d| d.expr.as_str())
        .unwrap_or("ds");
    let conv_time_dim = conv_model
        .1
        .dimensions
        .iter()
        .find(|d| d.dimension_type == "time")
        .map(|d| d.expr.as_str())
        .unwrap_or("ds");

    let base_relation = render_full_relation(base_model.1, dialect);
    let conv_relation = render_full_relation(conv_model.1, dialect);

    let base_cte_name = format!("{}_base", metric.name);
    let conv_cte_name = format!("{}_conv", metric.name);

    // Build extra SELECT columns for constant properties.
    let base_const_cols: String = cp
        .constant_properties
        .iter()
        .map(|(base_prop, _)| format!(", {base_prop}"))
        .collect();
    let conv_const_cols: String = cp
        .constant_properties
        .iter()
        .map(|(_, conv_prop)| format!(", {conv_prop}"))
        .collect();

    // CTE 1: raw base events with entity key, time, and constant property columns.
    let base_cte = format!(
        "SELECT {base_entity_expr} AS entity_key, {base_time_dim} AS metric_time{base_const_cols} FROM {base_relation}"
    );
    ctes.push((base_cte_name.clone(), base_cte));

    // CTE 2: raw conversion events with entity key, time, and constant property columns.
    let conv_cte = format!(
        "SELECT {conv_entity_expr} AS entity_key, {conv_time_dim} AS metric_time{conv_const_cols} FROM {conv_relation}"
    );
    ctes.push((conv_cte_name.clone(), conv_cte));

    // Build the window condition.
    // base event must occur before or at the conversion event, within the window.
    let window_condition = match (&cp.window_count, &cp.window_granularity) {
        (Some(count), Some(gran)) => {
            let interval = render_interval(*count, gran, dialect);
            format!("b.metric_time <= c.metric_time AND b.metric_time > c.metric_time - {interval}")
        }
        _ => "b.metric_time <= c.metric_time".to_string(),
    };

    // Constant property conditions.
    let const_conditions: Vec<String> = cp
        .constant_properties
        .iter()
        .map(|(base_prop, conv_prop)| format!("b.{base_prop} = c.{conv_prop}"))
        .collect();

    let mut join_conds = vec!["b.entity_key = c.entity_key".to_string(), window_condition];
    join_conds.extend(const_conditions);
    let join_condition = join_conds.join(" AND ");

    // CTE 3: the final conversion metric.
    let metric_expr = match cp.calculation.as_str() {
        "conversions" => {
            // Count of unique conversion events that had a matching base event.
            format!(
                "SELECT (SELECT COUNT(*) FROM (SELECT DISTINCT c.entity_key, c.metric_time \
                 FROM {conv_cte_name} c INNER JOIN {base_cte_name} b ON {join_condition}) matched) \
                 AS {}",
                metric.name
            )
        }
        _ => {
            // conversion_rate = matched_conversions / total_base_events
            let matched = format!(
                "(SELECT COUNT(*) FROM (SELECT DISTINCT c.entity_key, c.metric_time \
                 FROM {conv_cte_name} c INNER JOIN {base_cte_name} b ON {join_condition}) matched)"
            );
            let total_base = format!("(SELECT COUNT(*) FROM {base_cte_name})");
            let cast_matched = render_cast_double(&matched, dialect);
            let cast_base = render_cast_double(&total_base, dialect);
            format!(
                "SELECT {cast_matched} / NULLIF({cast_base}, 0) AS {}",
                metric.name
            )
        }
    };

    ctes.push((metric.name.clone(), metric_expr));
    Ok(())
}

/// Add JOIN clauses for dimensions from other models.
///
/// Scans both the `group_by` specs and any filter strings (metric-level or
/// user-supplied) for entity-prefixed `Dimension('entity__name')` references,
/// then emits one LEFT JOIN per referenced model that is not already joined.
#[allow(clippy::too_many_arguments)]
fn add_dimension_joins(
    spec: &SemanticQuerySpec,
    metric_filters: &[String],
    primary_model_name: &str,
    primary_alias: &str,
    model_aliases: &HashMap<String, (String, &ResolvedModel)>,
    join_edges: &[JoinEdge],
    dialect: Dialect,
    sql: &mut String,
) {
    let mut joined: HashSet<String> = HashSet::new();
    joined.insert(primary_model_name.to_string());

    // Collect (entity_name, dimension_name) pairs from group-by and filters.
    // We need both pieces to find the correct model: the entity tells us HOW
    // to join, and the dimension tells us WHICH model to join (the one that
    // actually owns the column).
    let mut needed: Vec<(String, Option<String>)> = Vec::new();

    for gb in &spec.group_by {
        if let GroupBySpec::Dimension {
            entity: Some(entity_name),
            name,
        } = gb
        {
            if !needed.iter().any(|(e, _)| e == entity_name) {
                needed.push((entity_name.clone(), Some(name.clone())));
            }
        }
    }

    // Extract entity prefixes from `Dimension('entity__dim')` in all filters.
    for filter in metric_filters.iter().chain(spec.where_filters.iter()) {
        let mut cursor = 0usize;
        while let Some(pos) = filter[cursor..].find("Dimension(") {
            let abs = cursor + pos;
            // Skip if this is actually "TimeDimension(" — check preceding char.
            let preceded_by_alpha = abs > 0 && filter.as_bytes()[abs - 1].is_ascii_alphabetic();
            if preceded_by_alpha {
                cursor = abs + 10;
                continue;
            }
            let inner_start = abs + 10;
            if let Some(paren_end) = filter[inner_start..].find(')') {
                let dim_ref = filter[inner_start..inner_start + paren_end]
                    .trim()
                    .trim_matches('\'')
                    .trim_matches('"');
                if let Some((entity_name, dim_name)) = dim_ref.split_once("__") {
                    let entity_name = entity_name.to_string();
                    if !needed.iter().any(|(e, _)| *e == entity_name) {
                        needed.push((entity_name, Some(dim_name.to_string())));
                    }
                }
                cursor = inner_start + paren_end + 1;
            } else {
                break;
            }
        }
    }

    // Emit one LEFT JOIN per needed entity, skipping already-joined models.
    // When a dimension name is known, prefer the model that actually owns that
    // dimension column (not just any model that shares the entity name).
    for (entity_name, dim_name) in &needed {
        // Find the best model: one that has both the entity AND the dimension.
        // Fall back to any model with the entity if no dimension match.
        let mut best: Option<&String> = None;
        let mut fallback: Option<&String> = None;
        for (model_name, (_alias, model)) in model_aliases {
            if joined.contains(model_name) {
                continue;
            }
            let has_entity = model.entities.iter().any(|e| e.name == *entity_name);
            if !has_entity {
                continue;
            }
            if let Some(dn) = dim_name {
                let has_dim = model.dimensions.iter().any(|d| d.name == *dn);
                if has_dim {
                    best = Some(model_name);
                    break;
                }
            }
            if fallback.is_none() {
                fallback = Some(model_name);
            }
        }
        let target = best.or(fallback);
        if let Some(model_name) = target {
            if let Some(path) = find_join_path(join_edges, primary_model_name, model_name) {
                // Emit joins for the entire path (may be multi-hop).
                for edge in &path {
                    if joined.contains(&edge.to_model) {
                        continue;
                    }
                    let left_alias = if edge.from_model == primary_model_name {
                        primary_alias
                    } else {
                        model_aliases
                            .get(&edge.from_model)
                            .map(|(a, _)| a.as_str())
                            .unwrap_or(primary_alias)
                    };
                    let (alias, model) = &model_aliases[&edge.to_model];
                    let join_relation = render_full_relation(model, dialect);
                    let _ = write!(
                        sql,
                        " LEFT JOIN {join_relation} AS {alias} ON {left_alias}.{} = {alias}.{}",
                        edge.from_expr, edge.to_expr,
                    );
                    joined.insert(edge.to_model.clone());
                }
            }
        }
    }
}

/// Build the final SQL from CTEs, combining all metric results.
#[allow(unused_variables)]
fn build_final_sql(
    spec: &SemanticQuerySpec,
    ctes: &[(String, String)],
    dialect: Dialect,
) -> Result<String, MetricFlowError> {
    if ctes.is_empty() {
        return Err(MetricFlowError::Other(
            "no metrics compiled — nothing to query".into(),
        ));
    }

    let mut sql = String::new();

    // WITH clause.
    let _ = writeln!(sql, "WITH");
    for (i, (name, cte_sql)) in ctes.iter().enumerate() {
        if i > 0 {
            let _ = writeln!(sql, ",");
        }
        let _ = write!(sql, "  {name} AS (\n    {cte_sql}\n  )");
    }
    let _ = writeln!(sql);

    // Final SELECT — reference the last CTE for each top-level metric.
    let group_by_cols = group_by_output_cols(&spec.group_by);

    // If there's only one top-level metric, just SELECT * from its CTE.
    if spec.metrics.len() == 1 {
        let _ = write!(sql, "SELECT *\nFROM {}", spec.metrics[0]);
    } else {
        // Multiple metrics: FULL OUTER JOIN their CTEs on group-by columns.
        let first = &spec.metrics[0];
        let first_alias = format!("{first}_final");

        let mut select_parts: Vec<String> = Vec::new();
        for col in &group_by_cols {
            let coalesce_parts: Vec<String> = spec
                .metrics
                .iter()
                .map(|m| format!("{m}_final.{col}"))
                .collect();
            if coalesce_parts.len() > 1 {
                select_parts.push(format!("COALESCE({}) AS {col}", coalesce_parts.join(", ")));
            } else {
                select_parts.push(format!("{first_alias}.{col}"));
            }
        }

        for metric_name in &spec.metrics {
            let alias = format!("{metric_name}_final");
            select_parts.push(format!("{alias}.{metric_name}"));
        }

        let _ = writeln!(sql, "SELECT {}", select_parts.join(", "));
        let _ = writeln!(sql, "FROM {first} AS {first_alias}");

        let mut joined_aliases: Vec<String> = vec![first_alias.clone()];
        for metric_name in spec.metrics.iter().skip(1) {
            let alias = format!("{metric_name}_final");
            let join_conditions: Vec<String> = group_by_cols
                .iter()
                .map(|col| {
                    if joined_aliases.len() == 1 {
                        format!(
                            "{}.{col} IS NOT DISTINCT FROM {alias}.{col}",
                            joined_aliases[0]
                        )
                    } else {
                        let coalesce = joined_aliases
                            .iter()
                            .map(|a| format!("{a}.{col}"))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("COALESCE({coalesce}) IS NOT DISTINCT FROM {alias}.{col}")
                    }
                })
                .collect();

            if join_conditions.is_empty() {
                let _ = writeln!(sql, "CROSS JOIN {metric_name} AS {alias}");
            } else {
                let _ = writeln!(
                    sql,
                    "FULL OUTER JOIN {metric_name} AS {alias} ON {}",
                    join_conditions.join(" AND ")
                );
            }
            joined_aliases.push(alias);
        }
    }

    // WHERE filters are pushed into each sub-CTE (compile_simple_metric_cte),
    // so no unresolved Jinja templates reach here.

    // Time constraint: applied at the outer level so it filters the final result.
    if let Some((start, end)) = &spec.time_constraint {
        // Find the metric_time column name.
        let time_col = group_by_cols
            .iter()
            .find(|c| c.contains("metric_time") || c.contains("__ds"))
            .map(|s| s.as_str())
            .unwrap_or("metric_time");
        let _ = write!(
            sql,
            "\nWHERE {time_col} >= '{start}' AND {time_col} <= '{end}'"
        );
    }

    // ORDER BY.
    if !spec.order_by.is_empty() {
        let order_parts: Vec<String> = spec
            .order_by
            .iter()
            .map(|o| {
                let col = resolve_order_by_col(&o.name, &spec.group_by);
                if o.descending {
                    format!("{col} DESC")
                } else {
                    format!("{col} ASC")
                }
            })
            .collect();
        let _ = write!(sql, "\nORDER BY {}", order_parts.join(", "));
    }

    // LIMIT.
    if let Some(limit) = spec.limit {
        let _ = write!(sql, "\nLIMIT {limit}");
    }

    Ok(sql)
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_agg() {
        assert_eq!(
            render_agg("sum", "o.amount", Dialect::DuckDB),
            "SUM(o.amount)"
        );
        assert_eq!(
            render_agg("count_distinct", "c.customer_id", Dialect::Snowflake),
            "COUNT(DISTINCT c.customer_id)"
        );
        assert_eq!(
            render_agg("average", "o.amount", Dialect::DuckDB),
            "AVG(o.amount)"
        );
    }

    #[test]
    fn test_render_date_trunc() {
        assert_eq!(
            render_date_trunc("day", "o.order_date", Dialect::DuckDB),
            "DATE_TRUNC('day', o.order_date)::DATE"
        );
        assert_eq!(
            render_date_trunc("week", "o.order_date", Dialect::Snowflake),
            "DATE_TRUNC('week', o.order_date)::DATE"
        );
    }

    #[test]
    fn test_parse_metric_filters() {
        let json = r#"{"where_filters": [{"where_sql_template": "{{ Dimension('order_id__status') }} = 'completed'"}]}"#;
        let filters = parse_metric_filters(json);
        assert_eq!(filters.len(), 1);
        assert!(filters[0].contains("Dimension('order_id__status')"));
    }

    #[test]
    fn test_parse_metric_filters_empty() {
        assert!(parse_metric_filters("").is_empty());
        assert!(parse_metric_filters("null").is_empty());
    }

    #[test]
    fn test_resolve_time_dimension_ref_with_entity_prefix() {
        // TimeDimension('user_account_activity__date_day', 'day') should resolve
        // to `alias.date_day` via entity prefix stripping, not fall back to the
        // raw string `user_account_activity__date_day` as a column name.
        let model = ResolvedModel {
            name: "fct_activities".into(),
            relation_name: "\"db\".\"main\".\"fct_activities\"".into(),
            alias: "fct_activities".into(),
            schema_name: "main".into(),
            database: "db".into(),
            primary_entity: None,
            entities: vec![EntityDef {
                name: "user_account_activity".into(),
                entity_type: "primary".into(),
                expr: "activity_id".into(),
            }],
            dimensions: vec![DimensionDef {
                name: "date_day".into(),
                dimension_type: "time".into(),
                expr: "date_day".into(),
                time_granularity: Some("day".into()),
            }],
        };

        let mut aliases: HashMap<String, (String, &ResolvedModel)> = HashMap::new();
        aliases.insert("fct_activities".into(), ("f".into(), &model));

        // Entity-prefixed form: must resolve to f.date_day, not the raw name.
        let resolved = resolve_time_dimension_ref(
            "user_account_activity__date_day",
            "day",
            &aliases,
            Dialect::Snowflake,
            "fct_activities",
        );
        assert_eq!(resolved, "DATE_TRUNC('day', f.date_day)::DATE");

        // Plain form (no entity prefix) should still work.
        let resolved_plain = resolve_time_dimension_ref(
            "date_day",
            "day",
            &aliases,
            Dialect::Snowflake,
            "fct_activities",
        );
        assert_eq!(resolved_plain, "DATE_TRUNC('day', f.date_day)::DATE");
    }

    #[test]
    fn test_resolve_where_filter() {
        // Build a simple model alias map.
        let model = ResolvedModel {
            name: "orders".into(),
            relation_name: "\"db\".\"main\".\"orders\"".into(),
            alias: "orders".into(),
            schema_name: "main".into(),
            database: "db".into(),
            primary_entity: None,
            entities: vec![EntityDef {
                name: "order_id".into(),
                entity_type: "primary".into(),
                expr: "order_id".into(),
            }],
            dimensions: vec![DimensionDef {
                name: "status".into(),
                dimension_type: "categorical".into(),
                expr: "status".into(),
                time_granularity: None,
            }],
        };

        let mut aliases: HashMap<String, (String, &ResolvedModel)> = HashMap::new();
        aliases.insert("orders".into(), ("o".into(), &model));

        let resolved = resolve_where_filter(
            "{{ Dimension('order_id__status') }} = 'completed'",
            &aliases,
            Dialect::DuckDB,
            "orders",
        );
        assert_eq!(resolved, "o.status = 'completed'");
    }

    #[test]
    fn test_find_join_path_bidirectional() {
        let edges = vec![
            JoinEdge {
                from_model: "order_items".into(),
                to_model: "orders".into(),
                from_expr: "order_id".into(),
                to_expr: "order_id".into(),
                entity_name: "order".into(),
            },
            JoinEdge {
                from_model: "orders".into(),
                to_model: "order_items".into(),
                from_expr: "order_id".into(),
                to_expr: "order_id".into(),
                entity_name: "order".into(),
            },
        ];
        // FK→PK direction
        assert!(find_join_path(&edges, "order_items", "orders").is_some());
        // PK→FK direction (was broken before bidirectional edges)
        assert!(find_join_path(&edges, "orders", "order_items").is_some());
    }

    // ── Helpers for compile() integration tests ──────────────────────────────

    /// Minimal MetricStore backed by in-memory vecs — no database required.
    struct MockStore {
        metrics: Vec<RawMetricRow>,
        models: Vec<RawModelRow>,
        entities: Vec<(String, Vec<RawEntityRow>)>, // (unique_id, rows)
        dimensions: Vec<(String, Vec<RawDimensionRow>)>, // (unique_id, rows)
        join_graph: Vec<RawJoinGraphRow>,
    }

    impl MockStore {
        fn new() -> Self {
            Self {
                metrics: vec![],
                models: vec![],
                entities: vec![],
                dimensions: vec![],
                join_graph: vec![],
            }
        }
    }

    impl MetricStore for MockStore {
        fn lookup_metric(&mut self, name: &str) -> Result<Option<RawMetricRow>, MetricFlowError> {
            Ok(self.metrics.iter().find(|m| m.name == name).cloned())
        }
        fn list_metric_names(&mut self) -> Result<Vec<String>, MetricFlowError> {
            Ok(self.metrics.iter().map(|m| m.name.clone()).collect())
        }
        fn lookup_semantic_model(
            &mut self,
            name: &str,
        ) -> Result<Option<RawModelRow>, MetricFlowError> {
            Ok(self.models.iter().find(|m| m.name == name).cloned())
        }
        fn lookup_model_entities(
            &mut self,
            unique_id: &str,
        ) -> Result<Vec<RawEntityRow>, MetricFlowError> {
            Ok(self
                .entities
                .iter()
                .find(|(id, _)| id == unique_id)
                .map(|(_, rows)| rows.clone())
                .unwrap_or_default())
        }
        fn lookup_model_dimensions(
            &mut self,
            unique_id: &str,
        ) -> Result<Vec<RawDimensionRow>, MetricFlowError> {
            Ok(self
                .dimensions
                .iter()
                .find(|(id, _)| id == unique_id)
                .map(|(_, rows)| rows.clone())
                .unwrap_or_default())
        }
        fn lookup_all_join_graph_entities(
            &mut self,
        ) -> Result<Vec<RawJoinGraphRow>, MetricFlowError> {
            Ok(self.join_graph.clone())
        }
        fn find_model_for_entity(
            &mut self,
            entity_name: &str,
            primary_or_unique_only: bool,
        ) -> Result<Option<String>, MetricFlowError> {
            Ok(self
                .join_graph
                .iter()
                .find(|r| {
                    r.entity_name == entity_name
                        && (!primary_or_unique_only
                            || r.entity_type == "primary"
                            || r.entity_type == "unique")
                })
                .map(|r| r.model_name.clone()))
        }
        fn check_entity_in_model(
            &mut self,
            model_name: &str,
            entity_name: &str,
        ) -> Result<bool, MetricFlowError> {
            Ok(self
                .join_graph
                .iter()
                .any(|r| r.model_name == model_name && r.entity_name == entity_name))
        }
        fn lookup_time_spine(&mut self) -> Result<Option<RawTimeSpineRow>, MetricFlowError> {
            Ok(None)
        }
    }

    // ── Bug fix: measure expr fallback to type_params.expr ───────────────────

    /// Regression: some manifest formats store the measure column at
    /// `type_params.expr` rather than `type_params.metric_aggregation_params.expr`.
    /// The compiler must fall back to `type_params.expr` so it generates
    /// `COUNT(DISTINCT alias.col)` instead of `COUNT(DISTINCT alias.)`.
    #[test]
    fn test_measure_expr_fallback_to_type_params_expr() {
        let mut store = MockStore::new();

        // type_params with expr at the top level (not inside metric_aggregation_params).
        let type_params = r#"{
            "expr": "customer_user_id",
            "metric_aggregation_params": {
                "semantic_model": "fct_users",
                "agg": "count_distinct",
                "agg_time_dimension": "created_at"
            }
        }"#;

        store.metrics.push(RawMetricRow {
            name: "active_users".into(),
            metric_type: "simple".into(),
            description: String::new(),
            type_params: type_params.into(),
            metric_filter: String::new(),
        });
        store.models.push(RawModelRow {
            name: "fct_users".into(),
            node_relation: r#""db"."main"."fct_users""#.into(),
            primary_entity: "user".into(),
            unique_id: "semantic_model.fct_users".into(),
        });
        store.entities.push((
            "semantic_model.fct_users".into(),
            vec![RawEntityRow {
                name: "user".into(),
                entity_type: "primary".into(),
                expr: "user_id".into(),
            }],
        ));
        store.dimensions.push((
            "semantic_model.fct_users".into(),
            vec![RawDimensionRow {
                name: "created_at".into(),
                dimension_type: "time".into(),
                expr: "created_at".into(),
                time_granularity: "day".into(),
            }],
        ));
        store.join_graph.push(RawJoinGraphRow {
            model_name: "fct_users".into(),
            entity_name: "user".into(),
            entity_type: "primary".into(),
            expr: "user_id".into(),
        });

        let spec = SemanticQuerySpec {
            metrics: vec!["active_users".into()],
            group_by: vec![],
            where_filters: vec![],
            order_by: vec![],
            limit: None,
            time_constraint: None,
        };

        let sql = compile(&mut store, &spec, Dialect::DuckDB).unwrap();
        // Must contain the column name, not a bare `f.` with nothing after it.
        assert!(
            sql.contains("COUNT(DISTINCT f.customer_user_id)"),
            "expected COUNT(DISTINCT f.customer_user_id) but got:\n{sql}"
        );
        assert!(
            !sql.contains("COUNT(DISTINCT f.)"),
            "bare `f.` with no column name should not appear:\n{sql}"
        );
    }

    // ── Bug fix: joins generated for dimensions in metric filters ────────────

    /// Regression: `add_dimension_joins` previously only generated JOINs for
    /// models referenced in `group_by`. A model referenced only in a metric
    /// filter (`Dimension('entity__col')`) was assigned an alias in the SQL but
    /// never joined, producing invalid SQL like `WHERE f3.col = X` with no
    /// corresponding `LEFT JOIN`.
    ///
    /// This must use a **derived** metric so the compiler takes the CTE path
    /// and calls `add_dimension_joins` — the function that was actually fixed.
    /// Simple metrics take `compile_simple_metrics`, which already had its own
    /// filter-scanning join logic and would not expose the bug.
    #[test]
    fn test_join_generated_for_filter_dimension_in_derived_metric() {
        let mut store = MockStore::new();

        // Base simple metric whose metric_filter references `user__is_internal`
        // — a dimension on a separate model not in group_by.
        let base_filter = r#"{"where_filters": [
            {"where_sql_template": "{{ Dimension('user__is_internal') }} = False"}
        ]}"#;
        let base_type_params = r#"{
            "expr": "order_id",
            "metric_aggregation_params": {
                "semantic_model": "fct_orders",
                "agg": "count_distinct",
                "agg_time_dimension": "order_date"
            }
        }"#;
        store.metrics.push(RawMetricRow {
            name: "order_count".into(),
            metric_type: "simple".into(),
            description: String::new(),
            type_params: base_type_params.into(),
            metric_filter: base_filter.into(),
        });

        // Derived metric wrapping the simple one — forces the CTE code path.
        let derived_type_params = r#"{
            "expr": "order_count",
            "metrics": [{"name": "order_count"}]
        }"#;
        store.metrics.push(RawMetricRow {
            name: "order_count_derived".into(),
            metric_type: "derived".into(),
            description: String::new(),
            type_params: derived_type_params.into(),
            metric_filter: String::new(),
        });

        // Primary model: fct_orders
        store.models.push(RawModelRow {
            name: "fct_orders".into(),
            node_relation: r#""db"."main"."fct_orders""#.into(),
            primary_entity: "order".into(),
            unique_id: "semantic_model.fct_orders".into(),
        });
        store.entities.push((
            "semantic_model.fct_orders".into(),
            vec![
                RawEntityRow {
                    name: "order".into(),
                    entity_type: "primary".into(),
                    expr: "order_id".into(),
                },
                RawEntityRow {
                    name: "user".into(),
                    entity_type: "foreign".into(),
                    expr: "user_id".into(),
                },
            ],
        ));
        store.dimensions.push((
            "semantic_model.fct_orders".into(),
            vec![RawDimensionRow {
                name: "order_date".into(),
                dimension_type: "time".into(),
                expr: "order_date".into(),
                time_granularity: "day".into(),
            }],
        ));

        // Secondary model: dim_users (holds is_internal)
        store.models.push(RawModelRow {
            name: "dim_users".into(),
            node_relation: r#""db"."main"."dim_users""#.into(),
            primary_entity: "user".into(),
            unique_id: "semantic_model.dim_users".into(),
        });
        store.entities.push((
            "semantic_model.dim_users".into(),
            vec![RawEntityRow {
                name: "user".into(),
                entity_type: "primary".into(),
                expr: "user_id".into(),
            }],
        ));
        store.dimensions.push((
            "semantic_model.dim_users".into(),
            vec![RawDimensionRow {
                name: "is_internal".into(),
                dimension_type: "categorical".into(),
                expr: "is_internal".into(),
                time_granularity: String::new(),
            }],
        ));

        // Join graph: fct_orders → dim_users via user_id
        store.join_graph.extend([
            RawJoinGraphRow {
                model_name: "fct_orders".into(),
                entity_name: "order".into(),
                entity_type: "primary".into(),
                expr: "order_id".into(),
            },
            RawJoinGraphRow {
                model_name: "fct_orders".into(),
                entity_name: "user".into(),
                entity_type: "foreign".into(),
                expr: "user_id".into(),
            },
            RawJoinGraphRow {
                model_name: "dim_users".into(),
                entity_name: "user".into(),
                entity_type: "primary".into(),
                expr: "user_id".into(),
            },
        ]);

        let spec = SemanticQuerySpec {
            metrics: vec!["order_count_derived".into()],
            // No group_by referencing the user model — the join must come from
            // the metric filter dimension alone.
            group_by: vec![],
            where_filters: vec![],
            order_by: vec![],
            limit: None,
            time_constraint: None,
        };

        let sql = compile(&mut store, &spec, Dialect::DuckDB).unwrap();
        // dim_users must be joined so the alias used in the WHERE clause is valid.
        assert!(
            sql.contains("LEFT JOIN"),
            "expected a LEFT JOIN for the filter dimension:\n{sql}"
        );
        assert!(
            sql.contains("dim_users"),
            "dim_users must appear in a JOIN clause:\n{sql}"
        );
        assert!(
            sql.contains("is_internal"),
            "is_internal column must appear in WHERE:\n{sql}"
        );
    }

    // ── Bug fix: ambiguous column in multi-metric simple join ─────────────────

    /// Regression: when two simple metrics come from different models, the
    /// secondary metric's measure `expr` is passed to `qualify_measure_expr`.
    /// That function qualifies simple identifiers (e.g. `user_id` → `f1.user_id`)
    /// but bails out on any expr that contains spaces or parentheses, returning
    /// it verbatim.  For the `account_signups` metric the expr is
    /// `case when account_id is not null then 1 else 0 end`.
    ///
    /// When the two models are joined, both the primary model and the secondary
    /// model's join key are named `account_id`, making the bare reference
    /// ambiguous.  Snowflake rejects the query with:
    ///   "SQL compilation error: ambiguous column name 'ACCOUNT_ID'"
    ///
    /// The fix must qualify every bare column reference inside complex exprs
    /// with the secondary model's alias (e.g. `a.account_id`).
    #[test]
    fn test_multi_metric_simple_join_qualifies_complex_measure_expr() {
        let mut store = MockStore::new();

        // Metric 1: count_distinct of user_id, on fct_user_activities.
        let user_count_params = r#"{
            "expr": "user_id",
            "metric_aggregation_params": {
                "semantic_model": "fct_user_activities",
                "agg": "count_distinct",
                "agg_time_dimension": "date_day"
            }
        }"#;
        store.metrics.push(RawMetricRow {
            name: "user_count".into(),
            metric_type: "simple".into(),
            description: String::new(),
            type_params: user_count_params.into(),
            metric_filter: String::new(),
        });

        // Metric 2: sum of a CASE expression, on account_signups.
        // The CASE expr references `account_id` which is also the join key on
        // the primary model side — making it ambiguous without a table alias.
        let signup_count_params = r#"{
            "expr": "case when account_id is not null then 1 else 0 end",
            "metric_aggregation_params": {
                "semantic_model": "account_signups",
                "agg": "sum",
                "agg_time_dimension": "created_at"
            }
        }"#;
        store.metrics.push(RawMetricRow {
            name: "signup_count".into(),
            metric_type: "simple".into(),
            description: String::new(),
            type_params: signup_count_params.into(),
            metric_filter: String::new(),
        });

        // Primary model: fct_user_activities
        store.models.push(RawModelRow {
            name: "fct_user_activities".into(),
            node_relation: r#""db"."main"."fct_user_activities""#.into(),
            primary_entity: "activity".into(),
            unique_id: "semantic_model.fct_user_activities".into(),
        });
        store.entities.push((
            "semantic_model.fct_user_activities".into(),
            vec![
                RawEntityRow {
                    name: "activity".into(),
                    entity_type: "primary".into(),
                    expr: "activity_id".into(),
                },
                RawEntityRow {
                    // FK linking to account_signups.
                    name: "account".into(),
                    entity_type: "foreign".into(),
                    expr: "account_id".into(),
                },
            ],
        ));
        store.dimensions.push((
            "semantic_model.fct_user_activities".into(),
            vec![RawDimensionRow {
                name: "date_day".into(),
                dimension_type: "time".into(),
                expr: "date_day".into(),
                time_granularity: "day".into(),
            }],
        ));

        // Secondary model: account_signups
        store.models.push(RawModelRow {
            name: "account_signups".into(),
            node_relation: r#""db"."main"."account_signups""#.into(),
            primary_entity: "account".into(),
            unique_id: "semantic_model.account_signups".into(),
        });
        store.entities.push((
            "semantic_model.account_signups".into(),
            vec![RawEntityRow {
                name: "account".into(),
                entity_type: "primary".into(),
                expr: "account_id".into(),
            }],
        ));
        store.dimensions.push((
            "semantic_model.account_signups".into(),
            vec![RawDimensionRow {
                name: "created_at".into(),
                dimension_type: "time".into(),
                expr: "created_at".into(),
                time_granularity: "day".into(),
            }],
        ));

        // Join graph: fct_user_activities (FK) → account_signups (PK) via account_id.
        store.join_graph.extend([
            RawJoinGraphRow {
                model_name: "fct_user_activities".into(),
                entity_name: "account".into(),
                entity_type: "foreign".into(),
                expr: "account_id".into(),
            },
            RawJoinGraphRow {
                model_name: "account_signups".into(),
                entity_name: "account".into(),
                entity_type: "primary".into(),
                expr: "account_id".into(),
            },
        ]);

        // Because the two metrics come from different semantic models, the router
        // sends this to compile_complex_metrics (CTE path).  Each metric gets its
        // own CTE with its own FROM clause, so column references inside either
        // metric's expression are unambiguous by construction — only one table is
        // in scope per CTE.  No JOIN between the two base tables occurs.

        let spec = SemanticQuerySpec {
            metrics: vec!["user_count".into(), "signup_count".into()],
            group_by: vec![],
            where_filters: vec![],
            order_by: vec![],
            limit: None,
            time_constraint: None,
        };

        let sql = compile(&mut store, &spec, Dialect::DuckDB).unwrap();

        // CTE path: each metric in its own subquery scope.
        assert!(sql.contains("WITH"), "expected CTE-based query:\n{sql}");
        // signup_count CTE evaluates the CASE with only account_signups in scope —
        // account_id is unambiguous without any table qualifier.
        assert!(
            sql.contains("case when account_id is not null"),
            "signup_count CTE must contain the CASE expression:\n{sql}"
        );
        // The two base tables must not be joined together.
        assert!(
            !sql.contains("LEFT JOIN"),
            "metrics from different models must not be combined via LEFT JOIN:\n{sql}"
        );
    }

    // ── Bug fix: derived-table wrapping of complex secondary measure exprs ────
    //
    // compile_simple_metrics wraps the JOIN for any secondary model whose metric
    // expression is "complex" (contains spaces or parens — e.g. a CASE expression)
    // inside a derived table that pre-computes the expr as a named column.
    // This makes all column references unambiguous by construction.
    //
    // The compile() router now sends multi-model simple metrics to the CTE path,
    // so this test calls compile_simple_metrics directly to exercise the mechanism.
    #[test]
    fn test_compile_simple_metrics_wraps_complex_secondary_expr_in_derived_table() {
        let primary = ResolvedModel {
            name: "fct_user_activities".into(),
            relation_name: "fct_user_activities".into(),
            alias: "fct_user_activities".into(),
            schema_name: "main".into(),
            database: "db".into(),
            primary_entity: Some("activity".into()),
            entities: vec![
                EntityDef {
                    name: "activity".into(),
                    entity_type: "primary".into(),
                    expr: "activity_id".into(),
                },
                EntityDef {
                    name: "account".into(),
                    entity_type: "foreign".into(),
                    expr: "account_id".into(),
                },
            ],
            dimensions: vec![DimensionDef {
                name: "date_day".into(),
                dimension_type: "time".into(),
                expr: "date_day".into(),
                time_granularity: Some("day".into()),
            }],
        };

        let secondary = ResolvedModel {
            name: "account_signups".into(),
            relation_name: "account_signups".into(),
            alias: "account_signups".into(),
            schema_name: "main".into(),
            database: "db".into(),
            primary_entity: Some("account".into()),
            entities: vec![EntityDef {
                name: "account".into(),
                entity_type: "primary".into(),
                expr: "account_id".into(),
            }],
            dimensions: vec![DimensionDef {
                name: "created_at".into(),
                dimension_type: "time".into(),
                expr: "created_at".into(),
                time_granularity: Some("day".into()),
            }],
        };

        let user_count = ResolvedMetric {
            name: "user_count".into(),
            metric_type: MetricType::Simple,
            description: String::new(),
            agg_params: Some(AggParams {
                semantic_model: "fct_user_activities".into(),
                agg: "count_distinct".into(),
                expr: "user_id".into(),
                agg_time_dimension: Some("date_day".into()),
                non_additive_dimension: None,
            }),
            metric_filters: vec![],
            derived_expr: None,
            input_metrics: vec![],
            numerator: None,
            denominator: None,
            cumulative_params: None,
            conversion_params: None,
            join_to_timespine: false,
            fill_nulls_with: None,
        };

        let signup_count = ResolvedMetric {
            name: "signup_count".into(),
            metric_type: MetricType::Simple,
            description: String::new(),
            agg_params: Some(AggParams {
                semantic_model: "account_signups".into(),
                agg: "sum".into(),
                // Complex expr: contains spaces — qualify_measure_expr returns verbatim,
                // but compile_simple_metrics must wrap it in a derived table JOIN.
                expr: "case when account_id is not null then 1 else 0 end".into(),
                agg_time_dimension: Some("created_at".into()),
                non_additive_dimension: None,
            }),
            metric_filters: vec![],
            derived_expr: None,
            input_metrics: vec![],
            numerator: None,
            denominator: None,
            cumulative_params: None,
            conversion_params: None,
            join_to_timespine: false,
            fill_nulls_with: None,
        };

        // account_signups (i=0) → alias "a"; fct_user_activities (i=1) → alias "f1"
        let model_aliases: HashMap<String, (String, &ResolvedModel)> = [
            ("account_signups".to_string(), ("a".to_string(), &secondary)),
            (
                "fct_user_activities".to_string(),
                ("f1".to_string(), &primary),
            ),
        ]
        .into();

        let join_edges = vec![JoinEdge {
            from_model: "fct_user_activities".into(),
            to_model: "account_signups".into(),
            from_expr: "account_id".into(),
            to_expr: "account_id".into(),
            entity_name: "account".into(),
        }];

        let all_metrics: HashMap<String, ResolvedMetric> = [
            ("user_count".to_string(), user_count.clone()),
            ("signup_count".to_string(), signup_count.clone()),
        ]
        .into();

        let spec = SemanticQuerySpec {
            metrics: vec!["user_count".into(), "signup_count".into()],
            group_by: vec![],
            where_filters: vec![],
            order_by: vec![],
            limit: None,
            time_constraint: None,
        };

        let sql = compile_simple_metrics(
            &spec,
            &[&user_count, &signup_count],
            &all_metrics,
            &model_aliases,
            &join_edges,
            Dialect::DuckDB,
        )
        .unwrap();

        // Complex expr must be pre-computed inside the derived table.
        assert!(
            sql.contains("SUM(a.__mf_signup_count_expr)"),
            "outer SELECT must reference the pre-computed derived column:\n{sql}"
        );
        assert!(
            sql.contains("__mf_signup_count_expr"),
            "derived table must define the named column:\n{sql}"
        );
        assert!(
            !sql.contains("SUM(case when"),
            "CASE expression must not appear directly in the outer aggregation:\n{sql}"
        );
    }

    // ── Bug: {{ Metric(...) }} filter inside a simple metric used by a derived metric ──
    //
    // When a simple metric's `filter` references `{{ Metric('x', group_by=['e']) }}`,
    // `resolve_where_filter` converts it to `__mf_x.e__x` — a reference to a CTE
    // that must be defined and joined before the WHERE clause is evaluated.
    //
    // `compile_simple_metric_cte` (used by the CTE/derived path) previously never
    // called `compile_metric_filter_ctes`, so the `__mf_*` CTE was never emitted and
    // the identifier was left dangling, producing a Snowflake/DuckDB error like:
    //   "unresolved identifier '__mf_opportunity_delta_average_arr'"
    //
    // This test mirrors the `arr_churn` case:
    //   arr_churn (derived)
    //     └─ gross_churn (simple, filter: {{ Metric('delta_arr', ['opportunity']) }} < 0)
    //   delta_arr (simple, the filter metric)
    //
    // The compiled SQL must:
    //   1. Define a `__mf_delta_arr` CTE before the `gross_churn` CTE.
    //   2. Add a `LEFT JOIN __mf_delta_arr` inside the `gross_churn` CTE body.
    //   3. NOT leave `__mf_delta_arr` as a bare unjoined identifier in WHERE.
    #[test]
    fn test_metric_filter_cte_emitted_inside_derived_metric_cte() {
        let mut store = MockStore::new();

        // The filter metric: delta_arr — simple SUM on fct_opportunities.
        store.metrics.push(RawMetricRow {
            name: "delta_arr".into(),
            metric_type: "simple".into(),
            description: String::new(),
            type_params: r#"{
                "expr": "delta_arr",
                "metric_aggregation_params": {
                    "semantic_model": "fct_opportunities",
                    "agg": "sum",
                    "agg_time_dimension": "close_date"
                }
            }"#
            .into(),
            metric_filter: String::new(),
        });

        // The filtered simple metric: gross_churn — SUM of delta_arr, but only
        // where the per-opportunity delta_arr metric is negative.
        store.metrics.push(RawMetricRow {
            name: "gross_churn".into(),
            metric_type: "simple".into(),
            description: String::new(),
            type_params: r#"{
                "expr": "delta_arr",
                "metric_aggregation_params": {
                    "semantic_model": "fct_opportunities",
                    "agg": "sum",
                    "agg_time_dimension": "close_date"
                }
            }"#
            .into(),
            // This is the filter that triggers the bug: {{ Metric(...) }} reference.
            metric_filter: r#"{"where_filters": [
                {"where_sql_template": "{{ Metric('delta_arr', group_by=['opportunity']) }} < 0"}
            ]}"#
            .into(),
        });

        // The derived metric: arr_churn — forces compilation through compile_complex_metrics.
        store.metrics.push(RawMetricRow {
            name: "arr_churn".into(),
            metric_type: "derived".into(),
            description: String::new(),
            type_params: r#"{
                "expr": "gross_churn",
                "metrics": [{"name": "gross_churn"}]
            }"#
            .into(),
            metric_filter: String::new(),
        });

        // Single semantic model: fct_opportunities with entity `opportunity`.
        store.models.push(RawModelRow {
            name: "fct_opportunities".into(),
            node_relation: r#""db"."main"."fct_opportunities""#.into(),
            primary_entity: "opportunity".into(),
            unique_id: "semantic_model.fct_opportunities".into(),
        });
        store.entities.push((
            "semantic_model.fct_opportunities".into(),
            vec![RawEntityRow {
                name: "opportunity".into(),
                entity_type: "primary".into(),
                expr: "opportunity_id".into(),
            }],
        ));
        store.dimensions.push((
            "semantic_model.fct_opportunities".into(),
            vec![RawDimensionRow {
                name: "close_date".into(),
                dimension_type: "time".into(),
                expr: "close_date".into(),
                time_granularity: "day".into(),
            }],
        ));
        store.join_graph.push(RawJoinGraphRow {
            model_name: "fct_opportunities".into(),
            entity_name: "opportunity".into(),
            entity_type: "primary".into(),
            expr: "opportunity_id".into(),
        });

        let spec = SemanticQuerySpec {
            metrics: vec!["arr_churn".into()],
            group_by: vec![],
            where_filters: vec![],
            order_by: vec![],
            limit: Some(5),
            time_constraint: None,
        };

        let sql = compile(&mut store, &spec, Dialect::DuckDB).unwrap();

        // The __mf_delta_arr CTE must be defined somewhere in the WITH block.
        assert!(
            sql.contains("__mf_delta_arr"),
            "__mf_delta_arr CTE must be emitted:\n{sql}"
        );
        // It must be defined as a CTE entry, not just referenced.
        assert!(
            sql.contains("__mf_delta_arr AS ("),
            "__mf_delta_arr must appear as a CTE definition:\n{sql}"
        );
        // The gross_churn CTE body must JOIN the filter CTE so the WHERE reference resolves.
        assert!(
            sql.contains("LEFT JOIN __mf_delta_arr"),
            "gross_churn CTE must LEFT JOIN __mf_delta_arr:\n{sql}"
        );
    }

    // ── Bug: granularity-qualified order-by is rejected even when the group-by matches ──
    //
    // `--group-by metric_time__month` stores the time dimension as
    // `GroupBySpec::TimeDimension { name: "metric_time", granularity: "month" }`.
    // The validation builds `group_by_names` from the `name` field only, so it
    // contains `"metric_time"` but not `"metric_time__month"`.  When the user then
    // passes `--order-by metric_time__month` (the same token they typed for group-by),
    // validation fails with "unknown order-by: metric_time__month".
    //
    // Fix (Option A): expand `group_by_names` to also include `{name}__{granularity}`
    // for every TimeDimension group-by, so both the base name and the qualified form
    // are accepted.

    /// Builds a minimal one-metric MockStore over a single model with a time dimension.
    /// Reused by both order-by tests below to avoid duplication.
    fn order_by_store() -> MockStore {
        let mut store = MockStore::new();
        store.metrics.push(RawMetricRow {
            name: "signups".into(),
            metric_type: "simple".into(),
            description: String::new(),
            type_params: r#"{
                "expr": "1",
                "metric_aggregation_params": {
                    "semantic_model": "fct_signups",
                    "agg": "sum",
                    "agg_time_dimension": "created_at"
                }
            }"#
            .into(),
            metric_filter: String::new(),
        });
        store.models.push(RawModelRow {
            name: "fct_signups".into(),
            node_relation: r#""db"."main"."fct_signups""#.into(),
            primary_entity: "signup".into(),
            unique_id: "semantic_model.fct_signups".into(),
        });
        store.entities.push((
            "semantic_model.fct_signups".into(),
            vec![RawEntityRow {
                name: "signup".into(),
                entity_type: "primary".into(),
                expr: "signup_id".into(),
            }],
        ));
        store.dimensions.push((
            "semantic_model.fct_signups".into(),
            vec![RawDimensionRow {
                name: "created_at".into(),
                dimension_type: "time".into(),
                expr: "created_at".into(),
                time_granularity: "day".into(),
            }],
        ));
        store.join_graph.push(RawJoinGraphRow {
            model_name: "fct_signups".into(),
            entity_name: "signup".into(),
            entity_type: "primary".into(),
            expr: "signup_id".into(),
        });
        store
    }

    /// Positive: `--order-by metric_time__month` must be accepted when the query
    /// already groups by `metric_time__month`.  The compiled SQL must contain an
    /// ORDER BY clause referencing the canonical column name `metric_time`.
    #[test]
    fn test_order_by_granularity_qualified_name_accepted() {
        let mut store = order_by_store();
        let spec = SemanticQuerySpec {
            metrics: vec!["signups".into()],
            group_by: vec![GroupBySpec::TimeDimension {
                name: "metric_time".into(),
                granularity: "month".into(),
                date_part: None,
            }],
            where_filters: vec![],
            order_by: vec![OrderBySpec {
                name: "metric_time__month".into(),
                descending: false,
            }],
            limit: Some(5),
            time_constraint: None,
        };

        let sql = compile(&mut store, &spec, Dialect::DuckDB).expect(
            "order-by metric_time__month should be valid when group-by is metric_time__month",
        );

        // Must use the canonical column name, not the granularity-qualified form.
        assert!(
            sql.contains("ORDER BY metric_time ASC"),
            "SQL must contain ORDER BY metric_time ASC (not metric_time__month):\n{sql}"
        );
        assert!(
            !sql.contains("metric_time__month"),
            "granularity qualifier must not appear in emitted SQL:\n{sql}"
        );
    }

    /// Negative: `--order-by metric_time__day` must still be rejected when the query
    /// groups by `metric_time__month` — the granularities don't match, so the day
    /// column does not exist in the output.
    #[test]
    fn test_order_by_mismatched_granularity_rejected() {
        let mut store = order_by_store();
        let spec = SemanticQuerySpec {
            metrics: vec!["signups".into()],
            group_by: vec![GroupBySpec::TimeDimension {
                name: "metric_time".into(),
                granularity: "month".into(),
                date_part: None,
            }],
            where_filters: vec![],
            order_by: vec![OrderBySpec {
                name: "metric_time__day".into(),
                descending: false,
            }],
            limit: Some(5),
            time_constraint: None,
        };

        let result = compile(&mut store, &spec, Dialect::DuckDB);
        assert!(
            result.is_err(),
            "order-by metric_time__day should be rejected when group-by is metric_time__month"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown order-by"),
            "error should mention 'unknown order-by':\n{err}"
        );
    }

    #[test]
    fn test_no_duplicate_cte_for_shared_derived_input() {
        let mut store = MockStore::new();

        store.metrics.push(RawMetricRow {
            name: "base".into(),
            metric_type: "simple".into(),
            description: String::new(),
            type_params: r#"{
                "metric_aggregation_params": {
                    "semantic_model": "fct_events",
                    "agg": "count",
                    "expr": "1",
                    "agg_time_dimension": "ds"
                }
            }"#
            .into(),
            metric_filter: String::new(),
        });

        store.metrics.push(RawMetricRow {
            name: "wrapped".into(),
            metric_type: "derived".into(),
            description: String::new(),
            type_params: r#"{
                "expr": "base_alias",
                "metrics": [{"name": "base", "alias": "base_alias", "offset_window": null, "offset_to_grain": null, "filter": null}]
            }"#
            .into(),
            metric_filter: String::new(),
        });

        store.metrics.push(RawMetricRow {
            name: "wrapped_last_period".into(),
            metric_type: "derived".into(),
            description: String::new(),
            type_params: r#"{
                "expr": "wrapped_period_alias",
                "metrics": [{"name": "wrapped", "alias": "wrapped_period_alias", "offset_window": "1 day", "offset_to_grain": null, "filter": null}]
            }"#
            .into(),
            metric_filter: String::new(),
        });

        store.metrics.push(RawMetricRow {
            name: "wrapped_growth".into(),
            metric_type: "derived".into(),
            description: String::new(),
            type_params: r#"{
                "expr": "wrapped - wrapped_last_period",
                "metrics": [
                    {"name": "wrapped",             "alias": null, "offset_window": null, "offset_to_grain": null, "filter": null},
                    {"name": "wrapped_last_period", "alias": null, "offset_window": null, "offset_to_grain": null, "filter": null}
                ]
            }"#
            .into(),
            metric_filter: String::new(),
        });

        store.models.push(RawModelRow {
            name: "fct_events".into(),
            node_relation: r#"{"relation_name": "\"db\".\"main\".\"fct_events\"", "alias": "fct_events", "schema_name": "main", "database": "db"}"#.into(),
            primary_entity: "event".into(),
            unique_id: "semantic_model.fct_events".into(),
        });
        store.dimensions.push((
            "semantic_model.fct_events".into(),
            vec![RawDimensionRow {
                name: "ds".into(),
                dimension_type: "time".into(),
                expr: "ds".into(),
                time_granularity: "day".into(),
            }],
        ));

        let spec = SemanticQuerySpec {
            metrics: vec!["wrapped_growth".into()],
            group_by: vec![GroupBySpec::TimeDimension {
                name: "metric_time".into(),
                granularity: "day".into(),
                date_part: None,
            }],
            where_filters: vec![],
            order_by: vec![],
            limit: None,
            time_constraint: None,
        };

        let sql = compile(&mut store, &spec, Dialect::DuckDB)
            .expect("wrapped_growth should compile without error");

        let cte_defs = sql.matches("wrapped AS (").count();
        assert_eq!(
            cte_defs, 1,
            "CTE 'wrapped' must be defined exactly once\n  SQL:\n{sql}"
        );
    }

    #[test]
    fn test_no_duplicate_cte_when_offset_alias_equals_metric_name() {
        let mut store = MockStore::new();

        store.metrics.push(RawMetricRow {
            name: "base".into(),
            metric_type: "simple".into(),
            description: String::new(),
            type_params: r#"{
                "metric_aggregation_params": {
                    "semantic_model": "fct_events",
                    "agg": "count",
                    "expr": "1",
                    "agg_time_dimension": "ds"
                }
            }"#
            .into(),
            metric_filter: String::new(),
        });

        store.metrics.push(RawMetricRow {
            name: "base_last_year".into(),
            metric_type: "derived".into(),
            description: String::new(),
            type_params: r#"{
                "expr": "base_last_year",
                "metrics": [{
                    "name": "base",
                    "alias": "base_last_year",
                    "offset_window": "1 year",
                    "offset_to_grain": null,
                    "filter": null
                }]
            }"#
            .into(),
            metric_filter: String::new(),
        });

        store.metrics.push(RawMetricRow {
            name: "yoy_growth".into(),
            metric_type: "derived".into(),
            description: String::new(),
            type_params: r#"{
                "expr": "base - base_last_year",
                "metrics": [
                    {"name": "base",           "alias": null, "offset_window": null, "offset_to_grain": null, "filter": null},
                    {"name": "base_last_year", "alias": null, "offset_window": null, "offset_to_grain": null, "filter": null}
                ]
            }"#
            .into(),
            metric_filter: String::new(),
        });

        store.models.push(RawModelRow {
            name: "fct_events".into(),
            node_relation: r#"{"relation_name": "\"db\".\"main\".\"fct_events\"", "alias": "fct_events", "schema_name": "main", "database": "db"}"#.into(),
            primary_entity: "event".into(),
            unique_id: "semantic_model.fct_events".into(),
        });
        store.dimensions.push((
            "semantic_model.fct_events".into(),
            vec![RawDimensionRow {
                name: "ds".into(),
                dimension_type: "time".into(),
                expr: "ds".into(),
                time_granularity: "day".into(),
            }],
        ));

        let spec = SemanticQuerySpec {
            metrics: vec!["yoy_growth".into()],
            group_by: vec![GroupBySpec::TimeDimension {
                name: "metric_time".into(),
                granularity: "month".into(),
                date_part: None,
            }],
            where_filters: vec![],
            order_by: vec![],
            limit: None,
            time_constraint: None,
        };

        let sql = compile(&mut store, &spec, Dialect::DuckDB)
            .expect("yoy_growth should compile without error");

        let cte_defs = sql.matches("base_last_year AS (").count();
        assert_eq!(
            cte_defs, 1,
            "CTE 'base_last_year' must be defined exactly once\n  SQL:\n{sql}"
        );
    }

    #[test]
    fn test_offset_window_object_format_is_applied() {
        let mut store = MockStore::new();

        store.metrics.push(RawMetricRow {
            name: "events".into(),
            metric_type: "simple".into(),
            description: String::new(),
            type_params: r#"{
                "metric_aggregation_params": {
                    "semantic_model": "fct_events",
                    "agg": "count",
                    "expr": "1",
                    "agg_time_dimension": "ds"
                }
            }"#
            .into(),
            metric_filter: String::new(),
        });

        store.metrics.push(RawMetricRow {
            name: "events_last_year".into(),
            metric_type: "derived".into(),
            description: String::new(),
            type_params: r#"{
                "expr": "events_last_year_alias",
                "metrics": [{
                    "name": "events",
                    "alias": "events_last_year_alias",
                    "offset_window": {"count": 1, "granularity": "year"},
                    "offset_to_grain": null,
                    "filter": null
                }]
            }"#
            .into(),
            metric_filter: String::new(),
        });

        store.models.push(RawModelRow {
            name: "fct_events".into(),
            node_relation: r#"{"relation_name": "\"db\".\"main\".\"fct_events\"", "alias": "fct_events", "schema_name": "main", "database": "db"}"#.into(),
            primary_entity: "event".into(),
            unique_id: "semantic_model.fct_events".into(),
        });
        store.dimensions.push((
            "semantic_model.fct_events".into(),
            vec![RawDimensionRow {
                name: "ds".into(),
                dimension_type: "time".into(),
                expr: "ds".into(),
                time_granularity: "day".into(),
            }],
        ));

        let spec = SemanticQuerySpec {
            metrics: vec!["events_last_year".into()],
            group_by: vec![GroupBySpec::TimeDimension {
                name: "metric_time".into(),
                granularity: "month".into(),
                date_part: None,
            }],
            where_filters: vec![],
            order_by: vec![],
            limit: None,
            time_constraint: None,
        };

        let sql = compile(&mut store, &spec, Dialect::DuckDB)
            .expect("events_last_year should compile without error");

        assert!(
            sql.contains("INTERVAL"),
            "SQL must contain an INTERVAL expression for the 1-year offset\n  SQL:\n{sql}"
        );
        assert!(
            sql.contains("INNER JOIN"),
            "offset CTE must use an INNER JOIN to apply the time shift\n  SQL:\n{sql}"
        );
    }

    #[test]
    fn test_filter_on_outer_derived_metric_propagates_to_inputs() {
        let mut store = MockStore::new();

        store.metrics.push(RawMetricRow {
            name: "count_nps".into(),
            metric_type: "simple".into(),
            description: String::new(),
            type_params: r#"{
                "expr": "response_id",
                "metric_aggregation_params": {
                    "semantic_model": "fct_nps",
                    "agg": "count",
                    "agg_time_dimension": "created_at"
                }
            }"#
            .into(),
            metric_filter: String::new(),
        });

        store.metrics.push(RawMetricRow {
            name: "nps".into(),
            metric_type: "derived".into(),
            description: String::new(),
            type_params: r#"{"expr": "count_nps", "metrics": [{"name": "count_nps"}]}"#.into(),
            metric_filter: String::new(),
        });

        store.metrics.push(RawMetricRow {
            name: "nps_developer".into(),
            metric_type: "derived".into(),
            description: String::new(),
            type_params: r#"{"expr": "nps", "metrics": [{"name": "nps"}]}"#.into(),
            metric_filter: r#"{"where_filters": [
                {"where_sql_template": "{{ Dimension('nps_survey__account_plan_tier') }} = 'developer'"}
            ]}"#
            .into(),
        });

        store.models.push(RawModelRow {
            name: "fct_nps".into(),
            node_relation: r#"{"relation_name": "\"db\".\"main\".\"fct_nps\"", "alias": "fct_nps", "schema_name": "main", "database": "db"}"#.into(),
            primary_entity: "nps_survey".into(),
            unique_id: "semantic_model.fct_nps".into(),
        });
        store.entities.push((
            "semantic_model.fct_nps".into(),
            vec![RawEntityRow {
                name: "nps_survey".into(),
                entity_type: "primary".into(),
                expr: "response_id".into(),
            }],
        ));
        store.dimensions.push((
            "semantic_model.fct_nps".into(),
            vec![
                RawDimensionRow {
                    name: "account_plan_tier".into(),
                    dimension_type: "categorical".into(),
                    expr: "account_plan_tier".into(),
                    time_granularity: String::new(),
                },
                RawDimensionRow {
                    name: "created_at".into(),
                    dimension_type: "time".into(),
                    expr: "created_at".into(),
                    time_granularity: "day".into(),
                },
            ],
        ));
        store.join_graph.push(RawJoinGraphRow {
            model_name: "fct_nps".into(),
            entity_name: "nps_survey".into(),
            entity_type: "primary".into(),
            expr: "response_id".into(),
        });

        let spec = SemanticQuerySpec {
            metrics: vec!["nps_developer".into()],
            group_by: vec![],
            where_filters: vec![],
            order_by: vec![],
            limit: None,
            time_constraint: None,
        };

        let sql = compile(&mut store, &spec, Dialect::DuckDB).unwrap();

        assert!(
            sql.contains("account_plan_tier"),
            "filter must propagate — account_plan_tier missing:\n{sql}"
        );
        assert!(
            sql.contains("'developer'"),
            "= 'developer' must appear in the generated SQL:\n{sql}"
        );
    }

    #[test]
    fn test_two_derived_metrics_with_different_filters_get_independent_ctes() {
        let mut store = MockStore::new();

        store.metrics.push(RawMetricRow {
            name: "count_nps".into(),
            metric_type: "simple".into(),
            description: String::new(),
            type_params: r#"{
                "expr": "response_id",
                "metric_aggregation_params": {
                    "semantic_model": "fct_nps",
                    "agg": "count",
                    "agg_time_dimension": "created_at"
                }
            }"#
            .into(),
            metric_filter: String::new(),
        });
        store.metrics.push(RawMetricRow {
            name: "nps".into(),
            metric_type: "derived".into(),
            description: String::new(),
            type_params: r#"{"expr": "count_nps", "metrics": [{"name": "count_nps"}]}"#.into(),
            metric_filter: String::new(),
        });
        store.metrics.push(RawMetricRow {
            name: "nps_developer".into(),
            metric_type: "derived".into(),
            description: String::new(),
            type_params: r#"{"expr": "nps", "metrics": [{"name": "nps"}]}"#.into(),
            metric_filter: r#"{"where_filters": [
                {"where_sql_template": "{{ Dimension('nps_survey__account_plan_tier') }} = 'developer'"}
            ]}"#
            .into(),
        });
        store.metrics.push(RawMetricRow {
            name: "nps_enterprise".into(),
            metric_type: "derived".into(),
            description: String::new(),
            type_params: r#"{"expr": "nps", "metrics": [{"name": "nps"}]}"#.into(),
            metric_filter: r#"{"where_filters": [
                {"where_sql_template": "{{ Dimension('nps_survey__account_plan_tier') }} = 'enterprise'"}
            ]}"#
            .into(),
        });

        store.models.push(RawModelRow {
            name: "fct_nps".into(),
            node_relation: r#"{"relation_name": "\"db\".\"main\".\"fct_nps\"", "alias": "fct_nps", "schema_name": "main", "database": "db"}"#.into(),
            primary_entity: "nps_survey".into(),
            unique_id: "semantic_model.fct_nps".into(),
        });
        store.entities.push((
            "semantic_model.fct_nps".into(),
            vec![RawEntityRow {
                name: "nps_survey".into(),
                entity_type: "primary".into(),
                expr: "response_id".into(),
            }],
        ));
        store.dimensions.push((
            "semantic_model.fct_nps".into(),
            vec![
                RawDimensionRow {
                    name: "account_plan_tier".into(),
                    dimension_type: "categorical".into(),
                    expr: "account_plan_tier".into(),
                    time_granularity: String::new(),
                },
                RawDimensionRow {
                    name: "created_at".into(),
                    dimension_type: "time".into(),
                    expr: "created_at".into(),
                    time_granularity: "day".into(),
                },
            ],
        ));
        store.join_graph.push(RawJoinGraphRow {
            model_name: "fct_nps".into(),
            entity_name: "nps_survey".into(),
            entity_type: "primary".into(),
            expr: "response_id".into(),
        });

        let spec = SemanticQuerySpec {
            metrics: vec!["nps_developer".into(), "nps_enterprise".into()],
            group_by: vec![],
            where_filters: vec![],
            order_by: vec![],
            limit: None,
            time_constraint: None,
        };

        let sql = compile(&mut store, &spec, Dialect::DuckDB).unwrap();

        assert!(
            sql.contains("'developer'"),
            "developer filter must appear in SQL:\n{sql}"
        );
        assert!(
            sql.contains("'enterprise'"),
            "enterprise filter must appear — base CTEs are being shared:\n{sql}"
        );
    }
}
