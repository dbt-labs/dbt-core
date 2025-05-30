use crate::schemas::common::Dimension;

use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct SemanticModelsProperties {
    pub name: String,
    pub model: String,
    pub config: Option<SemanticModelConfig>,
    pub defaults: Option<SemanticModelsDefaults>,
    pub description: Option<String>,
    pub dimensions: Option<Vec<Dimension>>,
    pub entities: Option<Vec<Entity>>,
    pub label: Option<String>,
    pub measures: Option<Vec<Measure>>,
    pub primary_entity: Option<String>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct SemanticModelConfig {
    pub enabled: Option<bool>,
    pub group: Option<String>,
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct SemanticModelsDefaults {
    pub agg_time_dimension: Option<String>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct Entity {
    pub config: EntityConfig,
    pub description: Option<String>,
    pub expr: Option<EntityExpr>,
    pub label: Option<String>,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: EntityType,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct EntityConfig {
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum EntityExpr {
    String(String),
    Bool(bool),
}

#[derive(Deserialize, Serialize, Debug, Clone, Default, JsonSchema)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
pub enum EntityType {
    #[default]
    PRIMARY,
    UNIQUE,
    FOREIGN,
    NATURAL,
    primary,
    unique,
    foreign,
    natural,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct Measure {
    pub agg: MeasureAgg,
    pub agg_params: Option<AggregationTypeParams>,
    pub agg_time_dimension: Option<String>,
    pub config: Option<serde_json::Value>,
    pub create_metric: Option<bool>,
    pub create_metric_display_name: Option<String>,
    pub description: Option<String>,
    pub expr: Option<_MeasureExpr>,
    pub label: Option<String>,
    pub name: String,
    pub non_additive_dimension: Option<NonAdditiveDimension>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default, JsonSchema)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
pub enum MeasureAgg {
    #[default]
    SUM,
    MIN,
    MAX,
    AVERAGE,
    COUNT_DISTINCT,
    SUM_BOOLEAN,
    COUNT,
    PERCENTILE,
    MEDIAN,
    sum,
    min,
    max,
    average,
    count_distinct,
    sum_boolean,
    count,
    percentile,
    median,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum _MeasureExpr {
    String(String),
    I32(i32),
    Bool(bool),
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct AggregationTypeParams {
    pub percentile: Option<f32>,
    pub use_approximate_percentile: Option<bool>,
    pub use_discrete_percentile: Option<bool>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct NonAdditiveDimension {
    pub name: String,
    pub window_choice: Option<NonAdditiveDimensionWindowChoice>,
    pub window_groupings: Option<Vec<String>>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default, JsonSchema)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
pub enum NonAdditiveDimensionWindowChoice {
    #[default]
    MIN,
    MAX,
    min,
    max,
}
