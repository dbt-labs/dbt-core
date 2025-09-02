use crate::schemas::{
    CommonAttributes,
    common::{Dimension, NodeDependsOn},
    manifest::common::SourceFileMetadata,
    project::SemanticModelConfig,
    ref_and_source::DbtRef,
    semantic_layer::semantic_manifest::SemanticLayerElementConfig,
};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DbtSemanticModel {
    pub __common_attr__: CommonAttributes,
    pub __semantic_model_attr__: DbtSemanticModelAttr,

    pub deprecated_config: SemanticModelConfig,

    pub __other__: BTreeMap<String, YmlValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DbtSemanticModelAttr {
    // Core semantic model attributes
    pub model: String,
    pub node_relation: Option<NodeRelation>,
    pub label: Option<String>,
    pub defaults: Option<SemanticModelDefaults>,
    pub entities: Vec<SemanticEntity>,
    pub measures: Vec<SemanticMeasure>,
    pub dimensions: Vec<Dimension>,
    pub metadata: Option<SourceFileMetadata>,
    pub primary_entity: Option<String>,

    // Node dependencies and references
    pub depends_on: NodeDependsOn,
    pub refs: Vec<DbtRef>,
    pub created_at: f64,
    pub unrendered_config: BTreeMap<String, YmlValue>,
    pub group: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRelation {
    pub alias: String,
    pub schema_name: String,
    pub database: Option<String>,
    pub relation_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SemanticModelDefaults {
    pub agg_time_dimension: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticEntity {
    pub name: String,
    #[serde(rename = "type")]
    pub entity_type: EntityType,
    pub description: Option<String>,
    pub label: Option<String>,
    pub role: Option<String>,
    pub expr: Option<String>,
    pub config: Option<SemanticLayerElementConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Foreign,
    Natural,
    Primary,
    Unique,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticMeasure {
    pub name: String,
    pub agg: AggregationType,
    pub description: Option<String>,
    pub label: Option<String>,
    pub create_metric: Option<bool>,
    pub expr: Option<String>,
    pub agg_params: Option<MeasureAggregationParameters>,
    pub non_additive_dimension: Option<NonAdditiveDimension>,
    pub agg_time_dimension: Option<String>,
    pub config: Option<SemanticLayerElementConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AggregationType {
    Sum,
    Min,
    Max,
    CountDistinct,
    SumBoolean,
    Average,
    Percentile,
    Median,
    Count,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasureAggregationParameters {
    pub percentile: Option<f64>,
    pub use_discrete_percentile: Option<bool>,
    pub use_approximate_percentile: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonAdditiveDimension {
    pub name: String,
    pub window_choice: AggregationType,
    pub window_groupings: Vec<String>,
}
