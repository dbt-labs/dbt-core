use crate::schemas::{
    common::{Dimension, SemanticModelDependsOn},
    manifest::DbtConfig,
    ref_and_source::DbtRef,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;
use std::{collections::BTreeMap, path::PathBuf};

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DbtSemanticModel {
    pub name: String,
    pub package_name: String,
    pub path: PathBuf,
    pub original_file_path: PathBuf,
    pub unique_id: String,
    pub fqn: Vec<String>,
    pub model: String,
    pub node_relation: NodeRelation,
    pub description: Option<String>,
    pub label: Option<String>,
    pub defaults: Option<SemanticModelDefaults>,
    pub entities: Vec<SemanticModelEntity>,
    pub measures: Vec<SemanticModelMeasure>,
    pub dimensions: Vec<Dimension>,
    pub metadata: Option<Value>,
    pub depends_on: SemanticModelDependsOn,
    pub refs: Vec<DbtRef>,
    pub created_at: Option<f64>,
    pub config: DbtConfig,
    pub unrendered_config: BTreeMap<String, Value>,
    pub primary_entity: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRelation {
    pub alias: String,
    pub schema_name: String,
    pub database: String,
    pub relation_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SemanticModelDefaults {
    pub agg_time_dimension: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticModelEntity {
    pub name: String,
    #[serde(rename = "type")]
    pub entity_type: String,
    pub description: Option<String>,
    pub label: Option<String>,
    pub role: Option<String>,
    pub expr: Option<String>,
    pub config: Option<EntityConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityConfig {
    pub meta: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticModelMeasure {
    pub name: String,
    pub agg: String,
    pub description: Option<String>,
    pub label: Option<String>,
    pub create_metric: bool,
    pub expr: Option<String>,
    pub agg_params: Option<Value>,
    pub non_additive_dimension: Option<NonAdditiveDimension>,
    pub agg_time_dimension: Option<String>,
    pub config: Option<MeasureConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonAdditiveDimension {
    pub name: String,
    pub window_choice: String,
    pub window_groupings: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasureConfig {
    pub meta: BTreeMap<String, Value>,
}
