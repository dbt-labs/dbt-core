use dbt_serde_yaml::JsonSchema;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::schemas::Nodes;
use crate::schemas::manifest::DbtManifest;
use crate::schemas::semantic_layer::metric::SemanticManifestMetric;
use crate::schemas::semantic_layer::saved_query::SemanticManifestSavedQuery;
use crate::schemas::semantic_layer::semantic_model::SemanticManifestSemanticModel;

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SemanticManifest {
    pub semantic_models: Vec<SemanticManifestSemanticModel>,
    pub metrics: Vec<SemanticManifestMetric>,
    pub project_configuration: SemanticManifestProjectConfiguration,
    pub saved_queries: Vec<SemanticManifestSavedQuery>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SemanticManifestProjectConfiguration {}

impl From<Nodes> for SemanticManifest {
    fn from(nodes: Nodes) -> Self {
        SemanticManifest {
            semantic_models: nodes
                .semantic_models
                .into_values()
                .map(|m| (*m).clone().into())
                .collect(),
            metrics: nodes
                .metrics
                .into_values()
                .map(|m| (*m).clone().into())
                .collect(),
            project_configuration: SemanticManifestProjectConfiguration {},
            saved_queries: nodes
                .saved_queries
                .into_values()
                .map(|m| (*m).clone().into())
                .collect(),
        }
    }
}

impl From<DbtManifest> for SemanticManifest {
    fn from(_manifest: DbtManifest) -> Self {
        SemanticManifest {
            semantic_models: vec![],
            metrics: vec![],
            project_configuration: SemanticManifestProjectConfiguration {},
            saved_queries: vec![],
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, JsonSchema)]
pub struct SemanticLayerElementConfig {
    pub meta: Option<BTreeMap<String, YmlValue>>,
}
