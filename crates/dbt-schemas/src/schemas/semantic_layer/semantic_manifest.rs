use dbt_serde_yaml::JsonSchema;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::schemas::Nodes;
use crate::schemas::semantic_layer::metric::SemanticManifestMetric;
use crate::schemas::semantic_layer::project_configuration::SemanticManifestProjectConfiguration;
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

impl From<Nodes> for SemanticManifest {
    fn from(nodes: Nodes) -> Self {
        SemanticManifest {
            semantic_models: nodes
                .semantic_models
                .clone()
                .into_values()
                .map(|m| (*m).clone().into())
                .collect(),
            metrics: nodes
                .metrics
                .into_values()
                .map(|m| {
                    // don't hydrate input measures into semantic_manifest.json (only used for manifest.json)
                    let mut semantic_manifest_metric: SemanticManifestMetric = (*m).clone().into();
                    semantic_manifest_metric.type_params.input_measures = Some(vec![]);
                    semantic_manifest_metric
                })
                .collect(),
            project_configuration: SemanticManifestProjectConfiguration {
                dsi_package_version: Default::default(),
                metadata: None,
                time_spines: nodes
                    .semantic_models
                    .into_values()
                    .filter(|m| m.__semantic_model_attr__.time_spine.is_some())
                    .map(|m| (*m).clone().__semantic_model_attr__.time_spine.unwrap())
                    .collect(),
                // deprecated fields
                time_spine_table_configurations: vec![],
            },
            saved_queries: nodes
                .saved_queries
                .into_values()
                .map(|m| (*m).clone().into())
                .collect(),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Eq, PartialEq, JsonSchema)]
pub struct SemanticLayerElementConfig {
    pub meta: Option<BTreeMap<String, YmlValue>>,
}
