use serde::{Deserialize, Serialize};

use crate::schemas::manifest::DbtManifest;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SemanticManifest {
    pub semantic_models: Vec<SemanticManifestSemanticModel>,
    pub metrics: Vec<SemanticManifestMetric>,
    pub project_configuration: SemanticManifestProjectConfiguration,
    pub saved_queries: Vec<SemanticManifestSavedQuery>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SemanticManifestSemanticModel {}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SemanticManifestMetric {}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SemanticManifestProjectConfiguration {}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SemanticManifestSavedQuery {}

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
