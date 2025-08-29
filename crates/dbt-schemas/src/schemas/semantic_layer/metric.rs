use serde::{Deserialize, Serialize};

use crate::schemas::{
    manifest::{
        DbtMetric, ManifestMetric,
        common::{SourceFileMetadata, WhereFilterIntersection},
        metric::{MetricType, MetricTypeParams},
    },
    semantic_layer::semantic_manifest::SemanticLayerElementConfig,
};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SemanticManifestMetric {
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub type_: MetricType,
    pub type_params: MetricTypeParams,
    pub filter: Option<WhereFilterIntersection>,
    pub metadata: Option<SourceFileMetadata>,
    pub label: Option<String>,
    pub config: Option<SemanticLayerElementConfig>,
    pub time_granularity: Option<String>,
}

// Decide whether to map from DbtMetric or DbtManifest
// since DbtManifest is going be legacy it's probably a good idea to map from DbtMetric
impl From<DbtMetric> for SemanticManifestMetric {
    fn from(metric: DbtMetric) -> Self {
        SemanticManifestMetric {
            name: metric.__common_attr__.name,
            description: metric.__common_attr__.description,
            type_: metric.__metric_attr__.metric_type,
            type_params: metric.__metric_attr__.type_params,
            filter: metric.__metric_attr__.filter,
            metadata: metric.__metric_attr__.metadata,
            label: Some(metric.__metric_attr__.label),
            config: Some(SemanticLayerElementConfig {
                meta: metric.deprecated_config.meta,
            }),
            time_granularity: metric.__metric_attr__.time_granularity,
        }
    }
}

impl From<ManifestMetric> for SemanticManifestMetric {
    fn from(metric: ManifestMetric) -> Self {
        SemanticManifestMetric {
            name: metric.__common_attr__.name,
            description: metric.__common_attr__.description,
            type_: metric.metric_type,
            type_params: metric.type_params,
            filter: metric.filter,
            metadata: metric.metadata,
            label: Some(metric.label),
            config: Some(SemanticLayerElementConfig {
                meta: metric.config.meta,
            }),
            time_granularity: metric.time_granularity,
        }
    }
}
