use crate::schemas::common::ConstraintType;
use crate::schemas::common::ModelFreshnessRules;
use crate::schemas::common::Versions;
use crate::schemas::data_tests::DataTests;
use crate::schemas::dbt_column::ColumnProperties;
use crate::schemas::dbt_column::ColumnPropertiesDimensionType;
use crate::schemas::dbt_column::ColumnPropertiesEntityType;
use crate::schemas::dbt_column::ColumnPropertiesGranularity;
use crate::schemas::project::ModelConfig;
use crate::schemas::properties::MetricsProperties;
use crate::schemas::properties::properties::GetConfig;
use crate::schemas::serde::FloatOrString;
use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

/// Model level contraint
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ModelConstraint {
    #[serde(rename = "type")]
    pub type_: ConstraintType,
    pub expression: Option<String>,
    pub name: Option<String>,
    // Only ForeignKey constraints accept: a relation input
    // ref(), source() etc
    pub to: Option<String>,
    /// Only ForeignKey constraints accept: a list columns in that table
    /// containing the corresponding primary or unique key.
    pub to_columns: Option<Vec<String>>,
    pub columns: Option<Vec<String>>,
    pub warn_unsupported: Option<bool>,
    pub warn_unenforced: Option<bool>,
}
// todo: consider revising this design: warn_unsupported, warn_unenforced are adapter specific constraint. You don't want to specify them on all models!

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ModelProperties {
    pub columns: Option<Vec<ColumnProperties>>,
    pub config: Option<ModelConfig>,
    pub constraints: Option<Vec<ModelConstraint>>,
    pub data_tests: Option<Vec<DataTests>>,
    pub deprecation_date: Option<String>,
    pub description: Option<String>,
    pub identifier: Option<String>,
    pub latest_version: Option<FloatOrString>,
    pub name: String,
    pub tests: Option<Vec<DataTests>>,
    pub time_spine: Option<ModelsTimeSpine>,
    pub versions: Option<Vec<Versions>>,

    pub semantic_model: Option<bool>,
    pub agg_time_dimension: Option<String>,
    // TODO: rename to metrics once we figure out how to not render jinja for metrics nested under models
    // Currently, dbt commands won't work because we attempt to render Jinja for model nodes, but with
    // metrics in models, we attempt to render the `{{ Dimension(...) }}` jinja that should NOT be rendered
    pub metrics_todo: Option<Vec<MetricsProperties>>,
    pub derived_semantics: Option<Vec<DerivedSemantics>>,
    pub primary_entity: Option<String>,
}

impl ModelProperties {
    pub fn empty(name: String) -> Self {
        Self {
            name,
            columns: None,
            config: None,
            constraints: None,
            data_tests: None,
            deprecation_date: None,
            description: None,
            identifier: None,
            latest_version: None,
            tests: None,
            time_spine: None,
            versions: None,
            semantic_model: None,
            agg_time_dimension: None,
            metrics_todo: None,
            derived_semantics: None,
            primary_entity: None,
        }
    }
}

impl GetConfig<ModelConfig> for ModelProperties {
    fn get_config(&self) -> Option<&ModelConfig> {
        self.config.as_ref()
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ModelsTimeSpine {
    pub custom_granularities: Option<Vec<CustomGranularity>>,
    pub standard_granularity_column: String,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct CustomGranularity {
    pub column_name: Option<String>,
    pub name: String,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
pub struct ModelFreshness {
    pub build_after: Option<ModelFreshnessRules>,
}

// derived_semantics properties nested in models
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
pub struct DerivedSemantics {
    pub dimensions: Option<Vec<DerivedDimension>>,
    pub entities: Option<Vec<DerivedEntity>>,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
pub struct DerivedDimension {
    pub name: String,
    pub expr: String,
    #[serde(rename = "type")]
    pub type_: Option<ColumnPropertiesDimensionType>,
    pub granularity: Option<ColumnPropertiesGranularity>,
    pub is_partition: Option<bool>,
    pub label: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
pub struct DerivedEntity {
    pub name: String,
    pub expr: String,
    #[serde(rename = "type")]
    pub type_: ColumnPropertiesEntityType,
    pub description: Option<String>,
}
