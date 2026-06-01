use crate::default_to;
use crate::schemas::common::ConstraintType;
use crate::schemas::common::DimensionValidityParams;
use crate::schemas::common::ModelFreshnessRules;
use crate::schemas::common::UpdatesOn;
use crate::schemas::common::Versions;
use crate::schemas::common::model_freshness_rules_or_duration;
use crate::schemas::data_tests::DataTests;
use crate::schemas::dbt_column::ColumnProperties;
use crate::schemas::dbt_column::ColumnPropertiesDimensionType;
use crate::schemas::dbt_column::ColumnPropertiesEntityType;
use crate::schemas::dbt_column::Granularity;
use crate::schemas::project::ModelConfig;
use crate::schemas::project::ResolvableConfig;
use crate::schemas::project::SemanticModelConfig;
use crate::schemas::project::configs::common::default_meta_and_tags;
use crate::schemas::project::configs::semantic_model_config::ResolvedSemanticModelConfig;
use crate::schemas::properties::MetricsProperties;
use crate::schemas::properties::properties::GetConfig;
use crate::schemas::semantic_layer::semantic_manifest::SemanticLayerElementConfig;
use crate::schemas::serde::FloatOrString;
use crate::schemas::serde::string_or_array;
use dbt_common::io_args::StaticAnalysisOffReason;
use dbt_yaml::{DbtSchema, Spanned};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

/// Model level contraint
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default, DbtSchema)]
#[serde(rename_all = "snake_case")]
pub struct ModelConstraint {
    #[serde(rename = "type")]
    pub type_: ConstraintType,
    pub expression: Option<String>,
    pub name: Option<String>,
    // Only ForeignKey constraints accept: a relation input
    // ref(), source() etc
    pub to: Option<Spanned<String>>,
    /// Only ForeignKey constraints accept: a list columns in that table
    /// containing the corresponding primary or unique key.
    #[serde(
        default,
        deserialize_with = "string_or_array",
        serialize_with = "crate::schemas::serde::serialize_option_as_empty_vec"
    )]
    pub to_columns: Option<Vec<String>>,
    #[serde(default, deserialize_with = "string_or_array")]
    pub columns: Option<Vec<String>>,
    pub warn_unsupported: Option<bool>,
    pub warn_unenforced: Option<bool>,
}
// todo: consider revising this design: warn_unsupported, warn_unenforced are adapter specific constraint. You don't want to specify them on all models!

#[skip_serializing_none]
#[derive(Default, Deserialize, Serialize, Debug, Clone, DbtSchema)]
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
    #[serde(skip_deserializing, default)]
    pub static_analysis_off_reason: Option<StaticAnalysisOffReason>,
    pub tests: Option<Vec<DataTests>>,
    pub time_spine: Option<ModelPropertiesTimeSpine>,
    pub versions: Option<Vec<Versions>>,

    pub semantic_model: Option<ModelPropertiesSemanticModelConfig>,
    pub agg_time_dimension: Option<String>,
    pub metrics: Option<Vec<MetricsProperties>>,
    pub derived_semantics: Option<DerivedSemantics>,
    pub primary_entity: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema, Default)]
pub struct ModelPropertiesSemanticModelConfig {
    pub enabled: bool,
    pub name: Option<String>,
    pub group: Option<String>,
    pub config: Option<SemanticLayerElementConfig>,
}

impl ResolvableConfig<SemanticModelConfig> for ModelPropertiesSemanticModelConfig {
    type Resolved = ResolvedSemanticModelConfig;
    type PackageDefaults = ();
    type ResolveDefaults = ();

    fn get_enabled_with_default(&self) -> bool {
        self.enabled
    }

    fn disable(&mut self) {
        self.enabled = false;
    }

    fn apply_package_defaults(&mut self, _: ()) {}

    fn finalize(self) -> ResolvedSemanticModelConfig {
        unreachable!("ModelPropertiesSemanticModelConfig is never finalized directly")
    }

    fn default_to(&mut self, parent: &SemanticModelConfig) {
        let enabled = &mut Some(self.enabled);
        let group = &mut self.group;
        let meta = &mut self.config.clone().unwrap_or_default().meta;
        let tags = &mut None;

        #[allow(unused, clippy::let_unit_value)]
        let meta = default_meta_and_tags(meta, &parent.meta, tags, &parent.tags);
        #[allow(unused)]
        let tags = ();

        default_to!(parent, [enabled, group]);
    }
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
            static_analysis_off_reason: None,
            tests: None,
            time_spine: None,
            versions: None,
            semantic_model: None,
            agg_time_dimension: None,
            metrics: None,
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
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct ModelPropertiesTimeSpine {
    pub custom_granularities: Option<Vec<TimeSpineCustomGranularity>>,
    pub standard_granularity_column: String,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct TimeSpineCustomGranularity {
    pub column_name: Option<String>,
    pub name: String,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema, PartialEq, Eq)]
pub struct ModelFreshness {
    pub build_after: Option<ModelFreshnessRules>,
}

#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StatePreClone {
    Never,
    IfMissing,
    Always,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema)]
pub struct ModelState {
    #[serde(default, deserialize_with = "model_freshness_rules_or_duration")]
    pub lag_tolerance: Option<ModelFreshnessRules>,
    pub require_fresh_data_from: Option<UpdatesOn>,
    pub evaluate_volatile_sql: Option<bool>,
    pub pre_clone: Option<StatePreClone>,
    #[serde(alias = "execute_hooks_on_reuse")]
    pub execute_hooks_on_any_reuse: Option<bool>,
}

impl PartialEq for ModelState {
    fn eq(&self, other: &Self) -> bool {
        self.lag_tolerance == other.lag_tolerance
            && updates_on_eq(
                &self.require_fresh_data_from,
                &other.require_fresh_data_from,
            )
            && self.evaluate_volatile_sql == other.evaluate_volatile_sql
            && self.pre_clone == other.pre_clone
            && self.execute_hooks_on_any_reuse == other.execute_hooks_on_any_reuse
    }
}

impl Eq for ModelState {}

fn updates_on_eq(a: &Option<UpdatesOn>, b: &Option<UpdatesOn>) -> bool {
    match (a.as_ref(), b.as_ref()) {
        (None, None) => true,
        (Some(a_val), Some(b_val)) => a_val == b_val,
        (None, Some(b_val)) => b_val == &UpdatesOn::default(),
        (Some(a_val), None) => a_val == &UpdatesOn::default(),
    }
}

// derived_semantics properties nested in models
#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema, PartialEq, Eq)]
pub struct DerivedSemantics {
    pub dimensions: Option<Vec<DerivedDimension>>,
    pub entities: Option<Vec<DerivedEntity>>,
}

impl Default for DerivedSemantics {
    fn default() -> Self {
        Self {
            dimensions: Some(vec![]),
            entities: Some(vec![]),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema, PartialEq, Eq)]
pub struct DerivedDimension {
    pub name: String,
    pub expr: String,
    #[serde(rename = "type")]
    pub type_: ColumnPropertiesDimensionType,
    pub granularity: Option<Granularity>,
    pub is_partition: Option<bool>,
    pub label: Option<String>,
    pub description: Option<String>,
    pub config: Option<SemanticLayerElementConfig>,
    pub validity_params: Option<DimensionValidityParams>,
}

#[derive(Deserialize, Serialize, Debug, Clone, DbtSchema, PartialEq, Eq)]
pub struct DerivedEntity {
    pub name: String,
    pub expr: String,
    #[serde(rename = "type")]
    pub type_: ColumnPropertiesEntityType,
    pub description: Option<String>,
    pub label: Option<String>,
    pub config: Option<SemanticLayerElementConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_yaml;

    #[test]
    fn model_state_eq_defaults_require_fresh_data_from_to_any() {
        let base = ModelState {
            require_fresh_data_from: None,
            lag_tolerance: None,
            evaluate_volatile_sql: None,
            pre_clone: None,
            execute_hooks_on_any_reuse: None,
        };
        let other = ModelState {
            require_fresh_data_from: Some(UpdatesOn::Any),
            lag_tolerance: None,
            evaluate_volatile_sql: None,
            pre_clone: None,
            execute_hooks_on_any_reuse: None,
        };

        assert_eq!(base, other);
    }

    #[test]
    fn model_state_eq_keeps_require_fresh_data_from_all_distinct() {
        let base = ModelState {
            require_fresh_data_from: None,
            lag_tolerance: None,
            evaluate_volatile_sql: None,
            pre_clone: None,
            execute_hooks_on_any_reuse: None,
        };
        let other = ModelState {
            require_fresh_data_from: Some(UpdatesOn::All),
            lag_tolerance: None,
            evaluate_volatile_sql: None,
            pre_clone: None,
            execute_hooks_on_any_reuse: None,
        };

        assert_ne!(base, other);
    }

    #[test]
    fn model_state_accepts_legacy_execute_hooks_on_reuse_key() {
        let yaml = r#"
execute_hooks_on_reuse: true
"#;
        let state: ModelState = dbt_yaml::from_str(yaml).unwrap();

        assert_eq!(state.execute_hooks_on_any_reuse, Some(true));
    }

    #[test]
    fn test_model_constraint_columns_as_string() {
        let yaml = r#"
type: primary_key
columns: mart_hashkey_order
"#;
        let constraint: ModelConstraint = dbt_yaml::from_str(yaml).unwrap();
        assert_eq!(
            constraint.columns,
            Some(vec!["mart_hashkey_order".to_string()])
        );
        assert_eq!(constraint.type_, ConstraintType::PrimaryKey);
    }

    #[test]
    fn test_model_constraint_columns_as_string_array() {
        let yaml = r#"
type: primary_key
columns: ["mart_hashkey_order"]
"#;
        let constraint: ModelConstraint = dbt_yaml::from_str(yaml).unwrap();
        assert_eq!(
            constraint.columns,
            Some(vec!["mart_hashkey_order".to_string()])
        );
        assert_eq!(constraint.type_, ConstraintType::PrimaryKey);
    }

    #[test]
    fn test_model_constraint_columns_as_array() {
        let yaml = r#"
type: primary_key
columns:
  - column1
  - column2
"#;
        let constraint: ModelConstraint = dbt_yaml::from_str(yaml).unwrap();
        assert_eq!(
            constraint.columns,
            Some(vec!["column1".to_string(), "column2".to_string()])
        );
        assert_eq!(constraint.type_, ConstraintType::PrimaryKey);
    }

    #[test]
    fn test_model_constraint_columns_as_null() {
        let yaml = r#"
type: check
expression: "amount > 0"
"#;
        let constraint: ModelConstraint = dbt_yaml::from_str(yaml).unwrap();
        assert_eq!(constraint.columns, None);
        assert_eq!(constraint.type_, ConstraintType::Check);
        assert_eq!(constraint.expression, Some("amount > 0".to_string()));
    }

    #[test]
    fn test_model_constraint_to_columns_as_string() {
        let yaml = r#"
type: foreign_key
columns: order_id
to: ref('orders')
to_columns: id
"#;
        let constraint: ModelConstraint = dbt_yaml::from_str(yaml).unwrap();
        assert_eq!(constraint.columns, Some(vec!["order_id".to_string()]));
        assert_eq!(constraint.to_columns, Some(vec!["id".to_string()]));
        assert_eq!(
            constraint.to.as_ref().map(|s| s.as_str()),
            Some("ref('orders')")
        );
        assert_eq!(constraint.type_, ConstraintType::ForeignKey);
    }

    #[test]
    fn test_model_constraint_to_columns_as_array() {
        let yaml = r#"
type: foreign_key
columns:
  - user_id
  - org_id
to: ref('users')
to_columns:
  - id
  - organization_id
"#;
        let constraint: ModelConstraint = dbt_yaml::from_str(yaml).unwrap();
        assert_eq!(
            constraint.columns,
            Some(vec!["user_id".to_string(), "org_id".to_string()])
        );
        assert_eq!(
            constraint.to_columns,
            Some(vec!["id".to_string(), "organization_id".to_string()])
        );
        assert_eq!(constraint.type_, ConstraintType::ForeignKey);
    }

    #[test]
    fn test_model_constraint_full_example() {
        let yaml = r#"
type: primary_key
name: pk_orders
columns: order_id
warn_unsupported: true
warn_unenforced: false
"#;
        let constraint: ModelConstraint = dbt_yaml::from_str(yaml).unwrap();
        assert_eq!(constraint.type_, ConstraintType::PrimaryKey);
        assert_eq!(constraint.name, Some("pk_orders".to_string()));
        assert_eq!(constraint.columns, Some(vec!["order_id".to_string()]));
        assert_eq!(constraint.warn_unsupported, Some(true));
        assert_eq!(constraint.warn_unenforced, Some(false));
    }

    #[test]
    fn test_model_constraint_to_span_ref_captures_line() {
        let yaml = "type: foreign_key\nto: ref('primary_model')\nto_columns: [id]\n";
        let constraint: ModelConstraint = dbt_yaml::from_str(yaml).unwrap();
        let spanned = constraint.to.as_ref().expect("to should be Some");
        assert_eq!(spanned.as_str(), "ref('primary_model')");
        assert!(spanned.span().is_valid(), "span should be valid");
        assert_eq!(spanned.span().start.line, 2, "to: should be on line 2");
    }

    #[test]
    fn test_model_constraint_to_span_source_captures_line() {
        let yaml = "type: foreign_key\nto: source('raw', 'users')\nto_columns: [id]\n";
        let constraint: ModelConstraint = dbt_yaml::from_str(yaml).unwrap();
        let spanned = constraint.to.as_ref().expect("to should be Some");
        assert_eq!(spanned.as_str(), "source('raw', 'users')");
        assert!(spanned.span().is_valid(), "span should be valid");
        assert_eq!(spanned.span().start.line, 2, "to: should be on line 2");
    }
}
