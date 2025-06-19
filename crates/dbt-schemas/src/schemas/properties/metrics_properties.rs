use crate::schemas::common::TimeGranularity;
use crate::schemas::project::MetricConfig;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct MetricsProperties {
    pub name: String,
    pub label: String,
    #[serde(rename = "type")]
    pub type_: MetricType,
    pub type_params: MetricTypeParams,
    pub description: Option<String>,
    pub config: Option<MetricConfig>,
    pub filter: Option<String>,
    pub time_granularity: Option<TimeGranularity>,
    // Flattened field:
    pub __additional_properties__: Verbatim<BTreeMap<String, dbt_serde_yaml::Value>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum MetricType {
    Simple,
    Ratio,
    Cumulative,
    Derived,
    Conversion,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum StringOrMetricInputMeasure {
    String(String),
    MetricInputMeasure(MetricInputMeasure),
}

#[skip_serializing_none]
#[derive(Default, Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct MetricTypeParams {
    pub measure: Option<StringOrMetricInputMeasure>,
    #[serde(default)]
    pub input_measures: Vec<MetricInputMeasure>,
    pub numerator: Option<MetricInput>,
    pub denominator: Option<MetricInput>,
    pub expr: Option<String>,
    pub window: Option<MetricTimeWindow>,
    pub metrics: Option<Vec<MetricInput>>,
    pub conversion_type_params: Option<ConversionTypeParams>,
    pub cumulative_type_params: Option<CumulativeTypeParams>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct MetricInputMeasure {
    pub name: String,
    pub filter: Option<WhereFilterIntersection>,
    pub alias: Option<String>,
    pub join_to_timepine: Option<bool>,
    pub fill_nulls_with: Option<i32>,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct WhereFilterIntersection {
    pub where_filters: Vec<WhereFilter>,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct WhereFilter {
    pub where_sql_template: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct MetricTimeWindow {
    pub count: i32,
    pub granularity: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ConversionTypeParams {
    pub base_measure: MetricInputMeasure,
    pub conversion_measure: MetricInputMeasure,
    pub entity: String,
    pub calculation: ConversionCalculationType,
    pub window: Option<MetricTimeWindow>,
    pub constant_properties: Option<Vec<ConstantPropertyInput>>,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct CumulativeTypeParams {
    pub window: Option<MetricTimeWindow>,
    pub grain_to_date: Option<String>,
    pub period_agg: PeriodAggregationType,
}

#[derive(Default, Deserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum PeriodAggregationType {
    #[default]
    First,
    Last,
    Average,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ConstantPropertyInput {
    pub base_property: String,
    pub conversion_property: String,
}

#[derive(Default, Deserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConversionCalculationType {
    Conversions,
    #[default]
    ConversionRate,
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct MetricInput {
    pub name: String,
    pub filter: Option<WhereFilterIntersection>,
    pub alias: Option<String>,
    pub offset_window: Option<MetricTimeWindow>,
    pub offset_to_grain: Option<String>,
}
