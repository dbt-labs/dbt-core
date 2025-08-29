use crate::schemas::common::TimeGranularity;
use crate::schemas::project::MetricConfig;
use crate::schemas::serde::StringOrArrayOfStrings;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::UntaggedEnumDeserialize;
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
    pub filter: Option<StringOrArrayOfStrings>,
    pub time_granularity: Option<TimeGranularity>,
    // Flattened field:
    pub __unused__: Verbatim<BTreeMap<String, dbt_serde_yaml::Value>>,
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

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum StringOrMetricInputMeasure {
    String(String),
    MetricInputMeasure(MetricInputMeasure),
}

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum StringOrMetricInput {
    String(String),
    MetricInput(MetricInput),
}

#[skip_serializing_none]
#[derive(Default, Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct MetricTypeParams {
    pub measure: Option<StringOrMetricInputMeasure>,
    pub numerator: Option<StringOrMetricInput>,
    pub denominator: Option<StringOrMetricInput>,
    pub expr: Option<String>,
    pub window: Option<String>,
    pub metrics: Option<Vec<StringOrMetricInput>>,
    pub conversion_type_params: Option<ConversionTypeParams>,
    pub cumulative_type_params: Option<CumulativeTypeParams>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct MetricInputMeasure {
    pub name: String,
    pub filter: Option<StringOrArrayOfStrings>,
    pub alias: Option<String>,
    pub join_to_timespine: Option<bool>,
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
    pub base_measure: StringOrMetricInputMeasure,
    pub conversion_measure: StringOrMetricInputMeasure,
    pub entity: String,
    pub calculation: ConversionCalculationType,
    pub window: Option<String>,
    pub constant_properties: Option<Vec<ConstantPropertyInput>>,
}

#[derive(Clone, Default, Deserialize, Serialize, Debug, JsonSchema)]
pub struct CumulativeTypeParams {
    pub window: Option<String>,
    pub grain_to_date: Option<String>,
    pub period_agg: Option<PeriodAggregationType>,
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
    pub filter: Option<StringOrArrayOfStrings>,
    pub alias: Option<String>,
    pub offset_window: Option<String>,
    pub offset_to_grain: Option<String>,
}
