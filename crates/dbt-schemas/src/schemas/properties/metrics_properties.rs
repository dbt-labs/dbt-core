use crate::schemas::dbt_column::ColumnPropertiesGranularity;
use crate::schemas::project::MetricConfig;
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
    pub description: Option<String>,
    pub label: Option<String>,
    #[serde(rename = "type")]
    pub type_: Option<MetricType>,
    pub agg: Option<AggregationType>,
    pub percentile: Option<f32>,
    pub percentile_type: Option<PercentileType>,
    pub join_to_timespine: Option<bool>,
    pub fill_nulls_with: Option<f32>,
    pub expr: Option<MetricExpr>,
    // TODO: can we add a macro to this field for it to be ignored during jinja transformation?
    pub filter: Option<String>,
    pub config: Option<MetricConfig>, // TODO -- does MetricConfig only allow meta? What about group, tags, etc.?
    pub non_additive_dimension: Option<NonAdditiveDimension>,
    pub agg_time_dimension: Option<String>,
    pub window: Option<String>,
    pub grain_to_date: Option<ColumnPropertiesGranularity>,
    pub period_agg: Option<PeriodAggregationType>,
    pub input_metric: Option<StringOrMetricReference>,
    pub numerator: Option<StringOrMetricReference>,
    pub denominator: Option<StringOrMetricReference>,
    pub metrics: Option<Vec<StringOrMetricReference>>,
    pub metric_aliases: Option<Vec<MetricReference>>,
    pub entity: Option<String>,
    pub calculation: Option<ConversionCalculationType>,
    pub base_metric: Option<StringOrMetricReference>,
    pub conversion_metric: Option<StringOrMetricReference>,
    pub constant_properties: Option<Vec<ConstantProperty>>,

    // Flattened field:
    pub __unused__: Verbatim<BTreeMap<String, dbt_serde_yaml::Value>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum MetricType {
    #[default]
    Simple,
    Ratio,
    Cumulative,
    Derived,
    Conversion,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PercentileType {
    Discrete,
    Continuous,
}

#[derive(Debug, Clone, Serialize, UntaggedEnumDeserialize, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
pub enum MetricExpr {
    String(String),
    Integer(i32),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct NonAdditiveDimension {
    pub name: String,
    pub window_agg: WindowChoice,
    pub group_by: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WindowChoice {
    Min,
    Max,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct MetricReference {
    pub metric: Option<String>,
    pub filter: Option<String>,
    pub alias: Option<String>,
    pub offset_window: Option<String>,
}

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum StringOrMetricReference {
    String(String),
    MetricReference(MetricReference),
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
#[allow(non_camel_case_types)]
pub enum ConversionCalculationType {
    conversions,
    conversion_rate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct ConstantProperty {
    pub base_property: String,
    pub conversion_property: String,
}

#[derive(Default, Deserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum PeriodAggregationType {
    #[default]
    First,
    Last,
    Average,
}
