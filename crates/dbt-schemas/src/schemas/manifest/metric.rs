use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

use crate::schemas::{
    CommonAttributes,
    common::NodeDependsOn,
    manifest::common::{SourceFileMetadata, WhereFilterIntersection},
    project::MetricConfig,
    ref_and_source::DbtRef,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DbtMetric {
    pub __common_attr__: CommonAttributes,
    pub __metric_attr__: DbtMetricAttr,

    // To be deprecated
    pub deprecated_config: MetricConfig,

    pub __other__: BTreeMap<String, YmlValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DbtMetricAttr {
    pub label: String,
    pub metric_type: MetricType,
    pub type_params: MetricTypeParams,
    pub filter: Option<WhereFilterIntersection>,
    pub metadata: Option<SourceFileMetadata>,
    pub time_granularity: Option<String>,
    pub unrendered_config: BTreeMap<String, YmlValue>,
    pub depends_on: NodeDependsOn,
    pub refs: Vec<DbtRef>,
    pub sources: Vec<Vec<String>>,
    pub metrics: Vec<Vec<String>>,
    pub created_at: f64,
    pub group: Option<String>,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetricType {
    #[default]
    Simple,
    Ratio,
    Cumulative,
    Derived,
    Conversion,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetricTypeParams {
    pub measure: Option<MetricInputMeasure>,
    pub input_measures: Option<Vec<MetricInputMeasure>>,
    pub numerator: Option<MetricInput>,
    pub denominator: Option<MetricInput>,
    pub expr: Option<String>,
    pub window: Option<MetricTimeWindow>,
    pub grain_to_date: Option<GrainToDate>,
    pub metrics: Option<Vec<MetricInput>>,
    pub conversion_type_params: Option<YmlValue>,
    pub cumulative_type_params: Option<CumulativeTypeParams>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricInputMeasure {
    pub name: String,
    pub filter: Option<WhereFilterIntersection>,
    pub alias: Option<String>,
    pub join_to_timespine: Option<bool>,
    pub fill_nulls_with: Option<i32>,
}

impl Default for MetricInputMeasure {
    fn default() -> Self {
        Self {
            name: String::new(),
            filter: None,
            alias: None,
            join_to_timespine: Some(false),
            fill_nulls_with: None,
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct MetricInput {
    pub name: String,
    pub filter: Option<WhereFilterIntersection>,
    pub alias: Option<String>,
    pub offset_window: Option<MetricTimeWindow>,
    pub offset_to_grain: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricTimeWindow {
    pub count: i32,
    pub granularity: String,
}

impl MetricTimeWindow {
    pub fn from_string(str: String) -> Self {
        let parts: Vec<&str> = str.split_whitespace().collect();
        let count = parts[0].parse().unwrap_or(1);
        // remove last 's' if plural, ex. 'days' -> 'day'
        let mut granularity = parts[1].parse().unwrap_or("month".to_string());
        if granularity.ends_with('s') {
            granularity.pop();
        }
        Self { count, granularity }
    }
}

impl Default for MetricTimeWindow {
    fn default() -> Self {
        Self {
            count: 1,
            granularity: String::from("day"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CumulativeTypeParams {
    pub window: Option<MetricTimeWindow>,
    pub grain_to_date: Option<String>,
    pub period_agg: Option<PeriodAggregationType>,
}

impl Default for CumulativeTypeParams {
    fn default() -> Self {
        Self {
            window: None,
            grain_to_date: None,
            period_agg: Some(PeriodAggregationType::First),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PeriodAggregationType {
    First,
    Last,
    Average,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum GrainToDate {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

// COMMENTED OUT TO BE REWRITTEN LATER
//
// // From implementations for converting from properties to manifest types
//
// impl From<StringOrMetricInputMeasure> for MetricInputMeasure {
//     fn from(source: StringOrMetricInputMeasure) -> Self {
//         match source {
//             StringOrMetricInputMeasure::String(name) => Self {
//                 name,
//                 ..Default::default()
//             },
//             StringOrMetricInputMeasure::MetricInputMeasure(measure) => Self::from(measure),
//         }
//     }
// }
//
// impl From<props::MetricInputMeasure> for MetricInputMeasure {
//     fn from(source: props::MetricInputMeasure) -> Self {
//         Self {
//             name: source.name,
//             filter: source.filter.map(Into::into),
//             alias: source.alias,
//             join_to_timespine: source.join_to_timespine.or(Some(false)),
//             fill_nulls_with: source.fill_nulls_with,
//         }
//     }
// }
//
// impl From<StringOrMetricInput> for MetricInput {
//     fn from(source: StringOrMetricInput) -> Self {
//         match source {
//             StringOrMetricInput::String(name) => Self {
//                 name,
//                 ..Default::default()
//             },
//             StringOrMetricInput::MetricInput(input) => Self::from(input),
//         }
//     }
// }
//
// impl From<props::MetricInput> for MetricInput {
//     fn from(source: props::MetricInput) -> Self {
//         Self {
//             name: source.name,
//             filter: source.filter.map(Into::into),
//             alias: source.alias,
//             offset_window: source.offset_window.map(MetricTimeWindow::from_string),
//             offset_to_grain: source.offset_to_grain,
//         }
//     }
// }
//
// impl From<props::PeriodAggregationType> for PeriodAggregationType {
//     fn from(source: props::PeriodAggregationType) -> Self {
//         match source {
//             props::PeriodAggregationType::First => Self::First,
//             props::PeriodAggregationType::Last => Self::Last,
//             props::PeriodAggregationType::Average => Self::Average,
//         }
//     }
// }
//
// impl From<props::CumulativeTypeParams> for CumulativeTypeParams {
//     fn from(source: props::CumulativeTypeParams) -> Self {
//         Self {
//             window: source.window.map(MetricTimeWindow::from_string),
//             grain_to_date: source.grain_to_date,
//             period_agg: Some(source.period_agg.unwrap_or_default().into()),
//         }
//     }
// }
//
// impl From<props::MetricsProperties> for MetricTypeParams {
//     fn from(_source: props::MetricsProperties) -> Self {
//         let numerator = source
//             .type_params
//             .numerator
//             .as_ref()
//             .map(|n| MetricInput::from(n.clone()));
//         let denominator = source
//             .type_params
//             .denominator
//             .as_ref()
//             .map(|d| MetricInput::from(d.clone()));
//
//         let input_measures = if matches!(source.type_, props::MetricType::Ratio)
//             && numerator.is_some()
//             && denominator.is_some()
//         {
//             // For ratio metrics, create input measures from numerator and denominator
//             let num = numerator.as_ref().unwrap();
//             let den = denominator.as_ref().unwrap();
//             Some(vec![
//                 MetricInputMeasure {
//                     name: num.name.clone(),
//                     filter: num.filter.clone(),
//                     alias: num.alias.clone(),
//                     join_to_timespine: Some(false),
//                     fill_nulls_with: None,
//                 },
//                 MetricInputMeasure {
//                     name: den.name.clone(),
//                     filter: den.filter.clone(),
//                     alias: den.alias.clone(),
//                     join_to_timespine: Some(false),
//                     fill_nulls_with: None,
//                 },
//             ])
//         } else if let Some(ref metrics) = source.type_params.metrics {
//             // If we have metrics, convert them to input measures
//             Some(
//                 metrics
//                     .iter()
//                     .map(|metric_input| match metric_input {
//                         StringOrMetricInput::String(name) => MetricInputMeasure {
//                             name: name.clone(),
//                             ..Default::default()
//                         },
//                         StringOrMetricInput::MetricInput(input) => MetricInputMeasure {
//                             name: input.name.clone(),
//                             filter: input.filter.as_ref().map(|f| f.clone().into()),
//                             alias: input.alias.clone(),
//                             join_to_timespine: Some(false), // Default for metrics converted to measures
//                             fill_nulls_with: None,
//                         },
//                     })
//                     .collect(),
//             )
//         } else if let Some(ref measure) = source.type_params.measure {
//             // Fallback to single measure as array
//             Some(vec![MetricInputMeasure::from(measure.clone())])
//         } else {
//             None
//         };
//
//         Self {
//             measure: source.type_params.measure.map(Into::into),
//             input_measures,
//             numerator,
//             denominator,
//             expr: source.type_params.expr,
//             window: source.type_params.window.map(MetricTimeWindow::from_string),
//             grain_to_date: None, // TODO: Convert appropriately
//             metrics: Some(
//                 source
//                     .type_params
//                     .metrics
//                     .unwrap_or_default()
//                     .iter()
//                     .map(|metric_input| metric_input.clone().into())
//                     .collect(),
//             ),
//             conversion_type_params: None, // TODO: Convert from source.type_params.conversion_type_params
//             cumulative_type_params: if matches!(source.type_, props::MetricType::Cumulative) {
//                 Some(
//                     source
//                         .type_params
//                         .cumulative_type_params
//                         .unwrap_or_default()
//                         .into(),
//                 )
//             } else {
//                 None
//             },
//         }
//     }
// }
