use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;

use crate::schemas::ref_and_source::DbtRef;

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DbtMetric {
    pub name: String,
    pub package_name: String,
    pub path: PathBuf,
    pub original_file_path: PathBuf,
    pub unique_id: String,
    pub fqn: Vec<String>,
    pub description: String,
    pub label: Option<String>,
    pub type_params: MetricTypeParams,
    pub filter: Option<MetricFilter>,
    pub metadata: Option<Value>,
    pub time_granularity: Option<String>,
    pub unrendered_config: BTreeMap<String, Value>,
    pub sources: Vec<Vec<String>>,
    pub depends_on: MetricDependsOn,
    pub refs: Vec<DbtRef>,
    pub metrics: Vec<Vec<String>>,
    pub created_at: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct MetricDependsOn {
    pub macros: Vec<String>,
    pub nodes: Vec<String>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricTypeParams {
    pub measure: Option<MetricMeasure>,
    pub input_measures: Option<Vec<Value>>,
    pub numerator: Option<Value>,
    pub denominator: Option<Value>,
    pub expr: Option<String>,
    pub window: Option<Value>,
    pub grain_to_date: Option<Value>,
    pub metrics: Option<Vec<Value>>,
    pub conversion_type_params: Option<Value>,
    pub cumulative_type_params: Option<Value>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricMeasure {
    pub name: String,
    pub filter: Option<Value>,
    pub alias: Option<String>,
    pub join_to_timespine: Option<bool>,
    pub fill_nulls_with: Option<Value>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricFilter {
    pub where_filters: Vec<MetricWhereFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricWhereFilter {
    pub where_sql_template: String,
}
