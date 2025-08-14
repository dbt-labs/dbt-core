use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

// Type aliases for clarity
type YmlValue = dbt_serde_yaml::Value;

use crate::schemas::CommonAttributes;

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtOperation {
    pub __common_attr__: CommonAttributes,
    pub __other__: BTreeMap<String, YmlValue>,
}
