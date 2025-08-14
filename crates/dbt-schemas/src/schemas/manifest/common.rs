use std::collections::BTreeMap;

use dbt_serde_yaml::{JsonSchema, Value};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::schemas::serde::StringOrArrayOfStrings;

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DbtOwner {
    pub email: Option<StringOrArrayOfStrings>,
    pub name: Option<String>,
    pub __other__: BTreeMap<String, Value>,
}
