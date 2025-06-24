use std::collections::BTreeMap;

use dbt_serde_yaml::{JsonSchema, Value};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::schemas::serde::string_or_array;

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DbtOwner {
    #[serde(deserialize_with = "string_or_array")]
    pub email: Option<Vec<String>>,
    pub name: Option<String>,
    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}
