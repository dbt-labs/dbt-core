use std::collections::BTreeMap;

use dbt_serde_yaml::{JsonSchema, Value};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DbtOwner {
    pub email: Option<String>,
    pub name: Option<String>,
    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}
