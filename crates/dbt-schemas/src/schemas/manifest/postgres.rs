use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct PostgresIndex {
    columns: Vec<String>,
    unique: Option<bool>,
    #[serde(rename = "type")]
    _type: Option<String>,
}
