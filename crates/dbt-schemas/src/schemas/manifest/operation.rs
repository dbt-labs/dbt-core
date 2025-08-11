use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

// Type aliases for clarity
type JsonValue = serde_json::Value;

use crate::schemas::CommonAttributes;

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtOperation {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,

    #[serde(flatten)]
    pub other: BTreeMap<String, JsonValue>,
}
