use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;

use crate::schemas::{CommonAttributes, NodeBaseAttributes};

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct DbtOperation {
    #[serde(flatten)]
    pub common_attr: CommonAttributes,

    #[serde(flatten)]
    pub base_attr: NodeBaseAttributes,

    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}
