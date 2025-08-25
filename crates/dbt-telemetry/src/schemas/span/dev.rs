use dbt_serde_yaml::JsonSchema;
#[cfg(test)]
use fake::Dummy;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use super::super::location::RecordCodeLocation;

#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(untagged)]
pub enum DebugValue {
    Float64(f64),
    Int64(i64),
    UInt64(u64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
}

#[skip_serializing_none]
#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Default)]
pub struct DevInternalInfo {
    /// Internal developer span name, often the function
    pub name: String,
    #[serde(flatten)]
    pub location: RecordCodeLocation,
    /// Arbitrary extra string for debugging purposes.
    pub extra: Option<std::collections::BTreeMap<String, DebugValue>>,
}

// Custom display implementation is used to derive a readable/helpful span name.
impl std::fmt::Display for DevInternalInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} | {}", self.name, self.location)
    }
}

#[skip_serializing_none]
#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Default)]
pub struct UnknownInfo {
    /// Internal developer span name, often the function
    pub name: String,
    #[serde(flatten)]
    pub location: RecordCodeLocation,
}
