use dbt_serde_yaml::JsonSchema;
#[cfg(test)]
use fake::Dummy;
use serde::{Deserialize, Serialize};

#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct WriteArtifactInfo {
    /// The path to the artifact.
    pub relative_path: Option<String>,
    /// Time it took to write the artifact in milliseconds.
    pub duration_ms: Option<u64>,
}
