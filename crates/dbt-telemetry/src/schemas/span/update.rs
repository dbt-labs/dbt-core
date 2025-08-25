use dbt_serde_yaml::JsonSchema;
#[cfg(test)]
use fake::Dummy;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct UpdateInfo {
    /// Update dbt to this version (e.g. 1.2.3) [default: latest version]
    pub version: Option<String>,
    /// Package to update (e.g. dbt) [default: dbt]
    pub package: Option<String>,
    /// The discovered path to the dbt executable
    pub exe_path: Option<String>,
}
