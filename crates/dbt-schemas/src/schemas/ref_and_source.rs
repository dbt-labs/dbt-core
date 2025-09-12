use dbt_common::CodeLocation;
use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

use super::serde::StringOrInteger;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DbtRef {
    pub name: String,
    pub package: Option<String>,
    pub version: Option<StringOrInteger>,
    #[serde(skip_serializing)]
    pub location: Option<CodeLocation>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DbtSourceWrapper {
    pub source: Vec<String>,
    pub location: Option<CodeLocation>,
}

impl Serialize for DbtSourceWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.source.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DbtSourceWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let source = Vec::deserialize(deserializer)?;
        Ok(DbtSourceWrapper {
            source,
            location: None,
        })
    }
}
