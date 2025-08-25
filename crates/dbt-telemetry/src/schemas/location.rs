use dbt_serde_yaml::JsonSchema;
#[cfg(test)]
use fake::Dummy;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use tracing::Metadata;

#[skip_serializing_none]
#[cfg_attr(test, derive(Dummy))]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default, PartialEq, Eq)]
#[serde(default)]
pub struct RecordCodeLocation {
    /// The file path
    pub file: Option<String>, // OTEL: CODE_FILE_PATH

    /// The line number
    pub line: Option<u32>, // OTEL: CODE_LINE_NUMBER

    /// The module qualified path
    pub module_path: Option<String>, // OTEL: CODE_FUNCTION_NAME seems irrelevant

    /// The log/span target. Internal developer name, often matching the module path.
    pub target: Option<String>, // OTEL doesn't have this field
}

impl std::fmt::Display for RecordCodeLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let target = self.target.as_deref().unwrap_or("log");

        let msg = if let Some(module_path) = &self.module_path
            && module_path != target
        {
            &format!("{target} | {module_path}")
        } else {
            target
        };

        write!(f, "{msg}")
    }
}

impl<'a> From<&'a Metadata<'a>> for RecordCodeLocation {
    fn from(value: &'a Metadata<'a>) -> Self {
        RecordCodeLocation {
            file: value.file().map(str::to_owned),
            line: value.line(),
            module_path: value.module_path().map(str::to_owned),
            target: Some(value.target().to_string()),
        }
    }
}

impl RecordCodeLocation {
    pub fn none() -> Self {
        RecordCodeLocation::default()
    }

    pub fn is_none(&self) -> bool {
        self.file.is_none()
            && self.line.is_none()
            && self.module_path.is_none()
            && self.target.is_none()
    }
}
