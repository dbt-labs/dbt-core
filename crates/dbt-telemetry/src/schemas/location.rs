use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use tracing::Metadata;

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(default)]
pub struct RecordCodeLocation {
    /// The file path
    #[serde(rename = "code.file.path")] // OTEL: CODE_FILE_PATH
    pub file: Option<String>,
    /// The line number
    #[serde(rename = "code.line.number")] // OTEL: CODE_LINE_NUMBER
    pub line: Option<u32>,
    /// The column number
    #[serde(rename = "code.column.number")] // OTEL: CODE_COLUMN_NUMBER
    pub column: Option<u32>,
    /// The module qualified path
    #[serde(rename = "code.module.path")] // OTEL: CODE_FUNCTION_NAME seems irrelevant
    pub module_path: Option<String>,
    /// The log/span target. Internal developer name, often matching the module path.
    #[serde(rename = "log.target")] // OTEL doesn't have this field
    pub target: Option<String>,
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
            column: None,
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
            && self.column.is_none()
            && self.module_path.is_none()
            && self.target.is_none()
    }
}
