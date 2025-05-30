use super::function::Function;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct Definition {
    // /// A table definition
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub table: Option<Table>,
    /// A function definition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<Function>,
}
