use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct SavedQueriesConfig {
    #[serde(rename = "+schema")]
    pub schema: Option<String>,
}
