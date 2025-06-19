use crate::schemas::data_tests::DataTests;
use crate::schemas::dbt_column::ColumnProperties;
use crate::schemas::project::SeedConfig;
use dbt_serde_yaml::JsonSchema;
use dbt_serde_yaml::Verbatim;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct SeedProperties {
    pub columns: Option<Vec<ColumnProperties>>,
    pub config: Option<SeedConfig>,
    pub data_tests: Verbatim<Option<Vec<DataTests>>>,
    pub description: Option<String>,
    pub name: String,
    pub tests: Option<Vec<DataTests>>,
}

impl SeedProperties {
    pub fn empty(name: String) -> Self {
        Self {
            name,
            columns: None,
            config: None,
            data_tests: Verbatim(None),
            description: None,
            tests: None,
        }
    }
}
