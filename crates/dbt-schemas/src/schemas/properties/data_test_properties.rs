use crate::schemas::{project::DataTestConfig, properties::GetConfig};
use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct DataTestProperties {
    pub config: Option<DataTestConfig>,
    pub description: Option<String>,
    pub name: String,
}

impl GetConfig<DataTestConfig> for DataTestProperties {
    fn get_config(&self) -> Option<&DataTestConfig> {
        self.config.as_ref()
    }
}

impl DataTestProperties {
    pub fn empty(model_name: String) -> Self {
        Self {
            config: None,
            description: None,
            name: model_name,
        }
    }
}
