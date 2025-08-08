use dbt_serde_yaml::{JsonSchema, UntaggedEnumDeserialize, Verbatim};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use crate::schemas::project::DataTestConfig;

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum DataTests {
    String(String),
    CustomTest(CustomTest),
}

#[derive(Debug, Clone, UntaggedEnumDeserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum CustomTest {
    MultiKey(Box<CustomTestMultiKey>),
    SimpleKeyValue(BTreeMap<String, CustomTestInner>),
}

#[skip_serializing_none]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct CustomTestInner {
    pub name: Option<String>,
    pub description: Option<String>,
    pub config: Option<DataTestConfig>,
    pub column_name: Option<String>,
    pub arguments: Verbatim<Option<dbt_serde_yaml::Value>>,
    #[serde(flatten)]
    pub deprecated_args_and_configs: Verbatim<BTreeMap<String, dbt_serde_yaml::Value>>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct CustomTestMultiKey {
    pub test_name: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub config: Option<DataTestConfig>,
    pub column_name: Option<String>,
    pub arguments: Verbatim<Option<dbt_serde_yaml::Value>>,
    #[serde(flatten)]
    pub deprecated_args_and_configs: Verbatim<BTreeMap<String, dbt_serde_yaml::Value>>,
}

// Helper to extract column name from DataTests
impl DataTests {
    pub fn column_name(&self) -> Option<&str> {
        match self {
            DataTests::String(_) => None,
            DataTests::CustomTest(test) => match test {
                CustomTest::MultiKey(test) => test.column_name.as_deref(),
                CustomTest::SimpleKeyValue(test) => {
                    test.values().next().and_then(|v| v.column_name.as_deref())
                }
            },
        }
    }
}
