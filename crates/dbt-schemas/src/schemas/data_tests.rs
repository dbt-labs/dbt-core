use dbt_serde_yaml::{JsonSchema, Spanned, UntaggedEnumDeserialize, Verbatim};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use crate::schemas::project::DataTestConfig;

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum DataTests {
    String(Spanned<String>),
    CustomTest(Spanned<CustomTest>),
}

impl DataTests {
    pub fn span(&self) -> &dbt_serde_yaml::Span {
        match self {
            DataTests::String(spanned) => spanned.span(),
            DataTests::CustomTest(spanned) => spanned.span(),
        }
    }
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
    pub __deprecated_args_and_configs__: Verbatim<BTreeMap<String, dbt_serde_yaml::Value>>,
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
    pub __deprecated_args_and_configs__: Verbatim<BTreeMap<String, dbt_serde_yaml::Value>>,
}

// Helper to extract column name from DataTests
impl DataTests {
    pub fn column_name(&self) -> Option<&str> {
        match self {
            DataTests::String(_) => None,
            DataTests::CustomTest(test) => match test.as_ref() {
                CustomTest::MultiKey(test) => test.column_name.as_deref(),
                CustomTest::SimpleKeyValue(test) => {
                    test.values().next().and_then(|v| v.column_name.as_deref())
                }
            },
        }
    }

    pub fn test_name(&self) -> Option<&str> {
        match self {
            DataTests::String(test) => Some(test),
            DataTests::CustomTest(test) => match test.as_ref() {
                CustomTest::MultiKey(test) => test.name.as_deref(),
                CustomTest::SimpleKeyValue(test) => {
                    test.values().next().and_then(|v| v.name.as_deref())
                }
            },
        }
    }
}
