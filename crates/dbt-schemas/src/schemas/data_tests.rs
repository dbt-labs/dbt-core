use dbt_serde_yaml::{JsonSchema, Spanned, UntaggedEnumDeserialize, Verbatim};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use crate::schemas::project::DataTestConfig;

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum ColumnDataTests {
    String(String),
    ColumnUniqueTest(ColumnUniqueTest),
    NotNullTest(NotNullTest),
    RelationshipsTest(RelationshipsTest),
    AcceptedValuesTest(AcceptedValuesTest),
    CustomTest(CustomTest),
}

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum ModelDataTests {
    String(String),
    ModelUniqueTest(ModelUniqueTest),
    ModelNotNullTest(ModelNotNullTest),
    ModelRelationshipsTest(ModelRelationshipsTest),
    ModelAcceptedValuesTest(ModelAcceptedValuesTest),
    ModelCustomTest(CustomTest),
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ColumnUniqueTest {
    pub unique: ColumnUniqueTestProperties,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ColumnUniqueTestProperties {
    pub name: Option<String>,
    pub description: Option<String>,
    pub config: Option<DataTestConfig>,
    #[serde(flatten)]
    pub deprecated_configs: Spanned<Option<BTreeMap<String, dbt_serde_yaml::Value>>>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ModelUniqueTest {
    pub unique: ModelUniqueTestProperties,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ModelUniqueTestProperties {
    pub column_name: String,
    #[serde(flatten)]
    pub __inner__: ColumnUniqueTestProperties,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ModelNotNullTest {
    pub not_null: ModelNotNullTestProperties,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ModelNotNullTestProperties {
    pub column_name: String,
    #[serde(flatten)]
    pub __inner__: NotNullTestProperties,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct NotNullTest {
    pub not_null: NotNullTestProperties,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct NotNullTestProperties {
    pub name: Option<String>,
    pub description: Option<String>,
    pub config: Option<DataTestConfig>,
    pub column: Option<String>,
    #[serde(flatten)]
    pub deprecated_configs: Spanned<Option<BTreeMap<String, dbt_serde_yaml::Value>>>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct RelationshipsTest {
    pub relationships: RelationshipsTestProperties,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ModelRelationshipsTest {
    pub relationships: ModelRelationshipsTestProperties,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ModelRelationshipsTestProperties {
    pub column_name: String,
    #[serde(flatten)]
    pub __inner__: RelationshipsTestProperties,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct RelationshipsTestProperties {
    pub name: Option<String>,
    pub description: Option<String>,
    pub config: Option<DataTestConfig>,
    pub field: String,
    pub to: Verbatim<String>,
    #[serde(flatten)]
    pub deprecated_configs: Spanned<Option<BTreeMap<String, dbt_serde_yaml::Value>>>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct AcceptedValuesTest {
    pub accepted_values: AcceptedValuesTestProperties,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ModelAcceptedValuesTest {
    pub accepted_values: ModelAcceptedValuesTestProperties,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ModelAcceptedValuesTestProperties {
    pub column_name: String,
    #[serde(flatten)]
    pub __inner__: AcceptedValuesTestProperties,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct AcceptedValuesTestProperties {
    pub name: Option<String>,
    pub description: Option<String>,
    pub config: Option<DataTestConfig>,
    pub values: Vec<serde_json::Value>,
    pub quote: Option<bool>,
    #[serde(flatten)]
    pub deprecated_configs: Spanned<Option<BTreeMap<String, dbt_serde_yaml::Value>>>,
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
    pub arguments: Verbatim<Option<dbt_serde_yaml::Value>>,
    #[serde(flatten)]
    pub deprecated_args_and_configs:
        Verbatim<Option<Spanned<BTreeMap<String, dbt_serde_yaml::Value>>>>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct CustomTestMultiKey {
    pub test_name: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub config: Option<DataTestConfig>,
    pub arguments: Verbatim<Option<dbt_serde_yaml::Value>>,
    #[serde(flatten)]
    pub deprecated_args_and_configs:
        Verbatim<Option<Spanned<BTreeMap<String, dbt_serde_yaml::Value>>>>,
}

// Into implementation to convert ModelDataTests to ColumnDataTests
impl From<ModelDataTests> for ColumnDataTests {
    fn from(model_test: ModelDataTests) -> Self {
        match model_test {
            ModelDataTests::String(s) => ColumnDataTests::String(s),
            ModelDataTests::ModelUniqueTest(test) => {
                ColumnDataTests::ColumnUniqueTest(ColumnUniqueTest {
                    unique: test.unique.__inner__,
                })
            }
            ModelDataTests::ModelNotNullTest(test) => ColumnDataTests::NotNullTest(NotNullTest {
                not_null: test.not_null.__inner__,
            }),
            ModelDataTests::ModelRelationshipsTest(test) => {
                ColumnDataTests::RelationshipsTest(RelationshipsTest {
                    relationships: test.relationships.__inner__,
                })
            }
            ModelDataTests::ModelAcceptedValuesTest(test) => {
                ColumnDataTests::AcceptedValuesTest(AcceptedValuesTest {
                    accepted_values: test.accepted_values.__inner__,
                })
            }
            ModelDataTests::ModelCustomTest(test) => ColumnDataTests::CustomTest(test),
        }
    }
}

// Helper to extract column name from ModelDataTests
impl ModelDataTests {
    pub fn column_name(&self) -> Option<&str> {
        match self {
            ModelDataTests::String(_) => None,
            ModelDataTests::ModelUniqueTest(test) => Some(&test.unique.column_name),
            ModelDataTests::ModelNotNullTest(test) => Some(&test.not_null.column_name),
            ModelDataTests::ModelRelationshipsTest(test) => Some(&test.relationships.column_name),
            ModelDataTests::ModelAcceptedValuesTest(test) => {
                Some(&test.accepted_values.column_name)
            }
            ModelDataTests::ModelCustomTest(_) => None,
        }
    }
}
