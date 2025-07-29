use dbt_serde_yaml::{JsonSchema, Spanned, UntaggedEnumDeserialize, Verbatim};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use crate::schemas::project::DataTestConfig;

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum DataTests {
    String(String),
    UniqueTest(UniqueTest),
    NotNullTest(NotNullTest),
    RelationshipsTest(RelationshipsTest),
    AcceptedValuesTest(AcceptedValuesTest),
    CustomTest(CustomTest),
}
#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct UniqueTest {
    pub unique: UniqueTestProperties,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct UniqueTestProperties {
    pub name: Option<String>,
    pub description: Option<String>,
    pub config: Option<DataTestConfig>,
    #[serde(flatten)]
    pub deprecated_configs: Spanned<Option<BTreeMap<String, dbt_serde_yaml::Value>>>,
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
