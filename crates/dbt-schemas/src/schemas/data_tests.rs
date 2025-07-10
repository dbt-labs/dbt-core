use dbt_serde_yaml::{JsonSchema, UntaggedEnumDeserialize, Verbatim};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::schemas::project::DataTestConfig;

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(untagged)]
pub enum DataTests {
    String(String),
    UniqueTest(UniqueTest),
    NotNullTest(NotNullTest),
    RelationshipsTest(RelationshipsTest),
    AcceptedValuesTest(AcceptedValuesTest),
    CustomTest(serde_json::Value),
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
}
