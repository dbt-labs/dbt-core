use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
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
    pub unique: serde_json::Value,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct NotNullTest {
    pub not_null: serde_json::Value,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct RelationshipsTest {
    pub relationships: serde_json::Value,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct AcceptedValuesTest {
    pub accepted_values: serde_json::Value,
}
