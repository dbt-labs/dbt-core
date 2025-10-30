use crate::schemas::data_tests::DataTests;
use crate::schemas::project::FunctionConfig;
use dbt_serde_yaml::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

/// Function kind enum with same values as UDFKind
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "lowercase")]
pub enum FunctionKind {
    #[serde(rename = "scalar")]
    #[default]
    Scalar,
    #[serde(rename = "aggregate")]
    Aggregate,
    #[serde(rename = "table")]
    Table,
}

/// Function volatility enum - defines the function's eligibility for certain optimizations
/// Matches the Python Volatility enum from dbt-core
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Volatility {
    /// Deterministic - An deterministic function will always return the same output when given the same input.
    #[serde(rename = "deterministic")]
    Deterministic,
    /// NonDeterministic - A non-deterministic function may change the return value from evaluation to evaluation.
    /// Multiple invocations of a non-deterministic function may return different results when used in the same query.
    #[serde(rename = "non-deterministic")]
    NonDeterministic,
    #[serde(rename = "stable")]
    Stable,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct FunctionArgument {
    pub name: Option<String>,
    pub data_type: Option<String>,
    pub description: Option<String>,
    pub default_value: Option<String>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct FunctionReturnType {
    pub data_type: Option<String>,
    pub description: Option<String>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct FunctionProperties {
    pub config: Option<FunctionConfig>,
    pub data_tests: Option<Vec<DataTests>>,
    pub description: Option<String>,
    pub identifier: Option<String>,
    pub name: String,
    pub tests: Option<Vec<DataTests>>,
    pub language: Option<String>,
    pub returns: Option<FunctionReturnType>,
    pub arguments: Option<Vec<FunctionArgument>>,
    #[serde(rename = "type")]
    pub function_kind: Option<FunctionKind>,
}

impl FunctionProperties {
    pub fn empty(name: String) -> Self {
        Self {
            name,
            config: None,
            data_tests: None,
            description: None,
            identifier: None,
            tests: None,
            language: None,
            returns: None,
            arguments: None,
            function_kind: None,
        }
    }
}

impl crate::schemas::properties::properties::GetConfig<FunctionConfig> for FunctionProperties {
    fn get_config(&self) -> Option<&FunctionConfig> {
        self.config.as_ref()
    }
}

impl crate::schemas::properties::properties::GetConfig<crate::schemas::project::ModelConfig>
    for FunctionProperties
{
    fn get_config(&self) -> Option<&crate::schemas::project::ModelConfig> {
        // Functions don't have ModelConfig, return None
        None
    }
}
