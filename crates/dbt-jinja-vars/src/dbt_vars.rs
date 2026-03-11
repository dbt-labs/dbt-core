use std::collections::BTreeMap;

use dbt_yaml::UntaggedEnumDeserialize;
use serde::Serialize;

/// Variable values in a dbt project.
#[derive(Clone, Debug, Serialize, UntaggedEnumDeserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum DbtVars {
    Vars(BTreeMap<String, DbtVars>),
    Value(dbt_yaml::Value),
}
