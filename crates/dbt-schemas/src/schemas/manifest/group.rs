use std::{collections::BTreeMap, path::PathBuf};

use dbt_serde_yaml::Value;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use super::common::DbtOwner;

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct DbtGroup {
    pub name: String,
    pub package_name: String,
    pub path: PathBuf,
    pub original_file_path: PathBuf,
    pub unique_id: String,
    pub owner: DbtOwner,
    #[serde(flatten)]
    pub other: BTreeMap<String, Value>,
}
