use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::schemas::nodes::DbtGroup;

use super::common::DbtOwner;

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct ManifestGroup {
    pub name: String,
    pub description: Option<String>,
    pub package_name: String,
    pub path: PathBuf,
    pub original_file_path: PathBuf,
    pub unique_id: String,
    pub owner: DbtOwner,
}

impl From<DbtGroup> for ManifestGroup {
    fn from(group: DbtGroup) -> Self {
        Self {
            name: group.__common_attr__.name,
            description: group.__common_attr__.description,
            package_name: group.__common_attr__.package_name,
            path: group.__common_attr__.path,
            original_file_path: group.__common_attr__.original_file_path,
            unique_id: group.__common_attr__.unique_id,
            owner: group.__group_attr__.owner,
        }
    }
}
