// dbt/adapters/databricks/relation_configs/column_comments.py

use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationResults,
};
use crate::AdapterResult;

use dbt_schemas::schemas::InternalDbtNodeAttributes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ColumnCommentsConfig {
    pub comments: BTreeMap<String, String>,
    pub quoted: BTreeMap<String, bool>,
    pub persist: bool,
}

impl ColumnCommentsConfig {
    pub fn new(
        comments: BTreeMap<String, String>,
        quoted: BTreeMap<String, bool>,
        persist: bool,
    ) -> Self {
        Self {
            comments,
            quoted,
            persist,
        }
    }

    pub fn get_diff(&self, other: &Self) -> Option<Self> {
        let mut comments = BTreeMap::new();

        if self.persist {
            for (column_name, comment) in &self.comments {
                if Some(comment) != other.comments.get(&column_name.to_lowercase()) {
                    let formatted_name = if self.quoted.get(column_name).copied().unwrap_or(false) {
                        format!("`{column_name}`")
                    } else {
                        column_name.clone()
                    };
                    comments.insert(formatted_name, comment.clone());
                }
            }

            if !comments.is_empty() {
                return Some(Self::new(comments, self.quoted.clone(), true));
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct ColumnCommentsProcessor;

impl DatabricksComponentProcessorProperties for ColumnCommentsProcessor {
    fn name(&self) -> &'static str {
        "column_comments"
    }
}

/// https://github.com/databricks/dbt-databricks/blob/4b5dcc534c74eba55ca75976277a82b94f5531ee/dbt/adapters/databricks/relation_configs/column_comments.py#L35
impl DatabricksComponentProcessor for ColumnCommentsProcessor {
    #[allow(clippy::wrong_self_convention)]
    fn from_relation_results(
        &self,
        _row: &DatabricksRelationResults,
    ) -> Option<DatabricksComponentConfig> {
        // TODO: implement
        None
    }

    fn from_relation_config(
        &self,
        _relation_config: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>> {
        // TODO: implement
        Ok(None)
    }
}
