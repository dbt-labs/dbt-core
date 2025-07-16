use crate::AdapterResult;
use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationResults,
};

use dbt_schemas::schemas::InternalDbtNodeAttributes;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CommentConfig {
    pub comment: Option<String>,
    pub persist: bool,
}

impl CommentConfig {
    pub fn new(comment: Option<String>, persist: bool) -> Self {
        Self { comment, persist }
    }

    pub fn get_diff(&self, other: &Self) -> Option<Self> {
        if self.persist && self.comment != other.comment {
            Some(self.clone())
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct CommentProcessor;

impl DatabricksComponentProcessorProperties for CommentProcessor {
    fn name(&self) -> &'static str {
        "comment"
    }
}

/// https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/relation_configs/comment.py#L23
impl DatabricksComponentProcessor for CommentProcessor {
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
        _model_node: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>> {
        let persist = false;

        Ok(Some(DatabricksComponentConfig::Comment(
            CommentConfig::new(None, persist),
        )))
    }
}
