use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationResults, get_config_value,
};

use crate::{
    AdapterResult,
    errors::{AdapterError, AdapterErrorKind},
};
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::serde::yml_value_to_string;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TagsConfig {
    pub set_tags: BTreeMap<String, String>,
    pub unset_tags: Vec<String>,
}

impl TagsConfig {
    pub fn new(set_tags: BTreeMap<String, String>, unset_tags: Vec<String>) -> Self {
        Self {
            set_tags,
            unset_tags,
        }
    }

    pub fn get_diff(&self, other: &Self) -> Option<Self> {
        let mut to_unset = Vec::new();
        for k in other.set_tags.keys() {
            if !self.set_tags.contains_key(k) {
                to_unset.push(k.clone());
            }
        }

        if self.set_tags != other.set_tags || !to_unset.is_empty() {
            Some(Self::new(self.set_tags.clone(), to_unset))
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct TagsProcessor;

impl DatabricksComponentProcessorProperties for TagsProcessor {
    fn name(&self) -> &'static str {
        "tags"
    }
}

/// https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/relation_configs/tags.py#L30
impl DatabricksComponentProcessor for TagsProcessor {
    fn from_relation_results(
        &self,
        _row: &DatabricksRelationResults,
    ) -> Option<DatabricksComponentConfig> {
        // TODO: implement
        None
    }

    fn from_relation_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>> {
        // todo: databricks_tags vs dbt tags
        let tags = get_config_value(relation_config, "databricks_tags");
        if tags.is_none() {
            return Ok(Some(DatabricksComponentConfig::Tags(TagsConfig::default())));
        }

        let tags = tags
            .unwrap()
            .as_mapping()
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::Configuration,
                    "databricks_tags must be an object".to_string(),
                )
            })?
            .iter()
            .map(|(k, v)| {
                let key = yml_value_to_string(k).ok_or_else(|| {
                    AdapterError::new(
                        AdapterErrorKind::Configuration,
                        format!("databricks_tags keys must be strings, got {k:?}"),
                    )
                })?;
                let value = yml_value_to_string(v).ok_or_else(|| {
                    AdapterError::new(
                        AdapterErrorKind::Configuration,
                        format!("databricks_tags values must be strings, numbers, or booleans, got {v:?}"),
                    )
                })?;
                Ok((key, value))
            })
            .collect::<AdapterResult<BTreeMap<String, String>>>()?;

        let tags_config = TagsConfig::new(tags, Vec::new());
        Ok(Some(DatabricksComponentConfig::Tags(tags_config)))
    }
}
