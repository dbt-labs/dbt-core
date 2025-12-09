use crate::relation::databricks::base::*;

use crate::{
    AdapterResult,
    errors::{AdapterError, AdapterErrorKind},
};
use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_serde_yaml::Value as YmlValue;
use minijinja::Value;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TagsConfig {
    pub set_tags: BTreeMap<String, String>,
}

impl TagsConfig {
    // Tags are now set only - in accordance with core behavior
    // https://github.com/databricks/dbt-databricks/issues/1069
    pub fn new(set_tags: BTreeMap<String, String>) -> Self {
        Self { set_tags }
    }

    pub fn get_diff(&self, other: &Self) -> Option<Self> {
        for (k, v) in &self.set_tags {
            if other.set_tags.get(k) != Some(v) {
                return Some(Self::new(self.set_tags.clone()));
            }
        }
        None
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
        results: &DatabricksRelationResults,
    ) -> Option<DatabricksComponentConfig> {
        let tags_table = results.get(&DatabricksRelationMetadataKey::InfoSchemaTags)?;

        let mut set_tags = BTreeMap::new();

        for row in tags_table.rows() {
            if let (Ok(tag_name_val), Ok(tag_value_val)) =
                (row.get_item(&Value::from(0)), row.get_item(&Value::from(1)))
                && let (Some(tag_name), Some(tag_value)) =
                    (tag_name_val.as_str(), tag_value_val.as_str())
            {
                set_tags.insert(tag_name.to_string(), tag_value.to_string());
            }
        }
        Some(DatabricksComponentConfig::Tags(TagsConfig::new(set_tags)))
    }

    fn from_relation_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>> {
        let mut databricks_tags = BTreeMap::new();
        if let Some(model) = relation_config.as_any().downcast_ref::<DbtModel>() {
            if let Some(databricks_attr) = &model.__adapter_attr__.databricks_attr
                && let Some(tags_map) = &databricks_attr.databricks_tags
            {
                for (key, value) in tags_map {
                    if let YmlValue::String(value_str, _) = value {
                        databricks_tags.insert(key.clone(), value_str.clone());
                    }
                }
            }
        } else {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "Tags are only supported for DbtModel",
            ));
        }

        Ok(Some(DatabricksComponentConfig::Tags(TagsConfig::new(
            databricks_tags,
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_processor_name() {
        let processor = TagsProcessor;
        assert_eq!(processor.name(), "tags");
    }

    #[test]
    fn test_get_diff_add_or_update() {
        let mut old_tags = BTreeMap::new();
        old_tags.insert("a".to_string(), "1".to_string());
        old_tags.insert("b".to_string(), "2".to_string());

        let mut new_tags = BTreeMap::new();
        new_tags.insert("b".to_string(), "3".to_string());
        new_tags.insert("c".to_string(), "4".to_string());

        let old_config = TagsConfig::new(old_tags);
        let new_config = TagsConfig::new(new_tags.clone());

        let diff = new_config.get_diff(&old_config);
        assert!(diff.is_some());

        assert_eq!(
            diff.as_ref().unwrap().set_tags.get("b"),
            Some(&"3".to_string())
        );
        assert_eq!(
            diff.as_ref().unwrap().set_tags.get("c"),
            Some(&"4".to_string())
        );
    }

    #[test]
    fn test_get_diff_no_change() {
        let mut tags = BTreeMap::new();
        tags.insert("a".to_string(), "1".to_string());
        tags.insert("b".to_string(), "2".to_string());

        let config1 = TagsConfig::new(tags.clone());
        let config2 = TagsConfig::new(tags);

        let diff = config1.get_diff(&config2);
        assert!(diff.is_none());
    }
}
