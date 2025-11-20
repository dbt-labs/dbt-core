use crate::AdapterResult;
use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationMetadataKey,
    DatabricksRelationResults,
};
use crate::errors::{AdapterError, AdapterErrorKind};

use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_serde_yaml::Value as YmlValue;
use minijinja::Value;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ColumnTagsConfig {
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl ColumnTagsConfig {
    pub fn new(tags: BTreeMap<String, BTreeMap<String, String>>) -> Self {
        Self { tags }
    }

    pub fn get_diff(&self, other: &Self) -> Option<Self> {
        let mut merged_column_tags = other.tags.clone();

        for (column_name, column_tag_map) in &self.tags {
            let column_entry = merged_column_tags.entry(column_name.clone()).or_default();
            for (tag_name, tag_value) in column_tag_map {
                column_entry.insert(tag_name.clone(), tag_value.clone());
            }
        }

        if merged_column_tags != other.tags {
            Some(Self::new(merged_column_tags))
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct ColumnTagsProcessor;

impl DatabricksComponentProcessorProperties for ColumnTagsProcessor {
    fn name(&self) -> &'static str {
        "column_tags"
    }
}

impl DatabricksComponentProcessor for ColumnTagsProcessor {
    fn from_relation_results(
        &self,
        results: &DatabricksRelationResults,
    ) -> Option<DatabricksComponentConfig> {
        let mut column_tags: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        if let Some(column_tags_table) =
            results.get(&DatabricksRelationMetadataKey::InfoSchemaColumnTags)
        {
            for row in column_tags_table.rows() {
                if let (Ok(column_name_val), Ok(tag_name_val), Ok(tag_value_val)) = (
                    row.get_item(&Value::from(0)),
                    row.get_item(&Value::from(1)),
                    row.get_item(&Value::from(2)),
                ) && let (Some(column_name), Some(tag_name), Some(tag_value)) = (
                    column_name_val.as_str(),
                    tag_name_val.as_str(),
                    tag_value_val.as_str(),
                ) {
                    column_tags
                        .entry(column_name.to_string())
                        .or_default()
                        .insert(tag_name.to_string(), tag_value.to_string());
                }
            }
        }

        Some(DatabricksComponentConfig::ColumnTags(
            ColumnTagsConfig::new(column_tags),
        ))
    }

    fn from_relation_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>> {
        let mut column_tags = BTreeMap::new();

        if let Some(model) = relation_config.as_any().downcast_ref::<DbtModel>() {
            for column in &model.__base_attr__.columns {
                if let Some(column_databricks_tags) = &column.databricks_tags {
                    let mut column_tag_map = BTreeMap::new();
                    for (tag_name, tag_value) in column_databricks_tags {
                        if let YmlValue::String(value_str, _) = tag_value {
                            column_tag_map.insert(tag_name.clone(), value_str.clone());
                        }
                    }
                    if !column_tag_map.is_empty() {
                        column_tags.insert(column.name.clone(), column_tag_map);
                    }
                }
            }
        } else {
            return Err(AdapterError::new(
                AdapterErrorKind::Configuration,
                "Column tags are only supported for DbtModel",
            ));
        }

        Ok(Some(DatabricksComponentConfig::ColumnTags(
            ColumnTagsConfig::new(column_tags),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_processor_name() {
        let processor = ColumnTagsProcessor;
        assert_eq!(processor.name(), "column_tags");
    }

    #[test]
    fn test_get_diff_column_tags() {
        let mut old_column_tags = BTreeMap::new();
        let mut old_col1_tags = BTreeMap::new();
        old_col1_tags.insert("old_tag".to_string(), "old_value".to_string());
        old_column_tags.insert("col1".to_string(), old_col1_tags);

        let mut new_column_tags = BTreeMap::new();
        let mut new_col1_tags = BTreeMap::new();
        new_col1_tags.insert("new_tag".to_string(), "new_value".to_string());
        new_column_tags.insert("col1".to_string(), new_col1_tags);

        let mut new_col2_tags = BTreeMap::new();
        new_col2_tags.insert("col2_tag".to_string(), "col2_value".to_string());
        new_column_tags.insert("col2".to_string(), new_col2_tags);

        let old_config = ColumnTagsConfig::new(old_column_tags);
        let new_config = ColumnTagsConfig::new(new_column_tags);

        let diff = new_config.get_diff(&old_config);
        assert!(diff.is_some());

        let diff_config = diff.as_ref().unwrap();

        let col1_tags = diff_config.tags.get("col1").unwrap();
        assert_eq!(col1_tags.get("old_tag"), Some(&"old_value".to_string()));
        assert_eq!(col1_tags.get("new_tag"), Some(&"new_value".to_string()));

        let col2_tags = diff_config.tags.get("col2").unwrap();
        assert_eq!(col2_tags.get("col2_tag"), Some(&"col2_value".to_string()));
    }

    #[test]
    fn test_get_diff_no_change() {
        let mut column_tags = BTreeMap::new();
        let mut col_tags = BTreeMap::new();
        col_tags.insert("tag1".to_string(), "value1".to_string());
        column_tags.insert("col1".to_string(), col_tags);

        let config1 = ColumnTagsConfig::new(column_tags.clone());
        let config2 = ColumnTagsConfig::new(column_tags);

        let diff = config1.get_diff(&config2);
        assert!(diff.is_none());
    }
}
