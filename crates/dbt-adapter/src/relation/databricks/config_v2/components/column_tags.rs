//! https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/column_tags.py

use crate::relation::config_v2::{
    ComponentConfig, ComponentConfigLoader, SimpleComponentConfigImpl,
};
use crate::relation::databricks::config_v2::{
    DatabricksRelationMetadata, DatabricksRelationMetadataKey,
};
use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_serde_yaml::Value as YmlValue;
use indexmap::IndexMap;
use minijinja::value::{Value, ValueMap};

pub(crate) const TYPE_NAME: &str = "column_tags";

/// Component for Databricks column tags
///
/// Holds a IndexMap of column name to column tags.
pub type ColumnTags = SimpleComponentConfigImpl<IndexMap<String, IndexMap<String, String>>>;

fn to_jinja(v: &IndexMap<String, IndexMap<String, String>>) -> Value {
    Value::from(ValueMap::from([(
        Value::from("tags"),
        Value::from_serialize(v),
    )]))
}

fn new(tags: IndexMap<String, IndexMap<String, String>>) -> ColumnTags {
    ColumnTags {
        type_name: TYPE_NAME,
        diff_fn: merge_tags_diff,
        to_jinja_fn: to_jinja,
        value: tags,
    }
}

fn merge_tags_diff(
    desired_state: &IndexMap<String, IndexMap<String, String>>,
    current_state: &IndexMap<String, IndexMap<String, String>>,
) -> Option<IndexMap<String, IndexMap<String, String>>> {
    let mut merged = current_state.clone();

    for (column_name, column_tag_map) in desired_state {
        let column_entry = merged.entry(column_name.clone()).or_default();
        for (tag_name, tag_value) in column_tag_map {
            column_entry.insert(tag_name.clone(), tag_value.clone());
        }
    }

    if &merged != current_state {
        Some(merged)
    } else {
        None
    }
}

fn from_remote_state(results: &DatabricksRelationMetadata) -> ColumnTags {
    let mut column_tags: IndexMap<String, IndexMap<String, String>> = IndexMap::new();
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

    new(column_tags)
}

fn from_local_config(relation_config: &dyn InternalDbtNodeAttributes) -> ColumnTags {
    let mut column_tags = IndexMap::new();

    if let Some(model) = relation_config.as_any().downcast_ref::<DbtModel>() {
        for column in &model.__base_attr__.columns {
            if let Some(column_databricks_tags) = &column.databricks_tags {
                let mut column_tag_map = IndexMap::new();
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
    }

    new(column_tags)
}

pub(crate) struct ColumnTagsLoader;

impl ColumnTagsLoader {
    pub fn new(tags: IndexMap<String, IndexMap<String, String>>) -> Box<dyn ComponentConfig> {
        Box::new(new(tags))
    }

    pub fn type_name() -> &'static str {
        TYPE_NAME
    }
}

impl ComponentConfigLoader<DatabricksRelationMetadata> for ColumnTagsLoader {
    fn type_name(&self) -> &'static str {
        TYPE_NAME
    }

    fn from_remote_state(
        &self,
        remote_state: &DatabricksRelationMetadata,
    ) -> Box<dyn ComponentConfig> {
        Box::new(from_remote_state(remote_state))
    }

    fn from_local_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> Box<dyn ComponentConfig> {
        Box::new(from_local_config(relation_config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_diff_column_tags() {
        let mut old_column_tags = IndexMap::new();
        let mut old_col1_tags = IndexMap::new();
        old_col1_tags.insert("old_tag".to_string(), "old_value".to_string());
        old_column_tags.insert("col1".to_string(), old_col1_tags);

        let mut new_column_tags = IndexMap::new();
        let mut new_col1_tags = IndexMap::new();
        new_col1_tags.insert("new_tag".to_string(), "new_value".to_string());
        new_column_tags.insert("col1".to_string(), new_col1_tags);

        let mut new_col2_tags = IndexMap::new();
        new_col2_tags.insert("col2_tag".to_string(), "col2_value".to_string());
        new_column_tags.insert("col2".to_string(), new_col2_tags);

        let diff = merge_tags_diff(&new_column_tags, &old_column_tags).unwrap();

        let col1_tags = diff.get("col1").unwrap();
        assert_eq!(col1_tags.get("old_tag"), Some(&"old_value".to_string()));
        assert_eq!(col1_tags.get("new_tag"), Some(&"new_value".to_string()));

        let col2_tags = diff.get("col2").unwrap();
        assert_eq!(col2_tags.get("col2_tag"), Some(&"col2_value".to_string()));
    }

    #[test]
    fn test_get_diff_no_change() {
        let mut column_tags = IndexMap::new();
        let mut col_tags = IndexMap::new();
        col_tags.insert("tag1".to_string(), "value1".to_string());
        column_tags.insert("col1".to_string(), col_tags);

        let diff = merge_tags_diff(&column_tags, &column_tags);
        assert!(diff.is_none());
    }
}
