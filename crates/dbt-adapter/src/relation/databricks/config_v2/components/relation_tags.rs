//! https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/tags.py

use crate::relation::config_v2::{
    ComponentConfig, ComponentConfigLoader, SimpleComponentConfigImpl, diff,
};
use crate::relation::databricks::config_v2::{
    DatabricksRelationMetadata, DatabricksRelationMetadataKey,
};
use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_serde_yaml::Value as YmlValue;
use indexmap::IndexMap;
use minijinja::value::{Value, ValueMap};

pub(crate) const TYPE_NAME: &str = "tags";

// TODO(serramatutu): reuse this for `tags` and `labels` in other warehouses
/// Component for Databricks tags
///
/// Holds a IndexMap of tag key and values.
pub type RelationTags = SimpleComponentConfigImpl<IndexMap<String, String>>;

fn to_jinja(v: &IndexMap<String, String>) -> Value {
    Value::from(ValueMap::from([(
        Value::from("set_tags"),
        Value::from_serialize(v),
    )]))
}

fn new(tags: IndexMap<String, String>) -> RelationTags {
    RelationTags {
        type_name: TYPE_NAME,
        diff_fn: diff::desired_state,
        to_jinja_fn: to_jinja,
        value: tags,
    }
}

fn from_remote_state(results: &DatabricksRelationMetadata) -> RelationTags {
    let Some(remote_tags) = results.get(&DatabricksRelationMetadataKey::InfoSchemaRelationTags)
    else {
        return new(IndexMap::new());
    };

    let mut tags = IndexMap::new();

    for row in remote_tags.rows() {
        if let (Ok(tag_name_val), Ok(tag_value_val)) =
            (row.get_item(&Value::from(0)), row.get_item(&Value::from(1)))
            && let (Some(tag_name), Some(tag_value)) =
                (tag_name_val.as_str(), tag_value_val.as_str())
        {
            tags.insert(tag_name.to_string(), tag_value.to_string());
        }
    }

    new(tags)
}

fn from_local_config(relation_config: &dyn InternalDbtNodeAttributes) -> RelationTags {
    let Some(model) = relation_config.as_any().downcast_ref::<DbtModel>() else {
        return new(IndexMap::new());
    };

    let mut tags = IndexMap::new();

    if let Some(databricks_attr) = &model.__adapter_attr__.databricks_attr
        && let Some(tags_map) = &databricks_attr.databricks_tags
    {
        for (key, value) in tags_map {
            if let YmlValue::String(value_str, _) = value {
                tags.insert(key.clone(), value_str.clone());
            }
        }
    }

    new(tags)
}

pub(crate) struct RelationTagsLoader;

impl RelationTagsLoader {
    pub fn new(tags: IndexMap<String, String>) -> Box<dyn ComponentConfig> {
        Box::new(new(tags))
    }

    pub fn type_name() -> &'static str {
        TYPE_NAME
    }
}

impl ComponentConfigLoader<DatabricksRelationMetadata> for RelationTagsLoader {
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
    use crate::relation::config_v2::ComponentConfig;

    #[test]
    fn test_get_diff_add_or_update() {
        let mut old_tags = IndexMap::new();
        old_tags.insert("a".to_string(), "1".to_string());
        old_tags.insert("b".to_string(), "2".to_string());

        let mut new_tags = IndexMap::new();
        new_tags.insert("b".to_string(), "3".to_string());
        new_tags.insert("c".to_string(), "4".to_string());

        let old_config = new(old_tags);
        let new_config = new(new_tags);

        let diff = RelationTags::diff_from(&new_config, Some(&old_config)).unwrap();
        let diff = diff.as_any().downcast_ref::<RelationTags>().unwrap();

        assert_eq!(diff.value.get("b"), Some(&"3".to_string()));
        assert_eq!(diff.value.get("c"), Some(&"4".to_string()));
    }

    #[test]
    fn test_get_diff_no_change() {
        let mut tags = IndexMap::new();
        tags.insert("a".to_string(), "1".to_string());
        tags.insert("b".to_string(), "2".to_string());

        let config = new(tags);
        let diff = RelationTags::diff_from(&config, Some(&config));

        assert!(diff.is_none());
    }
}
