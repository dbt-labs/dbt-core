//! https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/tags.py

use crate::errors::AdapterResult;
use crate::relation::config_v2::{
    ComponentConfig, ComponentConfigLoader, RelationConfig, SimpleComponentConfigImpl, impl_loader,
};
use crate::relation::databricks::config::{
    DatabricksRelationMetadata, DatabricksRelationMetadataKey,
};
use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_yaml::Value as YmlValue;
use indexmap::IndexMap;
use minijinja::value::{Value, ValueMap};

pub(crate) const TYPE_NAME: &str = "tags";

// TODO(serramatutu): reuse this for `tags` and `labels` in other warehouses
/// Component for Databricks tags.
pub type RelationTags = SimpleComponentConfigImpl<IndexMap<String, String>>;

fn to_jinja(v: &IndexMap<String, String>) -> Value {
    Value::from(ValueMap::from([(
        Value::from("set_tags"),
        Value::from_serialize(v),
    )]))
}

fn new_component(tags: IndexMap<String, String>) -> RelationTags {
    RelationTags {
        type_name: TYPE_NAME,
        diff_fn: set_only_diff,
        to_jinja_fn: to_jinja,
        value: tags,
    }
}

fn set_only_diff(
    desired_state: &IndexMap<String, String>,
    current_state: &IndexMap<String, String>,
) -> Option<IndexMap<String, String>> {
    desired_state
        .iter()
        .any(|(key, value)| current_state.get(key) != Some(value))
        .then(|| desired_state.clone())
}

fn from_remote_state(results: &DatabricksRelationMetadata) -> AdapterResult<RelationTags> {
    let Some(remote_tags) = results.get(&DatabricksRelationMetadataKey::InfoSchemaRelationTags)
    else {
        return Ok(new_component(IndexMap::new()));
    };

    let mut tags = IndexMap::new();

    for row in remote_tags.rows() {
        if let (Ok(tag_name_val), Ok(tag_value_val)) =
            (row.get_item(&Value::from(0)), row.get_item(&Value::from(1)))
            && let Some(tag_name) = tag_name_val.as_str()
        {
            let tag_value = if tag_value_val.is_none() {
                ""
            } else if let Some(tag_value) = tag_value_val.as_str() {
                tag_value
            } else {
                continue;
            };
            tags.insert(tag_name.to_string(), tag_value.to_string());
        }
    }

    Ok(new_component(tags))
}

fn python_string_repr(value: &str) -> String {
    let quote = if value.contains('\'') && !value.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut result = String::with_capacity(value.len() + 2);
    result.push(quote);
    for character in value.chars() {
        match character {
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            '\u{0008}' => result.push_str("\\b"),
            '\u{000c}' => result.push_str("\\f"),
            character if character == quote => {
                result.push('\\');
                result.push(character);
            }
            character => result.push(character),
        }
    }
    result.push(quote);
    result
}

fn yml_value_to_python_repr(value: &YmlValue) -> String {
    match value {
        YmlValue::Null(_) => "None".to_string(),
        YmlValue::Bool(value, _) => (if *value { "True" } else { "False" }).to_string(),
        YmlValue::Number(value, _) => value.to_string(),
        YmlValue::String(value, _) => python_string_repr(value),
        YmlValue::Tagged(tagged, _) => yml_value_to_python_repr(&tagged.value),
        YmlValue::Sequence(values, _) => format!(
            "[{}]",
            values
                .iter()
                .map(yml_value_to_python_repr)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        YmlValue::Mapping(values, _) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(key, value)| format!(
                    "{}: {}",
                    yml_value_to_python_repr(key),
                    yml_value_to_python_repr(value)
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn yml_value_to_python_string(value: &YmlValue) -> String {
    match value {
        YmlValue::Null(_) => String::new(),
        YmlValue::String(value, _) => value.clone(),
        YmlValue::Tagged(tagged, _) => yml_value_to_python_string(&tagged.value),
        _ => yml_value_to_python_repr(value),
    }
}

fn from_local_config(
    relation_config: &dyn InternalDbtNodeAttributes,
) -> AdapterResult<RelationTags> {
    let Some(model) = relation_config.as_any().downcast_ref::<DbtModel>() else {
        return Ok(new_component(IndexMap::new()));
    };

    let mut tags = IndexMap::new();

    if let Some(databricks_attr) = &model.__adapter_attr__.databricks_attr
        && let Some(tags_map) = &databricks_attr.databricks_tags
    {
        for (key, value) in tags_map {
            tags.insert(key.clone(), yml_value_to_python_string(value));
        }
    }

    Ok(new_component(tags))
}

impl_loader!(RelationTags, DatabricksRelationMetadata);

impl RelationTagsLoader {
    pub fn new_component_type_erased(tags: IndexMap<String, String>) -> Box<dyn ComponentConfig> {
        Box::new(new_component(tags))
    }
}

pub(crate) fn requires_server_metadata_for_diff(config: &RelationConfig) -> bool {
    config
        .get(TYPE_NAME)
        .and_then(|component| component.as_any().downcast_ref::<RelationTags>())
        .is_none_or(|tags| !tags.value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AdapterType;
    use crate::relation::config_v2::{ComponentConfig, ComponentConfigChange};
    use crate::relation::databricks::config::test_helpers::{
        TestModelConfig, create_mock_dbt_model,
    };

    #[test]
    fn test_get_diff_add_or_update() {
        let mut old_tags = IndexMap::new();
        old_tags.insert("a".to_string(), "1".to_string());
        old_tags.insert("b".to_string(), "2".to_string());

        let mut new_tags = IndexMap::new();
        new_tags.insert("b".to_string(), "3".to_string());
        new_tags.insert("c".to_string(), "4".to_string());

        let old_config = new_component(old_tags);
        let new_config = new_component(new_tags);

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

        let config = new_component(tags);
        let diff = RelationTags::diff_from(&config, Some(&config));

        assert!(diff.is_none());
    }

    #[test]
    fn test_get_diff_configured_removal_is_no_change() {
        let current = new_component(IndexMap::from_iter([(
            "deployment".to_string(),
            "NEXT".to_string(),
        )]));
        let desired = new_component(IndexMap::new());

        assert!(RelationTags::diff_from(&desired, Some(&current)).is_none());
    }

    fn never_full_refresh(_: &IndexMap<&'static str, ComponentConfigChange>) -> bool {
        false
    }

    #[test]
    fn test_server_metadata_requirement_follows_local_tags() {
        let empty = RelationConfig::new(
            AdapterType::Databricks,
            [Box::new(new_component(IndexMap::new())) as Box<dyn ComponentConfig>],
            never_full_refresh,
        );
        let tagged = RelationConfig::new(
            AdapterType::Databricks,
            [Box::new(new_component(IndexMap::from_iter([(
                "deployment".to_string(),
                "DBT".to_string(),
            )]))) as Box<dyn ComponentConfig>],
            never_full_refresh,
        );
        let missing = RelationConfig::new(
            AdapterType::Databricks,
            Vec::<Box<dyn ComponentConfig>>::new(),
            never_full_refresh,
        );

        assert!(!requires_server_metadata_for_diff(&empty));
        assert!(requires_server_metadata_for_diff(&tagged));
        assert!(requires_server_metadata_for_diff(&missing));
    }

    #[test]
    fn test_local_config_stringifies_scalar_tag_values() {
        let model = create_mock_dbt_model(TestModelConfig {
            raw_tags: Some(IndexMap::from_iter([
                ("priority".to_string(), YmlValue::from(0_i64)),
                ("enabled".to_string(), YmlValue::from(false)),
            ])),
            ..Default::default()
        });

        let tags = from_local_config(&model).unwrap();

        assert_eq!(tags.value.get("priority"), Some(&"0".to_string()));
        assert_eq!(tags.value.get("enabled"), Some(&"False".to_string()));
    }

    #[test]
    fn test_local_config_stringifies_collection_tag_values_like_v1() {
        let sequence: YmlValue = dbt_yaml::from_str("[1, two, false, null]").unwrap();
        let mapping: YmlValue = dbt_yaml::from_str("{team: data, active: true}").unwrap();
        let model = create_mock_dbt_model(TestModelConfig {
            raw_tags: Some(IndexMap::from_iter([
                ("sequence".to_string(), sequence),
                ("mapping".to_string(), mapping),
            ])),
            ..Default::default()
        });

        let tags = from_local_config(&model).unwrap();

        assert_eq!(
            tags.value.get("sequence"),
            Some(&"[1, 'two', False, None]".to_string())
        );
        assert_eq!(
            tags.value.get("mapping"),
            Some(&"{'team': 'data', 'active': True}".to_string())
        );
    }
}
