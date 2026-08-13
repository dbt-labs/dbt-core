use std::collections::BTreeMap;

use adbc_core::options::OptionValue;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};
use dbt_schemas::schemas::{DbtModel, DbtSeed, DbtSnapshot, DbtTest, DbtUnitTest};
use minijinja::State;
use serde::Deserialize;
use serde_json::Value;

const QUERY_TAG_OPTION_PREFIX: &str = "databricks.query_tag.";
const DBT_CORE_VERSION: &str = "@@dbt_core_version";
const DBT_MODEL_NAME: &str = "@@dbt_model_name";
const DBT_MATERIALIZED: &str = "@@dbt_materialized";
const DBT_DATABRICKS_VERSION: &str = "@@dbt_databricks_version";
const MAX_TAGS: usize = 20;
const MAX_VALUE_CHARS: usize = 128;
const RESERVED_KEYS: [&str; 4] = [
    DBT_CORE_VERSION,
    DBT_MODEL_NAME,
    DBT_MATERIALIZED,
    DBT_DATABRICKS_VERSION,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DatabricksQueryTags {
    tags: BTreeMap<String, String>,
}

impl DatabricksQueryTags {
    fn from_sources(
        profile_query_tags: Option<&str>,
        model_query_tags: Option<&str>,
        model_name: Option<&str>,
        materialized: Option<&str>,
    ) -> AdapterResult<Self> {
        let profile_tags = parse_user_tags(profile_query_tags, "Connection config")?;
        let model_tags = parse_user_tags(model_query_tags, "Model config")?;

        let mut tags = BTreeMap::from([(
            DBT_CORE_VERSION.to_string(),
            truncate_default(env!("CARGO_PKG_VERSION")),
        )]);
        if let Some(model_name) = model_name {
            tags.insert(DBT_MODEL_NAME.to_string(), truncate_default(model_name));
        }
        if let Some(materialized) = materialized {
            tags.insert(DBT_MATERIALIZED.to_string(), truncate_default(materialized));
        }
        tags.extend(profile_tags);
        tags.extend(model_tags);

        if tags.len() > MAX_TAGS {
            return configuration_error(format!(
                "Too many total query tags ({}). Maximum allowed is {MAX_TAGS}",
                tags.len()
            ));
        }

        Ok(Self { tags })
    }

    pub(super) fn into_statement_options(self) -> Vec<(String, OptionValue)> {
        self.tags
            .into_iter()
            .map(|(key, value)| {
                (
                    format!("{QUERY_TAG_OPTION_PREFIX}{key}"),
                    OptionValue::String(value),
                )
            })
            .collect()
    }
}

pub(super) fn query_tags_from_state(
    state: Option<&State>,
    profile_query_tags: Option<&str>,
) -> AdapterResult<DatabricksQueryTags> {
    let Some(node) = state.and_then(|state| state.lookup("model", &[])) else {
        return DatabricksQueryTags::from_sources(profile_query_tags, None, None, None);
    };
    let yaml_node = dbt_yaml::to_value(&node)
        .map_err(|error| AdapterError::new(AdapterErrorKind::Configuration, error.to_string()))?;
    query_tags_from_yaml_node(&yaml_node, profile_query_tags)
}

fn query_tags_from_yaml_node(
    yaml_node: &dbt_yaml::Value,
    profile_query_tags: Option<&str>,
) -> AdapterResult<DatabricksQueryTags> {
    if let Ok(model) = DbtModel::deserialize(yaml_node) {
        let model_query_tags = model
            .__adapter_attr__
            .databricks_attr
            .as_deref()
            .and_then(|attr| attr.query_tags.as_deref());
        return DatabricksQueryTags::from_sources(
            profile_query_tags,
            model_query_tags,
            Some(&model.__common_attr__.name),
            Some(&model.__base_attr__.materialized.to_string()),
        );
    }

    if let Ok(node) = DbtUnitTest::deserialize(yaml_node) {
        let query_tags = node
            .deprecated_config
            .__warehouse_specific_config__
            .query_tags
            .as_deref();
        return DatabricksQueryTags::from_sources(
            profile_query_tags,
            query_tags,
            Some(&node.__common_attr__.name),
            None,
        );
    }

    macro_rules! tags_for_node {
        ($node_type:ty) => {
            if let Ok(node) = <$node_type>::deserialize(yaml_node) {
                let query_tags = node
                    .deprecated_config
                    .__warehouse_specific_config__
                    .query_tags
                    .as_deref();
                return DatabricksQueryTags::from_sources(
                    profile_query_tags,
                    query_tags,
                    Some(&node.__common_attr__.name),
                    Some(&node.__base_attr__.materialized.to_string()),
                );
            }
        };
    }

    tags_for_node!(DbtTest);
    tags_for_node!(DbtSnapshot);
    tags_for_node!(DbtSeed);

    DatabricksQueryTags::from_sources(profile_query_tags, None, None, None)
}

fn parse_user_tags(
    query_tags: Option<&str>,
    source: &str,
) -> AdapterResult<BTreeMap<String, String>> {
    let Some(query_tags) = query_tags.filter(|value| !value.is_empty()) else {
        return Ok(BTreeMap::new());
    };

    let parsed: Value = serde_json::from_str(query_tags).map_err(|error| {
        AdapterError::new(
            AdapterErrorKind::Configuration,
            format!("Invalid JSON in query_tags: {error}"),
        )
    })?;
    let Value::Object(object) = parsed else {
        return configuration_error("query_tags must be a JSON object (dictionary)");
    };

    let reserved = object
        .keys()
        .filter(|key| RESERVED_KEYS.contains(&key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !reserved.is_empty() {
        return configuration_error(format!(
            "{source}: Cannot use reserved query tag keys: {}. Reserved keys are: {}",
            reserved.join(", "),
            RESERVED_KEYS.join(", ")
        ));
    }

    object
        .into_iter()
        .map(|(key, value)| {
            let Value::String(value) = value else {
                return configuration_error(format!(
                    "{source}: query_tags values must be strings for key '{key}'. Only string values are supported."
                ));
            };
            if escaped_value_len(&value) > MAX_VALUE_CHARS {
                return configuration_error(format!(
                    "{source}: Query tag values must be at most {MAX_VALUE_CHARS} characters after escaping. Key '{key}' exceeds the limit."
                ));
            }
            Ok((key, value))
        })
        .collect()
}

fn configuration_error<T>(message: impl Into<String>) -> AdapterResult<T> {
    Err(AdapterError::new(
        AdapterErrorKind::Configuration,
        message.into(),
    ))
}

fn truncate_default(value: &str) -> String {
    value.chars().take(MAX_VALUE_CHARS).collect()
}

fn escaped_value_len(value: &str) -> usize {
    value
        .chars()
        .map(|character| usize::from(matches!(character, '\\' | ',' | ':')) + 1)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::{DatabricksQueryTags, QUERY_TAG_OPTION_PREFIX, query_tags_from_yaml_node};
    use adbc_core::options::OptionValue;
    use dbt_schemas::schemas::{
        AdapterAttr, DbtModel, DbtSeed, DbtSnapshot, DbtTest, DbtUnitTest, manifest::DbtOperation,
        nodes::DatabricksAttr,
    };

    fn string_option<'a>(options: &'a [(String, OptionValue)], name: &str) -> Option<&'a str> {
        options.iter().find_map(|(option_name, value)| {
            if option_name == name
                && let OptionValue::String(value) = value
            {
                Some(value.as_str())
            } else {
                None
            }
        })
    }

    #[test]
    fn emits_driver_statement_options_with_model_precedence() {
        let options = DatabricksQueryTags::from_sources(
            Some(r#"{"team":"profile","profile_only":"yes"}"#),
            Some(r#"{"team":"model"}"#),
            Some("orders"),
            Some("incremental"),
        )
        .unwrap()
        .into_statement_options();

        assert_eq!(
            string_option(&options, &format!("{QUERY_TAG_OPTION_PREFIX}team")),
            Some("model")
        );
        assert_eq!(
            string_option(&options, &format!("{QUERY_TAG_OPTION_PREFIX}profile_only")),
            Some("yes")
        );
        assert_eq!(
            string_option(
                &options,
                &format!("{QUERY_TAG_OPTION_PREFIX}@@dbt_model_name")
            ),
            Some("orders")
        );
    }

    #[test]
    fn emits_empty_user_values() {
        let options = DatabricksQueryTags::from_sources(Some(r#"{"empty":""}"#), None, None, None)
            .unwrap()
            .into_statement_options();

        assert_eq!(
            string_option(&options, "databricks.query_tag.empty"),
            Some("")
        );
    }

    #[test]
    fn operations_emit_only_profile_and_core_tags() {
        let mut operation = DbtOperation::default();
        operation.__common_attr__.name = "on-run-start-0".to_string();
        let yaml = dbt_yaml::to_value(operation).unwrap();

        let options = query_tags_from_yaml_node(&yaml, Some(r#"{"team":"profile"}"#))
            .unwrap()
            .into_statement_options();
        let names = options
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>();

        assert!(names.contains(&"databricks.query_tag.team"));
        assert!(names.contains(&"databricks.query_tag.@@dbt_core_version"));
        assert!(!names.contains(&"databricks.query_tag.@@dbt_model_name"));
        assert!(!names.contains(&"databricks.query_tag.@@dbt_materialized"));
    }

    fn databricks_attr(query_tags: &str) -> AdapterAttr {
        AdapterAttr::default().with_databricks_attr(Some(Box::new(DatabricksAttr {
            query_tags: Some(query_tags.to_string()),
            ..Default::default()
        })))
    }

    fn assert_resource_tags(
        yaml: dbt_yaml::Value,
        resource_name: &str,
        materialized: Option<&str>,
    ) {
        let options = query_tags_from_yaml_node(&yaml, Some(r#"{"team":"profile"}"#))
            .unwrap()
            .into_statement_options();

        assert_eq!(
            string_option(&options, "databricks.query_tag.team"),
            Some("resource")
        );
        assert_eq!(
            string_option(&options, "databricks.query_tag.@@dbt_model_name"),
            Some(resource_name)
        );
        assert_eq!(
            string_option(&options, "databricks.query_tag.@@dbt_materialized"),
            materialized
        );
    }

    #[test]
    fn non_model_resources_emit_resource_query_tags() {
        let mut test = DbtTest::default();
        test.__common_attr__.name = "accepted_values_orders".to_string();
        test.deprecated_config
            .__warehouse_specific_config__
            .query_tags = Some(r#"{"team":"resource"}"#.to_string());
        let test_materialized = test.__base_attr__.materialized.to_string();
        assert_resource_tags(
            dbt_yaml::to_value(test).unwrap(),
            "accepted_values_orders",
            Some(&test_materialized),
        );

        let mut snapshot = DbtSnapshot::default();
        snapshot.__common_attr__.name = "orders_snapshot".to_string();
        snapshot
            .deprecated_config
            .__warehouse_specific_config__
            .query_tags = Some(r#"{"team":"resource"}"#.to_string());
        let snapshot_materialized = snapshot.__base_attr__.materialized.to_string();
        assert_resource_tags(
            dbt_yaml::to_value(snapshot).unwrap(),
            "orders_snapshot",
            Some(&snapshot_materialized),
        );

        let mut seed = DbtSeed::default();
        seed.__common_attr__.name = "orders_seed".to_string();
        seed.deprecated_config
            .__warehouse_specific_config__
            .query_tags = Some(r#"{"team":"resource"}"#.to_string());
        let seed_materialized = seed.__base_attr__.materialized.to_string();
        assert_resource_tags(
            dbt_yaml::to_value(seed).unwrap(),
            "orders_seed",
            Some(&seed_materialized),
        );

        let mut unit_test = DbtUnitTest::default();
        unit_test.__common_attr__.name = "orders_unit_test".to_string();
        unit_test
            .deprecated_config
            .__warehouse_specific_config__
            .query_tags = Some(r#"{"team":"resource"}"#.to_string());
        assert_resource_tags(
            dbt_yaml::to_value(unit_test).unwrap(),
            "orders_unit_test",
            None,
        );
    }

    #[test]
    fn model_state_reads_rendered_query_tags() {
        let mut model = DbtModel::default();
        model.__common_attr__.name = "orders".to_string();
        model.__adapter_attr__ = databricks_attr(r#"{"team":"model"}"#);
        let yaml = dbt_yaml::to_value(model).unwrap();

        let options = query_tags_from_yaml_node(&yaml, Some(r#"{"team":"profile"}"#))
            .unwrap()
            .into_statement_options();

        assert_eq!(
            string_option(&options, "databricks.query_tag.team"),
            Some("model")
        );
    }

    #[test]
    fn rejects_invalid_shapes_and_non_string_values() {
        let non_object =
            DatabricksQueryTags::from_sources(Some(r#"["not","an","object"]"#), None, None, None)
                .unwrap_err();
        assert!(non_object.message().contains("must be a JSON object"));

        let non_string =
            DatabricksQueryTags::from_sources(Some(r#"{"cost_center":3000}"#), None, None, None)
                .unwrap_err();
        assert!(non_string.message().contains("values must be strings"));
    }

    #[test]
    fn rejects_reserved_user_keys_from_both_sources() {
        for source in [
            r#"{"@@dbt_core_version":"override"}"#,
            r#"{"@@dbt_model_name":"override"}"#,
            r#"{"@@dbt_materialized":"override"}"#,
            r#"{"@@dbt_databricks_version":"override"}"#,
        ] {
            let profile_error =
                DatabricksQueryTags::from_sources(Some(source), None, None, None).unwrap_err();
            assert!(profile_error.message().contains("reserved query tag keys"));

            let model_error =
                DatabricksQueryTags::from_sources(None, Some(source), None, None).unwrap_err();
            assert!(model_error.message().contains("reserved query tag keys"));
        }
    }

    #[test]
    fn validates_escaped_value_length_and_total_tag_count() {
        let long_after_escaping = format!(r#"{{"value":"{}"}}"#, ",".repeat(65));
        let length_error =
            DatabricksQueryTags::from_sources(Some(&long_after_escaping), None, None, None)
                .unwrap_err();
        assert!(length_error.message().contains("at most 128 characters"));

        let too_many = serde_json::to_string(
            &(0..20)
                .map(|index| (format!("tag_{index}"), "value"))
                .collect::<std::collections::BTreeMap<_, _>>(),
        )
        .unwrap();
        let count_error =
            DatabricksQueryTags::from_sources(Some(&too_many), None, None, None).unwrap_err();
        assert!(
            count_error
                .message()
                .contains("Too many total query tags (21)")
        );
    }

    #[test]
    fn passes_raw_values_to_the_driver_and_truncates_automatic_values() {
        let model_name = format!("{}::", "x".repeat(127));
        let options = DatabricksQueryTags::from_sources(
            Some(r#"{"path":"folder\\name,a:b"}"#),
            None,
            Some(&model_name),
            Some("table"),
        )
        .unwrap()
        .into_statement_options();

        assert_eq!(
            string_option(&options, "databricks.query_tag.path"),
            Some(r#"folder\name,a:b"#)
        );
        assert_eq!(
            string_option(&options, "databricks.query_tag.@@dbt_model_name")
                .unwrap()
                .chars()
                .count(),
            128
        );
    }
}
