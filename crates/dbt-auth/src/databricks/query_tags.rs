use std::collections::BTreeMap;

use serde_json::Value;

use crate::AuthError;

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

/// Validated Databricks query tags, stored as raw values.
///
/// Raw values are retained so per-statement `SET QUERY_TAGS` commands preserve
/// the value displayed in query history. Escaping is applied only when building
/// the connector's comma-delimited session parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabricksQueryTags {
    tags: BTreeMap<String, String>,
}

impl DatabricksQueryTags {
    pub fn from_sources(
        profile_query_tags: Option<&str>,
        model_query_tags: Option<&str>,
        model_name: Option<&str>,
        materialized: Option<&str>,
    ) -> Result<Self, AuthError> {
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
            return Err(AuthError::config(format!(
                "Too many total query tags ({}). Maximum allowed is {MAX_TAGS}",
                tags.len()
            )));
        }

        Ok(Self { tags })
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.tags.get(key).map(String::as_str)
    }

    pub fn as_map(&self) -> &BTreeMap<String, String> {
        &self.tags
    }

    pub fn session_parameter(&self) -> String {
        self.tags
            .iter()
            .map(|(key, value)| format!("{key}:{}", escape_session_value(value)))
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn parse_user_tags(
    query_tags: Option<&str>,
    source: &str,
) -> Result<BTreeMap<String, String>, AuthError> {
    let Some(query_tags) = query_tags.filter(|value| !value.is_empty()) else {
        return Ok(BTreeMap::new());
    };

    let parsed: Value = serde_json::from_str(query_tags)
        .map_err(|error| AuthError::config(format!("Invalid JSON in query_tags: {error}")))?;
    let Value::Object(object) = parsed else {
        return Err(AuthError::config(
            "query_tags must be a JSON object (dictionary)",
        ));
    };

    let reserved = object
        .keys()
        .filter(|key| RESERVED_KEYS.contains(&key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !reserved.is_empty() {
        return Err(AuthError::config(format!(
            "{source}: Cannot use reserved query tag keys: {}. Reserved keys are: {}",
            reserved.join(", "),
            RESERVED_KEYS.join(", ")
        )));
    }

    object
        .into_iter()
        .map(|(key, value)| {
            let Value::String(value) = value else {
                return Err(AuthError::config(format!(
                    "{source}: query_tags values must be strings for key '{key}'. Only string values are supported."
                )));
            };
            if escape_session_value(&value).chars().count() > MAX_VALUE_CHARS {
                return Err(AuthError::config(format!(
                    "{source}: Query tag values must be at most {MAX_VALUE_CHARS} characters after escaping. Key '{key}' exceeds the limit."
                )));
            }
            Ok((key, value))
        })
        .collect()
}

fn truncate_default(value: &str) -> String {
    value.chars().take(MAX_VALUE_CHARS).collect()
}

fn escape_session_value(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace(',', r"\,")
        .replace(':', r"\:")
}

#[cfg(test)]
mod tests {
    use super::DatabricksQueryTags;

    #[test]
    fn merges_model_profile_and_automatic_tags_with_expected_precedence() {
        let tags = DatabricksQueryTags::from_sources(
            Some(r#"{"team":"profile","precedence":"profile"}"#),
            Some(r#"{"model_only":"yes","precedence":"model"}"#),
            Some("orders"),
            Some("incremental"),
        )
        .unwrap();

        assert_eq!(tags.get("team"), Some("profile"));
        assert_eq!(tags.get("model_only"), Some("yes"));
        assert_eq!(tags.get("precedence"), Some("model"));
        assert_eq!(tags.get("@@dbt_model_name"), Some("orders"));
        assert_eq!(tags.get("@@dbt_materialized"), Some("incremental"));
        assert_eq!(
            tags.get("@@dbt_core_version"),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(tags.as_map().len(), 6);
    }

    #[test]
    fn rejects_invalid_json_shapes_and_non_string_values() {
        let non_object =
            DatabricksQueryTags::from_sources(Some(r#"["not","an","object"]"#), None, None, None)
                .unwrap_err();
        assert!(non_object.msg().contains("must be a JSON object"));

        let non_string =
            DatabricksQueryTags::from_sources(Some(r#"{"cost_center":3000}"#), None, None, None)
                .unwrap_err();
        assert!(non_string.msg().contains("values must be strings"));
    }

    #[test]
    fn rejects_reserved_user_keys_from_both_sources() {
        for source in [
            Some(r#"{"@@dbt_core_version":"override"}"#),
            Some(r#"{"@@dbt_model_name":"override"}"#),
            Some(r#"{"@@dbt_materialized":"override"}"#),
            Some(r#"{"@@dbt_databricks_version":"override"}"#),
        ] {
            let profile_error =
                DatabricksQueryTags::from_sources(source, None, None, None).unwrap_err();
            assert!(profile_error.msg().contains("reserved query tag keys"));

            let model_error =
                DatabricksQueryTags::from_sources(None, source, None, None).unwrap_err();
            assert!(model_error.msg().contains("reserved query tag keys"));
        }
    }

    #[test]
    fn validates_escaped_user_value_length_and_total_tag_count() {
        let long_after_escaping = format!(r#"{{"value":"{}"}}"#, ",".repeat(65));
        let length_error =
            DatabricksQueryTags::from_sources(Some(&long_after_escaping), None, None, None)
                .unwrap_err();
        assert!(length_error.msg().contains("at most 128 characters"));

        let too_many = serde_json::to_string(
            &(0..20)
                .map(|index| (format!("tag_{index}"), "value"))
                .collect::<std::collections::BTreeMap<_, _>>(),
        )
        .unwrap();
        let count_error =
            DatabricksQueryTags::from_sources(Some(&too_many), None, None, None).unwrap_err();
        assert!(count_error.msg().contains("Too many total query tags (21)"));
    }

    #[test]
    fn session_parameter_is_deterministic_and_escapes_delimiters() {
        let tags = DatabricksQueryTags::from_sources(
            Some(r#"{"z":"folder\\name,a:b","a":"first"}"#),
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(
            tags.session_parameter(),
            format!(
                "@@dbt_core_version:{},a:first,z:folder\\\\name\\,a\\:b",
                env!("CARGO_PKG_VERSION")
            )
        );
    }

    #[test]
    fn automatic_values_are_truncated_before_session_escaping() {
        let model_name = format!("{}::", "x".repeat(127));
        let tags = DatabricksQueryTags::from_sources(None, None, Some(&model_name), Some("table"))
            .unwrap();

        assert_eq!(tags.get("@@dbt_model_name").unwrap().chars().count(), 128);
        assert!(
            tags.session_parameter()
                .contains(&format!("{}\\:", "x".repeat(127)))
        );
    }
}
