use std::{fmt::Debug, sync::LazyLock};

use dbt_common::adapter::AdapterType;
use dbt_schemas::schemas::project::QueryComment;
use minijinja::{Error, State, Value};
use regex::Regex;
use serde::Deserialize;

// Reference: https://github.com/dbt-labs/dbt-adapters/blob/317e809abd19026d3784e04281b307c5e6a9d469/dbt-adapters/src/dbt/adapters/contracts/connection.py#L197
pub const DEFAULT_QUERY_COMMENT: &str = "
{%- set comment_dict = {} -%}
{%- do comment_dict.update(
    app='dbt',
    dbt_version=dbt_version,
    profile_name=target.get('profile_name'),
    target_name=target.get('target_name'),
) -%}
{# Deviation from Core: use truthiness of `node` since `none` != `undefined`. #}
{%- if node -%}
  {%- do comment_dict.update(
    node_id=node.unique_id,
  ) -%}
{# Deviation from Core: `connection_name` is no longer used in Fusion. #}
{%- endif -%}
{{ return(tojson(comment_dict)) }}
";

#[derive(Debug)]
pub struct QueryCommentConfig {
    /// The (unresolved) query comment
    comment: String,
    /// Append or prepend the comment
    append: bool,
    /// (BigQuery only) Export comment to job labels
    job_label: bool,
}

pub static EMPTY_CONFIG: LazyLock<QueryCommentConfig> = LazyLock::new(|| QueryCommentConfig {
    comment: "".into(),
    append: false,
    job_label: false,
});

impl QueryCommentConfig {
    /// Build a comment config from a QueryComment
    pub fn from_query_comment(
        query_comment: Option<QueryComment>,
        adapter_type: AdapterType,
        use_default: bool,
    ) -> Self {
        #[derive(Debug, Default, Deserialize)]
        struct _QueryCommentConfig {
            comment: Option<String>,
            append: Option<bool>,
            #[serde(default, alias = "job-label")]
            job_label: bool,
        }

        let default_config = _QueryCommentConfig::default();

        let config = match query_comment {
            Some(QueryComment::String(value)) => _QueryCommentConfig {
                comment: Some(value),
                ..default_config
            },
            Some(QueryComment::Object(value)) => {
                _QueryCommentConfig::deserialize(value).unwrap_or(default_config)
            }
            None => default_config,
        };

        QueryCommentConfig {
            comment: config.comment.unwrap_or_else(|| {
                if use_default {
                    DEFAULT_QUERY_COMMENT.to_string()
                } else {
                    "".to_string()
                }
            }),
            append: config
                .append
                .unwrap_or(adapter_type == AdapterType::Snowflake),
            job_label: config.job_label && adapter_type == AdapterType::Bigquery,
        }
    }

    /// Resolve query comment given current Jinja state.
    pub fn resolve_comment(&self, state: &State) -> Result<String, Error> {
        state
            .env()
            .render_str(self.comment.as_str(), state.get_base_context(), &[])
    }

    /// Add query comment to SQL.
    pub fn add_comment(&self, state: &State, sql: String) -> Result<String, Error> {
        if self.comment.is_empty() {
            return Ok(sql);
        }

        let resolved_comment = self.resolve_comment(state)?;

        Ok(if self.append {
            format!("{sql}\n/* {resolved_comment} */")
        } else {
            format!("/* {resolved_comment} */\n{sql}")
        })
    }

    /// Reference: https://github.com/dbt-labs/dbt-adapters/blob/b0223a88d67012bcc4c6cce5449c4fe10c6ed198/dbt-bigquery/src/dbt/adapters/bigquery/connections.py#L629
    /// Return job labels from query comment
    pub fn get_labels_from_query_comment(&self, state: &State) -> Vec<Value> {
        let mut labels = Vec::new();

        if let Ok(resolved_comment) = self.resolve_comment(state) {
            if self.job_label {
                match serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(
                    &resolved_comment,
                ) {
                    Ok(json) => {
                        for (key, value) in json.into_iter() {
                            labels.push(Value::from_iter(vec![
                                Value::from(sanitize_label(key)),
                                Value::from(sanitize_label(value.to_string())),
                            ]));
                        }
                    }
                    Err(_) => {
                        labels.push(Value::from_iter(vec![
                            Value::from("query_comment"),
                            Value::from(sanitize_label(resolved_comment)),
                        ]));
                    }
                }
            }
        }

        labels
    }
}

const _SANITIZE_LABEL_PATTERN: &str = r"[^a-z0-9_-]";

const _VALIDATE_LABEL_LENGTH_LIMIT: usize = 63;

/// Reference: https://github.com/dbt-labs/dbt-adapters/blob/b0223a88d67012bcc4c6cce5449c4fe10c6ed198/dbt-bigquery/src/dbt/adapters/bigquery/connections.py#L640
/// Return a legal value for a BigQuery label.
fn sanitize_label(mut value: String) -> String {
    value.make_ascii_lowercase();

    let re = Regex::new(_SANITIZE_LABEL_PATTERN).unwrap();
    let sanitized_value = re.replace_all(&value, "_");

    if sanitized_value.len() > _VALIDATE_LABEL_LENGTH_LIMIT {
        sanitized_value[.._VALIDATE_LABEL_LENGTH_LIMIT].to_string()
    } else {
        sanitized_value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use dbt_common::adapter::AdapterType;
    use dbt_schemas::schemas::project::QueryComment;
    use serde::Deserialize;

    use crate::query_comment::{DEFAULT_QUERY_COMMENT, QueryCommentConfig};

    fn assert_configs_equal(left: &QueryCommentConfig, right: &QueryCommentConfig) {
        assert_eq!(left.comment, right.comment);
        assert_eq!(left.append, right.append);
        assert_eq!(left.job_label, right.job_label);
    }

    #[test]
    fn test_empty_query_comment() {
        // Test empty query comment with `use_default`
        for adapter_type in [
            AdapterType::Bigquery,
            AdapterType::Databricks,
            AdapterType::Redshift,
        ] {
            let config = QueryCommentConfig::from_query_comment(None, adapter_type, true);
            let expected_config = QueryCommentConfig {
                comment: DEFAULT_QUERY_COMMENT.to_string(),
                append: false,
                job_label: false,
            };
            assert_configs_equal(&config, &expected_config);
        }

        let config = QueryCommentConfig::from_query_comment(None, AdapterType::Snowflake, true);
        let expected_config = QueryCommentConfig {
            comment: DEFAULT_QUERY_COMMENT.to_string(),
            append: true,
            job_label: false,
        };
        assert_configs_equal(&config, &expected_config);

        // Test empty query comment with NO `use_default`
        for adapter_type in [
            AdapterType::Bigquery,
            AdapterType::Databricks,
            AdapterType::Redshift,
        ] {
            let config = QueryCommentConfig::from_query_comment(None, adapter_type, false);
            let expected_config = QueryCommentConfig {
                comment: "".to_string(),
                append: false,
                job_label: false,
            };
            assert_configs_equal(&config, &expected_config);
        }

        let config = QueryCommentConfig::from_query_comment(None, AdapterType::Snowflake, false);
        let expected_config = QueryCommentConfig {
            comment: "".to_string(),
            append: true,
            job_label: false,
        };
        assert_configs_equal(&config, &expected_config);
    }

    #[test]
    fn test_query_comment_string() {
        for adapter_type in [
            AdapterType::Bigquery,
            AdapterType::Databricks,
            AdapterType::Redshift,
        ] {
            let config = QueryCommentConfig::from_query_comment(
                Some(QueryComment::String("cool comment".to_string())),
                adapter_type,
                true,
            );
            let expected_config = QueryCommentConfig {
                comment: "cool comment".to_string(),
                append: false,
                job_label: false,
            };
            assert_configs_equal(&config, &expected_config);

            // Whether `use_default` is set should make no difference now
            let config = QueryCommentConfig::from_query_comment(
                Some(QueryComment::String("cool comment".to_string())),
                adapter_type,
                false,
            );
            assert_configs_equal(&config, &expected_config);
        }

        let config = QueryCommentConfig::from_query_comment(
            Some(QueryComment::String("cool comment".to_string())),
            AdapterType::Snowflake,
            true,
        );
        let expected_config = QueryCommentConfig {
            comment: "cool comment".to_string(),
            append: true,
            job_label: false,
        };
        assert_configs_equal(&config, &expected_config);

        let config = QueryCommentConfig::from_query_comment(
            Some(QueryComment::String("cool comment".to_string())),
            AdapterType::Snowflake,
            false,
        );
        assert_configs_equal(&config, &expected_config);
    }

    #[test]
    fn test_query_comment_object_comment() {
        let config_str = "comment: \"cool comment\"";
        let query_comment =
            QueryComment::deserialize(dbt_serde_yaml::Deserializer::from_str(config_str)).ok();

        for adapter_type in [
            AdapterType::Bigquery,
            AdapterType::Databricks,
            AdapterType::Redshift,
        ] {
            let config =
                QueryCommentConfig::from_query_comment(query_comment.clone(), adapter_type, true);
            let expected_config = QueryCommentConfig {
                comment: "cool comment".to_string(),
                append: false,
                job_label: false,
            };
            assert_configs_equal(&config, &expected_config);

            // Whether `use_default` is set should make no difference now
            let config =
                QueryCommentConfig::from_query_comment(query_comment.clone(), adapter_type, false);
            assert_configs_equal(&config, &expected_config);
        }

        let config = QueryCommentConfig::from_query_comment(
            query_comment.clone(),
            AdapterType::Snowflake,
            true,
        );
        let expected_config = QueryCommentConfig {
            comment: "cool comment".to_string(),
            append: true,
            job_label: false,
        };
        assert_configs_equal(&config, &expected_config);

        let config =
            QueryCommentConfig::from_query_comment(query_comment, AdapterType::Snowflake, false);
        assert_configs_equal(&config, &expected_config);
    }

    #[test]
    fn test_query_comment_object_append() {
        let config_str = "append: true";
        let query_comment =
            QueryComment::deserialize(dbt_serde_yaml::Deserializer::from_str(config_str)).ok();

        for adapter_type in [
            AdapterType::Bigquery,
            AdapterType::Databricks,
            AdapterType::Redshift,
            AdapterType::Snowflake,
        ] {
            let config =
                QueryCommentConfig::from_query_comment(query_comment.clone(), adapter_type, true);
            let expected_config = QueryCommentConfig {
                comment: DEFAULT_QUERY_COMMENT.to_string(),
                append: true,
                job_label: false,
            };
            assert_configs_equal(&config, &expected_config);
        }

        let config_str = "append: false";
        let query_comment =
            QueryComment::deserialize(dbt_serde_yaml::Deserializer::from_str(config_str)).ok();

        for adapter_type in [
            AdapterType::Bigquery,
            AdapterType::Databricks,
            AdapterType::Redshift,
            AdapterType::Snowflake,
        ] {
            let config =
                QueryCommentConfig::from_query_comment(query_comment.clone(), adapter_type, false);
            let expected_config = QueryCommentConfig {
                comment: "".to_string(),
                append: false,
                job_label: false,
            };
            assert_configs_equal(&config, &expected_config);
        }
    }

    #[test]
    fn test_query_comment_object_job_label() {
        let config_str = "job-label: true";
        let query_comment =
            QueryComment::deserialize(dbt_serde_yaml::Deserializer::from_str(config_str)).ok();

        // This should only work for BigQuery
        for adapter_type in [
            AdapterType::Databricks,
            AdapterType::Redshift,
            AdapterType::Snowflake,
        ] {
            let config =
                QueryCommentConfig::from_query_comment(query_comment.clone(), adapter_type, true);
            let expected_config = QueryCommentConfig {
                comment: DEFAULT_QUERY_COMMENT.to_string(),
                append: adapter_type == AdapterType::Snowflake,
                job_label: false,
            };
            assert_configs_equal(&config, &expected_config);
        }

        let config =
            QueryCommentConfig::from_query_comment(query_comment, AdapterType::Bigquery, true);
        let expected_config = QueryCommentConfig {
            comment: DEFAULT_QUERY_COMMENT.to_string(),
            append: false,
            job_label: true,
        };
        assert_configs_equal(&config, &expected_config);
    }
}
