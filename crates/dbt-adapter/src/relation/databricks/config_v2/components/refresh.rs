//! https://github.com/databricks/dbt-databricks/blob/main/dbt/adapters/databricks/relation_configs/refresh.py

use crate::relation::config_v2::{
    ComponentConfig, ComponentConfigLoader, SimpleComponentConfigImpl, diff,
};
use crate::relation::databricks::config_v2::{
    DatabricksRelationMetadata, DatabricksRelationMetadataKey,
};
use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use minijinja::Value;
use regex::Regex;

pub(crate) const TYPE_NAME: &str = "refresh";

#[derive(Debug, Clone, Eq)]
pub(crate) struct Config {
    pub cron: Option<String>,
    pub time_zone_value: Option<String>,
}

impl PartialEq for Config {
    // Reference: https://github.com/databricks/dbt-databricks/blob/87073fe7f26bede434a3bd783717a6e49d35893f/dbt/adapters/databricks/relation_configs/refresh.py#L28
    fn eq(&self, other: &Self) -> bool {
        self.cron == other.cron
            && (self.time_zone_value == other.time_zone_value
                || (self.time_zone_value.is_none()
                    && other
                        .time_zone_value
                        .as_ref()
                        .is_some_and(|value| value.to_lowercase().contains("utc"))))
    }
}

/// Component for Databricks refresh schedule
///
/// Holds a string representing the SQL query.
pub type Refresh = SimpleComponentConfigImpl<Config>;

fn new(cron: Option<String>, time_zone_value: Option<String>) -> Refresh {
    Refresh {
        type_name: TYPE_NAME,
        diff_fn: diff::desired_state,
        value: Config {
            cron,
            time_zone_value,
        },
    }
}

fn from_remote_state(results: &DatabricksRelationMetadata) -> Refresh {
    let Some(describe_extended) = results.get(&DatabricksRelationMetadataKey::DescribeExtended)
    else {
        return new(None, None);
    };

    // Parse CRON schedule format: "CRON '0 */6 * * *' AT TIME ZONE 'UTC'"
    let schedule_regex = Regex::new(r"CRON '(.*)' AT TIME ZONE '(.*)'").unwrap();

    for row in describe_extended.rows() {
        if let (Ok(key_val), Ok(value_val)) =
            (row.get_item(&Value::from(0)), row.get_item(&Value::from(1)))
            && let (Some(key_str), Some(value_str)) = (key_val.as_str(), value_val.as_str())
            && key_str == "Refresh Schedule"
        {
            if value_str == "MANUAL" {
                return new(None, None);
            }

            if let Some(captures) = schedule_regex.captures(value_str) {
                let cron = captures.get(1).map(|m| m.as_str().to_string());
                let time_zone_value = captures.get(2).map(|m| m.as_str().to_string());

                return new(cron, time_zone_value);
            }

            // Unparseable schedule format
            return new(None, None);
        }
    }

    // Default to manual refresh if no schedule found
    new(None, None)
}

fn from_local_config(relation_config: &dyn InternalDbtNodeAttributes) -> Refresh {
    let (cron, time_zone_value) = relation_config
        .as_any()
        .downcast_ref::<DbtModel>()
        .and_then(|model| model.__adapter_attr__.databricks_attr.as_ref())
        .and_then(|attr| attr.schedule.as_ref())
        .map(|schedule| (schedule.cron.clone(), schedule.time_zone_value.clone()))
        .unwrap_or((None, None));

    new(cron, time_zone_value)
}

pub(crate) struct RefreshLoader;

impl RefreshLoader {
    pub fn new(cron: Option<String>, time_zone_value: Option<String>) -> Box<dyn ComponentConfig> {
        Box::new(new(cron, time_zone_value))
    }

    pub fn type_name() -> &'static str {
        TYPE_NAME
    }
}

impl ComponentConfigLoader<DatabricksRelationMetadata> for RefreshLoader {
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
    use crate::relation::databricks::config_v2::test_helpers;
    use dbt_agate::AgateTable;
    use std::collections::HashMap;

    fn create_mock_describe_extended_table(schedule_info: Option<&str>) -> AgateTable {
        let comment_text = schedule_info.unwrap_or("MANUAL");
        test_helpers::create_mock_describe_extended_table([], [("Refresh Schedule", comment_text)])
    }

    fn create_mock_dbt_model(cron: Option<&str>, time_zone: Option<&str>) -> DbtModel {
        let cfg = test_helpers::TestModelConfig {
            cron: cron.map(|s| s.to_string()),
            time_zone: time_zone.map(|s| s.to_string()),
            ..Default::default()
        };

        test_helpers::create_mock_dbt_model(cfg)
    }

    #[test]
    fn test_from_remote_state_manual() {
        let table = create_mock_describe_extended_table(None); // MANUAL by default
        let results = HashMap::from([(DatabricksRelationMetadataKey::DescribeExtended, table)]);
        let config = from_remote_state(&results);

        assert_eq!(config.value.cron, None);
        assert_eq!(config.value.time_zone_value, None);
    }

    #[test]
    fn test_from_remote_state_cron_schedule() {
        let table =
            create_mock_describe_extended_table(Some("CRON '0 */6 * * *' AT TIME ZONE 'UTC'"));
        let results = HashMap::from([(DatabricksRelationMetadataKey::DescribeExtended, table)]);
        let config = from_remote_state(&results);

        assert_eq!(config.value.cron, Some("0 */6 * * *".to_string()));
        assert_eq!(config.value.time_zone_value, Some("UTC".to_string()));
    }

    #[test]
    fn test_from_local_config_with_schedule() {
        let model = create_mock_dbt_model(Some("0 */6 * * *"), Some("UTC"));
        let config = from_local_config(&model);

        assert_eq!(config.value.cron, Some("0 */6 * * *".to_string()));
        assert_eq!(config.value.time_zone_value, Some("UTC".to_string()));
    }

    #[test]
    fn test_from_local_config_cron_only() {
        let model = create_mock_dbt_model(Some("0 */12 * * *"), None);
        let config = from_local_config(&model);

        assert_eq!(config.value.cron, Some("0 */12 * * *".to_string()));
        assert_eq!(config.value.time_zone_value, None);
    }

    #[test]
    fn test_from_local_config_no_schedule() {
        let model = create_mock_dbt_model(None, None);
        let config = from_local_config(&model);

        assert_eq!(config.value.cron, None);
        assert_eq!(config.value.time_zone_value, None);
    }
}
