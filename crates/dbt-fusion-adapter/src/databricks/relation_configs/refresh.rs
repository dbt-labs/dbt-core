//! dbt/adapters/databricks/relation_configs/refresh.py

use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationMetadataKey,
    DatabricksRelationResults,
};
use dbt_schemas::schemas::DbtModel;

use crate::AdapterResult;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use minijinja::Value;
use regex::Regex;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshConfig {
    pub cron: Option<String>,
    pub time_zone_value: Option<String>,
    pub is_altered: bool,
}

impl RefreshConfig {
    pub fn new(cron: Option<String>, time_zone_value: Option<String>, is_altered: bool) -> Self {
        Self {
            cron,
            time_zone_value,
            is_altered,
        }
    }

    pub fn get_diff(&self, other: &Self) -> Option<Self> {
        if self != other {
            Some(Self::new(
                self.cron.clone(),
                self.time_zone_value.clone(),
                self.cron.is_some() && other.cron.is_some(),
            ))
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct RefreshProcessor;

impl DatabricksComponentProcessorProperties for RefreshProcessor {
    fn name(&self) -> &'static str {
        "refresh"
    }
}

/// https://github.com/databricks/dbt-databricks/blob/8fc69739c4885648bb95074e796c67a57fc9995f/dbt/adapters/databricks/relation_configs/refresh.py#L38
impl DatabricksComponentProcessor for RefreshProcessor {
    fn from_relation_results(
        &self,
        results: &DatabricksRelationResults,
    ) -> Option<DatabricksComponentConfig> {
        let describe_extended = results.get(&DatabricksRelationMetadataKey::DescribeExtended)?;

        // Parse CRON schedule format: "CRON '0 */6 * * *' AT TIME ZONE 'UTC'"
        let schedule_regex = Regex::new(r"CRON '(.*)' AT TIME ZONE '(.*)'").ok()?;

        for row in describe_extended.rows() {
            if let (Ok(key_val), Ok(value_val)) =
                (row.get_item(&Value::from(0)), row.get_item(&Value::from(1)))
            {
                if let (Some(key_str), Some(value_str)) = (key_val.as_str(), value_val.as_str()) {
                    if key_str == "Refresh Schedule" {
                        if value_str == "MANUAL" {
                            return Some(DatabricksComponentConfig::Refresh(RefreshConfig::new(
                                None, None, false,
                            )));
                        }

                        if let Some(captures) = schedule_regex.captures(value_str) {
                            let cron = captures.get(1).map(|m| m.as_str().to_string());
                            let time_zone_value = captures.get(2).map(|m| m.as_str().to_string());

                            return Some(DatabricksComponentConfig::Refresh(RefreshConfig::new(
                                cron,
                                time_zone_value,
                                false,
                            )));
                        }

                        return None; // Unparseable schedule format
                    }
                }
            }
        }

        // Default to manual refresh if no schedule found
        Some(DatabricksComponentConfig::Refresh(RefreshConfig::new(
            None, None, false,
        )))
    }

    fn from_relation_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>> {
        let (cron, time_zone_value) = relation_config
            .as_any()
            .downcast_ref::<DbtModel>()
            .and_then(|model| model.__adapter_attr__.databricks_attr.as_ref())
            .and_then(|attr| attr.schedule.as_ref())
            .map(|schedule| (schedule.cron.clone(), schedule.time_zone_value.clone()))
            .unwrap_or((None, None));

        Ok(Some(DatabricksComponentConfig::Refresh(
            RefreshConfig::new(cron, time_zone_value, false),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AdapterType;
    use crate::databricks::relation_configs::base::DatabricksRelationResultsBuilder;
    use dbt_agate::AgateTable;
    use dbt_schemas::schemas::{common::*, nodes::*, project::*};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn create_mock_describe_extended_table(schedule_info: Option<&str>) -> AgateTable {
        use arrow::csv::ReaderBuilder;
        use arrow_schema::{DataType, Field, Schema};
        use std::io;
        use std::sync::Arc;

        let mut csv_data = "key,value\n".to_string();
        csv_data.push_str("Table,test_table\n");
        csv_data.push_str("Owner,test_user\n");

        match schedule_info {
            Some(schedule) => csv_data.push_str(&format!("Refresh Schedule,{schedule}\n")),
            None => csv_data.push_str("Refresh Schedule,MANUAL\n"),
        }

        csv_data.push_str("# Detailed Table Information,\n");

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
        ]));

        let file = io::Cursor::new(csv_data);
        let mut reader = ReaderBuilder::new(schema)
            .with_header(true)
            .build(file)
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        AgateTable::from_record_batch(Arc::new(batch))
    }

    fn create_mock_dbt_model(schedule: Option<serde_json::Value>) -> DbtModel {
        use dbt_schemas::schemas::common::ScheduleConfig;
        use dbt_schemas::schemas::nodes::AdapterAttr;

        let schedule_config = if let Some(schedule_value) = schedule {
            if let Ok(schedule_obj) =
                serde_json::from_value::<serde_json::Map<String, serde_json::Value>>(schedule_value)
            {
                Some(ScheduleConfig {
                    cron: schedule_obj
                        .get("cron")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    time_zone_value: schedule_obj
                        .get("time_zone_value")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                })
            } else {
                None
            }
        } else {
            None
        };

        let warehouse_config = WarehouseSpecificNodeConfig {
            schedule: schedule_config,
            ..Default::default()
        };

        let deprecated_config = ModelConfig {
            __warehouse_specific_config__: warehouse_config.clone(),
            ..Default::default()
        };

        // Use the factory method to create adapter attributes
        let adapter_attr =
            AdapterAttr::from_config_and_dialect(&warehouse_config, AdapterType::Databricks);

        DbtModel {
            __common_attr__: CommonAttributes {
                name: "test_model".to_string(),
                fqn: vec!["test".to_string(), "test_model".to_string()],
                ..Default::default()
            },
            __base_attr__: NodeBaseAttributes {
                database: "test_db".to_string(),
                schema: "test_schema".to_string(),
                alias: "test_table".to_string(),
                relation_name: None,
                quoting: dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING,
                quoting_ignore_case: false,
                materialized: DbtMaterialization::StreamingTable,
                static_analysis: dbt_common::io_args::StaticAnalysisKind::On,
                enabled: true,
                extended_model: false,
                persist_docs: None,
                columns: BTreeMap::new(),
                refs: vec![],
                sources: vec![],
                metrics: vec![],
                depends_on: NodeDependsOn::default(),
            },
            __adapter_attr__: adapter_attr,
            deprecated_config,
            ..Default::default()
        }
    }

    #[test]
    fn test_processor_name() {
        let processor = RefreshProcessor;
        assert_eq!(processor.name(), "refresh");
    }

    #[test]
    fn test_from_relation_results_manual() {
        let processor = RefreshProcessor;
        let table = create_mock_describe_extended_table(None); // MANUAL by default

        let results = DatabricksRelationResultsBuilder::new()
            .with_describe_extended(table)
            .build();

        let config = processor.from_relation_results(&results).unwrap();

        if let DatabricksComponentConfig::Refresh(refresh_config) = config {
            assert_eq!(refresh_config.cron, None);
            assert_eq!(refresh_config.time_zone_value, None);
            assert!(!refresh_config.is_altered);
        } else {
            panic!("Expected Refresh config");
        }
    }

    #[test]
    fn test_from_relation_results_cron_schedule() {
        let processor = RefreshProcessor;
        let table =
            create_mock_describe_extended_table(Some("CRON '0 */6 * * *' AT TIME ZONE 'UTC'"));

        let results = DatabricksRelationResultsBuilder::new()
            .with_describe_extended(table)
            .build();

        let config = processor.from_relation_results(&results).unwrap();

        if let DatabricksComponentConfig::Refresh(refresh_config) = config {
            assert_eq!(refresh_config.cron, Some("0 */6 * * *".to_string()));
            assert_eq!(refresh_config.time_zone_value, Some("UTC".to_string()));
            assert!(!refresh_config.is_altered);
        } else {
            panic!("Expected Refresh config");
        }
    }

    #[test]
    fn test_from_relation_config_with_schedule() {
        let processor = RefreshProcessor;
        let schedule = json!({
            "cron": "0 */6 * * *",
            "time_zone_value": "UTC"
        });
        let model = create_mock_dbt_model(Some(schedule));

        let config = processor.from_relation_config(&model).unwrap().unwrap();

        if let DatabricksComponentConfig::Refresh(refresh_config) = config {
            assert_eq!(refresh_config.cron, Some("0 */6 * * *".to_string()));
            assert_eq!(refresh_config.time_zone_value, Some("UTC".to_string()));
            assert!(!refresh_config.is_altered);
        } else {
            panic!("Expected Refresh config");
        }
    }

    #[test]
    fn test_from_relation_config_cron_only() {
        let processor = RefreshProcessor;
        let schedule = json!({
            "cron": "0 */12 * * *"
        });
        let model = create_mock_dbt_model(Some(schedule));

        let config = processor.from_relation_config(&model).unwrap().unwrap();

        if let DatabricksComponentConfig::Refresh(refresh_config) = config {
            assert_eq!(refresh_config.cron, Some("0 */12 * * *".to_string()));
            assert_eq!(refresh_config.time_zone_value, None);
            assert!(!refresh_config.is_altered);
        } else {
            panic!("Expected Refresh config");
        }
    }

    #[test]
    fn test_from_relation_config_no_schedule() {
        let processor = RefreshProcessor;
        let model = create_mock_dbt_model(None);

        let config = processor.from_relation_config(&model).unwrap().unwrap();

        if let DatabricksComponentConfig::Refresh(refresh_config) = config {
            assert_eq!(refresh_config.cron, None);
            assert_eq!(refresh_config.time_zone_value, None);
            assert!(!refresh_config.is_altered);
        } else {
            panic!("Expected Refresh config");
        }
    }

    #[test]
    fn test_refresh_config_new() {
        let config = RefreshConfig::new(
            Some("0 */6 * * *".to_string()),
            Some("UTC".to_string()),
            true,
        );

        assert_eq!(config.cron, Some("0 */6 * * *".to_string()));
        assert_eq!(config.time_zone_value, Some("UTC".to_string()));
        assert!(config.is_altered);
    }
}
