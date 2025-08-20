//! dbt/adapters/databricks/relation_configs/tblproperties.py

use crate::AdapterResult;
use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationMetadataKey,
    DatabricksRelationResults,
};
use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use minijinja::Value;
use serde::{Deserialize, Serialize};

use std::collections::BTreeMap;

pub const IGNORE_LIST: &[&str] = &[
    "pipelines.pipelineId",
    "delta.enableChangeDataFeed",
    "delta.minReaderVersion",
    "delta.minWriterVersion",
    "pipeline_internal.catalogType",
    "pipelines.metastore.tableName",
    "pipeline_internal.enzymeMode",
    "clusterByAuto",
    "clusteringColumns",
    "delta.enableRowTracking",
    "delta.feature.appendOnly",
    "delta.feature.changeDataFeed",
    "delta.feature.checkConstraints",
    "delta.feature.domainMetadata",
    "delta.feature.generatedColumns",
    "delta.feature.invariants",
    "delta.feature.rowTracking",
    "delta.rowTracking.materializedRowCommitVersionColumnName",
    "delta.rowTracking.materializedRowIdColumnName",
    "spark.internal.pipelines.top_level_entry.user_specified_name",
    "delta.columnMapping.maxColumnId",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TblPropertiesConfig {
    pub tblproperties: BTreeMap<String, String>,
    pub pipeline_id: Option<String>,
}

impl PartialEq for TblPropertiesConfig {
    fn eq(&self, other: &Self) -> bool {
        let without_ignore_list = |map: &BTreeMap<String, String>| {
            map.iter()
                .filter(|(k, _)| !IGNORE_LIST.contains(&k.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<BTreeMap<_, _>>()
        };

        without_ignore_list(&self.tblproperties) == without_ignore_list(&other.tblproperties)
    }
}

impl Eq for TblPropertiesConfig {}

impl TblPropertiesConfig {
    pub fn new(tblproperties: BTreeMap<String, String>, pipeline_id: Option<String>) -> Self {
        Self {
            tblproperties,
            pipeline_id,
        }
    }
}

#[derive(Debug)]
pub struct TblPropertiesProcessor;

impl DatabricksComponentProcessorProperties for TblPropertiesProcessor {
    fn name(&self) -> &'static str {
        "tblproperties"
    }
}

/// https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/relation_configs/comment.py#L23
impl DatabricksComponentProcessor for TblPropertiesProcessor {
    fn from_relation_results(
        &self,
        results: &DatabricksRelationResults,
    ) -> Option<DatabricksComponentConfig> {
        let show_tblproperties = results.get(&DatabricksRelationMetadataKey::ShowTblProperties);

        let mut tblproperties = BTreeMap::new();
        let mut pipeline_id = None;

        if let Some(table) = show_tblproperties {
            for row in table.rows() {
                if let (Ok(key_val), Ok(value_val)) =
                    (row.get_item(&Value::from(0)), row.get_item(&Value::from(1)))
                {
                    if let (Some(key_str), Some(value_str)) = (key_val.as_str(), value_val.as_str())
                    {
                        if key_str == "pipelines.pipelineId" {
                            pipeline_id = Some(value_str.to_string());
                        } else if !IGNORE_LIST.contains(&key_str) {
                            tblproperties.insert(key_str.to_string(), value_str.to_string());
                        }
                    }
                }
            }
        }

        Some(DatabricksComponentConfig::TblProperties(
            TblPropertiesConfig::new(tblproperties, pipeline_id),
        ))
    }

    fn from_relation_config(
        &self,
        relation_config: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>> {
        use dbt_serde_yaml::Value as YmlValue;

        let mut tblproperties = BTreeMap::new();

        if let Some(model) = relation_config.as_any().downcast_ref::<DbtModel>() {
            // Extract tblproperties from databricks_attr
            if let Some(databricks_attr) = &model.__adapter_attr__.databricks_attr {
                if let Some(props_map) = &databricks_attr.tblproperties {
                    for (key, value) in props_map {
                        if let YmlValue::String(value_str, _) = value {
                            tblproperties.insert(key.clone(), value_str.clone());
                        }
                    }
                }
            }

            // Check for Iceberg table format using direct field
            let is_iceberg = model
                .deprecated_config
                .table_format
                .as_ref()
                .is_some_and(|s| s == "iceberg");

            if is_iceberg {
                tblproperties.insert(
                    "delta.enableIcebergCompatV2".to_string(),
                    "true".to_string(),
                );
                tblproperties.insert(
                    "delta.universalFormat.enabledFormats".to_string(),
                    "iceberg".to_string(),
                );
            }
        }

        Ok(Some(DatabricksComponentConfig::TblProperties(
            TblPropertiesConfig::new(tblproperties, None),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::databricks::relation_configs::base::DatabricksRelationResultsBuilder;
    use dbt_agate::AgateTable;
    use dbt_schemas::schemas::{common::*, nodes::*, project::*};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn create_mock_show_tblproperties_table(properties: Vec<(&str, &str)>) -> AgateTable {
        use arrow::csv::ReaderBuilder;
        use arrow_schema::{DataType, Field, Schema};
        use std::io;
        use std::sync::Arc;

        let mut csv_data = "key,value\n".to_string();
        for (key, value) in properties {
            csv_data.push_str(&format!("{key},{value}\n"));
        }

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

    fn create_mock_dbt_model(
        tblproperties: Option<serde_json::Value>,
        table_format: Option<&str>,
    ) -> DbtModel {
        use dbt_schemas::schemas::nodes::AdapterAttr;
        use dbt_serde_yaml::Value as YmlValue;

        let meta = BTreeMap::new();
        let mut databricks_tblproperties = None;

        if let Some(props) = tblproperties {
            // Convert tblproperties to YmlValue format
            if let Ok(props_map) = serde_json::from_value::<BTreeMap<String, YmlValue>>(props) {
                databricks_tblproperties = Some(props_map);
            }
        }

        let warehouse_config = WarehouseSpecificNodeConfig {
            tblproperties: databricks_tblproperties,
            ..Default::default()
        };

        let deprecated_config = ModelConfig {
            meta: if meta.is_empty() { None } else { Some(meta) },
            table_format: table_format.map(|s| s.to_string()),
            __warehouse_specific_config__: warehouse_config.clone(),
            ..Default::default()
        };

        // Use the factory method to create adapter attributes
        let adapter_attr = AdapterAttr::from_config_and_dialect(&warehouse_config, "databricks");

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
        let processor = TblPropertiesProcessor;
        assert_eq!(processor.name(), "tblproperties");
    }

    #[test]
    fn test_from_relation_results() {
        let processor = TblPropertiesProcessor;
        let table = create_mock_show_tblproperties_table(vec![
            ("streaming.checkpointLocation", "/tmp/checkpoint"),
            ("streaming.outputMode", "append"),
            ("custom.property", "test_value"),
            ("pipelines.pipelineId", "pipeline123"),
            ("delta.enableChangeDataFeed", "true"), // Should be ignored
        ]);

        let results = DatabricksRelationResultsBuilder::new()
            .with_show_tblproperties(table)
            .build();

        let config = processor.from_relation_results(&results).unwrap();

        if let DatabricksComponentConfig::TblProperties(tbl_config) = config {
            assert_eq!(tbl_config.tblproperties.len(), 3); // Ignores pipeline and delta properties
            assert_eq!(
                tbl_config.tblproperties.get("streaming.checkpointLocation"),
                Some(&"/tmp/checkpoint".to_string())
            );
            assert_eq!(
                tbl_config.tblproperties.get("streaming.outputMode"),
                Some(&"append".to_string())
            );
            assert_eq!(
                tbl_config.tblproperties.get("custom.property"),
                Some(&"test_value".to_string())
            );
            assert_eq!(tbl_config.pipeline_id, Some("pipeline123".to_string()));
            assert!(
                !tbl_config
                    .tblproperties
                    .contains_key("delta.enableChangeDataFeed")
            );
        } else {
            panic!("Expected TblProperties config");
        }
    }

    #[test]
    fn test_from_relation_config() {
        let processor = TblPropertiesProcessor;
        let props = json!({
            "streaming.checkpointLocation": "/tmp/checkpoint",
            "streaming.outputMode": "append",
            "custom.property": "test_value"
        });
        let model = create_mock_dbt_model(Some(props), None);

        let config = processor.from_relation_config(&model).unwrap().unwrap();

        if let DatabricksComponentConfig::TblProperties(tbl_config) = config {
            assert_eq!(tbl_config.tblproperties.len(), 3);
            assert_eq!(
                tbl_config.tblproperties.get("streaming.checkpointLocation"),
                Some(&"/tmp/checkpoint".to_string())
            );
            assert_eq!(
                tbl_config.tblproperties.get("streaming.outputMode"),
                Some(&"append".to_string())
            );
            assert_eq!(
                tbl_config.tblproperties.get("custom.property"),
                Some(&"test_value".to_string())
            );
            assert_eq!(tbl_config.pipeline_id, None);
        } else {
            panic!("Expected TblProperties config");
        }
    }

    #[test]
    fn test_from_relation_config_iceberg() {
        let processor = TblPropertiesProcessor;
        let props = json!({
            "custom.property": "test_value"
        });
        let model = create_mock_dbt_model(Some(props), Some("iceberg"));

        let config = processor.from_relation_config(&model).unwrap().unwrap();

        if let DatabricksComponentConfig::TblProperties(tbl_config) = config {
            assert_eq!(tbl_config.tblproperties.len(), 3); // custom + 2 iceberg properties
            assert_eq!(
                tbl_config.tblproperties.get("custom.property"),
                Some(&"test_value".to_string())
            );
            assert_eq!(
                tbl_config.tblproperties.get("delta.enableIcebergCompatV2"),
                Some(&"true".to_string())
            );
            assert_eq!(
                tbl_config
                    .tblproperties
                    .get("delta.universalFormat.enabledFormats"),
                Some(&"iceberg".to_string())
            );
        } else {
            panic!("Expected TblProperties config");
        }
    }

    #[test]
    fn test_from_relation_config_empty() {
        let processor = TblPropertiesProcessor;
        let model = create_mock_dbt_model(None, None);

        let config = processor.from_relation_config(&model).unwrap().unwrap();

        if let DatabricksComponentConfig::TblProperties(tbl_config) = config {
            assert!(tbl_config.tblproperties.is_empty());
            assert_eq!(tbl_config.pipeline_id, None);
        } else {
            panic!("Expected TblProperties config");
        }
    }
}
