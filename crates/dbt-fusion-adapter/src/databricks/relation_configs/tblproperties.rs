//! dbt/adapters/databricks/relation_configs/tblproperties.py

use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationResults,
};

use crate::AdapterResult;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
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
        _row: &DatabricksRelationResults,
    ) -> Option<DatabricksComponentConfig> {
        // TODO: implement
        None
    }

    fn from_relation_config(
        &self,
        _relation_config: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>> {
        // TODO: implement
        Ok(None)
    }
}
