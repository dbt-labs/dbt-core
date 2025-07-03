//! dbt/adapters/databricks/relation_configs/refresh.py

use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
    DatabricksComponentProcessorProperties, DatabricksRelationResults,
};

use crate::AdapterResult;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
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
        _row: &DatabricksRelationResults,
    ) -> Option<DatabricksComponentConfig> {
        // TODO: implement
        Some(DatabricksComponentConfig::Refresh(RefreshConfig::new(
            None, None, false,
        )))
    }

    fn from_relation_config(
        &self,
        _relation_config: &dyn InternalDbtNodeAttributes,
    ) -> AdapterResult<Option<DatabricksComponentConfig>> {
        // TODO: implement
        Ok(None)
    }
}
