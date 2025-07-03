//! reference: dbt/adapters/databricks/relation_configs/incremental.py

use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor, DatabricksRelationConfigBase,
    DatabricksRelationConfigBaseObject,
};
use crate::databricks::relation_configs::column_comments::ColumnCommentsProcessor;
use crate::databricks::relation_configs::comment::CommentProcessor;
use crate::databricks::relation_configs::configs::DatabricksRelationConfig;
use crate::databricks::relation_configs::constraints::ConstraintsProcessor;
use crate::databricks::relation_configs::liquid_clustering::LiquidClusteringProcessor;
use crate::databricks::relation_configs::tags::TagsProcessor;
use crate::databricks::relation_configs::tblproperties::TblPropertiesProcessor;

use dbt_schemas::schemas::relations::relation_configs::{BaseRelationConfig, RelationChangeSet};
use std::any::Any;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, Default)]
pub struct IncrementalTableConfig {
    config: BTreeMap<String, DatabricksComponentConfig>,
}

impl DatabricksRelationConfig for IncrementalTableConfig {
    fn config_components() -> Vec<Arc<dyn DatabricksComponentProcessor>> {
        vec![
            Arc::new(CommentProcessor),
            Arc::new(ColumnCommentsProcessor),
            Arc::new(ConstraintsProcessor),
            Arc::new(TagsProcessor),
            Arc::new(TblPropertiesProcessor),
            Arc::new(LiquidClusteringProcessor),
        ]
    }

    fn new(config: BTreeMap<String, DatabricksComponentConfig>) -> Self {
        Self { config }
    }

    fn get_config(&self, key: &str) -> Option<DatabricksComponentConfig> {
        self.config.get(key).cloned()
    }
}

impl BaseRelationConfig for IncrementalTableConfig {
    // defer to above impl
    fn get_changeset(
        &self,
        existing: Option<&dyn BaseRelationConfig>,
    ) -> Option<Arc<dyn RelationChangeSet>> {
        // For now, return None - this will be implemented when we have proper change detection
        if let Some(existing) = existing {
            let existing_value = existing.to_value();
            DatabricksRelationConfigBase::get_changeset(self, existing_value)
        } else {
            None
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_value(&self) -> minijinja::Value {
        let arc_config = Arc::new(self.clone()) as Arc<dyn DatabricksRelationConfigBase>;
        let result = DatabricksRelationConfigBaseObject::new(arc_config);
        minijinja::Value::from_object(result)
    }
}

impl DatabricksRelationConfigBase for IncrementalTableConfig {
    fn config_components_(&self) -> Vec<Arc<dyn DatabricksComponentProcessor>> {
        IncrementalTableConfig::config_components()
    }

    fn config(&self) -> BTreeMap<String, DatabricksComponentConfig> {
        self.config.clone()
    }

    fn get_component(&self, key: &str) -> Option<DatabricksComponentConfig> {
        self.config.get(key).cloned()
    }
}
