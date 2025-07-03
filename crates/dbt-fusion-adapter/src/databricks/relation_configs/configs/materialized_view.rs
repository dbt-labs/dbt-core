//! reference: dbt/adapters/databricks/relation_configs/materialized_view.py

use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor, DatabricksRelationChangeSet,
    DatabricksRelationConfigBase, DatabricksRelationConfigBaseObject,
};
use crate::databricks::relation_configs::comment::CommentProcessor;
use crate::databricks::relation_configs::configs::DatabricksRelationConfig;
use crate::databricks::relation_configs::partitioning::PartitionedByProcessor;
use crate::databricks::relation_configs::query::QueryProcessor;
use crate::databricks::relation_configs::refresh::RefreshProcessor;
use crate::databricks::relation_configs::tblproperties::TblPropertiesProcessor;
use dbt_schemas::schemas::relations::relation_configs::{
    BaseRelationConfig, ComponentConfig, RelationChangeSet,
};
use minijinja::Value as MiniJinjaValue;

use std::any::Any;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, Default)]
pub struct MaterializedViewConfig {
    config: BTreeMap<String, DatabricksComponentConfig>,
}

impl DatabricksRelationConfig for MaterializedViewConfig {
    fn config_components() -> Vec<Arc<dyn DatabricksComponentProcessor>> {
        vec![
            Arc::new(PartitionedByProcessor),
            Arc::new(CommentProcessor),
            Arc::new(TblPropertiesProcessor),
            Arc::new(RefreshProcessor),
            Arc::new(QueryProcessor),
        ]
    }

    fn new(config: BTreeMap<String, DatabricksComponentConfig>) -> Self {
        Self { config }
    }

    fn get_config(&self, key: &str) -> Option<DatabricksComponentConfig> {
        self.config.get(key).cloned()
    }
}

impl DatabricksRelationConfigBase for MaterializedViewConfig {
    fn config_components_(&self) -> Vec<Arc<dyn DatabricksComponentProcessor>> {
        MaterializedViewConfig::config_components()
    }

    fn config(&self) -> BTreeMap<String, DatabricksComponentConfig> {
        self.config.clone()
    }

    fn get_component(&self, key: &str) -> Option<DatabricksComponentConfig> {
        self.config.get(key).cloned()
    }

    fn get_changeset(&self, existing: MiniJinjaValue) -> Option<Arc<dyn RelationChangeSet>> {
        let mut changes = BTreeMap::new();
        let mut requires_refresh = false;
        let existing = existing.downcast_object::<DatabricksRelationConfigBaseObject>()?;

        for component in self.config_components_() {
            let key = component.name();
            if let (Some(value), Some(existing_value)) =
                (self.get_config(key), existing.get_component(key))
            {
                if let Some(diff) = value.get_diff(&existing_value) {
                    requires_refresh = requires_refresh || key != "refresh";
                    changes.insert(key.to_string(), diff);
                }
            }
        }

        if !changes.is_empty() {
            Some(Arc::new(DatabricksRelationChangeSet::new(
                changes,
                requires_refresh,
            )))
        } else {
            None
        }
    }
}

impl BaseRelationConfig for MaterializedViewConfig {
    fn get_changeset(
        &self,
        _existing: Option<&dyn BaseRelationConfig>,
    ) -> Option<Arc<dyn RelationChangeSet>> {
        // For now, return None - this will be implemented when we have proper change detection
        None
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
