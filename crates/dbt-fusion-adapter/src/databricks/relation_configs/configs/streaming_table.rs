//! reference: dbt/adapters/databricks/relation_configs/streaming_table.py

use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor, DatabricksRelationChangeSet,
    DatabricksRelationConfigBase, DatabricksRelationConfigBaseObject,
};
use crate::databricks::relation_configs::comment::CommentProcessor;
use crate::databricks::relation_configs::configs::DatabricksRelationConfig;
use crate::databricks::relation_configs::partitioning::PartitionedByProcessor;
use crate::databricks::relation_configs::refresh::RefreshProcessor;
use crate::databricks::relation_configs::tblproperties::TblPropertiesProcessor;
use dbt_schemas::schemas::relations::relation_configs::{
    BaseRelationConfig, ComponentConfig, RelationChangeSet,
};
use minijinja::Value as MiniJinjaValue;

use std::any::Any;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, Default)]
pub struct StreamingTableConfig {
    config: BTreeMap<String, DatabricksComponentConfig>,
}

impl DatabricksRelationConfig for StreamingTableConfig {
    fn config_components() -> Vec<Arc<dyn DatabricksComponentProcessor>> {
        vec![
            Arc::new(PartitionedByProcessor),
            Arc::new(CommentProcessor),
            Arc::new(TblPropertiesProcessor),
            Arc::new(RefreshProcessor),
        ]
    }

    fn new(config: BTreeMap<String, DatabricksComponentConfig>) -> Self {
        Self { config }
    }

    fn get_config(&self, key: &str) -> Option<DatabricksComponentConfig> {
        self.config.get(key).cloned()
    }
}

impl DatabricksRelationConfigBase for StreamingTableConfig {
    fn config_components_(&self) -> Vec<Arc<dyn DatabricksComponentProcessor>> {
        StreamingTableConfig::config_components()
    }

    fn config(&self) -> BTreeMap<String, DatabricksComponentConfig> {
        self.config.clone()
    }

    fn get_component(&self, key: &str) -> Option<DatabricksComponentConfig> {
        self.config.get(key).cloned()
    }

    // Reference: https://github.com/databricks/dbt-databricks/blob/87073fe7f26bede434a3bd783717a6e49d35893f/dbt/adapters/databricks/relation_configs/streaming_table.py#L30
    fn get_changeset(&self, existing: MiniJinjaValue) -> Option<Arc<dyn RelationChangeSet>> {
        let mut changes = BTreeMap::new();
        let mut requires_refresh = false;
        let mut requires_replace = false;
        let existing = existing.downcast_object::<DatabricksRelationConfigBaseObject>()?;

        for component in self.config_components_() {
            let key = component.name();
            if let (Some(value), Some(existing_value)) =
                (self.get_config(key), existing.get_component(key))
            {
                let diff = value.get_diff(&existing_value);

                // Special handling for partition_by changes
                if key == "partition_by" && diff.is_some() {
                    requires_refresh = true;
                }

                if diff.is_some_and(|diff| {
                    !matches!(
                        diff.as_any().downcast_ref::<DatabricksComponentConfig>(),
                        Some(DatabricksComponentConfig::Refresh(_))
                    )
                }) {
                    requires_replace = true;
                }

                let diff = value
                    .get_diff(&existing_value)
                    .or_else(|| Some(Arc::new(value.clone()) as Arc<dyn ComponentConfig>));

                // Only add to changes if it's not a RefreshConfig
                if let Some(diff) = diff {
                    if !matches!(
                        diff.as_any().downcast_ref::<DatabricksComponentConfig>(),
                        Some(DatabricksComponentConfig::Refresh(_))
                    ) {
                        changes.insert(key.to_string(), diff);
                    }
                }
            }
        }

        if requires_replace {
            return Some(Arc::new(DatabricksRelationChangeSet::new(
                changes,
                requires_refresh,
            )));
        }
        None
    }
}

impl BaseRelationConfig for StreamingTableConfig {
    // defer to above impl
    fn get_changeset(
        &self,
        existing: Option<&dyn BaseRelationConfig>,
    ) -> Option<Arc<dyn RelationChangeSet>> {
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
