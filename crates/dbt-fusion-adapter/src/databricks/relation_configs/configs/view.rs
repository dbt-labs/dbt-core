//! reference: dbt/adapters/databricks/relation_configs/view.py

use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor, DatabricksRelationChangeSet,
    DatabricksRelationConfigBase, DatabricksRelationConfigBaseObject,
};
use crate::databricks::relation_configs::column_comments::ColumnCommentsProcessor;
use crate::databricks::relation_configs::comment::CommentProcessor;
use crate::databricks::relation_configs::configs::DatabricksRelationConfig;
use crate::databricks::relation_configs::query::QueryProcessor;
use crate::databricks::relation_configs::tags::TagsProcessor;
use crate::databricks::relation_configs::tblproperties::TblPropertiesProcessor;
use dbt_schemas::schemas::relations::relation_configs::{BaseRelationConfig, RelationChangeSet};
use minijinja::Value as MiniJinjaValue;

use std::any::Any;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, Default)]
pub struct ViewConfig {
    config: BTreeMap<String, DatabricksComponentConfig>,
}

impl DatabricksRelationConfig for ViewConfig {
    fn config_components() -> Vec<Arc<dyn DatabricksComponentProcessor>> {
        vec![
            Arc::new(TagsProcessor),
            Arc::new(TblPropertiesProcessor),
            Arc::new(QueryProcessor),
            Arc::new(CommentProcessor),
            Arc::new(ColumnCommentsProcessor),
        ]
    }

    fn new(config: BTreeMap<String, DatabricksComponentConfig>) -> Self {
        Self { config }
    }

    fn get_config(&self, key: &str) -> Option<DatabricksComponentConfig> {
        self.config.get(key).cloned()
    }
}

impl DatabricksRelationConfigBase for ViewConfig {
    fn config_components_(&self) -> Vec<Arc<dyn DatabricksComponentProcessor>> {
        ViewConfig::config_components()
    }

    fn config(&self) -> BTreeMap<String, DatabricksComponentConfig> {
        self.config.clone()
    }

    fn get_component(&self, key: &str) -> Option<DatabricksComponentConfig> {
        self.config.get(key).cloned()
    }

    fn get_changeset(&self, existing: MiniJinjaValue) -> Option<Arc<dyn RelationChangeSet>> {
        let mut changeset = self.get_changeset_default(existing);

        if let Some(changeset) = &mut changeset {
            if changeset.changes().contains_key("comment") {
                // We can't modify the requires_full_refresh field directly since it's a method
                // Instead, we'll create a new changeset with the updated flag
                let changes = changeset.changes().clone();
                return Some(Arc::new(DatabricksRelationChangeSet::new(changes, true)));
            }
        }
        changeset
    }
}

impl BaseRelationConfig for ViewConfig {
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
