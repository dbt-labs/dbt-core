use crate::databricks::relation_configs::base::{
    DatabricksComponentConfig, DatabricksComponentProcessor,
};

use dbt_schemas::schemas::relations::relation_configs::BaseRelationConfig;

use std::{collections::BTreeMap, sync::Arc};

pub mod incremental;
pub mod materialized_view;
pub mod streaming_table;
pub mod view;

/// Shared trait for all relation configs
///
/// The main purpose of this trait is to hold type-associated methods
/// while keeping `RelationConfigBase` dyn-compatible
///
/// Though `RelationConfigBase` is `RelationConfig`'s super trait,
/// it still has `Base` in its name, this is to match `DatabricksRelationConfigBase` from the Python impl
pub trait DatabricksRelationConfig: BaseRelationConfig + Send + Sync + std::fmt::Debug {
    fn config_components() -> Vec<Arc<dyn DatabricksComponentProcessor>>;

    fn new(config: BTreeMap<String, DatabricksComponentConfig>) -> Self;

    fn get_config(&self, key: &str) -> Option<DatabricksComponentConfig>;
}
