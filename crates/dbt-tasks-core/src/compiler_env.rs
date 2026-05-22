use std::sync::Arc;

use dbt_adapter::Adapter;
use dbt_schema_store::{DataStoreTrait, store::SchemaStore};

#[derive(Clone)]
pub struct CompilerEnv {
    pub schema_store: Arc<SchemaStore>,
    pub data_store: Arc<dyn DataStoreTrait>,
    pub adapter: Arc<Adapter>,
}
