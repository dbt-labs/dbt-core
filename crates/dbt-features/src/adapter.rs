use std::sync::Arc;

use dbt_adapter::adapter::AdapterFactory;

pub struct AdapterFeature {
    pub adapter_factory: Arc<dyn AdapterFactory>,
}
