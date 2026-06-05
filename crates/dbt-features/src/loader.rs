use std::sync::Arc;

use dbt_loader::loader_hooks::{LoaderHooks, NoOpLoaderHooks};

pub struct LoaderFeature {
    pub hooks: Arc<dyn LoaderHooks>,
}

impl Default for LoaderFeature {
    fn default() -> Self {
        Self {
            hooks: Arc::new(NoOpLoaderHooks),
        }
    }
}
