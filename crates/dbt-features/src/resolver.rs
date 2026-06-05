use std::sync::Arc;

use dbt_parser::resolver_hooks::{NoOpResolverHooks, ResolverHooks};

pub struct ResolverFeature {
    pub hooks: Arc<dyn ResolverHooks>,
}

impl Default for ResolverFeature {
    fn default() -> Self {
        Self {
            hooks: Arc::new(NoOpResolverHooks),
        }
    }
}
