use std::sync::Arc;

use crate::value::Object;

#[derive(Debug, Default)]
pub(crate) struct NamespaceName {
    name: Arc<str>,
}

impl Object for NamespaceName {}

impl NamespaceName {
    pub(crate) fn new(name: &str) -> Self {
        Self { name: name.into() }
    }

    pub(crate) fn get_name(&self) -> &str {
        &self.name
    }
}
