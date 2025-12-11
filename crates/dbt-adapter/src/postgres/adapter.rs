use crate::adapter_engine::AdapterEngine;
use crate::base_adapter::AdapterTyping;
use crate::metadata::*;
use crate::typed_adapter::TypedBaseAdapter;

use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

/// An adapter for Postgres.
#[derive(Clone)]
pub struct PostgresAdapter {
    engine: Arc<AdapterEngine>,
}

impl PostgresAdapter {
    pub fn new(engine: Arc<AdapterEngine>) -> Self {
        Self { engine }
    }
}

impl AdapterTyping for PostgresAdapter {
    fn as_metadata_adapter(&self) -> Option<&dyn MetadataAdapter> {
        None // TODO: implement metadata_adapter() for PostgresAdapter
    }

    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        self
    }

    fn engine(&self) -> &Arc<AdapterEngine> {
        &self.engine
    }
}

impl Debug for PostgresAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.adapter_type())
    }
}

impl TypedBaseAdapter for PostgresAdapter {}
