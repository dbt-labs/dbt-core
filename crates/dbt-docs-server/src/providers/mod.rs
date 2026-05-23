//! Provider bundle for the docs server.

use std::sync::Arc;

pub use dbt_index_core::{
    Backend, BackendError, ColumnImpactArgs, ColumnImpactNode, ColumnImpactProvider,
    ColumnLineageArgs, ColumnLineageEdge, ColumnLineageProvider, LineageError, Provider,
    UnavailableBackend, UnavailableColumnImpact, UnavailableColumnLineage,
};

use crate::DistInfo;

pub trait DistInfoProvider: Send + Sync {
    fn dist_info(&self) -> DistInfo;
}

pub struct DefaultDistInfoProvider;

impl DistInfoProvider for DefaultDistInfoProvider {
    fn dist_info(&self) -> DistInfo {
        DistInfo {
            name: "oss".to_string(),
            version: env!("CARGO_PKG_VERSION"),
            is_logged_in: false,
        }
    }
}

/// Bundle of pluggable providers passed to the docs server at startup.
///
/// TODO(felipecrv): unbundle the providers to reduce indirection. AppState is the
/// bundle of providers and any other shared state.
#[derive(Clone)]
pub struct Providers {
    pub backend: Arc<dyn Backend>,
    pub column_lineage: Arc<ColumnLineageProvider>,
    pub column_impact: Arc<ColumnImpactProvider>,
    pub dist_info: Arc<dyn DistInfoProvider>,
}

impl Default for Providers {
    fn default() -> Self {
        Providers {
            backend: Arc::new(UnavailableBackend),
            column_lineage: Arc::new(UnavailableColumnLineage::new()),
            column_impact: Arc::new(UnavailableColumnImpact::new()),
            dist_info: Arc::new(DefaultDistInfoProvider),
        }
    }
}
