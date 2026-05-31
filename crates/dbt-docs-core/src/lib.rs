use std::sync::Arc;

use dbt_index_core::{
    Backend, ColumnImpactProvider, ColumnLineageProvider, UnavailableBackend,
    UnavailableColumnImpact, UnavailableColumnLineage,
};
use serde::Serialize;

/// Metadata about the running distribution. Returned by `GET /api/v1/distribution`.
#[derive(Debug, Clone, Serialize)]
pub struct DistInfo {
    pub name: String,
    pub version: &'static str,
    pub is_logged_in: bool,
}

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
