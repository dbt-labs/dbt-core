//! Provider bundle for the docs server.
//!
//! Every capability the docs server consumes is gated through a trait
//! defined in `dbt-index-core` (untyped SQL access via [`Backend`],
//! typed feature dispatch via [`Provider<Args, Output>`]). The source-
//! available distribution wires the no-op stubs ([`UnavailableBackend`],
//! [`UnavailableColumnLineage`], [`UnavailableColumnImpact`]) — every
//! request gets a "feature not available" response that the UI turns
//! into a PLG upsell. The proprietary distribution swaps in the real
//! impls from `dbt-index` via `dbt-cli` at startup.

use std::sync::Arc;

pub use dbt_index_core::{
    Backend, BackendError, ColumnImpactArgs, ColumnImpactNode, ColumnImpactProvider,
    ColumnLineageArgs, ColumnLineageEdge, ColumnLineageProvider, LineageError, Provider,
    UnavailableBackend, UnavailableColumnImpact, UnavailableColumnLineage,
};

/// Bundle of pluggable providers passed to the docs server at startup.
///
/// Cloning is cheap — every field is `Arc<...>`.
#[derive(Clone)]
pub struct Providers {
    pub backend: Arc<dyn Backend>,
    pub column_lineage: Arc<ColumnLineageProvider>,
    pub column_impact: Arc<ColumnImpactProvider>,
}

impl Providers {
    /// SA defaults: every capability reports unavailable. Hosts that ship
    /// the proprietary distribution swap individual fields after building.
    pub fn unavailable() -> Self {
        Self {
            backend: Arc::new(UnavailableBackend),
            column_lineage: Arc::new(UnavailableColumnLineage::new()),
            column_impact: Arc::new(UnavailableColumnImpact::new()),
        }
    }
}

impl Default for Providers {
    fn default() -> Self {
        Self::unavailable()
    }
}
