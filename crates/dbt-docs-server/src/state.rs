use std::path::PathBuf;
use std::sync::Arc;

use serde::Serialize;

use crate::providers::Providers;

/// Shared application state held by the axum router.
///
/// Holds the [`Providers`] (which carry all proprietary surfaces behind
/// dyn-compatible traits) plus precomputed [`Capabilities`] read by
/// `/api/v1/capabilities`.
pub struct AppState {
    pub index_dir: PathBuf,
    pub providers: Providers,
    pub capabilities: Capabilities,
    pub server_version: &'static str,
}

pub type SharedState = Arc<AppState>;

/// Gated feature surfaces — `true` only when both the running distribution
/// and the artifact set on disk support the surface. The UI reads this
/// via `GET /api/v1/capabilities` to decide which features to enable
/// versus surface as PLG upsells.
///
/// Only column-level lineage is gated today. As more gated surfaces are
/// added (sample data, AI features, etc.) they get their own `has_*`
/// field here and a matching `Provider` in [`Providers`]. Everything else
/// (nodes, sources, exposures, metrics, run results, etc.) is just a
/// plain SQL-over-parquet read and always available — no capability flag
/// is needed.
#[derive(Debug, Clone, Serialize, Default)]
pub struct Capabilities {
    pub has_column_lineage: bool,
}

impl AppState {
    /// Build state from injected providers. Probes capabilities through
    /// the trait surface — never touches a concrete backend type.
    pub fn new(index_dir: PathBuf, providers: Providers) -> Self {
        let capabilities = compute_capabilities(&providers);
        Self {
            index_dir,
            providers,
            capabilities,
            server_version: env!("CARGO_PKG_VERSION"),
        }
    }
}

fn compute_capabilities(providers: &Providers) -> Capabilities {
    Capabilities {
        has_column_lineage: providers.column_lineage.is_available(),
    }
}
