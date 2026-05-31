use std::path::PathBuf;
use std::sync::Arc;

use serde::Serialize;

use crate::providers::Providers;
pub use dbt_docs_core::DistInfo;

/// Shared application state held by the axum router.
pub struct AppState {
    pub index_dir: PathBuf,
    pub providers: Providers,
}

pub type SharedState = Arc<AppState>;

/// Gated feature surfaces — `true` only when the running distribution
/// supports the feature. The UI reads this via `GET /api/v1/capabilities`
/// to decide which features are enabled.
#[derive(Debug, Clone, Serialize)]
pub struct Capabilities {
    pub has_column_lineage: bool,
}

impl AppState {
    pub fn new(index_dir: PathBuf, providers: Providers) -> Self {
        Self {
            index_dir,
            providers,
        }
    }

    pub fn dist_info(&self) -> DistInfo {
        self.providers.dist_info.dist_info()
    }

    pub fn server_version(&self) -> &'static str {
        self.providers.dist_info.dist_info().version
    }

    pub fn has_column_lineage(&self) -> bool {
        self.providers.column_lineage.is_available()
    }

    pub fn capabilities(&self) -> Capabilities {
        Capabilities {
            has_column_lineage: self.has_column_lineage(),
        }
    }
}
