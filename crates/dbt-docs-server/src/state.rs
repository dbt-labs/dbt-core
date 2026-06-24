use std::path::PathBuf;
use std::sync::Arc;

use serde::Serialize;

use crate::providers::Providers;
pub use dbt_docs_core::DistInfo;

/// Shared application state held by the axum router.
pub struct AppState {
    pub index_dir: PathBuf,
    pub providers: Providers,
    pub has_dbt_state: bool,
    pub do_not_track: bool,
    pub send_anonymous_usage_stats: bool,
}

pub type SharedState = Arc<AppState>;

/// Gated feature surfaces — `true` only when the running distribution
/// supports the feature. The UI reads this via `GET /api/v1/capabilities`
/// to decide which features are enabled.
#[derive(Debug, Clone, Serialize)]
pub struct Capabilities {
    pub has_column_lineage: bool,
    pub has_dbt_state: bool,
}

impl AppState {
    pub fn new(
        index_dir: PathBuf,
        providers: Providers,
        has_dbt_state: bool,
        send_anonymous_usage_stats: bool,
    ) -> Self {
        let do_not_track = std::env::var("DO_NOT_TRACK").as_deref() == Ok("1");
        Self {
            index_dir,
            providers,
            has_dbt_state,
            do_not_track,
            send_anonymous_usage_stats,
        }
    }

    /// Override `do_not_track` for testing — avoids env var mutation in tests.
    #[cfg(test)]
    pub fn with_do_not_track(mut self, value: bool) -> Self {
        self.do_not_track = value;
        self
    }

    #[cfg(test)]
    pub fn with_send_anonymous_usage_stats(mut self, value: bool) -> Self {
        self.send_anonymous_usage_stats = value;
        self
    }

    pub fn dist_info(&self) -> DistInfo {
        self.providers.dist_info.dist_info()
    }

    pub fn server_version(&self) -> &'static str {
        self.providers.dist_info.server_version()
    }

    pub fn has_column_lineage(&self) -> bool {
        self.providers.column_lineage.is_available()
    }

    pub fn has_dbt_state(&self) -> bool {
        self.has_dbt_state
    }

    pub fn capabilities(&self) -> Capabilities {
        Capabilities {
            has_column_lineage: self.has_column_lineage(),
            has_dbt_state: self.has_dbt_state(),
        }
    }
}
