use async_trait::async_trait;
use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::schemas::profiles::DbConfig;

/// Outcome of verifying that a write made through a lake compute
/// target has become visible via the profile's native connection (e.g. a
/// linked-catalog round trip).
#[derive(Debug, Clone)]
pub enum LakeComputePropagationOutcome {
    /// The probe object was visible through the native connection.
    Verified,
    /// The probe object was not yet visible after waiting. Not necessarily a
    /// failure: propagation to the native connection can be asynchronous.
    NotYetVisible {
        waited_secs: u64,
        configured_refresh_secs: Option<u64>,
    },
}

/// Extension point for verifying propagation between a lake compute
/// target and the profile's native connection during `dbt debug`. A build
/// that doesn't support this check simply doesn't register an implementation
/// (see `lake_compute_propagation_checker()` on the CLI hooks it's wired through).
#[async_trait]
pub trait LakeComputePropagationChecker: Send + Sync {
    // `linked_database` is the Snowflake database linked to the external
    // catalog (`catalogs.yml`'s `catalog_database` / `catalog_linked_database`),
    // i.e. where a propagated write should become visible.
    async fn check_lake_compute_propagation(
        &self,
        native_db_config: &DbConfig,
        lake_compute_db_config: &DbConfig,
        linked_database: &str,
        token: CancellationToken,
    ) -> FsResult<LakeComputePropagationOutcome>;
}
