use async_trait::async_trait;
use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::schemas::profiles::DbConfig;

/// Outcome of verifying that a write made through an alt/remote compute
/// target has become visible via the profile's native connection (e.g. a
/// linked-catalog round trip).
#[derive(Debug, Clone)]
pub enum AltPropagationOutcome {
    /// The probe object was visible through the native connection.
    Verified,
    /// The probe object was not yet visible after waiting. Not necessarily a
    /// failure: propagation to the native connection can be asynchronous.
    NotYetVisible {
        waited_secs: u64,
        configured_refresh_secs: Option<u64>,
    },
}

/// Extension point for verifying propagation between an alt/remote compute
/// target and the profile's native connection during `dbt debug`. A build
/// that doesn't support this check simply doesn't register an implementation
/// (see `alt_propagation_checker()` on the CLI hooks it's wired through).
#[async_trait]
pub trait AltPropagationChecker: Send + Sync {
    // `linked_database` is the Snowflake database linked to the external
    // catalog (`catalogs.yml`'s `catalog_database` / `catalog_linked_database`),
    // i.e. where a propagated write should become visible.
    async fn check_alt_propagation(
        &self,
        native_db_config: &DbConfig,
        alt_db_config: &DbConfig,
        linked_database: &str,
        token: CancellationToken,
    ) -> FsResult<AltPropagationOutcome>;
}
