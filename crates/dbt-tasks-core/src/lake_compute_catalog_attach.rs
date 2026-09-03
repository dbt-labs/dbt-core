use async_trait::async_trait;
use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::schemas::profiles::DbConfig;

/// Outcome of asking a lake compute target to attach the catalogs a
/// project declares in `catalogs.yml`.
#[derive(Debug, Clone)]
pub enum LakeComputeCatalogAttachOutcome {
    /// Every declared catalog attached. Carries the catalog names that were
    /// checked, in declaration order, so the caller can name them in its
    /// output.
    Attached { catalogs: Vec<String> },
    /// The project declares no catalogs this check applies to, so nothing was
    /// attempted. Not a failure.
    NothingToCheck,
}

/// Extension point for checking, during `dbt debug`, that the catalogs a
/// project declares are reachable and authorized from a lake compute
/// target -- a cheaper, earlier failure than exercising a full write. A build
/// that doesn't support this check simply doesn't register an implementation
/// (see `lake_compute_catalog_attach_checker()` on the CLI hooks it's wired through).
#[async_trait]
pub trait LakeComputeCatalogAttachChecker: Send + Sync {
    /// `native_db_config` is the profile's active target, used to obtain a
    /// short-lived credential for the declared catalogs; `lake_compute_db_config` is
    /// the target asked to perform the attach.
    async fn check_catalog_attach(
        &self,
        native_db_config: &DbConfig,
        lake_compute_db_config: &DbConfig,
        token: CancellationToken,
    ) -> FsResult<LakeComputeCatalogAttachOutcome>;
}
