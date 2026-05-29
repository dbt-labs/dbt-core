//! Extension points for customizing loader behavior.
use async_trait::async_trait;
use dbt_cloud_config::ResolvedCloudConfig;
use dbt_common::{FsResult, io_args::IoArgs};
use dbt_schemas::schemas::packages::UpstreamProject;

/// Hooks called during the load phase.
#[async_trait]
pub trait LoaderHooks: Send + Sync {
    async fn download_artifacts(
        &self,
        _upstream_projects: &[UpstreamProject],
        _cloud_config: &Option<ResolvedCloudConfig>,
        _io: &IoArgs,
    ) -> FsResult<()> {
        Ok(())
    }
}

pub struct NoOpLoaderHooks;

#[async_trait]
impl LoaderHooks for NoOpLoaderHooks {}
