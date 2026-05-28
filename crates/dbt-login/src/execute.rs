use dbt_common::FsResult;

use crate::LicenseFetcher;

pub async fn execute_login(fetcher: &dyn LicenseFetcher) -> FsResult<()> {
    // TODO: run OAuth flow via dbt-platform-auth (AuthChain), then fetch and cache license
    fetcher.fetch_and_cache_license().await
}
