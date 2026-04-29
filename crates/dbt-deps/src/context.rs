use std::collections::BTreeMap;

use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::IoArgs;
use dbt_jinja_utils::jinja_environment::JinjaEnv;

use crate::git_client::GitClientContext;
use crate::hub_client::{DBT_HUB_URL, HubClient};
use crate::network_client::retrying_http_client;
use crate::tarball_client::TarballClient;

/// Shared runtime dependencies for deps resolve/install flows.
pub struct DepsOperationContext<'a> {
    pub io: &'a IoArgs,
    pub vars: &'a BTreeMap<String, dbt_yaml::Value>,
    pub jinja_env: &'a JinjaEnv,
    pub cancellation: &'a CancellationToken,
    pub hub_registry: HubClient,
    pub tarball_client: TarballClient,
    pub git_client: GitClientContext,
    pub skip_private_deps: bool,
    pub version_check: bool,
    pub use_v2_compatible_package_downloads: bool,
}

impl<'a> DepsOperationContext<'a> {
    pub fn from_entry(
        io: &'a IoArgs,
        vars: &'a BTreeMap<String, dbt_yaml::Value>,
        jinja_env: &'a JinjaEnv,
        cancellation: &'a CancellationToken,
        skip_private_deps: bool,
        version_check: bool,
        use_v2_compatible_package_downloads: bool,
    ) -> Self {
        let hub_url_from_env = std::env::var("DBT_PACKAGE_HUB_URL");
        let hub_url = hub_url_from_env
            .as_deref()
            .map(|s| {
                if s.ends_with('/') {
                    // dbt-core required a trailing slash - here we support but do not require it.
                    &s[0..s.len() - 1]
                } else {
                    s
                }
            })
            .unwrap_or(DBT_HUB_URL);
        let http_client = retrying_http_client();
        let tarball_client = TarballClient::from_client(http_client.clone());

        Self {
            io,
            vars,
            jinja_env,
            cancellation,
            hub_registry: HubClient::with_client(hub_url, http_client.clone()),
            tarball_client: tarball_client.clone(),
            git_client: GitClientContext::from_clients(http_client, tarball_client),
            skip_private_deps,
            version_check,
            use_v2_compatible_package_downloads,
        }
    }
}
