use dbt_common::ErrorCode;
use dbt_common::io_args::FsCommand;
use dbt_common::tracing::dbt_metrics::{FusionMetricKey, RunCacheServiceMetricKey};
use dbt_common::tracing::emit::{
    emit_debug_log_message, emit_info_log_message, emit_warn_log_message,
};
use dbt_common::tracing::metrics::increment_metric;
use dbt_run_cache::metadata_cache::RunCacheMetadataCache;
use dbt_run_cache::service_client::{
    ClientVersionStatus, GrpcRunCacheServiceClient, SharedRunCacheServiceClient,
    format_error_chain, shared_run_cache_service_client, validate_client_version_fail_open,
};
use dbt_run_cache::service_config::RunCacheServiceConfig;
use dbt_schemas::schemas::profiles::Execute;
use std::sync::Arc;

use crate::RunTasksArgs;

#[derive(Clone)]
pub(crate) struct RunCacheServiceLifecycle {
    pub(crate) requested: bool,
    pub(crate) config: Option<RunCacheServiceConfig>,
    pub(crate) client: Option<SharedRunCacheServiceClient>,
}

#[derive(Clone)]
pub struct RunCacheLifecycle {
    pub(crate) service: RunCacheServiceLifecycle,
    pub(crate) metadata: Arc<RunCacheMetadataCache>,
}

impl RunCacheLifecycle {
    pub async fn initialize(arg: &RunTasksArgs, execute: Execute) -> Self {
        let service = initialize_run_cache_service(arg, execute).await;
        let metadata_ttl_seconds = service
            .config
            .as_ref()
            .map(|config| config.metadata_cache_ttl_seconds)
            .unwrap_or_default();

        Self {
            service,
            metadata: Arc::new(RunCacheMetadataCache::with_ttl_seconds(
                metadata_ttl_seconds,
            )),
        }
    }

    pub fn is_requested(&self) -> bool {
        self.service.requested
    }
}

async fn initialize_run_cache_service(
    arg: &RunTasksArgs,
    execute: Execute,
) -> RunCacheServiceLifecycle {
    let service_requested =
        arg.run_cache_service || RunCacheServiceConfig::is_explicitly_requested_from_env();
    if !service_requested || execute != Execute::Remote {
        increment_metric(
            FusionMetricKey::RunCacheService(RunCacheServiceMetricKey::Disabled),
            1,
        );
        return RunCacheServiceLifecycle {
            requested: false,
            config: None,
            client: None,
        };
    }

    let config = match RunCacheServiceConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            increment_metric(
                FusionMetricKey::RunCacheService(RunCacheServiceMetricKey::Disabled),
                1,
            );
            increment_metric(
                FusionMetricKey::RunCacheService(RunCacheServiceMetricKey::ClientInitFailure),
                1,
            );
            emit_warn_log_message(
                ErrorCode::RunCacheServiceWarn,
                format!(
                    "dbt State service config failed: {}; executing normally",
                    format_error_chain(&err)
                ),
                None,
            );
            return RunCacheServiceLifecycle {
                requested: true,
                config: None,
                client: None,
            };
        }
    };

    if !config.enabled {
        increment_metric(
            FusionMetricKey::RunCacheService(RunCacheServiceMetricKey::Disabled),
            1,
        );
        // User-facing operational signal: the service was explicitly requested but
        // disabled by config, so visibility into why no caching is happening should
        // be reachable with `--log-level debug` rather than trace-only.
        emit_debug_log_message("dbt State service disabled by configuration; executing normally");
        return RunCacheServiceLifecycle {
            requested: false,
            config: None,
            client: None,
        };
    }

    increment_metric(
        FusionMetricKey::RunCacheService(RunCacheServiceMetricKey::Enabled),
        1,
    );

    let client = match GrpcRunCacheServiceClient::connect(config.clone()).await {
        Ok(client) => {
            increment_metric(
                FusionMetricKey::RunCacheService(RunCacheServiceMetricKey::ClientInitSuccess),
                1,
            );
            client
        }
        Err(err) => {
            increment_metric(
                FusionMetricKey::RunCacheService(RunCacheServiceMetricKey::ClientInitFailure),
                1,
            );
            emit_warn_log_message(
                ErrorCode::RunCacheServiceWarn,
                format!(
                    "dbt State service client initialization failed: {}; executing normally",
                    format_error_chain(&err)
                ),
                None,
            );
            return RunCacheServiceLifecycle {
                requested: true,
                config: Some(config),
                client: None,
            };
        }
    };

    let validation_status = validate_client_version_fail_open(&client).await;
    match validation_status {
        ClientVersionStatus::Supported => {
            increment_metric(
                FusionMetricKey::RunCacheService(RunCacheServiceMetricKey::ValidationSupported),
                1,
            );
            emit_info_log_message(format!(
                "dbt State is enabled (endpoint {}, defer_to {})",
                config.endpoint_uri(),
                config.defer_to
            ));
            let shared_client = shared_run_cache_service_client(client);
            RunCacheServiceLifecycle {
                requested: true,
                config: Some(config),
                client: Some(shared_client),
            }
        }
        ClientVersionStatus::Unsupported => {
            increment_metric(
                FusionMetricKey::RunCacheService(RunCacheServiceMetricKey::ValidationUnsupported),
                1,
            );
            emit_warn_log_message(
                ErrorCode::RunCacheServiceWarn,
                "dbt State service does not support this client version; executing normally",
                None,
            );
            RunCacheServiceLifecycle {
                requested: true,
                config: Some(config),
                client: None,
            }
        }
        ClientVersionStatus::Skipped => {
            increment_metric(
                FusionMetricKey::RunCacheService(RunCacheServiceMetricKey::ValidationSkipped),
                1,
            );
            emit_warn_log_message(
                ErrorCode::RunCacheServiceWarn,
                "dbt State service validation was skipped; executing normally",
                None,
            );
            RunCacheServiceLifecycle {
                requested: true,
                config: Some(config),
                client: None,
            }
        }
    }
}

pub fn run_cache_auto_defer_command(command: FsCommand) -> bool {
    matches!(
        command,
        FsCommand::Compile
            | FsCommand::Run
            | FsCommand::Build
            | FsCommand::Test
            | FsCommand::Seed
            | FsCommand::Snapshot
    )
}
