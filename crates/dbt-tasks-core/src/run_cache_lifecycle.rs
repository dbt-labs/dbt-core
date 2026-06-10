use dbt_adapter_core::AdapterType;
use dbt_common::ErrorCode;
use dbt_common::io_args::FsCommand;
use dbt_common::tracing::dbt_emit::{
    emit_debug_log_message, emit_info_log_message, emit_warn_log_message,
};
use dbt_common::tracing::dbt_metrics::{FusionMetricKey, RunCacheServiceMetricKey};
use dbt_common::tracing::metrics::increment_metric;
use dbt_schemas::schemas::profiles::Execute;
use dbt_state::metadata_cache::RunCacheMetadataCache;
use dbt_state::service_client::{
    ClientVersionStatus, GrpcRunCacheServiceClient, SharedRunCacheServiceClient,
    format_error_chain, shared_run_cache_service_client, validate_client_version_fail_open,
};
use dbt_state::service_config::RunCacheServiceConfig;
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
    pub async fn initialize(
        arg: &RunTasksArgs,
        execute: Execute,
        adapter_type: AdapterType,
    ) -> Self {
        let service = initialize_run_cache_service(arg, execute, adapter_type).await;
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
    adapter_type: AdapterType,
) -> RunCacheServiceLifecycle {
    if !should_initialize_run_cache_service(
        arg,
        execute,
        RunCacheServiceConfig::is_explicitly_requested_from_env(),
        adapter_type,
    ) {
        increment_metric(
            FusionMetricKey::RunCacheService(RunCacheServiceMetricKey::Disabled),
            1,
        );
        if execute == Execute::Remote && RunCacheServiceConfig::is_explicitly_disabled_from_env() {
            emit_debug_log_message(
                "dbt State service disabled by configuration; executing normally",
            );
        }
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
                ErrorCode::StateServiceWarn,
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
                ErrorCode::StateServiceWarn,
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
                ErrorCode::StateServiceWarn,
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
                ErrorCode::StateServiceWarn,
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

fn should_initialize_run_cache_service(
    arg: &RunTasksArgs,
    execute: Execute,
    env_requested: bool,
    adapter_type: AdapterType,
) -> bool {
    execute == Execute::Remote
        && adapter_supports_dbt_state(adapter_type)
        && (arg.run_cache_service || env_requested)
}

/// Returns true when the adapter is supported by the dbt State service.
pub fn adapter_supports_dbt_state(adapter_type: AdapterType) -> bool {
    matches!(
        adapter_type,
        AdapterType::Snowflake
            | AdapterType::Databricks
            | AdapterType::Spark
            | AdapterType::Redshift
            | AdapterType::Bigquery
    )
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

#[cfg(test)]
mod tests {
    use dbt_adapter_core::AdapterType;
    use dbt_schemas::schemas::profiles::Execute;

    use super::{RunTasksArgs, adapter_supports_dbt_state, should_initialize_run_cache_service};

    fn args() -> RunTasksArgs {
        RunTasksArgs::default()
    }

    #[test]
    fn lifecycle_does_not_request_service_without_explicit_request() {
        assert!(!should_initialize_run_cache_service(
            &args(),
            Execute::Remote,
            false,
            AdapterType::Snowflake,
        ));
    }

    #[test]
    fn lifecycle_requests_service_from_explicit_env_opt_in() {
        assert!(should_initialize_run_cache_service(
            &args(),
            Execute::Remote,
            true,
            AdapterType::Snowflake,
        ));
    }

    #[test]
    fn lifecycle_requests_service_from_cli_flag() {
        let mut args = args();
        args.run_cache_service = true;

        assert!(should_initialize_run_cache_service(
            &args,
            Execute::Remote,
            false,
            AdapterType::Snowflake,
        ));
    }

    #[test]
    fn lifecycle_requires_remote_compute() {
        assert!(!should_initialize_run_cache_service(
            &args(),
            Execute::Local,
            true,
            AdapterType::Snowflake,
        ));

        let mut args = args();
        args.run_cache_service = true;

        assert!(!should_initialize_run_cache_service(
            &args,
            Execute::Local,
            false,
            AdapterType::Snowflake,
        ));
    }

    #[test]
    fn lifecycle_requires_supported_adapter() {
        let mut requested_args = args();
        requested_args.run_cache_service = true;

        assert!(!should_initialize_run_cache_service(
            &requested_args,
            Execute::Remote,
            false,
            AdapterType::DuckDB,
        ));
        assert!(!should_initialize_run_cache_service(
            &args(),
            Execute::Remote,
            true,
            AdapterType::DuckDB,
        ));
    }

    #[test]
    fn dbt_state_supported_adapters_are_explicit() {
        assert!(adapter_supports_dbt_state(AdapterType::Snowflake));
        assert!(adapter_supports_dbt_state(AdapterType::Databricks));
        assert!(adapter_supports_dbt_state(AdapterType::Spark));
        assert!(adapter_supports_dbt_state(AdapterType::Redshift));
        assert!(adapter_supports_dbt_state(AdapterType::Bigquery));

        assert!(!adapter_supports_dbt_state(AdapterType::DuckDB));
        assert!(!adapter_supports_dbt_state(AdapterType::Postgres));
        assert!(!adapter_supports_dbt_state(AdapterType::ClickHouse));
        assert!(!adapter_supports_dbt_state(AdapterType::Fabric));
        assert!(!adapter_supports_dbt_state(AdapterType::Salesforce));
    }
}
