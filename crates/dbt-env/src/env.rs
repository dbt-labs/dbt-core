use std::env;
use std::sync::OnceLock;

use dbt_schemas::schemas::ResolvedCloudConfig;

#[derive(Debug)]
pub struct VortexConfig {
    pub base_url: String,
    pub ingest_endpoint: String,
    pub dev_mode: String,
    pub dev_mode_output_path: String,
}

impl VortexConfig {}

#[derive(Debug)]
pub struct InvocationConfig {
    pub dbt_version: String,
    pub environment: String,
    pub account_identifier: String,
    pub project_id: String,
    pub environment_id: String,
    pub job_id: String,
}

#[derive(Debug)]
pub struct InternalEnv {
    vortex_config: VortexConfig,
    invocation_config: InvocationConfig,
}

impl InternalEnv {
    fn from_env() -> Self {
        Self::new(None)
    }

    pub fn new(cloud_config: Option<&ResolvedCloudConfig>) -> Self {
        let version = env!("CARGO_PKG_VERSION").to_string();
        Self {
            vortex_config: VortexConfig {
                base_url: env::var("VORTEX_BASE_URL")
                    .unwrap_or_else(|_| "https://p.vx.dbt.com".to_string()),
                ingest_endpoint: env::var("VORTEX_INGEST_ENDPOINT")
                    .unwrap_or_else(|_| "/v1/ingest/protobuf".to_string()),
                dev_mode: env::var("VORTEX_DEV_MODE").unwrap_or_else(|_| "false".to_string()),
                dev_mode_output_path: env::var("VORTEX_DEV_MODE_OUTPUT_PATH")
                    .unwrap_or_else(|_| "/tmp/vortex_dev_mode_output.jsonl".to_string()),
            },
            invocation_config: InvocationConfig {
                dbt_version: version,
                environment: env::var("DBT_INVOCATION_ENV")
                    .unwrap_or_else(|_| "manual".to_string()),
                // TODO(fde-502): Remove env var fallbacks once InternalEnv is no longer
                // a global singleton. The global path (from_env) passes None for
                // cloud_config, so we fall back to env vars to preserve telemetry fields.
                account_identifier: cloud_config
                    .and_then(|c| c.account_identifier.clone())
                    .or_else(|| env::var("DBT_CLOUD_ACCOUNT_IDENTIFIER").ok())
                    .unwrap_or_default(),
                project_id: cloud_config
                    .and_then(|c| c.project_id.clone())
                    .or_else(|| env::var("DBT_CLOUD_PROJECT_ID").ok())
                    .unwrap_or_default(),
                environment_id: cloud_config
                    .and_then(|c| c.environment_id.clone())
                    .or_else(|| env::var("DBT_CLOUD_ENVIRONMENT_ID").ok())
                    .unwrap_or_default(),
                job_id: env::var("DBT_CLOUD_JOB_ID").unwrap_or_default(),
            },
        }
    }

    pub fn global() -> &'static Self {
        static INSTANCE: OnceLock<InternalEnv> = OnceLock::new();
        INSTANCE.get_or_init(InternalEnv::from_env)
    }

    pub fn vortex_config(&self) -> &VortexConfig {
        &self.vortex_config
    }

    pub fn invocation_config(&self) -> &InvocationConfig {
        &self.invocation_config
    }
}

const TRUE_VALUES: [&str; 4] = ["1", "true", "yes", "on"];
const FALSE_VALUES: [&str; 5] = ["0", "false", "no", "off", ""];

/// Returns `true` if the environment variable is unset or set to a recognized falsy value.
///
/// Unrecognized values return `true` (treat as not enabled).
pub fn env_var_is_disabled(var_name: &str) -> bool {
    matches!(env_var_bool(var_name), Ok(false) | Err(_))
}

/// Parse a boolean from an environment variable.
///
/// Returns `Ok(false)` if the variable is not set.
/// Returns `Err` with a descriptive message if the value is not a recognized boolean.
///
/// Recognized true values: `1`, `true`, `yes`, `on` (case-insensitive)
/// Recognized false values: `0`, `false`, `no`, `off`, `` (empty string, case-insensitive)
pub fn env_var_bool(var_name: &str) -> Result<bool, String> {
    match env::var_os(var_name) {
        Some(val) => {
            if TRUE_VALUES.iter().any(|s| val.eq_ignore_ascii_case(s)) {
                Ok(true)
            } else if FALSE_VALUES.iter().any(|s| val.eq_ignore_ascii_case(s)) {
                Ok(false)
            } else {
                Err(format!(
                    "Invalid value for environment variable {var_name:?}: {val:?}. Expected one of: {} (true) or {} (false).",
                    TRUE_VALUES.join(", "),
                    FALSE_VALUES.join(", ")
                ))
            }
        }
        None => Ok(false),
    }
}
