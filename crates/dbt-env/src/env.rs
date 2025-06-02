use std::env;
use std::sync::OnceLock;

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
                account_identifier: env::var("DBT_CLOUD_ACCOUNT_IDENTIFIER")
                    .unwrap_or_else(|_| "".to_string()),
                project_id: env::var("DBT_CLOUD_PROJECT_ID").unwrap_or_else(|_| "".to_string()),
                environment_id: env::var("DBT_CLOUD_ENVIRONMENT_ID")
                    .unwrap_or_else(|_| "".to_string()),
                job_id: env::var("DBT_CLOUD_JOB_ID").unwrap_or_else(|_| "".to_string()),
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
