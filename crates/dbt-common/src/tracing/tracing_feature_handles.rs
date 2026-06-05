use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::constants::DBT_FUSION;
use crate::warn_error_options::WarnErrorOptions;

/// Provider of runtime tracing features, such as configuration mutation,
/// or accessing tracing state.
///
/// This trait should be extended when runtime code needs to mutate or read
/// tracing settings.
pub trait TracingConfigProvider: Send + Sync {
    fn set_warn_error_options(&self, warn_error_options: WarnErrorOptions);
    fn get_file_log_path(&self) -> Option<&Path>;
    /// User-facing CLI brand name (e.g. "dbt-core") for the version banner
    /// and JSON log lines.
    fn get_command_name(&self) -> &'static str;
}

struct NoOpTracingConfigProvider;

impl TracingConfigProvider for NoOpTracingConfigProvider {
    fn set_warn_error_options(&self, _warn_error_options: WarnErrorOptions) {}
    fn get_file_log_path(&self) -> Option<&Path> {
        None
    }
    fn get_command_name(&self) -> &'static str {
        DBT_FUSION
    }
}

pub fn noop_tracing_config_provider() -> Box<dyn TracingConfigProvider> {
    Box::new(NoOpTracingConfigProvider)
}

// TODO: move to dbt-features
struct FsTracingConfigProvider {
    pub warn_error_options: Arc<RwLock<WarnErrorOptions>>,
    pub file_log_path: Option<PathBuf>,
    pub command_name: &'static str,
}

impl TracingConfigProvider for FsTracingConfigProvider {
    fn set_warn_error_options(&self, warn_error_options: WarnErrorOptions) {
        *self
            .warn_error_options
            .write()
            .expect("warn_error_options lock should not be poisoned") = warn_error_options;
    }

    fn get_file_log_path(&self) -> Option<&Path> {
        self.file_log_path.as_deref()
    }

    fn get_command_name(&self) -> &'static str {
        self.command_name
    }
}

pub fn create_tracing_config_provider(
    warn_error_options: Arc<RwLock<WarnErrorOptions>>,
    file_log_path: Option<PathBuf>,
    command_name: &'static str,
) -> Box<dyn TracingConfigProvider> {
    let provider = FsTracingConfigProvider {
        warn_error_options,
        file_log_path,
        command_name,
    };
    Box::new(provider)
}
