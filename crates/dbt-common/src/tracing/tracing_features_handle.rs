use std::path::Path;

use crate::warn_error_options::WarnErrorOptions;

/// Provider of runtime tracing features, such as configuration mutation,
/// or accessing tracing state.
///
/// This trait should be extended when runtime code needs to mutate or read
/// tracing settings.
pub trait TracingFeatures: Send + Sync {
    fn set_warn_error_options(&self, warn_error_options: WarnErrorOptions);
    fn get_file_log_path(&self) -> Option<&Path>;
}

pub type TracingFeaturesHandle = Box<dyn TracingFeatures>;

struct NoOpTracingHandle;

impl TracingFeatures for NoOpTracingHandle {
    fn set_warn_error_options(&self, _warn_error_options: WarnErrorOptions) {}
    fn get_file_log_path(&self) -> Option<&Path> {
        None
    }
}

pub fn noop_tracing_handle() -> TracingFeaturesHandle {
    Box::new(NoOpTracingHandle)
}
