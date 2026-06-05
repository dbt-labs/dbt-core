use dbt_error::{ErrorCode, FsError};

use super::error::TracingError;

/// Fusion error bridge for tracing-owned errors.
///
/// This module's placement under tracing is temporary. It should move out of
/// tracing once the generic tracing code is extracted into its own crate and
/// `dbt-error` can depend on that crate directly.
impl From<TracingError> for FsError {
    fn from(error: TracingError) -> Self {
        let code = match error {
            TracingError::AlreadyInitialized | TracingError::SetGlobalSubscriber => {
                ErrorCode::Unexpected
            }
            TracingError::Io(_)
            | TracingError::ThreadJoin(_)
            | TracingError::ChannelClosed(_)
            | TracingError::Shutdown(_) => ErrorCode::IoError,
        };

        FsError::new(code, error.to_string())
    }
}
