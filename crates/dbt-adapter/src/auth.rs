use std::sync::Arc;

pub use dbt_auth::Auth;
pub use dbt_auth::NoopAuthWarningPrinter;

use dbt_auth::AuthWarningPrinter;
use dbt_common::ErrorCode;
use dbt_common::io_utils::StatusReporter;
use dbt_common::tracing::dbt_emit::emit_warn_log_message;

pub struct DefaultAuthWarningPrinter {
    status_reporter: Option<Arc<dyn StatusReporter>>,
}

impl DefaultAuthWarningPrinter {
    pub fn new(status_reporter: Option<Arc<dyn StatusReporter>>) -> Self {
        Self { status_reporter }
    }
}

impl AuthWarningPrinter for DefaultAuthWarningPrinter {
    fn warn(&self, msg: &str) {
        emit_warn_log_message(ErrorCode::Generic, msg, self.status_reporter.as_ref());
    }
}
