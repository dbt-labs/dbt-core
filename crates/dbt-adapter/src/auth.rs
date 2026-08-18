pub use dbt_auth::Auth;
pub use dbt_auth::NoopAuthWarningPrinter;

use dbt_auth::AuthWarningPrinter;
use dbt_common::ErrorCode;
use dbt_common::tracing::dbt_emit::emit_warn_log_message;

pub struct DefaultAuthWarningPrinter;

impl DefaultAuthWarningPrinter {
    pub fn new() -> Self {
        Self
    }
}

impl AuthWarningPrinter for DefaultAuthWarningPrinter {
    fn warn(&self, msg: &str) {
        emit_warn_log_message(ErrorCode::Generic, msg);
    }
}
