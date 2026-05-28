use dbt_error::ErrorCode;
use dbt_telemetry::{LogMessage, LogRecordInfo};

/// Checks if this log record is from an ErrorCode::ExitWithStatus which is
/// a pseudo error used to short-circuit execution after a real error
/// has already been emitted elsewhere.
///
/// dbt-facing sinks should treat it as control flow, not as a user-visible error event.
pub fn is_exit_with_status_log(log_record: &LogRecordInfo) -> bool {
    log_record
        .attributes
        .downcast_ref::<LogMessage>()
        .and_then(|message| message.code)
        .and_then(|code| u16::try_from(code).ok())
        .and_then(|code| ErrorCode::try_from(code).ok())
        == Some(ErrorCode::ExitWithStatus)
}
