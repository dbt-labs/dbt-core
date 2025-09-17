use crate::v1::public::events::fusion::compat::SeverityNumber;

/// Convert our proto defined severity level to OpenTelemetry severity number.
/// Panics if `SeverityNumber::Unspecified` is provided.
pub const fn level_to_otel_severity_text(severity_number: &SeverityNumber) -> &'static str {
    match severity_number {
        SeverityNumber::Unspecified => panic!("Do not use unspecified severity level!"),
        SeverityNumber::Trace => "TRACE",
        SeverityNumber::Debug => "DEBUG",
        SeverityNumber::Info => "INFO",
        SeverityNumber::Warn => "WARN",
        SeverityNumber::Error => "ERROR",
    }
}
