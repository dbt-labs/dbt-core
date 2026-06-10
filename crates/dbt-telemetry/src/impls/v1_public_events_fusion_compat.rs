use crate::proto::v1::public::events::fusion::compat::SeverityNumber as ProtoSeverityNumber;
use dbt_tracing::SeverityNumber;

/// Convert the proto-derived severity into the tracing-library `SeverityNumber`.
///
/// Both enums share the same variants and discriminants; this bridges the
/// generated proto type used for `LogMessage` serde to the crate-owned type.
impl From<ProtoSeverityNumber> for SeverityNumber {
    fn from(value: ProtoSeverityNumber) -> Self {
        match value {
            ProtoSeverityNumber::Unspecified => SeverityNumber::Unspecified,
            ProtoSeverityNumber::Trace => SeverityNumber::Trace,
            ProtoSeverityNumber::Debug => SeverityNumber::Debug,
            ProtoSeverityNumber::Info => SeverityNumber::Info,
            ProtoSeverityNumber::Warn => SeverityNumber::Warn,
            ProtoSeverityNumber::Error => SeverityNumber::Error,
        }
    }
}
