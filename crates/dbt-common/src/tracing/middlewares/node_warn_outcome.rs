use dbt_telemetry::{
    LogMessage, LogRecordInfo, NodeEvaluated, SeverityNumber, TelemetryAttributes,
    set_node_warning_outcome_warned,
};

use super::super::{data_provider::DataProvider, layer::TelemetryMiddleware};

/// Middleware that automatically marks the current node span as having produced
/// warnings whenever a `Warn`-severity log record passes through the pipeline.
///
/// This must be placed **after** [`super::warn_error_options::TelemetryWarnErrorOptionsMiddleware`]
/// so that:
/// - Silenced warnings (dropped by that middleware) never reach here and don't set `WithWarnings`.
/// - Warnings upgraded to errors are seen at `Error` severity and are ignored here.
pub struct TelemetryNodeWarnOutcome;

impl TelemetryMiddleware for TelemetryNodeWarnOutcome {
    fn on_log_record(
        &self,
        log_record: LogRecordInfo,
        data_provider: &mut DataProvider<'_>,
    ) -> Option<LogRecordInfo> {
        if log_record.attributes.is::<LogMessage>()
            && log_record.severity_number == SeverityNumber::Warn
        {
            data_provider.with_ancestor_mut::<NodeEvaluated, TelemetryAttributes>(|attrs| {
                if let Some(ev) = attrs.downcast_mut::<NodeEvaluated>() {
                    set_node_warning_outcome_warned(ev);
                }
            });
        }
        Some(log_record)
    }
}
