use dbt_telemetry::{
    LogMessage, LogRecordInfo, NodeEvaluated, NodeOutcomeDetail, SeverityNumber, SpanEndInfo,
};

use super::super::{
    data_provider::DataProviderMut, layer::TelemetryMiddleware, metrics::MetricKey,
};

/// Middleware that aggregates telemetry metrics from span and log records.
pub struct TelemetryMetricAggregator;

impl TelemetryMiddleware for TelemetryMetricAggregator {
    fn on_span_end(
        &self,
        span: SpanEndInfo,
        metric_provider: &mut DataProviderMut<'_>,
    ) -> Option<SpanEndInfo> {
        if let Some(node_evaluated) = span.attributes.downcast_ref::<NodeEvaluated>() {
            let test_outcome = node_evaluated
                .node_outcome_detail
                .and_then(|detail| match detail {
                    NodeOutcomeDetail::NodeTestDetail(test_detail) => {
                        Some(test_detail.test_outcome())
                    }
                    _ => None,
                });

            metric_provider.increment_metric(
                MetricKey::NodeOutcomeCounts(
                    node_evaluated.node_outcome(),
                    node_evaluated.node_skip_reason(),
                    test_outcome,
                ),
                1,
            );
        }

        Some(span)
    }

    fn on_log_record(
        &self,
        log_record: LogRecordInfo,
        metric_provider: &mut DataProviderMut<'_>,
    ) -> Option<LogRecordInfo> {
        if log_record.attributes.is::<LogMessage>() {
            match log_record.severity_number {
                SeverityNumber::Error => {
                    metric_provider.increment_metric(MetricKey::TotalErrors, 1);
                }
                SeverityNumber::Warn => {
                    metric_provider.increment_metric(MetricKey::TotalWarnings, 1);
                }
                _ => {}
            }
        }

        Some(log_record)
    }
}
