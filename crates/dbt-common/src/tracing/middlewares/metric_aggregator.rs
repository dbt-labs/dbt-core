use dbt_telemetry::{
    Invocation, InvocationMetrics, LogMessage, LogRecordInfo, NodeOutcome, NodeSkipReason,
    SeverityNumber, SpanEndInfo, TestOutcome,
};

use super::super::{
    data_provider::DataProvider,
    layer::TelemetryMiddleware,
    metrics::{InvocationMetricKey, MetricKey},
};

/// Middleware that aggregates telemetry metrics from span and log records.
pub struct TelemetryMetricAggregator;

impl TelemetryMiddleware for TelemetryMetricAggregator {
    fn on_span_end(
        &self,
        mut span: SpanEndInfo,
        data_provider: &mut DataProvider<'_>,
    ) -> Option<SpanEndInfo> {
        if let Some(invocation) = span.attributes.downcast_mut::<Invocation>() {
            debug_assert!(
                span.parent_span_id.is_none(),
                "Expected Invocation span to be root, found paren id: {:?}",
                span.parent_span_id
            );

            // Aggregate node outcome metrics into top-level metrics on invocation end
            let mut success = 0u64;
            let mut warning = 0u64;
            let mut error = 0u64;
            let mut skipped = 0u64;
            let mut reused = 0u64;
            let mut canceled = 0u64;

            for ((outcome, skip_reason, test_outcome), count) in data_provider
                .get_all_metrics()
                .iter()
                .filter_map(|(key, count)| match key {
                    MetricKey::NodeOutcomeCounts(outcome_key) if *count > 0 => {
                        Some((outcome_key.into_parts(), *count))
                    }
                    _ => None,
                })
            {
                match outcome {
                    NodeOutcome::Success => match test_outcome {
                        Some(TestOutcome::Failed) => error += count,
                        Some(TestOutcome::Warned) => warning += count,
                        _ => success += count,
                    },
                    NodeOutcome::Error => error += count,
                    NodeOutcome::Skipped => {
                        if skip_reason == NodeSkipReason::Cached {
                            reused += count;
                        } else {
                            skipped += count;
                        }
                    }
                    NodeOutcome::Canceled => canceled += count,
                    NodeOutcome::Unspecified => {}
                }
            }

            // Update aggregated metrics
            data_provider.increment_metric(
                MetricKey::InvocationMetric(InvocationMetricKey::NodeTotalsSuccess),
                success,
            );
            data_provider.increment_metric(
                MetricKey::InvocationMetric(InvocationMetricKey::NodeTotalsWarning),
                warning,
            );
            data_provider.increment_metric(
                MetricKey::InvocationMetric(InvocationMetricKey::NodeTotalsError),
                error,
            );
            data_provider.increment_metric(
                MetricKey::InvocationMetric(InvocationMetricKey::NodeTotalsReused),
                reused,
            );
            data_provider.increment_metric(
                MetricKey::InvocationMetric(InvocationMetricKey::NodeTotalsSkipped),
                skipped,
            );
            data_provider.increment_metric(
                MetricKey::InvocationMetric(InvocationMetricKey::NodeTotalsCanceled),
                canceled,
            );

            // Store totals in invocation attributes
            invocation.metrics = Some(InvocationMetrics {
                total_errors: Some(data_provider.get_metric(MetricKey::InvocationMetric(
                    InvocationMetricKey::TotalErrors,
                ))),
                total_warnings: Some(data_provider.get_metric(MetricKey::InvocationMetric(
                    InvocationMetricKey::TotalWarnings,
                ))),
                autofix_suggestions: Some(data_provider.get_metric(MetricKey::InvocationMetric(
                    InvocationMetricKey::AutoFixSuggestions,
                ))),
            });
        }

        Some(span)
    }

    fn on_log_record(
        &self,
        log_record: LogRecordInfo,
        data_provider: &mut DataProvider<'_>,
    ) -> Option<LogRecordInfo> {
        if log_record.attributes.is::<LogMessage>() {
            match log_record.severity_number {
                SeverityNumber::Error => {
                    data_provider.increment_metric(
                        MetricKey::InvocationMetric(InvocationMetricKey::TotalErrors),
                        1,
                    );
                }
                SeverityNumber::Warn => {
                    data_provider.increment_metric(
                        MetricKey::InvocationMetric(InvocationMetricKey::TotalWarnings),
                        1,
                    );
                }
                _ => {}
            }
        }

        Some(log_record)
    }
}
