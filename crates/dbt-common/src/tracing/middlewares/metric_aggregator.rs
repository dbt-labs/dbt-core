use dbt_telemetry::{
    Invocation, InvocationMetrics, LogMessage, LogRecordInfo, NodeOutcome, NodeProcessed,
    NodeSkipReason, SeverityNumber, SpanEndInfo, TestOutcome, node_processed::NodeOutcomeDetail,
};
use std::collections::HashMap;

use crate::tracing::metrics::NodeOutcomeCountsKey;

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
            // The Invocation span should be the root of our tracing span tree.
            // However, it may have a parent_span_id if one was explicitly provided via CLI
            // for OTEL trace correlation. In that case, span.parent_span_id should match
            // invocation.parent_span_id. If they differ, it means the span inherited a
            // parent from the tracing context, which shouldn't happen for Invocation spans.
            debug_assert!(
                span.parent_span_id.is_none() || span.parent_span_id == invocation.parent_span_id,
                "Expected Invocation span to be root, found inherited parent id: {:?} (invocation parent: {:?})",
                span.parent_span_id,
                invocation.parent_span_id
            );

            // Aggregate node outcome metrics into top-level metrics on invocation end
            let mut success = 0u64;
            let mut warning = 0u64;
            let mut error = 0u64;
            let mut skipped = 0u64;
            let mut reused = 0u64;
            let mut canceled = 0u64;
            let mut no_op = 0u64;

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
                        } else if skip_reason == NodeSkipReason::NoOp {
                            no_op += count;
                        } else {
                            skipped += count;
                        }
                    }
                    NodeOutcome::Canceled => canceled += count,
                    NodeOutcome::Unspecified => no_op += count,
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
            data_provider.increment_metric(
                MetricKey::InvocationMetric(InvocationMetricKey::NodeTotalsNoOp),
                no_op,
            );

            // Build node_type_counts hashmap from NodeCounts metrics
            let node_type_counts: HashMap<String, u64> = data_provider
                .get_all_metrics()
                .iter()
                .filter_map(|(key, count)| match key {
                    MetricKey::NodeCounts(node_type) if *count > 0 => {
                        Some((node_type.as_static_ref().to_string(), *count))
                    }
                    _ => None,
                })
                .collect();

            // Build status_counts hashmap from aggregated node outcome totals
            let mut status_counts = HashMap::new();

            for (key, val) in &[
                ("success", success),
                ("warn", warning),
                ("error", error),
                ("reused", reused),
                ("skipped", skipped),
                ("canceled", canceled),
                ("no_op", no_op),
            ] {
                if *val == 0 {
                    continue;
                }

                status_counts.insert((*key).to_string(), *val);
            }

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
                node_type_counts,
                status_counts,
            });
        }

        // Count node processed spans
        if let Some(attrs) = span.attributes.downcast_ref::<NodeProcessed>()
            && attrs.in_selection
        {
            let key = NodeOutcomeCountsKey::new(
                attrs.node_outcome(),
                attrs.node_skip_reason(),
                if let Some(NodeOutcomeDetail::NodeTestDetail(ted)) = &attrs.node_outcome_detail {
                    Some(ted.test_outcome())
                } else {
                    None
                },
            );
            data_provider.increment_metric(MetricKey::NodeOutcomeCounts(key), 1);
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
