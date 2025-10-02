use dbt_telemetry::{NodeOutcome, NodeSkipReason, NodeType, TestOutcome};
use tracing_subscriber::registry::Extensions;

use super::span_info::{SpanAccess, with_root_span};
use std::sync::atomic::AtomicU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricKey {
    TotalErrors,
    TotalWarnings,
    AutoFixSuggestions,
    NodeCounts(NodeType),
    NodeOutcomeCounts(NodeOutcome, NodeSkipReason, Option<TestOutcome>),
}

#[derive(Debug, Default)]
pub(super) struct MetricCounters {
    // Using AtomicU64 for most used top-level metrics
    total_errors: AtomicU64,
    total_warnings: AtomicU64,
    auto_fix_suggestions: AtomicU64,
    // Other metrics with complex keys stored in a map
    metrics: scc::HashMap<MetricKey, u64>,
}

impl MetricCounters {
    pub(super) fn new() -> Self {
        Self::default()
    }

    fn increment(&self, key: MetricKey, value: u64) {
        match key {
            MetricKey::TotalErrors => {
                self.total_errors
                    .fetch_add(value, std::sync::atomic::Ordering::Relaxed);
            }
            MetricKey::TotalWarnings => {
                self.total_warnings
                    .fetch_add(value, std::sync::atomic::Ordering::Relaxed);
            }
            MetricKey::AutoFixSuggestions => {
                self.auto_fix_suggestions
                    .fetch_add(value, std::sync::atomic::Ordering::Relaxed);
            }
            _ => {
                self.metrics
                    .entry_sync(key)
                    .and_modify(|v| *v = v.saturating_add(value))
                    .or_insert(value);
            }
        }
    }

    fn get(&self, key: MetricKey) -> u64 {
        match key {
            MetricKey::TotalErrors => self.total_errors.load(std::sync::atomic::Ordering::Relaxed),
            MetricKey::TotalWarnings => self
                .total_warnings
                .load(std::sync::atomic::Ordering::Relaxed),
            MetricKey::AutoFixSuggestions => self
                .auto_fix_suggestions
                .load(std::sync::atomic::Ordering::Relaxed),
            _ => self.metrics.read_sync(&key, |_, v| *v).unwrap_or_default(),
        }
    }
}

/// Increments an invocation metric counter
pub fn increment_metric(key: MetricKey, value: u64) {
    with_root_span(|root_span| {
        increment_metric_on_span(&root_span as &dyn SpanAccess, key, value);
    });
}

/// Increments a metric counter on span extensions directly. Caller is
/// responsible for ensuring that the extension belongs to the correct (invocation) span.
///
/// Note: This function never takes a mutable lock on extensions to avoid global contention.
/// Metric storage is pre-initialized in data layer when any root span is created.
///
/// It will silently do nothing if the extension is not found.
pub(super) fn increment_metric_on_span(root_span: &dyn SpanAccess, key: MetricKey, value: u64) {
    // By default do not take a mutable lock on extensions to avoid global contention
    if let Some(metrics) = root_span.extensions().get::<MetricCounters>() {
        metrics.increment(key, value);
    };
}

/// Gets a specific invocation totals metrics directly from span extension. Caller is
/// responsible for ensuring that the extension belongs to the correct (invocation) span.
pub(super) fn get_metric_from_span_extension(span_ext: &Extensions<'_>, key: MetricKey) -> u64 {
    span_ext
        .get::<MetricCounters>()
        .map(|counters| counters.get(key))
        .unwrap_or_default()
}

/// Gets a specific invocation totals metrics (stored in the root invocation span).
pub fn get_metric(key: MetricKey) -> u64 {
    with_root_span(|root_span| get_metric_from_span_extension(&root_span.extensions(), key))
        .unwrap_or_default()
}
