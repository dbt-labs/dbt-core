use dbt_telemetry::{NodeOutcome, NodeSkipReason, NodeType, TestOutcome};
use tracing_subscriber::registry::{Extensions, ExtensionsMut};

use super::span_info::with_root_span;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricKey {
    TotalErrors,
    TotalWarnings,
    AutoFixSuggestions,
    NodeCounts(NodeType),
    NodeOutcomeCounts(NodeOutcome, NodeSkipReason, Option<TestOutcome>),
}

#[derive(Debug, Clone, Default)]
pub(super) struct MetricCounters {
    // It is thread-safe to use simple map and u64 for counters
    // because we hold them in span extensions, which provide thread-safe access
    counters: HashMap<MetricKey, u64>,
}

impl MetricCounters {
    fn increment(&mut self, key: MetricKey, value: u64) {
        *self.counters.entry(key).or_insert(0) += value;
    }

    fn get(&self, key: MetricKey) -> u64 {
        self.counters.get(&key).copied().unwrap_or(0)
    }
}

/// Increments a metric counter in the root span extensions
pub fn increment_metric(key: MetricKey, value: u64) {
    with_root_span(|root_span| {
        let mut extensions = root_span.extensions_mut();
        extensions
            .get_mut::<MetricCounters>()
            .map(|c| {
                c.increment(key, value);
            })
            .unwrap_or_else(|| {
                let mut new_counters = MetricCounters::default();
                new_counters.increment(key, value);
                extensions.insert(new_counters);
            });
    });
}

/// Increments a metric counter on span extensions directly. Caller is
/// responsible for ensuring that the extension belongs to the correct (invocation) span.
pub(super) fn increment_metric_on_span_extension(
    span_ext: &mut ExtensionsMut<'_>,
    key: MetricKey,
    value: u64,
) {
    span_ext
        .get_mut::<MetricCounters>()
        .map(|c| {
            c.increment(key, value);
        })
        .unwrap_or_else(|| {
            let mut new_counters = MetricCounters::default();
            new_counters.increment(key, value);
            span_ext.insert(new_counters);
        });
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
