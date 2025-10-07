use dbt_telemetry::{NodeOutcome, NodeSkipReason, NodeType, TestOutcome};
use strum::EnumCount as _;
#[cfg(test)]
use strum_macros::EnumIter;
use strum_macros::{EnumCount, FromRepr};
use tracing_subscriber::registry::Extensions;

use super::span_info::{SpanAccess, with_root_span};
use std::sync::atomic::AtomicU64;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumCount, FromRepr)]
#[cfg_attr(test, derive(EnumIter))]
pub enum InvocationMetricKey {
    TotalErrors = 0,
    TotalWarnings,
    AutoFixSuggestions,
    // Run summary totals based on node outcomes. These may change or fold into
    // becoming an actual log report later.
    NodeTotalsSuccess,
    NodeTotalsWarning,
    NodeTotalsError,
    NodeTotalsReused,
    NodeTotalsSkipped,
    NodeTotalsCanceled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeOutcomeCountsKey(NodeOutcome, NodeSkipReason, Option<TestOutcome>);

impl NodeOutcomeCountsKey {
    pub fn new(
        outcome: NodeOutcome,
        skip_reason: NodeSkipReason,
        test_outcome: Option<TestOutcome>,
    ) -> Self {
        Self(outcome, skip_reason, test_outcome)
    }

    pub fn into_parts(self) -> (NodeOutcome, NodeSkipReason, Option<TestOutcome>) {
        (self.0, self.1, self.2)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricKey {
    InvocationMetric(InvocationMetricKey),
    NodeCounts(NodeType),
    NodeOutcomeCounts(NodeOutcomeCountsKey),
}

#[derive(Debug)]
pub(super) struct MetricCounters {
    // Using AtomicU64 for invocation metrics
    invocation_counters: [AtomicU64; InvocationMetricKey::COUNT],
    // Other metrics with complex keys stored in a map
    metrics: scc::HashMap<MetricKey, u64>,
}

impl MetricCounters {
    pub(super) fn new() -> Self {
        Self {
            invocation_counters: std::array::from_fn(|_| AtomicU64::new(0)),
            metrics: scc::HashMap::new(),
        }
    }

    fn increment(&self, key: MetricKey, value: u64) {
        match key {
            MetricKey::InvocationMetric(im) => {
                // SAFETY: arry size is statically defined by enum count and pre-allocated on creation.
                // Enum discriminant is u8 starting from 0, so index is always valid. Also we do exhaustive testing,
                self.invocation_counters[im as usize]
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
            MetricKey::InvocationMetric(im) => {
                // SAFETY: arry size is statically defined by enum count and pre-allocated on creation.
                // Enum discriminant is u8 starting from 0, so index is always valid. Also we do exhaustive testing,
                self.invocation_counters[im as usize].load(std::sync::atomic::Ordering::Relaxed)
            }
            _ => self.metrics.read_sync(&key, |_, v| *v).unwrap_or_default(),
        }
    }

    fn iter(&self) -> impl Iterator<Item = (MetricKey, u64)> + '_ {
        let invocation_metrics = (0..InvocationMetricKey::COUNT).map(|i| {
            let key = MetricKey::InvocationMetric(
                InvocationMetricKey::from_repr(i as u8).expect("Must be valid"),
            );
            let value = self.invocation_counters[i].load(std::sync::atomic::Ordering::Relaxed);
            (key, value)
        });

        let mut other_metrics = Vec::new();
        self.metrics.iter_sync(|k, v| {
            other_metrics.push((*k, *v));
            true
        });

        invocation_metrics.chain(other_metrics)
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

pub(super) fn get_all_metrics_from_span_extension(
    span_ext: &Extensions<'_>,
) -> Vec<(MetricKey, u64)> {
    span_ext
        .get::<MetricCounters>()
        .map(|counters| counters.iter().collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use strum::IntoEnumIterator;

    #[test]
    fn test_increment_and_get_all_metrics() {
        let metrics = MetricCounters::new();

        // Test invocation metrics
        for key in InvocationMetricKey::iter() {
            let key = MetricKey::InvocationMetric(key);
            assert_eq!(metrics.get(key), 0);
            metrics.increment(key, 5);
            assert_eq!(metrics.get(key), 5);
            metrics.increment(key, 3);
            assert_eq!(metrics.get(key), 8);
        }

        // Test NodeType metrics
        for key in [
            MetricKey::NodeCounts(Default::default()),
            MetricKey::NodeOutcomeCounts(NodeOutcomeCountsKey(
                Default::default(),
                Default::default(),
                Default::default(),
            )),
        ] {
            assert_eq!(metrics.get(key), 0);
            metrics.increment(key, 2);
            assert_eq!(metrics.get(key), 2);
            metrics.increment(key, 4);
            assert_eq!(metrics.get(key), 6);
        }
    }

    #[test]
    fn test_iterator_contains_all_keys() {
        let metrics = MetricCounters::new();

        // Check iterator only returns simple keys when no metrics have been added
        let all_keys: HashSet<MetricKey> = metrics.iter().map(|(k, _)| k).collect();

        let exporter_keys: HashSet<MetricKey> = InvocationMetricKey::iter()
            .map(MetricKey::InvocationMetric)
            .collect();
        assert_eq!(all_keys, exporter_keys);
    }
}
