use super::span_info::with_root_span;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricKey {
    TotalErrors,
    TotalWarnings,
    AutoFixSuggestions,
}

#[derive(Debug, Clone, Default)]
pub struct MetricCounters {
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

/// Gets a specific metric counter value from the root span extensions.
/// None means either the metric was never incremented or you are
/// running with uninitialized tracing.
pub fn get_metric(key: MetricKey) -> Option<u64> {
    with_root_span(|root_span| {
        root_span
            .extensions()
            .get::<MetricCounters>()
            .map(|counters| counters.get(key))
    })
    .flatten()
}
