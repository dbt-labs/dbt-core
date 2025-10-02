use super::{data_provider::DataProvider, layer::TelemetryConsumer};
use dbt_telemetry::{LogRecordInfo, SpanEndInfo, SpanStartInfo};
use tracing::Metadata;

/// A bitmask used to represent which consumers are not interested in a given
/// telemetry span. Consumers are indexed by their position in the consumer list
/// of a TelemetryDataLayer.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct FilterMask(u64);

impl FilterMask {
    /// Returns an empty filter mask (meaning no consumers are disabled)
    pub(super) fn empty() -> Self {
        Self::default()
    }

    /// Returns a full filter mask (meaning all consumers are disabled)
    pub(super) fn disabled() -> Self {
        Self(u64::MAX)
    }

    /// Returns true if this is a filter mask that disables all consumers
    pub(super) fn is_disabled(&self) -> bool {
        self.0 == u64::MAX
    }

    pub(super) fn set_filtered(&mut self, index: usize) {
        // Shift would panic in debug if index >= 64, but we add a nice message via debug_assert
        debug_assert!(
            index < 64,
            "Exceeding mask length. Index must be less than 64"
        );
        self.0 |= 1 << index;
    }

    pub(super) fn is_filtered(&self, index: usize) -> bool {
        // Shift would panic in debug if index >= 64, but we add a nice message via debug_assert
        debug_assert!(
            index < 64,
            "Exceeding mask length. Index must be less than 64"
        );
        self.0 & (1 << index) != 0
    }
}

pub trait TelemetryFilter {
    fn is_span_enabled(&self, span: &SpanStartInfo, meta: &Metadata) -> bool;

    fn is_log_enabled(&self, span: &LogRecordInfo, meta: &Metadata) -> bool;
}

pub struct FilteredTelemetryConsumer<C, F>
where
    C: TelemetryConsumer,
    F: TelemetryFilter,
{
    inner: C,
    filter: F,
}

impl<C, F> FilteredTelemetryConsumer<C, F>
where
    C: TelemetryConsumer,
    F: TelemetryFilter,
{
    pub fn new(consumer: C, filter: F) -> Self {
        Self {
            inner: consumer,
            filter,
        }
    }
}

impl<C, F> TelemetryConsumer for FilteredTelemetryConsumer<C, F>
where
    C: TelemetryConsumer,
    F: TelemetryFilter,
{
    fn is_span_enabled(&self, span: &SpanStartInfo, meta: &Metadata) -> bool {
        self.filter.is_span_enabled(span, meta) && self.inner.is_span_enabled(span, meta)
    }

    fn is_log_enabled(&self, span: &LogRecordInfo, meta: &Metadata) -> bool {
        self.filter.is_log_enabled(span, meta) && self.inner.is_log_enabled(span, meta)
    }

    fn on_span_start(&self, span: &SpanStartInfo, data_provider: &DataProvider<'_>) {
        self.inner.on_span_start(span, data_provider);
    }

    fn on_span_end(&self, span: &SpanEndInfo, data_provider: &DataProvider<'_>) {
        self.inner.on_span_end(span, data_provider);
    }

    fn on_log_record(&self, event: &LogRecordInfo, data_provider: &DataProvider<'_>) {
        self.inner.on_log_record(event, data_provider);
    }
}

/// A convenience [`TelemetryFilter`] that delegates filtering decisions to user
/// supplied closure(s).
///
/// The generic parameter S is a closure (Fn) that receives a SpanStartInfo and its
/// Metadata and returns true if the span should be enabled. Likewise, L is a
/// closure that receives a LogRecordInfo and its Metadata to decide whether the
/// log event should be enabled.
///
/// We provide convenience functions `enable_all_spans`, `disable_all_spans`,
/// `enable_all_logs`, and `disable_all_logs` that can be used when you only want
/// to filter one of the two types of telemetry records.
///
/// This is useful when constructing lightweight, ad‑hoc filters without having
/// to implement a dedicated type.
///
/// Note that for filtering based on level you can use [`tracing::level_filters::LevelFilter`]
/// directly as it implements the filter trait.
pub struct TelemetryFilterFn<S, L>
where
    S: Fn(&SpanStartInfo, &Metadata) -> bool,
    L: Fn(&LogRecordInfo, &Metadata) -> bool,
{
    is_span_enabled: S,
    is_log_enabled: L,
}

impl<S, L> TelemetryFilterFn<S, L>
where
    S: Fn(&SpanStartInfo, &Metadata) -> bool,
    L: Fn(&LogRecordInfo, &Metadata) -> bool,
{
    /// Creates a [`TelemetryFilterFn`] with optional span and log filters.
    pub fn new(is_span_enabled: S, is_log_enabled: L) -> Self {
        Self {
            is_span_enabled,
            is_log_enabled,
        }
    }
}

// Convenience functions for common cases
pub fn disable_all_spans(_span: &SpanStartInfo, _meta: &Metadata) -> bool {
    false
}

pub fn enable_all_spans(_span: &SpanStartInfo, _meta: &Metadata) -> bool {
    true
}

pub fn disable_all_logs(_log: &LogRecordInfo, _meta: &Metadata) -> bool {
    false
}

pub fn enable_all_logs(_log: &LogRecordInfo, _meta: &Metadata) -> bool {
    true
}

impl<S, L> TelemetryFilter for TelemetryFilterFn<S, L>
where
    S: Fn(&SpanStartInfo, &Metadata) -> bool,
    L: Fn(&LogRecordInfo, &Metadata) -> bool,
{
    fn is_span_enabled(&self, span: &SpanStartInfo, meta: &Metadata) -> bool {
        (self.is_span_enabled)(span, meta)
    }

    fn is_log_enabled(&self, span: &LogRecordInfo, meta: &Metadata) -> bool {
        (self.is_log_enabled)(span, meta)
    }
}

impl TelemetryFilter for tracing::level_filters::LevelFilter {
    fn is_span_enabled(&self, _span: &SpanStartInfo, meta: &Metadata) -> bool {
        meta.level() <= self
    }

    fn is_log_enabled(&self, _span: &LogRecordInfo, meta: &Metadata) -> bool {
        meta.level() <= self
    }
}

#[cfg(test)]
mod tests {
    use super::FilterMask;

    #[test]
    fn empty_mask_reports_empty() {
        let mask = FilterMask::empty();

        for i in 0..64 {
            assert!(!mask.is_filtered(i));
        }
    }

    #[test]
    fn full_mask_reports_full() {
        let mask = FilterMask::disabled();

        assert!(mask.is_disabled());
        for i in 0..64 {
            assert!(mask.is_filtered(i));
        }
    }

    #[test]
    fn set_filtered_marks_bits() {
        let mut mask = FilterMask::empty();

        assert!(!mask.is_filtered(1));
        assert!(!mask.is_filtered(63));

        mask.set_filtered(1);
        mask.set_filtered(63);

        assert!(mask.is_filtered(1));
        assert!(mask.is_filtered(63));
        assert!(!mask.is_filtered(0));
    }
}
