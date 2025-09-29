use super::metrics::{
    MetricKey, get_metric_from_span_extension, increment_metric_on_span_extension,
};
use tracing_subscriber::registry::{Extensions, ExtensionsMut};

/// A read-only data provider allowing safe controlled access to metrics and
/// span storage to consumer layers.
pub struct DataProvider<'a> {
    span_ext: Option<Extensions<'a>>,
}

impl<'a> DataProvider<'a> {
    /// Creates a new data provider from the span extensions
    pub(super) fn new(span_ext: Extensions<'a>) -> Self {
        Self {
            span_ext: Some(span_ext),
        }
    }

    pub(super) fn none() -> Self {
        Self { span_ext: None }
    }

    /// Gets a specific invocation totals metrics (stored in the root invocation span).
    pub fn get_metric(&self, key: MetricKey) -> u64 {
        self.span_ext
            .as_ref()
            .map(|ext| get_metric_from_span_extension(ext, key))
            .unwrap_or_default()
    }
}

/// A mutable data provider to allow middleware layers to store additional
/// data in span extensions, or update metrics on the invocation span.
pub struct DataProviderMut<'a> {
    span_ext: Option<ExtensionsMut<'a>>,
}

impl<'a> DataProviderMut<'a> {
    /// Creates a new mutable data provider from the span extensions
    pub(super) fn new(span_ext: ExtensionsMut<'a>) -> Self {
        Self {
            span_ext: Some(span_ext),
        }
    }

    /// Creates a new empty data provider that does nothing
    pub(super) fn none() -> Self {
        Self { span_ext: None }
    }

    /// Increments a metric counter on the invocation span extensions directly.
    pub fn increment_metric(&mut self, key: MetricKey, value: u64) {
        if let Some(span_ext) = self.span_ext.as_mut() {
            increment_metric_on_span_extension(span_ext, key, value);
        }
    }
}
