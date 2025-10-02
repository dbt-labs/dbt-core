use super::{
    metrics::{MetricKey, get_metric_from_span_extension, increment_metric_on_span},
    span_info::SpanAccess,
};
use tracing_subscriber::registry::{LookupSpan, SpanRef};

/// A read-only data provider allowing safe controlled access to metrics and
/// span storage to consumer layers.
///
/// Implements on-demand access to root span extensions that avoids long living
/// locks on span extensions. Root span is shared among all threads, so we use
/// this to avoid contention on span extensions.
pub struct DataProvider<'a> {
    root_span: Option<&'a dyn SpanAccess>,
}

impl<'a> DataProvider<'a> {
    /// Creates a new data provider from the span
    pub(super) fn new<'sp, R>(root_span: &'a SpanRef<'sp, R>) -> Self
    where
        R: LookupSpan<'sp>,
    {
        Self {
            root_span: Some(root_span as &dyn SpanAccess),
        }
    }

    pub(super) fn none() -> Self {
        Self { root_span: None }
    }

    pub fn with<T>(&self, f: impl FnOnce(&T))
    where
        T: Send + Sync + 'static,
    {
        self.root_span
            .map(|root_span| root_span.extensions().get::<T>().map(f));
    }

    /// Gets a specific invocation totals metrics (stored in the root invocation span).
    pub fn get_metric(&self, key: MetricKey) -> u64 {
        self.root_span
            .map(|root_span| get_metric_from_span_extension(&root_span.extensions(), key))
            .unwrap_or_default()
    }
}

/// A mutable data provider to allow middleware layers to store additional
/// data in span extensions, or update metrics on the invocation span.
///
/// Implements on-demand access to root span extensions that avoids long living
/// locks on span extensions. Root span is shared among all threads, so we use
/// this to avoid contention on span extensions.
pub struct DataProviderMut<'a> {
    root_span: Option<&'a dyn SpanAccess>,
}

impl<'a> DataProviderMut<'a> {
    /// Creates a new mutable data provider from the span extensions
    pub(super) fn new<'sp, R>(root_span: &'a SpanRef<'sp, R>) -> Self
    where
        R: LookupSpan<'sp>,
    {
        Self {
            root_span: Some(root_span as &dyn SpanAccess),
        }
    }

    pub(super) fn none() -> Self {
        Self { root_span: None }
    }

    /// Initializes an extension value on the root span.
    ///
    /// Note, that it will replace any existing value of the same type.
    ///
    /// We require `&mut self` to avoid pitfalls of self-locking since this
    /// function may try to acquire a write lock on span extensions.
    ///
    /// # Returns
    ///
    /// `Some(T)` if the root span exists and a previous value was replaced.
    pub fn init<T>(&mut self, initializer: impl FnOnce() -> T) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        self.root_span.and_then(|root_span| {
            let mut mut_extensions = root_span.extensions_mut();
            mut_extensions.replace(initializer())
        })
    }

    /// Accesses an extension value on the root span.
    pub fn with<T>(&self, f: impl FnOnce(&T))
    where
        T: Send + Sync + 'static,
    {
        if let Some(root_span) = self.root_span
            && let Some(ext) = root_span.extensions().get::<T>()
        {
            f(ext)
        };
    }

    /// Increments a metric counter on the invocation span extensions directly.
    pub fn increment_metric(&self, key: MetricKey, value: u64) {
        if let Some(root_span) = self.root_span {
            increment_metric_on_span(root_span, key, value);
        }
    }
}
