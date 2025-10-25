use super::{
    metrics::{
        MetricKey, get_all_metrics_from_span_extension, get_metric_from_span_extension,
        increment_metric_on_span,
    },
    span_info::SpanAccess,
};
use tracing_subscriber::registry::{LookupSpan, SpanRef};

/// A data provider allowing safe access to metrics and efficient, thread-safe
/// storage of arbutrary data on a per-invocation basis, that can be used
/// by consumer and middleware layers. E.g. to delay exporting of events.
///
/// Technical note:
/// This is a wrapper around tracing lib's span extensions that provides a safer
/// API (prevents deadlocks) and implements on-demand access to root span extensions
/// that avoids long living locks on span extensions. Root span is shared among all threads, so we use
/// this to avoid contention on invocation level data.
///
/// Even though this provider uses interior mutability through the span extension system,
/// some write operations require `&mut self` receiver to prevent self-deadlocks.
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

    /// Initializes an extension value on the root span.
    ///
    /// Note, that it will replace any existing value of the same type.
    ///
    /// # Returns
    ///
    /// `Some(T)` if the root span exists and a previous value was replaced.
    pub fn init<T>(&self, value: T) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        self.root_span.and_then(|root_span| {
            let mut mut_extensions = root_span.extensions_mut();
            mut_extensions.replace(value)
        })
    }

    /// Accesses an extension value on the root span for reading.
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

    /// Accesses an extension value on the root span for mutation.
    ///
    /// The closure is called with a mutable reference to the extension value if it exists.
    ///
    /// We require `&mut self` to avoid pitfalls of self-locking since this
    /// function will acquire a write lock on span extensions and closure may
    /// try using the same data provider reference again.
    pub fn with_mut<T>(&mut self, f: impl FnOnce(&mut T))
    where
        T: Send + Sync + 'static,
    {
        if let Some(root_span) = self.root_span
            && let Some(ext) = root_span.extensions_mut().get_mut::<T>()
        {
            f(ext)
        };
    }

    /// Gets a specific per-invocation metric (stored in the root invocation span).
    pub fn get_metric(&self, key: MetricKey) -> u64 {
        self.root_span
            .map(|root_span| get_metric_from_span_extension(&root_span.extensions(), key))
            .unwrap_or_default()
    }

    /// Gets all per-invocation metrics (stored in the root invocation span).
    pub fn get_all_metrics(&self) -> Vec<(MetricKey, u64)> {
        self.root_span
            .map(|root_span| get_all_metrics_from_span_extension(&root_span.extensions()))
            .unwrap_or_default()
    }

    /// Increments a per-invocation metric counter on the invocation span extensions directly.
    pub fn increment_metric(&self, key: MetricKey, value: u64) {
        if let Some(root_span) = self.root_span {
            increment_metric_on_span(root_span, key, value);
        }
    }
}
