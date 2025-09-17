use dbt_telemetry::{LogRecordInfo, TelemetryAttributes};
use std::cell::RefCell;
use tracing::Event;

// Tracing doesn't provide a thread safe storage for arbitrary event data, only for spans (via extensions).
// But since there is no identifying information for an event, we can't store
// event data on the parent span. Multiple events (logs) may be emitted from different
// threads simultaneously that have the same parent span, and thus data layer may overwrite
// one event data with another before it is handled by consumer layers.
// Hence a separate storage.
//
// NOTE: this assumes that consuming layer always read strucutered data from the same thread
// as the data layer that wrote it, so make sure no downstream layer uses async/spawn
// until it read the data into locals.
thread_local! {
    /// Thread-local storage for full structured log record prepared by data layer.
    /// This is used to pass structured data to consumer layers as tracing library
    /// doesn't provide a native storage for logs (unlike spans)
    static CURRENT_LOG_RECORD: RefCell<Option<LogRecordInfo>> = const { RefCell::new(None) };
    /// Thread-local storage for structured event attributes. Used to efficiently
    /// pass structured data to data layer without serialization through tracing fields.
    /// Used for spans and logs.
    static CURRENT_EVENT_ATTRIBUTES: RefCell<Option<TelemetryAttributes>> = const { RefCell::new(None) };
}

/// A private API for tracing infra to set current log record being processed. Only data layer
/// is allowed to update it.
pub(super) fn set_current_log_record(record: LogRecordInfo) {
    CURRENT_LOG_RECORD.with(|cell| {
        *cell.borrow_mut() = Some(record);
    });
}

/// Pre-saves structured event attributes to be immediately consumed by tracing span/log call.
///
/// If you want to emit a log or create a new span, prefer - `dbt_common::tracing::emit::...` macros to avoid mistakes.
///
/// The only use case where this API should be used outside of `dbt_common::tracing` is
/// in conjunction with `#[tracing::instrument]`:
/// ```no_run
/// use dbt_common::tracing::event_info::store_event_attributes;
///
/// #[tracing::instrument(
///    skip_all,
///    fields(
///        _e = ?store_event_attributes(/* your AnyTelemetryEvent value here */),
///    )
///)]
/// fn your_function(...) { ... }
/// ```
///
/// Note that `_e` field name is irrelevant, and only necessary to inject
/// the call to `store_event_attributes()` into instrumented function. We
/// actually use it's side effect of storing the attributes in thread-local storage.
///
/// ALso note that `?` is necessary due to `tracing` macro limitations.
pub fn store_event_attributes(attrs: TelemetryAttributes) {
    CURRENT_EVENT_ATTRIBUTES.with(|cell| {
        *cell.borrow_mut() = Some(attrs);
    });
}

/// A private API for Data Layer to access pre-populated structured event attributes.
pub(super) fn take_event_attributes() -> Option<TelemetryAttributes> {
    CURRENT_EVENT_ATTRIBUTES.with(|cell| cell.take())
}

/// Access the structured log record being processed by the current thread.
///
/// This data is available for all layers from the moment the event is emitted
/// and until all layers have processed it.
pub fn with_current_thread_log_record<F>(f: F)
where
    F: FnOnce(&LogRecordInfo),
{
    CURRENT_LOG_RECORD.with(|cell| {
        if let Some(ref record) = *cell.borrow() {
            f(record);
        }
    });
}

pub(super) fn get_log_message(event: &Event<'_>) -> String {
    struct MessageVisitor<'a>(&'a mut String);

    impl<'a> tracing::field::Visit for MessageVisitor<'a> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0.push_str(&format!("{value:?}"));
            }
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "message" {
                self.0.push_str(value);
            }
        }
    }

    let mut message = String::new();
    let mut visitor = MessageVisitor(&mut message);
    event.record(&mut visitor);

    message
}
