use dbt_telemetry::{LogRecordInfo, TelemetryAttributes};
use std::cell::RefCell;
use tracing::Event;

use super::{constants::TRACING_ATTR_FIELD, shared::Recordable};

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
    /// Thread-local storage for full structured event data prepared by data layer.
    static CURRENT_EVENT_DATA: RefCell<Option<LogRecordInfo>> = const { RefCell::new(None) };
    /// Thread-local storage for structured event attributes. Can be used to efficiently
    /// pass structured data to data layer without serialization through tracing fields.
    static CURRENT_EVENT_ATTRIBUTES: RefCell<Option<TelemetryAttributes>> = const { RefCell::new(None) };
}

/// A private API for tracing infra to set structured event data. Only data layer
/// is allowed to update it.
pub(super) fn store_event_data(record: LogRecordInfo) {
    CURRENT_EVENT_DATA.with(|cell| {
        *cell.borrow_mut() = Some(record);
    });
}

/// A "private" API for tracing infra to pre-populate structured event attributes.
/// Only our custom logging/event APIs are allowed to update it. Do NOT use outside
/// of `tracing::emit::...` macros.
pub fn store_event_attributes(attrs: TelemetryAttributes) {
    CURRENT_EVENT_ATTRIBUTES.with(|cell| {
        *cell.borrow_mut() = Some(attrs);
    });
}

/// A private API for Data Layer to access pre-populated structured event attributes.
pub(super) fn take_event_attributes() -> Option<TelemetryAttributes> {
    CURRENT_EVENT_ATTRIBUTES.with(|cell| cell.take())
}

/// Access the structured event data being processed by the current thread.
///
/// This data is available for all layers from the moment the event is emitted
/// and until all layers have processed it.
pub fn with_current_thread_event_data<F>(f: F)
where
    F: FnOnce(&LogRecordInfo),
{
    CURRENT_EVENT_DATA.with(|cell| {
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

/// Helper that extracts a `LogEventAttributes` from a `ValueSet`.
pub(super) fn get_log_event_attrs(values: Recordable<'_>) -> Option<TelemetryAttributes> {
    struct LogEventAttributesVisitor(Option<TelemetryAttributes>);

    impl tracing::field::Visit for LogEventAttributesVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == TRACING_ATTR_FIELD {
                self.0 = Some(
                    serde_json::from_str(&format!("{value:?}"))
                        .expect("Failed to deserialize log event attributes. Are you sure you've used the correct type?"),
                );
            }
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == TRACING_ATTR_FIELD {
                self.0 = Some(
                    serde_json::from_str(value)
                        .expect("Failed to deserialize log event attributes. Are you sure you've used the correct type?"),
                );
            }
        }
    }

    let mut visitor = LogEventAttributesVisitor(None);
    values.record(&mut visitor);

    visitor.0
}
