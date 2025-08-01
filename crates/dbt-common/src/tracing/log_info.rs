use dbt_telemetry::LogAttributes;
use tracing::Event;

use super::{constants::TRACING_ATTR_FIELD, shared::Recordable};

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
pub(super) fn get_log_event_attrs(values: Recordable<'_>) -> Option<LogAttributes> {
    struct LogEventAttributesVisitor(Option<LogAttributes>);

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
