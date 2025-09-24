use super::super::shared_writer::SharedWriter;
use dbt_telemetry::{AnyTelemetryEvent, TelemetryEventRecType, TelemetryOutputFlags};
use dbt_telemetry::{LogRecordInfo, SpanEndInfo, SpanStartInfo};
use serde::Serialize;
use std::{
    io,
    sync::{Arc, Mutex},
};
use tracing::{Subscriber, span};
use tracing_subscriber::{Layer, layer::Context};

fn serialize_flags<S>(flags: &TelemetryOutputFlags, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_u32(flags.bits())
}

// Mock dynamic span event with instance-based export flags
#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub struct MockDynSpanEvent {
    pub name: String,
    #[serde(serialize_with = "serialize_flags")]
    pub flags: TelemetryOutputFlags,
    pub has_sensitive: bool,
    pub was_scrubbed: bool,
}

impl AnyTelemetryEvent for MockDynSpanEvent {
    fn event_type(&self) -> &'static str {
        "v1.public.events.fusion.dev.MockDynSpanEvent"
    }

    fn event_display_name(&self) -> String {
        format!("Mock Dyn Span Event: {}", self.name)
    }

    fn record_category(&self) -> TelemetryEventRecType {
        TelemetryEventRecType::Span
    }

    fn output_flags(&self) -> TelemetryOutputFlags {
        self.flags
    }

    fn event_eq(&self, other: &dyn AnyTelemetryEvent) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|rhs| rhs == self)
    }

    fn has_sensitive_data(&self) -> bool {
        self.has_sensitive
    }

    fn clone_without_sensitive_data(&self) -> Option<Box<dyn AnyTelemetryEvent>> {
        Some(Box::new(Self {
            name: self.name.clone(),
            flags: self.flags,
            has_sensitive: self.has_sensitive,
            was_scrubbed: true,
        }))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn clone_box(&self) -> Box<dyn AnyTelemetryEvent> {
        Box::new(self.clone())
    }

    fn to_json(&self) -> Result<serde_json::Value, String> {
        serde_json::to_value(self).map_err(|e| format!("Failed to serialize: {e}"))
    }
}

// Mock dynamic log event with instance-based export flags
#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub struct MockDynLogEvent {
    pub code: i32,
    #[serde(serialize_with = "serialize_flags")]
    pub flags: TelemetryOutputFlags,
    pub has_sensitive: bool,
    pub was_scrubbed: bool,
}

impl AnyTelemetryEvent for MockDynLogEvent {
    fn event_type(&self) -> &'static str {
        "v1.public.events.fusion.dev.MockDynLogEvent"
    }

    fn event_display_name(&self) -> String {
        format!("Mock Dyn Log Event: {}", self.code)
    }

    fn record_category(&self) -> TelemetryEventRecType {
        TelemetryEventRecType::Log
    }

    fn output_flags(&self) -> TelemetryOutputFlags {
        self.flags
    }

    fn event_eq(&self, other: &dyn AnyTelemetryEvent) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|rhs| rhs == self)
    }

    fn has_sensitive_data(&self) -> bool {
        self.has_sensitive
    }

    fn clone_without_sensitive_data(&self) -> Option<Box<dyn AnyTelemetryEvent>> {
        Some(Box::new(Self {
            code: self.code,
            flags: self.flags,
            has_sensitive: self.has_sensitive,
            was_scrubbed: true,
        }))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn clone_box(&self) -> Box<dyn AnyTelemetryEvent> {
        Box::new(self.clone())
    }

    fn to_json(&self) -> Result<serde_json::Value, String> {
        serde_json::to_value(self).map_err(|e| format!("Failed to serialize: {e}"))
    }
}

#[derive(Clone, Default)]
pub struct TestWriter {
    is_terminal: bool,
    lines: Arc<Mutex<Vec<String>>>,
}

impl TestWriter {
    pub fn non_terminal() -> Self {
        Default::default()
    }

    pub fn terminal() -> Self {
        Self {
            is_terminal: true,
            lines: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_lines(&self) -> Vec<String> {
        self.lines.lock().expect("writer mutex poisoned").clone()
    }
}

impl SharedWriter for TestWriter {
    fn write(&self, data: &str) -> io::Result<()> {
        self.lines
            .lock()
            .expect("writer mutex poisoned")
            .push(data.to_string());
        Ok(())
    }

    fn writeln(&self, data: &str) -> io::Result<()> {
        self.lines
            .lock()
            .expect("writer mutex poisoned")
            .push(format!("{data}\n"));
        Ok(())
    }

    fn is_terminal(&self) -> bool {
        self.is_terminal
    }
}

// Shared capture layer used by multiple tests to collect structured telemetry
#[derive(Clone)]
pub struct TestLayer {
    pub span_starts: Arc<Mutex<Vec<SpanStartInfo>>>,
    pub span_ends: Arc<Mutex<Vec<SpanEndInfo>>>,
    pub log_records: Arc<Mutex<Vec<LogRecordInfo>>>,
}

impl TestLayer {
    #[allow(clippy::type_complexity)]
    pub fn new() -> (
        Self,
        Arc<Mutex<Vec<SpanStartInfo>>>,
        Arc<Mutex<Vec<SpanEndInfo>>>,
        Arc<Mutex<Vec<LogRecordInfo>>>,
    ) {
        let span_starts = Arc::new(Mutex::new(Vec::new()));
        let span_ends = Arc::new(Mutex::new(Vec::new()));
        let log_records = Arc::new(Mutex::new(Vec::new()));

        let layer = Self {
            span_starts: span_starts.clone(),
            span_ends: span_ends.clone(),
            log_records: log_records.clone(),
        };

        (layer, span_starts, span_ends, log_records)
    }
}

impl<S> Layer<S> for TestLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_new_span(&self, _attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(id)
            .expect("Span must exist for id in the current context");

        if let Some(record) = span.extensions().get::<SpanStartInfo>() {
            self.span_starts.lock().unwrap().push(record.clone());
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(&id)
            .expect("Span must exist for id in the current context");

        if let Some(record) = span.extensions().get::<SpanEndInfo>() {
            self.span_ends.lock().unwrap().push(record.clone());
        }
    }

    fn on_event(&self, _event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        crate::tracing::event_info::with_current_thread_log_record(|log_record| {
            self.log_records.lock().unwrap().push(log_record.clone());
        });
    }
}
