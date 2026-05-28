use super::super::{
    data_provider::DataProvider,
    layer::{TelemetryConsumer, TelemetryMiddleware},
    shared_writer::SharedWriter,
};
use dbt_telemetry::{AnyTelemetryEvent, TelemetryEventRecType, TelemetryOutputFlags};
use dbt_telemetry::{LogRecordInfo, SpanEndInfo, SpanStartInfo};
use serde::Serialize;
use std::sync::{Arc, Mutex};

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
    fn write(&self, data: &str) {
        self.lines
            .lock()
            .expect("writer mutex poisoned")
            .push(data.to_string());
    }

    fn writeln(&self, data: &str) {
        self.lines
            .lock()
            .expect("writer mutex poisoned")
            .push(format!("{data}\n"));
    }

    fn is_terminal(&self) -> bool {
        self.is_terminal
    }
}

type SpanStartConsumerHandler = dyn for<'a> Fn(&SpanStartInfo, &mut DataProvider<'a>) + Send + Sync;
type SpanEndConsumerHandler = dyn for<'a> Fn(&SpanEndInfo, &mut DataProvider<'a>) + Send + Sync;
type LogRecordConsumerHandler = dyn for<'a> Fn(&LogRecordInfo, &mut DataProvider<'a>) + Send + Sync;

// Shared capture layer used by multiple tests to collect structured telemetry
pub struct TestLayer {
    span_starts: Arc<Mutex<Vec<SpanStartInfo>>>,
    span_ends: Arc<Mutex<Vec<SpanEndInfo>>>,
    log_records: Arc<Mutex<Vec<LogRecordInfo>>>,
    on_span_start: Box<SpanStartConsumerHandler>,
    on_span_end: Box<SpanEndConsumerHandler>,
    on_log_record: Box<LogRecordConsumerHandler>,
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
            on_span_start: Box::new(|_, _| {}),
            on_span_end: Box::new(|_, _| {}),
            on_log_record: Box::new(|_, _| {}),
        };

        (layer, span_starts, span_ends, log_records)
    }

    #[allow(dead_code)]
    pub fn with_span_start<F>(mut self, f: F) -> Self
    where
        F: for<'a> Fn(&SpanStartInfo, &mut DataProvider<'a>) + Send + Sync + 'static,
    {
        self.on_span_start = Box::new(f);
        self
    }

    #[allow(dead_code)]
    pub fn with_span_end<F>(mut self, f: F) -> Self
    where
        F: for<'a> Fn(&SpanEndInfo, &mut DataProvider<'a>) + Send + Sync + 'static,
    {
        self.on_span_end = Box::new(f);
        self
    }

    #[allow(dead_code)]
    pub fn with_log_record<F>(mut self, f: F) -> Self
    where
        F: for<'a> Fn(&LogRecordInfo, &mut DataProvider<'a>) + Send + Sync + 'static,
    {
        self.on_log_record = Box::new(f);
        self
    }
}

impl TelemetryConsumer for TestLayer {
    fn on_span_start(&self, span: &SpanStartInfo, data_provider: &mut DataProvider<'_>) {
        (self.on_span_start)(span, data_provider);
        self.span_starts.lock().unwrap().push(span.clone());
    }

    fn on_span_end(&self, span: &SpanEndInfo, data_provider: &mut DataProvider<'_>) {
        (self.on_span_end)(span, data_provider);
        self.span_ends.lock().unwrap().push(span.clone());
    }

    fn on_log_record(&self, record: &LogRecordInfo, data_provider: &mut DataProvider<'_>) {
        (self.on_log_record)(record, data_provider);
        self.log_records.lock().unwrap().push(record.clone());
    }
}

type SpanStartMiddlewareHandler =
    dyn for<'a> Fn(SpanStartInfo, &mut DataProvider<'a>) -> Option<SpanStartInfo> + Send + Sync;
type SpanEndMiddlewareHandler =
    dyn for<'a> Fn(SpanEndInfo, &mut DataProvider<'a>) -> Option<SpanEndInfo> + Send + Sync;
type LogRecordMiddlewareHandler =
    dyn for<'a> Fn(LogRecordInfo, &mut DataProvider<'a>) -> Option<LogRecordInfo> + Send + Sync;

/// A configurable middleware used to test how telemetry data passes through middleware hooks.
pub struct MockMiddleware {
    span_start: Box<SpanStartMiddlewareHandler>,
    span_end: Box<SpanEndMiddlewareHandler>,
    log_record: Box<LogRecordMiddlewareHandler>,
}

impl Default for MockMiddleware {
    fn default() -> Self {
        Self {
            span_start: Box::new(|span, _| Some(span)),
            span_end: Box::new(|span, _| Some(span)),
            log_record: Box::new(|record, _| Some(record)),
        }
    }
}

impl MockMiddleware {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_span_start<F>(mut self, f: F) -> Self
    where
        F: for<'a> Fn(SpanStartInfo, &mut DataProvider<'a>) -> Option<SpanStartInfo>
            + Send
            + Sync
            + 'static,
    {
        self.span_start = Box::new(f);
        self
    }

    #[allow(dead_code)]
    pub fn with_span_end<F>(mut self, f: F) -> Self
    where
        F: for<'a> Fn(SpanEndInfo, &mut DataProvider<'a>) -> Option<SpanEndInfo>
            + Send
            + Sync
            + 'static,
    {
        self.span_end = Box::new(f);
        self
    }

    pub fn with_log_record<F>(mut self, f: F) -> Self
    where
        F: for<'a> Fn(LogRecordInfo, &mut DataProvider<'a>) -> Option<LogRecordInfo>
            + Send
            + Sync
            + 'static,
    {
        self.log_record = Box::new(f);
        self
    }
}

impl TelemetryMiddleware for MockMiddleware {
    fn on_span_start(
        &self,
        span: SpanStartInfo,
        data_provider: &mut DataProvider<'_>,
    ) -> Option<SpanStartInfo> {
        (self.span_start)(span, data_provider)
    }

    fn on_span_end(
        &self,
        span: SpanEndInfo,
        data_provider: &mut DataProvider<'_>,
    ) -> Option<SpanEndInfo> {
        (self.span_end)(span, data_provider)
    }

    fn on_log_record(
        &self,
        record: LogRecordInfo,
        data_provider: &mut DataProvider<'_>,
    ) -> Option<LogRecordInfo> {
        (self.log_record)(record, data_provider)
    }
}
