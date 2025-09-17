use std::sync::{Arc, Mutex};

use dbt_telemetry::{LogRecordInfo, SpanEndInfo, SpanStartInfo};
use tracing::{Subscriber, span};
use tracing_subscriber::{Layer, layer::Context};

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
