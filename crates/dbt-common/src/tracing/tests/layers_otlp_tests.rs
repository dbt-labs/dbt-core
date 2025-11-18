use std::sync::{Arc, Mutex};

use crate::tracing::{
    emit::{create_root_info_span, emit_info_event},
    init::create_tracing_subcriber_with_layer,
    layer::ConsumerLayer,
    layers::{data_layer::TelemetryDataLayer, otlp::OTLPExporterLayer},
};

use super::mocks::{MockDynLogEvent, MockDynSpanEvent};
use dbt_telemetry::TelemetryOutputFlags;
use opentelemetry::Value as OtelValue;
use opentelemetry_sdk as sdk;

#[derive(Debug)]
struct TestSpanExporter {
    pub spans: Arc<Mutex<Vec<sdk::trace::SpanData>>>,
}

impl TestSpanExporter {
    fn new() -> (Self, Arc<Mutex<Vec<sdk::trace::SpanData>>>) {
        let shared = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                spans: shared.clone(),
            },
            shared,
        )
    }
}

impl sdk::trace::SpanExporter for TestSpanExporter {
    fn export(
        &self,
        batch: Vec<sdk::trace::SpanData>,
    ) -> impl Future<Output = sdk::error::OTelSdkResult> + Send {
        let spans = self.spans.clone();
        async move {
            let mut guard = spans.lock().unwrap();
            guard.extend(batch);
            Ok(())
        }
    }
}

#[derive(Debug)]
struct TestLogExporter {
    pub logs: Arc<Mutex<Vec<sdk::logs::SdkLogRecord>>>,
}

impl TestLogExporter {
    fn new() -> (Self, Arc<Mutex<Vec<sdk::logs::SdkLogRecord>>>) {
        let shared = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                logs: shared.clone(),
            },
            shared,
        )
    }
}

impl sdk::logs::LogExporter for TestLogExporter {
    fn export(
        &self,
        batch: sdk::logs::LogBatch<'_>,
    ) -> impl Future<Output = sdk::error::OTelSdkResult> + Send {
        let logs = self.logs.clone();
        async move {
            let mut guard = logs.lock().unwrap();
            for (rec, _scope) in batch.iter() {
                guard.push(rec.clone());
            }
            Ok(())
        }
    }
}

#[test]
fn test_otlp_layer_exports_only_marked_records() {
    let trace_id = rand::random::<u128>();

    // Create test exporters and share state
    let (trace_exporter, spans) = TestSpanExporter::new();
    let (log_exporter, logs) = TestLogExporter::new();

    // Build OTLP layer with test exporters
    let otlp_layer = OTLPExporterLayer::new(trace_exporter, log_exporter);
    // Clone providers for graceful shutdown later (batch processors flush on shutdown)
    let trace_provider = otlp_layer.tracer_provider();
    let log_provider = otlp_layer.logger_provider();

    // Init telemetry using internal API allowing to set thread local subscriber.
    // This avoids collisions with other unit tests
    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::empty(),
            std::iter::once(Box::new(otlp_layer) as ConsumerLayer),
        ),
    );

    // Emit events under the thread-local subscriber
    tracing::subscriber::with_default(subscriber, || {
        let exportable_span = create_root_info_span(MockDynSpanEvent {
            name: "exportable".to_string(),
            flags: TelemetryOutputFlags::EXPORT_OTLP,
            ..Default::default()
        });

        exportable_span.in_scope(|| {
            emit_info_event(
                MockDynLogEvent {
                    code: 1,
                    flags: TelemetryOutputFlags::EXPORT_OTLP,
                    ..Default::default()
                },
                Some("included log"),
            );
            emit_info_event(
                MockDynLogEvent {
                    code: 2,
                    flags: TelemetryOutputFlags::EXPORT_JSONL, // Not OTLP-exportable
                    ..Default::default()
                },
                Some("excluded log"),
            );
        });

        // This span should not be exported to OTLP
        let _non_exportable_span = create_root_info_span(MockDynSpanEvent {
            name: "non_exportable".to_string(),
            flags: TelemetryOutputFlags::EXPORT_JSONL, // Not OTLP-exportable
            ..Default::default()
        });
    });

    // Shutdown telemetry to ensure all data is flushed to the file
    trace_provider
        .shutdown()
        .expect("Failed to shutdown telemetry");
    log_provider
        .shutdown()
        .expect("Failed to shutdown telemetry");

    // Validate we exported exactly 1 span and 1 log
    let exported_spans = spans.lock().unwrap().clone();
    let exported_logs = logs.lock().unwrap().clone();

    assert_eq!(exported_spans.len(), 1, "expected one OTLP-exported span");
    assert_eq!(exported_logs.len(), 1, "expected one OTLP-exported log");

    // Validate span attributes include name=exportable
    let span = &exported_spans[0];
    let has_name_attr = span.attributes.iter().any(|kv| {
        kv.key.as_str() == "name"
            && matches!(&kv.value, OtelValue::String(s) if s.as_ref() == "exportable")
    });
    assert!(
        has_name_attr,
        "exported span should contain attribute name=exportable"
    );

    // Validate log: event name and attributes include code=1
    let log = &exported_logs[0];
    assert_eq!(
        log.event_name(),
        Some("v1.public.events.fusion.dev.MockDynLogEvent"),
        "expected event name on log record"
    );
    let has_code_1 = log.attributes_iter().any(|(k, v)| {
        k.as_str() == "code" && matches!(v, opentelemetry::logs::AnyValue::Int(i) if *i == 1)
    });
    assert!(has_code_1, "expected log attributes to contain code=1");
}

#[test]
fn test_otlp_export_with_links() {
    // Test that links are exported to OTLP
    let trace_id = rand::random::<u128>();

    let (trace_exporter, spans) = TestSpanExporter::new();
    let (log_exporter, _logs) = TestLogExporter::new();

    let otlp_layer = OTLPExporterLayer::new(trace_exporter, log_exporter);
    let trace_provider = otlp_layer.tracer_provider();

    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::empty(),
            std::iter::once(Box::new(otlp_layer) as ConsumerLayer),
        ),
    );

    tracing::subscriber::with_default(subscriber, || {
        let span1 = create_root_info_span(MockDynSpanEvent {
            name: "span_1".to_string(),
            flags: TelemetryOutputFlags::EXPORT_OTLP,
            ..Default::default()
        });

        let span2 = create_root_info_span(MockDynSpanEvent {
            name: "span_2".to_string(),
            flags: TelemetryOutputFlags::EXPORT_OTLP,
            ..Default::default()
        });

        // span2 follows from span1
        span2.follows_from(&span1);
    });

    trace_provider
        .shutdown()
        .expect("Failed to shutdown telemetry");

    let exported_spans = spans.lock().unwrap().clone();

    assert_eq!(exported_spans.len(), 2, "Should have exported 2 spans");

    // Find span2 (the one with links) - MockDynSpanEvent adds a prefix to the name
    let span2_data = exported_spans
        .iter()
        .find(|s| s.name.contains("span_2"))
        .unwrap_or_else(|| {
            panic!(
                "span2 not found in exported spans. Available spans: {:?}",
                exported_spans.iter().map(|s| &s.name).collect::<Vec<_>>()
            )
        });

    assert_eq!(
        span2_data.links.len(),
        1,
        "span2 should have exactly 1 link"
    );

    // Verify the link points to span1
    let span1_data = exported_spans
        .iter()
        .find(|s| s.name.contains("span_1"))
        .expect("span1 not found in exported spans");

    let link = &span2_data.links[0];
    assert_eq!(
        link.span_context.span_id(),
        span1_data.span_context.span_id(),
        "link should point to span1"
    );
    assert_eq!(
        link.span_context.trace_id(),
        span1_data.span_context.trace_id(),
        "link should have the same trace_id as span1"
    );
}
