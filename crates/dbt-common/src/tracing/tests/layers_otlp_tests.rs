use std::sync::{Arc, Mutex};

use crate::logging::LogFormat;
use crate::tracing::{
    FsTraceConfig,
    init::{TelemetryHandle, create_tracing_subcriber_with_layer},
    layers::otlp::OTLPExporterLayer,
};

use super::events::{MockDynLogEvent, MockDynSpanEvent};
use dbt_telemetry::TelemetryExportFlags;
use opentelemetry::Value as OtelValue;
use opentelemetry_sdk as sdk;
use tracing_subscriber::{EnvFilter, Registry, layer::Layered};

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
    let invocation_id = uuid::Uuid::new_v4();

    // Create test exporters and share state
    let (trace_exporter, spans) = TestSpanExporter::new();
    let (log_exporter, logs) = TestLogExporter::new();

    // Build OTLP layer with test exporters
    let otlp_layer: OTLPExporterLayer<Layered<EnvFilter, Registry>> =
        OTLPExporterLayer::new(trace_exporter, log_exporter);
    // Clone providers for graceful shutdown later (batch processors flush on shutdown)
    let trace_provider = otlp_layer.tracer_provider();
    let log_provider = otlp_layer.logger_provider();

    // Init telemetry using internal API, disabling default OTLP (we pass custom layer)
    let (subscriber, mut shutdown_items) = create_tracing_subcriber_with_layer(
        FsTraceConfig {
            package: "test_package",
            max_log_verbosity: tracing::level_filters::LevelFilter::TRACE,
            invocation_id,
            otm_file_path: None,
            otm_parquet_file_path: None,
            enable_progress: false,
            export_to_otlp: false,
            log_format: LogFormat::Default,
        },
        otlp_layer,
    )
    .expect("Failed to initialize tracing");

    let dummy_root_span = tracing::info_span!("not used");
    // Add OTLP providers to shutdown list so that data is flushed
    shutdown_items.push(Box::new(trace_provider));
    shutdown_items.push(Box::new(log_provider));
    let mut telemetry_handle = TelemetryHandle::new(shutdown_items, dummy_root_span);

    // Emit events under the thread-local subscriber
    tracing::subscriber::with_default(subscriber, || {
        let exportable_span = create_root_info_span!(
            MockDynSpanEvent {
                name: "exportable".to_string(),
                flags: TelemetryExportFlags::EXPORT_OTLP,
                ..Default::default()
            }
            .into()
        );

        exportable_span.in_scope(|| {
            emit_tracing_event!(
                MockDynLogEvent {
                    code: 1,
                    flags: TelemetryExportFlags::EXPORT_OTLP,
                    ..Default::default()
                }
                .into(),
                "included log"
            );
            emit_tracing_event!(
                MockDynLogEvent {
                    code: 2,
                    flags: TelemetryExportFlags::EXPORT_JSONL, // Not OTLP-exportable
                    ..Default::default()
                }
                .into(),
                "excluded log"
            );
        });

        // This span should not be exported to OTLP
        let _non_exportable_span = create_root_info_span!(
            MockDynSpanEvent {
                name: "non_exportable".to_string(),
                flags: TelemetryExportFlags::EXPORT_JSONL, // Not OTLP-exportable
                ..Default::default()
            }
            .into()
        );
    });

    let shutdown_errs = telemetry_handle.shutdown();
    assert_eq!(shutdown_errs.len(), 0);

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
