mod config;
pub mod constants;
mod convert;
#[macro_use]
pub mod emit;
pub mod event_info;
mod file_writer;
mod init;
mod layers;
pub mod metrics;
mod shared;
pub mod span_info;

pub use config::FsTraceConfig;
pub use convert::log_level_to_severity;
pub use init::{TelemetryShutdown, init_tracing};
pub use shared::ToTracingValue;

#[cfg(test)]
mod tests {
    use super::*;

    use dbt_telemetry::{
        LogAttributes, LogRecordInfo, RecordCodeLocation, SeverityNumber, SpanAttributes,
        SpanEndInfo, SpanStartInfo, TelemetryRecord,
    };
    use event_info::with_current_thread_event_data;
    use init::{TelemetryHandle, create_tracing_subcriber_with_layer};
    use std::fs;
    use std::panic::Location;
    use std::sync::{Arc, Mutex};
    use tracing::{Subscriber, span};
    use tracing_subscriber::{
        EnvFilter, Layer, Registry,
        layer::{Context, Layered},
    };

    // Custom layer to capture telemetry data
    #[derive(Clone)]
    struct TestLayer {
        span_starts: Arc<Mutex<Vec<SpanStartInfo>>>,
        span_ends: Arc<Mutex<Vec<SpanEndInfo>>>,
        log_records: Arc<Mutex<Vec<LogRecordInfo>>>,
    }

    impl TestLayer {
        #[allow(clippy::type_complexity)]
        fn new() -> (
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
            with_current_thread_event_data(|log_record| {
                self.log_records.lock().unwrap().push(log_record.clone());
            });
        }
    }

    #[test]
    fn test_emit_event() {
        // Initialize tracing with a custom layer to capture events
        let invocation_id = uuid::Uuid::new_v4();
        let trace_id = invocation_id.as_u128();

        let (test_layer, _, span_ends, log_records) = TestLayer::new();

        // Init telemetry using internal API allowing to set thread local subscriber.
        // This avoids collisions with other unit tests, but prevents us from testing
        // the fallback logic with the global parent span
        let (subscriber, shutdown_items) = create_tracing_subcriber_with_layer(
            FsTraceConfig {
                max_log_verbosity: tracing::level_filters::LevelFilter::TRACE,
                invocation_id,
                otm_file_path: None,
                enable_progress: false,
                export_to_otlp: false,
            },
            test_layer,
        )
        .expect("Failed to initialize tracing");

        let dummy_root_span = tracing::info_span!("not used");

        let mut telemetry_handle = TelemetryHandle::new(shutdown_items, dummy_root_span);

        let test_attrs = LogAttributes::Log {
            code: Some(42),
            dbt_core_code: Some("test_code".to_string()),
            original_severity_number: SeverityNumber::Warn,
            original_severity_text: Some("WARN".to_string()),
            // This is important. Our infra will auto-populate the location from the callsite,
            // and we want to test that it works correctly, capturing real callsite
            location: RecordCodeLocation::none(),
        };

        // We do not need location here, but this is easier than unwrapping later
        let mut test_location = Location::caller();

        tracing::subscriber::with_default(subscriber, || {
            tracing::info_span!("test_root_span").in_scope(|| {
                // Emit the event & save the location (almost, one line off)
                test_location = Location::caller();
                emit_tracing_event!(test_attrs.clone(), "Test info event");
            })
        });

        // Shutdown telemetry to ensure all data is processed
        let shutdown_errs = telemetry_handle.shutdown();
        assert_eq!(shutdown_errs.len(), 0);

        // Get captured data
        let log_records = Arc::into_inner(log_records)
            .expect("Should have no refs")
            .into_inner()
            .expect("Should have no locks");
        let span_ends = Arc::into_inner(span_ends)
            .expect("Should have no refs")
            .into_inner()
            .expect("Should have no locks");

        // Verify captured data
        assert_eq!(span_ends.len(), 1, "Expected 1 span end record");

        let (span_id, span_name) = (span_ends[0].span_id, span_ends[0].name.clone());

        assert_eq!(log_records.len(), 1, "Expected 1 log record");
        let log_record = &log_records[0];

        assert_eq!(log_record.trace_id, trace_id);
        assert_eq!(log_record.span_id, Some(span_id));
        assert_eq!(log_record.span_name, Some(span_name));
        assert_eq!(log_record.severity_number, SeverityNumber::Info);
        assert_eq!(log_record.severity_text, Some("INFO".to_string()));
        assert_eq!(log_record.body, "Test info event".to_string());

        // Now, the actual attributes that we should get back must include the location
        let expected_location = RecordCodeLocation {
            file: Some(test_location.file().to_string()),
            line: Some(test_location.line() + 1),
            column: None, // Tracing lib doesn't report column number...
            module_path: Some(std::module_path!().to_string()),
            target: Some(std::module_path!().to_string()),
        };

        assert_eq!(
            log_record.attributes,
            test_attrs.with_location(expected_location)
        );
    }

    #[test]
    fn test_tracing_jsonl() {
        let invocation_id = uuid::Uuid::new_v4();
        let trace_id = invocation_id.as_u128();

        // Create a temporary file for the OTM output
        let temp_dir = std::env::temp_dir();
        let temp_file_path = temp_dir.join("test_otm.jsonl");

        // Init telemetry using internal API allowing to set thread local subscriber.
        // This avoids collisions with other unit tests, but prevents us from testing
        // the fallback logic with the global parent span
        let (subscriber, shutdown_items) = create_tracing_subcriber_with_layer(
            FsTraceConfig {
                max_log_verbosity: tracing::level_filters::LevelFilter::TRACE,
                invocation_id,
                otm_file_path: Some(temp_file_path.clone()),
                enable_progress: false,
                export_to_otlp: false,
            },
            None::<Box<dyn Layer<Layered<EnvFilter, Registry>> + Send + Sync>>,
        )
        .expect("Failed to initialize tracing");

        let dummy_root_span = tracing::info_span!("not used");

        let mut telemetry_handle = TelemetryHandle::new(shutdown_items, dummy_root_span);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info_span!("test_root_span").in_scope(|| {
                tracing::info!("Log message in root span");

                let span = tracing::info_span!("test_child_span");
                let _enter = span.enter();

                tracing::info!("Log message in child span");
                // Span will be created and closed automatically
            })
        });

        // Shutdown telemetry to ensure all data is flushed to the file
        let shutdown_errs = telemetry_handle.shutdown();
        assert_eq!(shutdown_errs.len(), 0);

        // Read the temporary file
        let file_contents =
            fs::read_to_string(&temp_file_path).expect("Failed to read temporary OTM file");

        // Clean up the temporary file
        fs::remove_file(&temp_file_path).expect("Failed to remove temporary file");

        let records: Vec<TelemetryRecord> = file_contents
            .lines()
            .map(|line| {
                serde_json::from_str::<TelemetryRecord>(line)
                    .expect("Failed to parse TelemetryRecord from line")
            })
            .collect();

        assert_eq!(
            records.len(),
            6,
            "Expected exactly 6 telemetry records (2x2 spans + 2 logs)"
        );

        // Test root span is present
        assert!(records.iter().any(|r| matches!(
            r,
            TelemetryRecord::SpanStart(SpanStartInfo {
                trace_id: deserialized_trace_id,
                name: span_type,
                parent_span_id: None,
                attributes: SpanAttributes::Unknown { name, .. },
                ..
            }) if span_type == "Unknown" && name == "test_root_span" && *deserialized_trace_id == trace_id
        )));
        assert!(records.iter().any(|r| matches!(
            r,
            TelemetryRecord::SpanEnd(SpanEndInfo {
                trace_id: deserialized_trace_id,
                name: span_type,
                parent_span_id: None,
                attributes: SpanAttributes::Unknown { name, .. },
                ..
            }) if span_type == "Unknown" && name == "test_root_span" && *deserialized_trace_id == trace_id
        )));

        // Extract root span ID
        let root_span_id = records
            .iter()
            .find_map(|r| {
                if let TelemetryRecord::SpanStart(SpanStartInfo {
                    span_id,
                    attributes: SpanAttributes::Unknown { name, .. },
                    ..
                }) = r
                    && name == "test_root_span"
                {
                    Some(*span_id)
                } else {
                    None
                }
            })
            .unwrap();

        // Test child span is present
        assert!(records.iter().any(|r| matches!(
            r,
            TelemetryRecord::SpanStart(SpanStartInfo {
                trace_id: deserialized_trace_id,
                name: span_type,
                parent_span_id: Some(parent_id),
                attributes: SpanAttributes::Unknown { name, .. },
                ..
            }) if span_type == "Unknown" && name == "test_child_span" && *deserialized_trace_id == trace_id && *parent_id == root_span_id
        )));
        assert!(records.iter().any(|r| matches!(
            r,
            TelemetryRecord::SpanEnd(SpanEndInfo {
                trace_id: deserialized_trace_id,
                name: span_type,
                parent_span_id: Some(parent_id),
                attributes: SpanAttributes::Unknown { name, .. },
                ..
            }) if span_type == "Unknown" && name == "test_child_span" && *deserialized_trace_id == trace_id && *parent_id == root_span_id
        )));

        // Test log records are present
        assert!(records.iter().any(|r| matches!(
            r,
            TelemetryRecord::LogRecord(LogRecordInfo {
                trace_id: deserialized_trace_id,
                span_name: Some(span_name),
                body,
                span_id: Some(span_id),
                ..
            }) if *deserialized_trace_id == trace_id && span_name == "Unknown" && body == "Log message in root span" && *span_id == root_span_id
        )));

        assert!(records.iter().any(|r| matches!(
            r,
            TelemetryRecord::LogRecord(LogRecordInfo {
                trace_id: deserialized_trace_id,
                span_name: Some(span_name),
                body,
                span_id: Some(span_id),
                ..
            }) if *deserialized_trace_id == trace_id && span_name == "Unknown" && body == "Log message in child span" && *span_id != root_span_id
        )));
    }

    #[test]
    fn test_tracing_with_custom_layer() {
        let invocation_id = uuid::Uuid::new_v4();
        let trace_id = invocation_id.as_u128();

        let (test_layer, span_starts, span_ends, log_records) = TestLayer::new();

        // Init telemetry using internal API allowing to set thread local subscriber.
        // This avoids collisions with other unit tests, but prevents us from testing
        // the fallback logic with the global parent span
        let (subscriber, shutdown_items) = create_tracing_subcriber_with_layer(
            FsTraceConfig {
                max_log_verbosity: tracing::level_filters::LevelFilter::TRACE,
                invocation_id,
                otm_file_path: None,
                enable_progress: false,
                export_to_otlp: false,
            },
            test_layer,
        )
        .expect("Failed to initialize tracing");

        let dummy_root_span = tracing::info_span!("not used");

        let mut telemetry_handle = TelemetryHandle::new(shutdown_items, dummy_root_span);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info_span!("test_root_span").in_scope(|| {
                tracing::info!("Log message in root span");

                let span = tracing::info_span!("test_child_span");
                let _enter = span.enter();

                tracing::info!("Log message in child span");
                // Span will be created and closed automatically
            })
        });

        // Shutdown telemetry to ensure all data is processed
        let shutdown_errs = telemetry_handle.shutdown();
        assert_eq!(shutdown_errs.len(), 0);

        // Verify captured data
        let span_starts = Arc::into_inner(span_starts)
            .expect("Should have no refs")
            .into_inner()
            .expect("Should have no locks");
        let span_ends = Arc::into_inner(span_ends)
            .expect("Should have no refs")
            .into_inner()
            .expect("Should have no locks");
        let log_records = Arc::into_inner(log_records)
            .expect("Should have no refs")
            .into_inner()
            .expect("Should have no locks");

        // Should have 2 user spans
        assert_eq!(span_starts.len(), 2, "Expected 2 span starts");
        assert_eq!(span_ends.len(), 2, "Expected 2 span ends");

        // Should have 2 log records
        assert_eq!(log_records.len(), 2, "Expected 2 log records");

        // Test root span is present
        assert!(span_starts.iter().any(|r| matches!(
            r,
            SpanStartInfo {
                trace_id: deserialized_trace_id,
                name: span_type,
                parent_span_id: None,
                attributes: SpanAttributes::Unknown { name, .. },
                ..
            } if span_type == "Unknown" && name == "test_root_span" && *deserialized_trace_id == trace_id
        )));
        assert!(span_ends.iter().any(|r| matches!(
            r,
            SpanEndInfo {
                trace_id: deserialized_trace_id,
                name: span_type,
                parent_span_id: None,
                attributes: SpanAttributes::Unknown { name, .. },
                ..
            } if span_type == "Unknown" && name == "test_root_span" && *deserialized_trace_id == trace_id
        )));

        // Extract root span ID
        let root_span_id = span_starts
            .iter()
            .find_map(|r| {
                if let SpanStartInfo {
                    span_id,
                    attributes: SpanAttributes::Unknown { name, .. },
                    ..
                } = r
                    && name == "test_root_span"
                {
                    Some(*span_id)
                } else {
                    None
                }
            })
            .unwrap();

        // Test child span is present
        assert!(span_starts.iter().any(|r| matches!(
            r,
            SpanStartInfo {
                trace_id: deserialized_trace_id,
                name: span_type,
                parent_span_id: Some(parent_id),
                attributes: SpanAttributes::Unknown { name, .. },
                ..
            } if span_type == "Unknown" && name == "test_child_span" && *deserialized_trace_id == trace_id && *parent_id == root_span_id
        )));
        assert!(span_ends.iter().any(|r| matches!(
            r,
            SpanEndInfo {
                trace_id: deserialized_trace_id,
                name: span_type,
                parent_span_id: Some(parent_id),
                attributes: SpanAttributes::Unknown { name, .. },
                ..
            } if span_type == "Unknown" && name == "test_child_span" && *deserialized_trace_id == trace_id && *parent_id == root_span_id
        )));

        // Test log records are present
        assert!(log_records.iter().any(|r| matches!(
            r,
            LogRecordInfo {
                trace_id: deserialized_trace_id,
                span_name: Some(span_name),
                body,
                span_id: Some(span_id),
                ..
            } if *deserialized_trace_id == trace_id && span_name == "Unknown" && body == "Log message in root span" && *span_id == root_span_id
        )));

        assert!(log_records.iter().any(|r| matches!(
            r,
            LogRecordInfo {
                trace_id: deserialized_trace_id,
                span_name: Some(span_name),
                body,
                span_id: Some(span_id),
                ..
            } if *deserialized_trace_id == trace_id && span_name == "Unknown" && body == "Log message in child span" && *span_id != root_span_id
        )));
    }

    #[test]
    fn test_tracing_log_record_poisoning() {
        use std::sync::Condvar;
        use std::thread;

        struct SharedLayer {
            pair: Arc<(Mutex<bool>, Condvar)>,
        }

        impl<S> Layer<S> for SharedLayer
        where
            S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
        {
            fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
                // If we are in thread 1 - wait until thread 2 has finished emitting
                // event before getting the structured data. This effectively tests
                // whether events from other threads may pollute current thread
                if event.metadata().target() == "thread 1" {
                    let (lock, cvar) = &*self.pair;
                    let mut finished = lock.lock().unwrap();
                    while !*finished {
                        finished = cvar.wait(finished).unwrap();
                    }
                }

                with_current_thread_event_data(|log_record| {
                    assert_eq!(
                        log_record.body,
                        format!("event from {}", event.metadata().target())
                    );
                });
            }
        }

        let invocation_id = uuid::Uuid::new_v4();

        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let test_layer = SharedLayer { pair: pair.clone() };

        // Init telemetry using internal API allowing to set thread local subscriber.
        // This avoids collisions with other unit tests, but prevents us from testing
        // the fallback logic with the global parent span
        let (subscriber, shutdown_items) = create_tracing_subcriber_with_layer(
            FsTraceConfig {
                max_log_verbosity: tracing::level_filters::LevelFilter::TRACE,
                invocation_id,
                otm_file_path: None,
                enable_progress: false,
                export_to_otlp: false,
            },
            test_layer,
        )
        .expect("Failed to initialize tracing");

        let dummy_root_span = tracing::info_span!("not used");

        let mut telemetry_handle = TelemetryHandle::new(shutdown_items, dummy_root_span);

        let subscriber = Arc::new(subscriber);

        tracing::subscriber::with_default(subscriber.clone(), || {
            let shared_span = tracing::info_span!("test_root_span");
            let shared_span_clone = shared_span.clone();

            // Thread 1
            let subscriber1 = subscriber.clone();
            let t1 = thread::spawn(move || {
                tracing::subscriber::with_default(subscriber1, || {
                    let _g = shared_span.entered();
                    tracing::info!(target: "thread 1", "event from thread 1");
                })
            });

            // Thread 2
            let subscriber2 = subscriber.clone();
            let t2 = thread::spawn(move || {
                tracing::subscriber::with_default(subscriber2, || {
                    let _g = shared_span_clone.entered();
                    tracing::info!(target: "thread 2","event from thread 2");

                    let (lock, cvar) = &*pair;
                    let mut finished = lock.lock().unwrap();
                    *finished = true;
                    // We notify the condvar that the value has changed.
                    cvar.notify_one();
                })
            });

            t1.join().unwrap();
            t2.join().unwrap();
        });

        // Shutdown telemetry to ensure all data is processed
        let shutdown_errs = telemetry_handle.shutdown();
        assert_eq!(shutdown_errs.len(), 0);
    }
}
