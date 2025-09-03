mod config;
pub mod constants;
mod convert;
#[macro_use]
pub mod emit;
mod background_writer;
pub mod event_info;
mod init;
mod invocation;
mod layers;
pub mod metrics;
mod shared;
mod shared_writer;
pub mod span_info;

pub use config::FsTraceConfig;
pub use convert::log_level_to_severity;
pub use init::{TelemetryShutdown, init_tracing};
pub use invocation::create_invocation_attributes;
pub use shared::ToTracingValue;

#[cfg(test)]
mod tests {
    use crate::logging::LogFormat;

    use super::*;

    use constants::TRACING_ATTR_FIELD;
    use dbt_telemetry::{
        DebugValue, DevInternalInfo, LegacyLogEventInfo, LogEventInfo, LogRecordInfo,
        RecordCodeLocation, SeverityNumber, SpanEndInfo, SpanStartInfo, TelemetryAttributes,
        TelemetryRecord, UnknownInfo,
        serialize::arrow::{create_arrow_schema, deserialize_from_arrow},
    };
    use event_info::with_current_thread_event_data;
    use init::{TelemetryHandle, create_tracing_subcriber_with_layer};
    use std::sync::{Arc, Mutex};
    use std::{collections::BTreeMap, fs};
    use std::{panic::Location, time::SystemTime};
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
                package: "test_package",
                max_log_verbosity: tracing::level_filters::LevelFilter::TRACE,
                invocation_id,
                otm_file_path: None,
                otm_parquet_file_path: None,
                enable_progress: false,
                export_to_otlp: false,
                log_format: LogFormat::Default,
            },
            test_layer,
        )
        .expect("Failed to initialize tracing");

        let dummy_root_span = tracing::info_span!("not used");

        let mut telemetry_handle = TelemetryHandle::new(shutdown_items, dummy_root_span);

        let test_attrs = TelemetryAttributes::Log(LogEventInfo {
            code: Some(42),
            dbt_core_code: Some("test_code".to_string()),
            original_severity_number: SeverityNumber::Warn,
            original_severity_text: "WARN".to_string(),
            // This is important. Our infra will auto-populate the location from the callsite,
            // and we want to test that it works correctly, capturing real callsite
            location: RecordCodeLocation::none(),
        });

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

        let (span_id, span_name) = (span_ends[0].span_id, span_ends[0].span_name.clone());

        assert_eq!(log_records.len(), 1, "Expected 1 log record");
        let log_record = &log_records[0];

        assert_eq!(log_record.trace_id, trace_id);
        assert_eq!(log_record.span_id, Some(span_id));
        assert_eq!(log_record.span_name, Some(span_name));
        assert_eq!(log_record.severity_number, SeverityNumber::Info);
        assert_eq!(log_record.severity_text, "INFO".to_string());
        assert_eq!(log_record.body, "Test info event".to_string());

        // Now, the actual attributes that we should get back must include the location
        let expected_location = RecordCodeLocation {
            file: Some(test_location.file().to_string()),
            line: Some(test_location.line() + 1),
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
                package: "test_package",
                max_log_verbosity: tracing::level_filters::LevelFilter::TRACE,
                invocation_id,
                otm_file_path: Some(temp_file_path.clone()),
                otm_parquet_file_path: None,
                enable_progress: false,
                export_to_otlp: false,
                log_format: LogFormat::Default,
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
                span_name: span_type,
                parent_span_id: None,
                attributes: TelemetryAttributes::Unknown(UnknownInfo { name, .. }),
                ..
            }) if span_type == "Unknown" && name == "test_root_span" && *deserialized_trace_id == trace_id
        )));
        assert!(records.iter().any(|r| matches!(
            r,
            TelemetryRecord::SpanEnd(SpanEndInfo {
                trace_id: deserialized_trace_id,
                span_name: span_type,
                parent_span_id: None,
                attributes: TelemetryAttributes::Unknown(UnknownInfo { name, .. }),
                ..
            }) if span_type == "Unknown" && name == "test_root_span" && *deserialized_trace_id == trace_id
        )));

        // Extract root span ID
        let root_span_id = records
            .iter()
            .find_map(|r| {
                if let TelemetryRecord::SpanStart(SpanStartInfo {
                    span_id,
                    attributes: TelemetryAttributes::Unknown(UnknownInfo { name, .. }),
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
                span_name: span_type,
                parent_span_id: Some(parent_id),
                attributes: TelemetryAttributes::Unknown(UnknownInfo { name, .. }),
                ..
            }) if span_type == "Unknown" && name == "test_child_span" && *deserialized_trace_id == trace_id && *parent_id == root_span_id
        )));
        assert!(records.iter().any(|r| matches!(
            r,
            TelemetryRecord::SpanEnd(SpanEndInfo {
                trace_id: deserialized_trace_id,
                span_name: span_type,
                parent_span_id: Some(parent_id),
                attributes: TelemetryAttributes::Unknown(UnknownInfo { name, .. }),
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
                package: "test_package",
                max_log_verbosity: tracing::level_filters::LevelFilter::TRACE,
                invocation_id,
                otm_file_path: None,
                otm_parquet_file_path: None,
                enable_progress: false,
                export_to_otlp: false,
                log_format: LogFormat::Default,
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
                span_name: span_type,
                parent_span_id: None,
                attributes: TelemetryAttributes::Unknown(UnknownInfo { name, .. }),
                ..
            } if span_type == "Unknown" && name == "test_root_span" && *deserialized_trace_id == trace_id
        )));
        assert!(span_ends.iter().any(|r| matches!(
            r,
            SpanEndInfo {
                trace_id: deserialized_trace_id,
                span_name: span_type,
                parent_span_id: None,
                attributes: TelemetryAttributes::Unknown(UnknownInfo { name, .. }),
                ..
            } if span_type == "Unknown" && name == "test_root_span" && *deserialized_trace_id == trace_id
        )));

        // Extract root span ID
        let root_span_id = span_starts
            .iter()
            .find_map(|r| {
                if let SpanStartInfo {
                    span_id,
                    attributes: TelemetryAttributes::Unknown(UnknownInfo { name, .. }),
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
                span_name: span_type,
                parent_span_id: Some(parent_id),
                attributes: TelemetryAttributes::Unknown(UnknownInfo { name, .. }),
                ..
            } if span_type == "Unknown" && name == "test_child_span" && *deserialized_trace_id == trace_id && *parent_id == root_span_id
        )));
        assert!(span_ends.iter().any(|r| matches!(
            r,
            SpanEndInfo {
                trace_id: deserialized_trace_id,
                span_name: span_type,
                parent_span_id: Some(parent_id),
                attributes: TelemetryAttributes::Unknown(UnknownInfo { name, .. }),
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
                package: "test_package",
                max_log_verbosity: tracing::level_filters::LevelFilter::TRACE,
                invocation_id,
                otm_file_path: None,
                otm_parquet_file_path: None,
                enable_progress: false,
                export_to_otlp: false,
                log_format: LogFormat::Default,
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

    #[test]
    fn test_emit_macros() {
        // Initialize tracing with a custom layer to capture events
        let invocation_id = uuid::Uuid::new_v4();
        let trace_id = invocation_id.as_u128();

        let (test_layer, span_starts, span_ends, log_records) = TestLayer::new();

        // Init telemetry using internal API allowing to set thread local subscriber.
        // This avoids collisions with other unit tests, but prevents us from testing
        // the fallback logic with the global parent span
        let (subscriber, shutdown_items) = create_tracing_subcriber_with_layer(
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
            test_layer,
        )
        .expect("Failed to initialize tracing");

        let dummy_root_span = tracing::info_span!("not used");

        let mut telemetry_handle = TelemetryHandle::new(shutdown_items, dummy_root_span);

        // Create different test attributes for each call
        let root_attrs = TelemetryAttributes::Log(LogEventInfo {
            code: Some(100),
            dbt_core_code: Some("root_code".to_string()),
            original_severity_number: SeverityNumber::Info,
            original_severity_text: "INFO".to_string(),
            location: RecordCodeLocation::none(),
        });

        let child_attrs = TelemetryAttributes::Log(LogEventInfo {
            code: Some(200),
            dbt_core_code: Some("child_code".to_string()),
            original_severity_number: SeverityNumber::Debug,
            original_severity_text: "DEBUG".to_string(),
            location: RecordCodeLocation::none(),
        });

        let event1_attrs = TelemetryAttributes::Log(LogEventInfo {
            code: Some(300),
            dbt_core_code: Some("event1_code".to_string()),
            original_severity_number: SeverityNumber::Warn,
            original_severity_text: "WARN".to_string(),
            location: RecordCodeLocation::none(),
        });

        let event2_attrs = TelemetryAttributes::Log(LogEventInfo {
            code: Some(400),
            dbt_core_code: Some("event2_code".to_string()),
            original_severity_number: SeverityNumber::Error,
            original_severity_text: "ERROR".to_string(),
            location: RecordCodeLocation::none(),
        });

        // Capture locations for verification
        let mut root_location = Location::caller();
        let mut child_location = Location::caller();
        let mut event1_location = Location::caller();
        let mut event2_location = Location::caller();

        tracing::subscriber::with_default(subscriber, || {
            // Test create_root_info_span macro
            root_location = Location::caller();
            let root_span = create_root_info_span!(root_attrs.clone());
            let _root_guard = root_span.enter();

            // Test create_info_span macro (creates child span)
            child_location = Location::caller();
            let child_span = create_info_span!(child_attrs.clone());
            let _child_guard = child_span.enter();

            // Test emit_tracing_event with message
            event1_location = Location::caller();
            emit_tracing_event!(event1_attrs.clone(), "Event with message");

            // Test emit_tracing_event without message
            event2_location = Location::caller();
            emit_tracing_event!(event2_attrs.clone());
        });

        // Shutdown telemetry to ensure all data is processed
        let shutdown_errs = telemetry_handle.shutdown();
        assert_eq!(shutdown_errs.len(), 0);

        // Get captured data
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

        // Verify we captured 2 spans and 2 events
        assert_eq!(span_starts.len(), 2, "Expected 2 span starts");
        assert_eq!(span_ends.len(), 2, "Expected 2 span ends");
        assert_eq!(log_records.len(), 2, "Expected 2 log records");

        // Verify root span has correct attributes (no parent)
        let root_span_start = span_starts
            .iter()
            .find(|s| s.parent_span_id.is_none())
            .expect("Should find root span");

        assert_eq!(root_span_start.trace_id, trace_id);
        let expected_root_location = RecordCodeLocation {
            file: Some(root_location.file().to_string()),
            line: Some(root_location.line() + 1),
            module_path: Some(std::module_path!().to_string()),
            target: Some(std::module_path!().to_string()),
        };
        assert_eq!(
            root_span_start.attributes,
            root_attrs.with_location(expected_root_location)
        );

        // Verify child span has correct attributes and parent
        let child_span_start = span_starts
            .iter()
            .find(|s| s.parent_span_id.is_some())
            .expect("Should find child span");

        assert_eq!(child_span_start.trace_id, trace_id);
        assert_eq!(
            child_span_start.parent_span_id,
            Some(root_span_start.span_id)
        );
        let expected_child_location = RecordCodeLocation {
            file: Some(child_location.file().to_string()),
            line: Some(child_location.line() + 1),
            module_path: Some(std::module_path!().to_string()),
            target: Some(std::module_path!().to_string()),
        };
        assert_eq!(
            child_span_start.attributes,
            child_attrs.with_location(expected_child_location)
        );

        // Verify first event (with message)
        let event1 = log_records
            .iter()
            .find(|r| r.body == "Event with message")
            .expect("Should find event with message");

        assert_eq!(event1.trace_id, trace_id);
        assert_eq!(event1.span_id, Some(child_span_start.span_id));
        assert_eq!(event1.severity_number, SeverityNumber::Info);
        assert_eq!(event1.severity_text, "INFO");
        let expected_event1_location = RecordCodeLocation {
            file: Some(event1_location.file().to_string()),
            line: Some(event1_location.line() + 1),
            module_path: Some(std::module_path!().to_string()),
            target: Some(std::module_path!().to_string()),
        };
        assert_eq!(
            event1.attributes,
            event1_attrs.with_location(expected_event1_location)
        );

        // Verify second event (without message)
        let event2 = log_records
            .iter()
            .find(|r| r.body.is_empty())
            .expect("Should find event without message");

        assert_eq!(event2.trace_id, trace_id);
        assert_eq!(event2.span_id, Some(child_span_start.span_id));
        assert_eq!(event2.severity_number, SeverityNumber::Info);
        assert_eq!(event2.severity_text, "INFO");
        let expected_event2_location = RecordCodeLocation {
            file: Some(event2_location.file().to_string()),
            line: Some(event2_location.line() + 1),
            module_path: Some(std::module_path!().to_string()),
            target: Some(std::module_path!().to_string()),
        };
        assert_eq!(
            event2.attributes,
            event2_attrs.with_location(expected_event2_location)
        );
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_tracing_parquet_filtering() {
        let invocation_id = uuid::Uuid::new_v4();

        // Create a temporary file for the parquet output
        let temp_dir = std::env::temp_dir();
        let temp_file_path = temp_dir.join("test_telemetry_filtering.parquet");

        // Init telemetry using internal API allowing to set thread local subscriber.
        // This avoids collisions with other unit tests, but prevents us from testing
        // the fallback logic with the global parent span
        let (subscriber, shutdown_items) = create_tracing_subcriber_with_layer(
            FsTraceConfig {
                package: "test_package",
                max_log_verbosity: tracing::level_filters::LevelFilter::TRACE,
                invocation_id,
                otm_file_path: None,
                otm_parquet_file_path: Some(temp_file_path.clone()),
                enable_progress: false,
                export_to_otlp: false,
                log_format: LogFormat::Default,
            },
            None::<Box<dyn Layer<Layered<EnvFilter, Registry>> + Send + Sync>>,
        )
        .expect("Failed to initialize tracing");

        let dummy_root_span = tracing::info_span!("not used");
        let mut telemetry_handle = TelemetryHandle::new(shutdown_items, dummy_root_span);

        // Pre-create attrs to compare them later
        let test_legacy_log_attrs = TelemetryAttributes::LegacyLog(LegacyLogEventInfo {
            original_severity_number: SeverityNumber::Warn,
            original_severity_text: "WARN".to_string(),
            location: RecordCodeLocation::none(),
        });

        let test_log_attrs = TelemetryAttributes::Log(LogEventInfo {
            code: Some(42),
            dbt_core_code: Some("test_code".to_string()),
            original_severity_number: SeverityNumber::Warn,
            original_severity_text: "WARN".to_string(),
            location: RecordCodeLocation::none(),
        });

        let mut extra_map = BTreeMap::new();
        extra_map.insert("key".to_string(), DebugValue::Bool(true));

        let dev_span_attrs = TelemetryAttributes::DevInternal(DevInternalInfo {
            name: "dev_test".to_string(),
            location: RecordCodeLocation::none(),
            // Add extra attributes to ensure they are filtered out
            extra: Some(extra_map),
        });

        let before_start = SystemTime::now();

        let dev_span_attrs_expected = TelemetryAttributes::DevInternal(DevInternalInfo {
            name: "dev_test".to_string(),
            location: RecordCodeLocation::none(),
            extra: None,
        });

        // We do not need location here, but this is easier than unwrapping later
        let mut test_location = Location::caller();

        tracing::subscriber::with_default(subscriber, || {
            // Use DevInternal type as we currently forced to include it in the output
            let _dev_span = tracing::trace_span!(
                "dev_internal_span",
                { TRACING_ATTR_FIELD } = dev_span_attrs.clone().to_tracing_value()
            )
            .entered();

            // Emit a log with Log attributes (should be included) & save the location (almost, one line off)
            test_location = Location::caller();
            emit_tracing_event!(test_log_attrs.clone(), "Valid log message");

            // Emit a log with LegacyLog attributes (should be filtered out)
            emit_tracing_event!(test_legacy_log_attrs.clone(), "Legacy log message");
        });

        // Shutdown telemetry to ensure all data is flushed to the file
        let shutdown_errs = telemetry_handle.shutdown();
        assert_eq!(shutdown_errs.len(), 0);

        // Verify the parquet file was created
        assert!(temp_file_path.exists(), "Parquet file should exist");

        // Read back and deserialize the parquet file
        let file = fs::File::open(&temp_file_path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let mut all_records = Vec::new();
        for batch_result in reader {
            let batch = batch_result.unwrap();
            let (serialisable_schema, _) = create_arrow_schema().unwrap();
            let records = deserialize_from_arrow(&batch, &serialisable_schema).unwrap();
            all_records.extend(records);
        }

        // Verify filtering worked correctly - should have 2 records (1 SpanEnd with Process attrs, 1 LogRecord with Log attrs)
        assert_eq!(all_records.len(), 2, "Expected 2 records after filtering");

        // Verify we have the correct records
        let span_end_record = all_records
            .iter()
            .find(|r| matches!(r, TelemetryRecord::SpanEnd(_)))
            .expect("Expected a SpanEnd record");
        let log_record_record = all_records
            .iter()
            .find(|r| matches!(r, TelemetryRecord::LogRecord(_)))
            .expect("Expected a LogRecord");

        // Verify the SpanEnd record is the valid one
        if let TelemetryRecord::SpanEnd(SpanEndInfo {
            trace_id,
            span_id,
            span_name,
            parent_span_id,
            start_time_unix_nano,
            end_time_unix_nano,
            severity_number,
            severity_text,
            status,
            attributes,
        }) = span_end_record
        {
            assert_eq!(*trace_id, invocation_id.as_u128());
            assert_eq!(*span_id, 1);
            assert_eq!(span_name, "DevInternal(dev_test | log)");
            assert!(parent_span_id.is_none());
            assert_eq!(*severity_number, SeverityNumber::Trace);
            assert_eq!(severity_text, "TRACE");
            assert!(*start_time_unix_nano > before_start);
            assert!(*end_time_unix_nano > before_start);
            assert_eq!(*status, None);
            assert_eq!(*attributes, dev_span_attrs_expected);
        } else {
            panic!("Expected a SpanEnd record");
        };

        // Verify the LogRecord is the valid one (Log attributes)
        if let TelemetryRecord::LogRecord(LogRecordInfo {
            trace_id,
            span_id,
            span_name,
            time_unix_nano,
            body,
            severity_number,
            severity_text,
            attributes,
        }) = log_record_record
        {
            assert_eq!(*trace_id, invocation_id.as_u128());
            assert_eq!(*span_id, Some(1));
            assert_eq!(*span_name, Some("DevInternal(dev_test | log)".to_string()));
            assert!(*time_unix_nano > before_start);
            assert_eq!(body, "Valid log message");
            assert_eq!(*severity_number, SeverityNumber::Info);
            assert_eq!(*severity_text, "INFO");

            // Now, the actual attributes that we should get back must include the location
            let expected_location = RecordCodeLocation {
                file: Some(test_location.file().to_string()),
                line: Some(test_location.line() + 1),
                module_path: Some(std::module_path!().to_string()),
                target: Some(std::module_path!().to_string()),
            };

            assert_eq!(*attributes, test_log_attrs.with_location(expected_location));
        } else {
            panic!("Expected a LogRecord");
        }

        // Clean up
        let _ = fs::remove_file(&temp_file_path);
    }
}
