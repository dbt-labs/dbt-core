use std::fs;

use crate::logging::LogFormat;
use crate::tracing::{
    FsTraceConfig,
    init::{TelemetryHandle, create_tracing_subcriber_with_layer},
};

use super::mocks::{MockDynLogEvent, MockDynSpanEvent};
use dbt_telemetry::TelemetryOutputFlags;
use tracing_subscriber::{EnvFilter, Layer, Registry, layer::Layered};

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
            log_path: temp_dir,
            enable_query_log: false,
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

    // NOTE: TelemetryRecord no longer implements Deserialize for JSONL output.
    // We deserialize each line into a generic JSON value (via dbt_serde_yaml which
    // is compatible with serde_json) and assert on the JSON structure.
    let trace_id_hex = format!("{trace_id:032x}");

    let records: Vec<serde_json::Value> = file_contents
        .lines()
        .map(|line| {
            dbt_serde_yaml::from_str(line).expect("Failed to parse telemetry JSON line into Value")
        })
        .collect();

    assert_eq!(
        records.len(),
        6,
        "Expected exactly 6 telemetry records (2x2 spans + 2 logs)"
    );

    // Root span start
    let root_start = records
        .iter()
        .find(|r| {
            r["record_type"] == "SpanStart"
                && r["span_name"]
                    .as_str()
                    .expect("span_name must be set")
                    .starts_with("Unknown")
                && r["attributes"]["name"] == "test_root_span"
                && r["trace_id"] == trace_id_hex
                && r["parent_span_id"].is_null()
                && r["event_type"]
                    .as_str()
                    .map(|s| s.ends_with(".dev.Unknown"))
                    .unwrap_or(false)
        })
        .expect("Root SpanStart record not found");

    // Root span end
    assert!(
        records.iter().any(|r| {
            r["record_type"] == "SpanEnd"
                && r["span_name"]
                    .as_str()
                    .expect("span_name must be set")
                    .starts_with("Unknown")
                && r["attributes"]["name"] == "test_root_span"
                && r["trace_id"] == trace_id_hex
                && r["parent_span_id"].is_null()
        }),
        "Root SpanEnd record not found"
    );

    let root_span_id = root_start["span_id"]
        .as_str()
        .expect("root span_id must be a string")
        .to_string();

    // Child span start
    let child_start = records
        .iter()
        .find(|r| {
            r["record_type"] == "SpanStart"
                && r["span_name"]
                    .as_str()
                    .expect("span_name must be set")
                    .starts_with("Unknown")
                && r["attributes"]["name"] == "test_child_span"
                && r["trace_id"] == trace_id_hex
                && r["parent_span_id"] == root_span_id
        })
        .expect("Child SpanStart record not found");

    // Child span end
    assert!(
        records.iter().any(|r| {
            r["record_type"] == "SpanEnd"
                && r["span_name"]
                    .as_str()
                    .expect("span_name must be set")
                    .starts_with("Unknown")
                && r["attributes"]["name"] == "test_child_span"
                && r["trace_id"] == trace_id_hex
                && r["parent_span_id"] == root_span_id
        }),
        "Child SpanEnd record not found"
    );

    let child_span_id = child_start["span_id"]
        .as_str()
        .expect("child span_id must be a string")
        .to_string();

    // Log in root span
    assert!(
        records.iter().any(|r| {
            r["record_type"] == "LogRecord"
                && r["span_name"]
                    .as_str()
                    .expect("span_name must be set")
                    .starts_with("Unknown")
                && r["body"] == "Log message in root span"
                && r["trace_id"] == trace_id_hex
                && r["span_id"] == root_span_id
        }),
        "Root span log record not found"
    );

    // Log in child span
    assert!(
        records.iter().any(|r| {
            r["record_type"] == "LogRecord"
                && r["span_name"]
                    .as_str()
                    .expect("span_name must be set")
                    .starts_with("Unknown")
                && r["body"] == "Log message in child span"
                && r["trace_id"] == trace_id_hex
                && r["span_id"] == child_span_id
        }),
        "Child span log record not found"
    );
}

#[test]
fn test_jsonl_dynamic_output_flags_filtering() {
    let invocation_id = uuid::Uuid::new_v4();
    let temp_dir = std::env::temp_dir();
    let temp_file_path = temp_dir.join("test_jsonl_dyn_filtering.jsonl");

    let (subscriber, shutdown_items) = create_tracing_subcriber_with_layer(
        FsTraceConfig {
            package: "test_package_dyn",
            max_log_verbosity: tracing::level_filters::LevelFilter::TRACE,
            invocation_id,
            otm_file_path: Some(temp_file_path.clone()),
            otm_parquet_file_path: None,
            enable_progress: false,
            export_to_otlp: false,
            log_format: LogFormat::Default,
            log_path: temp_dir,
            enable_query_log: false,
        },
        None::<Box<dyn Layer<Layered<EnvFilter, Registry>> + Send + Sync>>,
    )
    .expect("Failed to initialize tracing");

    let dummy_root_span = tracing::info_span!("not used");
    let mut telemetry_handle = TelemetryHandle::new(shutdown_items, dummy_root_span);

    tracing::subscriber::with_default(subscriber, || {
        let exportable_span = create_root_info_span!(
            MockDynSpanEvent {
                name: "exportable".to_string(),
                flags: TelemetryOutputFlags::EXPORT_JSONL,
                ..Default::default()
            }
            .into()
        );
        exportable_span.in_scope(|| {
            emit_tracing_event!(
                MockDynLogEvent {
                    code: 1,
                    flags: TelemetryOutputFlags::EXPORT_JSONL,
                    ..Default::default()
                }
                .into(),
                "included log"
            );
            emit_tracing_event!(
                MockDynLogEvent {
                    code: 2,
                    flags: TelemetryOutputFlags::EXPORT_PARQUET,
                    ..Default::default()
                }
                .into(),
                "excluded log"
            );
        });

        let _non_exportable_span = create_root_info_span!(
            MockDynSpanEvent {
                name: "non_exportable".to_string(),
                flags: TelemetryOutputFlags::EXPORT_PARQUET,
                ..Default::default()
            }
            .into()
        );
    });

    let shutdown_errs = telemetry_handle.shutdown();
    assert_eq!(shutdown_errs.len(), 0);

    let file_contents = fs::read_to_string(&temp_file_path).expect("read jsonl");
    fs::remove_file(&temp_file_path).expect("remove jsonl");

    let records: Vec<serde_json::Value> = file_contents
        .lines()
        .map(|line| dbt_serde_yaml::from_str(line).expect("parse jsonl"))
        .collect();

    // Expect exactly 3 records: SpanStart + SpanEnd for exportable span, and one included log
    assert_eq!(records.len(), 3);

    assert!(records.iter().any(|r| r["record_type"] == "SpanStart"
        && r["event_type"] == "v1.public.events.fusion.dev.MockDynSpanEvent"
        && r["attributes"]["name"] == "exportable"));
    assert!(records.iter().any(|r| r["record_type"] == "SpanEnd"
        && r["event_type"] == "v1.public.events.fusion.dev.MockDynSpanEvent"
        && r["attributes"]["name"] == "exportable"));
    assert!(records.iter().any(|r| r["record_type"] == "LogRecord"
        && r["event_type"] == "v1.public.events.fusion.dev.MockDynLogEvent"
        && r["body"] == "included log"));

    assert!(
        !records
            .iter()
            .any(|r| r["record_type"] == "LogRecord" && r["body"] == "excluded log")
    );
    assert!(!records.iter().any(|r| r["event_type"]
        == "v1.public.events.fusion.dev.MockDynSpanEvent"
        && r["attributes"]["name"] == "non_exportable"));
}
