use std::fs;

use super::mocks::{MockDynLogEvent, MockDynSpanEvent};
use crate::tracing::emit::{create_root_info_span, emit_info_event};
use crate::tracing::init::create_tracing_subcriber_with_layer;
use crate::tracing::layers::data_layer::TelemetryDataLayer;
use crate::tracing::layers::jsonl_writer::build_jsonl_layer_with_background_writer;
use dbt_telemetry::TelemetryOutputFlags;
use tracing::Level;

#[test]
fn test_tracing_jsonl() {
    // Initialize tracing with a custom layer to capture events
    let trace_id = rand::random::<u128>();

    // Create a temporary file for the OTM output
    let temp_dir = std::env::temp_dir();
    let temp_file_path = temp_dir.join("test_otm.jsonl");

    // Init telemetry using internal API allowing to set thread local subscriber.
    // This avoids collisions with other unit tests
    let max_log_verbosity = tracing::level_filters::LevelFilter::TRACE;

    let (jsonl_layer, mut shutdown_handle) = build_jsonl_layer_with_background_writer(
        fs::File::create(&temp_file_path).expect("Failed to create temporary OTM file"),
        max_log_verbosity,
    );

    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::empty(),
            std::iter::once(jsonl_layer),
        ),
    );

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
    shutdown_handle
        .shutdown()
        .expect("Failed to shutdown telemetry");

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
    // Initialize tracing with a custom layer to capture events
    let trace_id = rand::random::<u128>();

    // Create a temporary file for the OTM output
    let temp_dir = std::env::temp_dir();
    let temp_file_path = temp_dir.join("test_jsonl_dyn_filtering.jsonl");

    // Init telemetry using internal API allowing to set thread local subscriber.
    // This avoids collisions with other unit tests
    let max_log_verbosity = tracing::level_filters::LevelFilter::TRACE;

    let (jsonl_layer, mut shutdown_handle) = build_jsonl_layer_with_background_writer(
        fs::File::create(&temp_file_path).expect("Failed to create temporary OTM file"),
        max_log_verbosity,
    );

    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::empty(),
            std::iter::once(jsonl_layer),
        ),
    );

    tracing::subscriber::with_default(subscriber, || {
        let exportable_span = create_root_info_span(MockDynSpanEvent {
            name: "exportable".to_string(),
            flags: TelemetryOutputFlags::EXPORT_JSONL,
            ..Default::default()
        });
        exportable_span.in_scope(|| {
            emit_info_event(
                MockDynLogEvent {
                    code: 1,
                    flags: TelemetryOutputFlags::EXPORT_JSONL,
                    ..Default::default()
                },
                Some("included log"),
            );
            emit_info_event(
                MockDynLogEvent {
                    code: 2,
                    flags: TelemetryOutputFlags::EXPORT_PARQUET,
                    ..Default::default()
                },
                Some("excluded log"),
            );
        });

        let _non_exportable_span = create_root_info_span(MockDynSpanEvent {
            name: "non_exportable".to_string(),
            flags: TelemetryOutputFlags::EXPORT_PARQUET,
            ..Default::default()
        });
    });

    // Shutdown telemetry to ensure all data is flushed to the file
    shutdown_handle
        .shutdown()
        .expect("Failed to shutdown telemetry");

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

#[test]
fn test_jsonl_basic_follows_from() {
    // Test that a single follows_from relationship creates a link
    let trace_id = rand::random::<u128>();

    let temp_dir = std::env::temp_dir();
    let temp_file_path = temp_dir.join("test_jsonl_basic_follows_from.jsonl");

    let (jsonl_layer, mut shutdown_handle) = build_jsonl_layer_with_background_writer(
        fs::File::create(&temp_file_path).expect("Failed to create temporary file"),
        tracing::level_filters::LevelFilter::TRACE,
    );

    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::empty(),
            std::iter::once(jsonl_layer),
        ),
    );

    tracing::subscriber::with_default(subscriber, || {
        let span1 = tracing::span!(Level::INFO, "span_1");
        let span2 = tracing::span!(Level::DEBUG, "span_2");
        // span2 follows from span1 (must happen after span1 is initialized)
        span2.follows_from(&span1);
    });

    shutdown_handle
        .shutdown()
        .expect("Failed to shutdown telemetry");

    let file_contents = fs::read_to_string(&temp_file_path).expect("Failed to read temporary file");
    fs::remove_file(&temp_file_path).expect("Failed to remove temporary file");

    let records: Vec<serde_json::Value> = file_contents
        .lines()
        .map(|line| dbt_serde_yaml::from_str(line).expect("Failed to parse JSON line"))
        .collect();

    // Find span1 start to get its ID
    let span1_start = records
        .iter()
        .find(|r| r["record_type"] == "SpanStart" && r["attributes"]["name"] == "span_1")
        .expect("span1 start not found");

    let span1_id = span1_start["span_id"]
        .as_str()
        .expect("span1_id must be a string");

    // Note: SpanStart won't have links because it's emitted before follows_from is called.
    // We check SpanEnd instead, which is emitted after follows_from.

    // Check SpanEnd - this should have the links since follows_from is called before span close
    let span2_end = records
        .iter()
        .find(|r| r["record_type"] == "SpanEnd" && r["attributes"]["name"] == "span_2")
        .expect("span2 end not found");

    let end_links = span2_end["links"]
        .as_array()
        .expect("links in SpanEnd must be an array");

    assert_eq!(end_links.len(), 1, "span2 end should have exactly 1 link");

    let end_link = &end_links[0];
    assert_eq!(
        end_link["span_id"]
            .as_str()
            .expect("link span_id must be a string"),
        span1_id,
        "SpanEnd link should point to span1"
    );
}

#[test]
fn test_jsonl_multiple_follows_from() {
    // Test that a span can follow from multiple spans
    let trace_id = rand::random::<u128>();

    let temp_dir = std::env::temp_dir();
    let temp_file_path = temp_dir.join("test_jsonl_multiple_follows_from.jsonl");

    let (jsonl_layer, mut shutdown_handle) = build_jsonl_layer_with_background_writer(
        fs::File::create(&temp_file_path).expect("Failed to create temporary file"),
        tracing::level_filters::LevelFilter::TRACE,
    );

    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::empty(),
            std::iter::once(jsonl_layer),
        ),
    );

    tracing::subscriber::with_default(subscriber, || {
        let span1 = tracing::span!(Level::INFO, "span_1");
        let span2 = tracing::span!(Level::INFO, "span_2");
        let span3 = tracing::span!(Level::INFO, "span_3");
        // span3 follows from both span1 and span2
        span3.follows_from(&span1);
        span3.follows_from(&span2);
    });

    shutdown_handle
        .shutdown()
        .expect("Failed to shutdown telemetry");

    let file_contents = fs::read_to_string(&temp_file_path).expect("Failed to read temporary file");
    fs::remove_file(&temp_file_path).expect("Failed to remove temporary file");

    let records: Vec<serde_json::Value> = file_contents
        .lines()
        .map(|line| dbt_serde_yaml::from_str(line).expect("Failed to parse JSON line"))
        .collect();

    // Find span3 end and verify it has links to both span1 and span2
    // (SpanStart won't have links as it's emitted before follows_from)
    let span3_end = records
        .iter()
        .find(|r| r["record_type"] == "SpanEnd" && r["attributes"]["name"] == "span_3")
        .expect("span3 end not found");

    let links = span3_end["links"]
        .as_array()
        .expect("links must be an array");

    assert_eq!(links.len(), 2, "span3 should have exactly 2 links");
}

#[test]
fn test_jsonl_missing_followed_span() {
    // Test that follows_from handles missing spans gracefully
    let trace_id = rand::random::<u128>();

    let temp_dir = std::env::temp_dir();
    let temp_file_path = temp_dir.join("test_jsonl_missing_followed_span.jsonl");

    let (jsonl_layer, mut shutdown_handle) = build_jsonl_layer_with_background_writer(
        fs::File::create(&temp_file_path).expect("Failed to create temporary file"),
        tracing::level_filters::LevelFilter::TRACE,
    );

    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::empty(),
            std::iter::once(jsonl_layer),
        ),
    );

    tracing::subscriber::with_default(subscriber, || {
        let span1 = tracing::span!(Level::INFO, "span_1");

        // Create a span ID that doesn't exist
        let fake_id = tracing::Id::from_u64(999999);

        // span1 follows from a non-existent span - should not panic
        span1.follows_from(&fake_id);
    });

    shutdown_handle
        .shutdown()
        .expect("Failed to shutdown telemetry");

    let file_contents = fs::read_to_string(&temp_file_path).expect("Failed to read temporary file");
    fs::remove_file(&temp_file_path).expect("Failed to remove temporary file");

    let records: Vec<serde_json::Value> = file_contents
        .lines()
        .map(|line| dbt_serde_yaml::from_str(line).expect("Failed to parse JSON line"))
        .collect();

    // Find span1 end and verify it has no links (graceful handling)
    let span1_end = records
        .iter()
        .find(|r| r["record_type"] == "SpanEnd" && r["attributes"]["name"] == "span_1")
        .expect("span1 end not found");

    // links should be null or an empty array when the followed span doesn't exist
    let links_exist = span1_end
        .get("links")
        .and_then(|v| v.as_array())
        .map(|arr| !arr.is_empty())
        .unwrap_or(false);

    assert!(
        !links_exist,
        "span1 should have no links when followed span doesn't exist"
    );
}
