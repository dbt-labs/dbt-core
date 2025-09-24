use super::mocks::TestWriter;
use crate::{
    logging::LogFormat,
    tracing::{
        FsTraceConfig,
        filter::WithOutputFilter as _,
        init::{TelemetryHandle, create_tracing_subcriber_with_layer},
        layers::pretty_writer::TelemetryPrettyWriterLayer,
    },
};

use super::mocks::{MockDynLogEvent, MockDynSpanEvent};
use dbt_telemetry::{TelemetryOutputFlags, TelemetryRecordRef};
use tracing_subscriber::Layer;

fn mock_format_telemetry_record(record: TelemetryRecordRef, _is_tty: bool) -> Option<String> {
    match record {
        TelemetryRecordRef::LogRecord(log) => Some(format!(
            "[LOG] msg=\"{}\" span=\"{}\"",
            log.body,
            log.span_name.as_deref().unwrap_or_default()
        )),
        TelemetryRecordRef::SpanStart(span) => Some(format!(
            "[SPAN START] name=\"{}\"",
            span.attributes.event_display_name()
        )),
        TelemetryRecordRef::SpanEnd(span) => Some(format!(
            "[SPAN END] name=\"{}\"",
            span.attributes.event_display_name()
        )),
    }
}

#[test]
fn pretty_layer_applies_filter_and_formatting() {
    let invocation_id = uuid::Uuid::new_v4();
    let file_writer = TestWriter::non_terminal();
    let tty_writer = TestWriter::terminal();

    let layer = TelemetryPrettyWriterLayer::new(file_writer.clone(), mock_format_telemetry_record)
        .and_then(TelemetryPrettyWriterLayer::new(
            tty_writer.clone(),
            mock_format_telemetry_record,
        ))
        .with_output_filter(|record, _| match record {
            TelemetryRecordRef::SpanStart(span_start) => span_start.span_name.contains("keep"),
            TelemetryRecordRef::SpanEnd(span_end) => span_end.span_name.contains("keep"),
            TelemetryRecordRef::LogRecord(_) => true,
        });
    let extra_layer: Option<_> = Some(layer);

    let (subscriber, shutdown_items) = create_tracing_subcriber_with_layer(
        FsTraceConfig {
            package: "pretty-test",
            max_log_verbosity: tracing::level_filters::LevelFilter::TRACE,
            invocation_id,
            otm_file_path: None,
            otm_parquet_file_path: None,
            enable_progress: false,
            export_to_otlp: false,
            log_format: LogFormat::Default,
            log_path: Default::default(),
            enable_query_log: false,
        },
        extra_layer,
    )
    .expect("init tracing");

    let dummy_root_span = tracing::info_span!("not used");
    let mut telemetry_handle = TelemetryHandle::new(shutdown_items, dummy_root_span);

    tracing::subscriber::with_default(subscriber, || {
        let span = create_root_info_span!(
            MockDynSpanEvent {
                name: "span-keep".into(),
                flags: TelemetryOutputFlags::OUTPUT_LOG_FILE,
                ..Default::default()
            }
            .into()
        );

        span.in_scope(|| {
            create_root_info_span!(
                MockDynSpanEvent {
                    name: "span-drop".into(),
                    flags: TelemetryOutputFlags::ALL,
                    ..Default::default()
                }
                .into()
            )
            .in_scope(|| {
                emit_tracing_event!(
                    MockDynLogEvent {
                        code: 1,
                        flags: TelemetryOutputFlags::OUTPUT_LOG_FILE,
                        ..Default::default()
                    }
                    .into(),
                    "file only"
                );

                emit_tracing_event!(
                    MockDynLogEvent {
                        code: 2,
                        flags: TelemetryOutputFlags::OUTPUT_CONSOLE,
                        ..Default::default()
                    }
                    .into(),
                    "console only"
                );
            });
        });
    });

    let shutdown_errs = telemetry_handle.shutdown();
    assert!(shutdown_errs.is_empty());

    // Check that the file writer (non-TTY) received the expected lines

    let file_log = file_writer.get_lines().join("");
    let tty_log = tty_writer.get_lines().join("");

    // Note that log reports span-drop as parent - that's the current
    // limitation of our custom filtering - see comments in event_info.rs
    assert_eq!(
        file_log,
        r#"
[SPAN START] name="Mock Dyn Span Event: span-keep"
[LOG] msg="file only" span="Mock Dyn Span Event: span-drop"
[SPAN END] name="Mock Dyn Span Event: span-keep"
"#
        .trim_start(),
        "Unexpected file (non-TTY) log: {file_log}"
    );

    assert_eq!(
        tty_log,
        r#"
[LOG] msg="console only" span="Mock Dyn Span Event: span-drop"
"#
        .trim_start(),
        "Unexpected TTY log: {tty_log}"
    );
}
