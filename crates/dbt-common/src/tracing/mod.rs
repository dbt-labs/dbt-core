mod config;
pub mod constants;
mod convert;
mod file_writer;
mod init;
mod layers;
pub mod log_info;
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
        LogRecordInfo, SpanAttributes, SpanEndInfo, SpanStartInfo, TelemetryRecord,
    };
    use std::fs;

    #[test]
    fn test_split_layers_work_together() {
        let invocation_id = uuid::Uuid::new_v4();
        let trace_id = invocation_id.as_u128();

        // Create a temporary file for the OTM output
        let temp_dir = std::env::temp_dir();
        let temp_file_path = temp_dir.join("test_otm.jsonl");

        // Init telemetry
        let mut telemetry_handle = init_tracing(FsTraceConfig {
            max_log_level: tracing::level_filters::LevelFilter::TRACE,
            invocation_id,
            otm_file_path: Some(temp_file_path.clone()),
            print_to_stdout: false,
            #[cfg(all(debug_assertions, feature = "otlp"))]
            export_to_otlp: false,
        })
        .expect("Failed to initialize tracing");

        tracing::info_span!("test_root_span").in_scope(|| {
            tracing::info!("Log message in root span");

            let span = tracing::info_span!("test_child_span");
            let _enter = span.enter();

            tracing::info!("Log message in child span");
            // Span will be created and closed automatically
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
            8,
            "Expected exactly 8 telemetry records (1 process span + 4 spans + 2 logs)"
        );

        // Test root span is present
        assert!(records.iter().any(|r| matches!(
            r,
            TelemetryRecord::SpanStart(SpanStartInfo {
                trace_id: deserialized_trace_id,
                name: span_type,
                parent_span_id: Some(1),
                attributes: SpanAttributes::Unknown { name, .. },
                ..
            }) if span_type == "Unknown" && name == "test_root_span" && *deserialized_trace_id == trace_id
        )));
        assert!(records.iter().any(|r| matches!(
            r,
            TelemetryRecord::SpanEnd(SpanEndInfo {
                trace_id: deserialized_trace_id,
                name: span_type,
                parent_span_id: Some(1),
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
                span_name,
                body,
                span_id,
                ..
            }) if *deserialized_trace_id == trace_id && span_name == "Unknown" && body == "Log message in root span" && *span_id == root_span_id
        )));

        assert!(records.iter().any(|r| matches!(
            r,
            TelemetryRecord::LogRecord(LogRecordInfo {
                trace_id: deserialized_trace_id,
                span_name,
                body,
                span_id,
                ..
            }) if *deserialized_trace_id == trace_id && span_name == "Unknown" && body == "Log message in child span" && *span_id != root_span_id
        )));
    }
}
