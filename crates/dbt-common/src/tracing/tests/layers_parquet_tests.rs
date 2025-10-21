use crate::tracing::{
    emit::{create_info_span_with_parent, emit_info_event},
    event_info::store_event_attributes,
    init::create_tracing_subcriber_with_layer,
    layers::{
        data_layer::{TelemetryDataLayer, get_span_start_info_from_span},
        parquet_writer::build_parquet_writer_layer,
    },
    span_info,
};
use dbt_telemetry::{
    CallTrace, LogMessage, LogRecordInfo, PackageUpdate, RecordCodeLocation, SeverityNumber,
    SpanEndInfo, TelemetryAttributes, TelemetryEventTypeRegistry, TelemetryRecord,
    serialize::arrow::deserialize_from_arrow,
};
use std::{collections::BTreeMap, fs, panic::Location, time::SystemTime};

#[test]
#[allow(clippy::cognitive_complexity)]
fn test_tracing_parquet_filtering() {
    let trace_id = rand::random::<u128>();

    // Create a temporary file for the parquet output
    let temp_dir = std::env::temp_dir();
    let temp_file_path = temp_dir.join("test_telemetry_filtering.parquet");

    let (parquet_layer, mut shutdown_handle) = build_parquet_writer_layer(
        fs::File::create(&temp_file_path).expect("Failed to create temporary OTM file"),
    )
    .expect("Failed to create parquet layer");

    // Init telemetry using internal API allowing to set thread local subscriber.
    // This avoids collisions with other unit tests
    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::empty(),
            std::iter::once(parquet_layer),
        ),
    );

    // Pre-create attrs to compare them later
    let mut test_log_attrs: TelemetryAttributes = LogMessage {
        code: Some(42),
        dbt_core_event_code: Some("test_code".to_string()),
        original_severity_number: SeverityNumber::Warn as i32,
        original_severity_text: "WARN".to_string(),
        package_name: None,
        unique_id: None,
        phase: None,
        file: None,
        line: None,
    }
    .into();

    let mut extra_map = BTreeMap::new();
    extra_map.insert("key".to_string(), true.into());

    let mut dev_span_attrs: TelemetryAttributes = CallTrace {
        name: "dev_test".to_string(),
        file: None,
        line: None,
        // Add extra attributes to ensure they are filtered out
        extra: extra_map,
    }
    .into();

    let before_start = SystemTime::now();

    // We do not need location here, but this is easier than unwrapping later
    let mut test_span_location = Location::caller();
    let mut test_log_location = Location::caller();
    // Same for expected span id
    let mut expected_span_id = 0;

    tracing::subscriber::with_default(subscriber, || {
        test_span_location = Location::caller();
        let dev_span = tracing::trace_span!(
            "dev_internal_span",
            _e = ?store_event_attributes(dev_span_attrs.clone())
        );

        let _sp = dev_span.enter();

        span_info::with_span(&dev_span, |span_ref| {
            expected_span_id = get_span_start_info_from_span(&span_ref).unwrap().span_id;
        });

        // Emit a log with Log attributes (should be included) & save the location (almost, one line off)
        test_log_location = Location::caller();
        emit_info_event(test_log_attrs.clone(), Some("Valid log message"));

        // Emit package update event, should be filtered out as it is marked as
        // non-exportable via parquet
        create_info_span_with_parent(
            dev_span.id(),
            PackageUpdate {
                version: "1.0.0".to_string(),
                package: "test_package".to_string(),
                exe_path: None,
            },
        );
    });

    // Shutdown telemetry to ensure all data is flushed to the file
    shutdown_handle
        .shutdown()
        .expect("Failed to shutdown telemetry");

    // Verify the parquet file was created
    assert!(temp_file_path.exists(), "Parquet file should exist");

    // Read back and deserialize the parquet file
    let file = fs::File::open(&temp_file_path).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let mut all_records = Vec::new();
    let registry = TelemetryEventTypeRegistry::public();
    for batch_result in reader {
        let batch = batch_result.unwrap();
        let records = deserialize_from_arrow(&batch, registry).unwrap();
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
        trace_id: recorded_trace_id,
        span_id,
        span_name,
        parent_span_id,
        links,
        start_time_unix_nano,
        end_time_unix_nano,
        severity_number,
        severity_text,
        status,
        attributes,
    }) = span_end_record
    {
        assert_eq!(*recorded_trace_id, trace_id);
        assert_eq!(*span_id, expected_span_id);
        assert!(span_name.starts_with("Dev trace: dev_test ("));
        assert!(parent_span_id.is_none());
        assert!(links.is_none());
        assert_eq!(*severity_number, SeverityNumber::Trace);
        assert_eq!(severity_text, "TRACE");
        assert!(*start_time_unix_nano > before_start);
        assert!(*end_time_unix_nano > before_start);
        assert_eq!(*status, None);

        // Now, the actual attributes that we should get back must include the location
        let expected_location = RecordCodeLocation {
            file: Some(test_span_location.file().to_string()),
            line: Some(test_span_location.line() + 1),
            module_path: Some(std::module_path!().to_string()),
            target: Some(std::module_path!().to_string()),
        };

        dev_span_attrs
            .inner_mut()
            .with_code_location(expected_location);

        assert_eq!(*attributes, dev_span_attrs);
    } else {
        panic!("Expected a SpanEnd record");
    };

    // Verify the LogRecord is the valid one (Log attributes)
    if let TelemetryRecord::LogRecord(LogRecordInfo {
        trace_id: recorded_trace_id,
        span_id,
        event_id: _,
        span_name,
        time_unix_nano,
        body,
        severity_number,
        severity_text,
        attributes,
    }) = log_record_record
    {
        assert_eq!(*recorded_trace_id, trace_id);
        assert_eq!(*span_id, Some(expected_span_id));
        assert!(
            span_name
                .clone()
                .expect("Span must be set")
                .starts_with("Dev trace: dev_test (")
        );
        assert!(*time_unix_nano > before_start);
        assert_eq!(body, "Valid log message");
        assert_eq!(*severity_number, SeverityNumber::Info);
        assert_eq!(*severity_text, "INFO");

        // Now, the actual attributes that we should get back must include the location
        let expected_location = RecordCodeLocation {
            file: Some(test_log_location.file().to_string()),
            line: Some(test_log_location.line() + 1),
            module_path: Some(std::module_path!().to_string()),
            target: Some(std::module_path!().to_string()),
        };

        test_log_attrs
            .inner_mut()
            .with_code_location(expected_location);
        assert_eq!(*attributes, test_log_attrs);
    } else {
        panic!("Expected a LogRecord");
    }

    // Clean up
    let _ = fs::remove_file(&temp_file_path);
}
