use dbt_telemetry::{
    CallTrace, ExecutionPhase, LogMessage, LogRecordInfo, NodeEvaluated, RecordCodeLocation,
    SeverityNumber, SpanEndInfo, SpanStartInfo, TelemetryAttributes, TelemetryOutputFlags, Unknown,
};
use std::collections::BTreeMap;
use std::panic::Location;
use std::sync::Arc;

use crate::tracing::{
    data_provider::DataProvider, layer::ConsumerLayer, layers::data_layer::TelemetryDataLayer,
};

use super::{
    super::{init::create_tracing_subcriber_with_layer, layer::TelemetryConsumer},
    mocks::{MockDynSpanEvent, TestLayer},
};

#[test]
fn test_emit_event_and_apply_context() {
    // Initialize tracing with a custom layer to capture events
    let trace_id = rand::random::<u128>();

    let (test_layer, _, span_ends, log_records) = TestLayer::new();

    // Init telemetry using internal API allowing to set thread local subscriber.
    // This avoids collisions with other unit tests
    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::empty(),
            std::iter::once(Box::new(test_layer) as ConsumerLayer),
        ),
    );

    let mut test_attrs: TelemetryAttributes = LogMessage {
        code: Some(42),
        dbt_core_event_code: Some("test_code".to_string()),
        original_severity_number: SeverityNumber::Warn as i32,
        original_severity_text: "WARN".to_string(),
        // The rest will be auto injected
        // This is important. Our infra will auto-populate the location from the callsite,
        // as well as context (phase & unique_id)
        // and we want to test that it works correctly, capturing real callsite
        unique_id: None,
        phase: None,
        file: None,
        line: None,
    }
    .into();

    let mut test_location = Location::caller();
    let expected_node_unique_id = "model.test.my_model";
    let expected_node_phase = ExecutionPhase::Render;

    tracing::subscriber::with_default(subscriber, || {
        let _rs = create_root_info_span!(
            MockDynSpanEvent {
                name: "root".to_string(),
                flags: TelemetryOutputFlags::ALL,
                ..Default::default()
            }
            .into()
        )
        .entered();

        let node_span = create_info_span!(
            NodeEvaluated {
                unique_id: expected_node_unique_id.into(),
                phase: expected_node_phase as i32,
                ..Default::default()
            }
            .into()
        );
        node_span.in_scope(|| {
            // Emit the event & save the location (almost, one line off)
            test_location = Location::caller();
            emit_tracing_event!(test_attrs.clone(), "Test info event");
        });
    });

    let log_records = {
        let lr = log_records.lock().expect("Should have no locks");
        lr.clone()
    };
    let span_ends = {
        let se = span_ends.lock().expect("Should have no locks");
        se.clone()
    };

    // Verify captured data
    assert_eq!(span_ends.len(), 2, "Expected 2 span end record");

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
    test_attrs.inner_mut().with_code_location(expected_location);

    // Also expect unique_id (and phase) to be injected from the NodeEvaluated context
    if let Some(lm) = test_attrs.downcast_mut::<LogMessage>() {
        lm.unique_id = Some(expected_node_unique_id.into());
        lm.phase = Some(expected_node_phase as i32);
    }

    assert_eq!(log_record.attributes, test_attrs);
}

#[test]
fn test_tracing_with_custom_layer() {
    // Initialize tracing with a custom layer to capture events
    let trace_id = rand::random::<u128>();

    let (test_layer, span_starts, span_ends, log_records) = TestLayer::new();

    // Init telemetry using internal API allowing to set thread local subscriber.
    // This avoids collisions with other unit tests
    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::empty(),
            std::iter::once(Box::new(test_layer) as ConsumerLayer),
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

    // Verify captured data
    let span_starts = {
        let ss = span_starts.lock().expect("Should have no locks");
        ss.clone()
    };
    let span_ends = {
        let se = span_ends.lock().expect("Should have no locks");
        se.clone()
    };
    let log_records = {
        let lr = log_records.lock().expect("Should have no locks");
        lr.clone()
    };

    // Should have 2 user spans
    assert_eq!(span_starts.len(), 2, "Expected 2 span starts");
    assert_eq!(span_ends.len(), 2, "Expected 2 span ends");

    // Should have 2 log records
    assert_eq!(log_records.len(), 2, "Expected 2 log records");

    // Test root span is present
    assert!(span_starts.iter().any(|r| {
        if let SpanStartInfo {
            trace_id: deserialized_trace_id,
            span_name,
            parent_span_id: None,
            attributes,
            ..
        } = r
        {
            let name = attributes
                .downcast_ref::<Unknown>()
                .expect("Must be of Unknown type")
                .name
                .as_str();
            span_name.starts_with("Unknown")
                && name == "test_root_span"
                && *deserialized_trace_id == trace_id
        } else {
            false
        }
    }));
    assert!(span_ends.iter().any(|r| {
        if let SpanEndInfo {
            trace_id: deserialized_trace_id,
            span_name,
            parent_span_id: None,
            attributes,
            ..
        } = r
        {
            let name = attributes
                .downcast_ref::<Unknown>()
                .expect("Must be of Unknown type")
                .name
                .as_str();
            span_name.starts_with("Unknown")
                && name == "test_root_span"
                && *deserialized_trace_id == trace_id
        } else {
            false
        }
    }));

    // Extract root span ID
    let root_span_id = span_starts
        .iter()
        .find_map(|r| {
            let SpanStartInfo {
                span_id,
                attributes,
                ..
            } = r;

            let name = attributes
                .downcast_ref::<Unknown>()
                .expect("Must be of Unknown type")
                .name
                .as_str();
            if name == "test_root_span" {
                Some(*span_id)
            } else {
                None
            }
        })
        .unwrap();

    // Test child span is present
    assert!(span_starts.iter().any(|r| {
        if let SpanStartInfo {
            trace_id: deserialized_trace_id,
            span_name,
            parent_span_id: Some(parent_id),
            attributes,
            ..
        } = r
        {
            let name = attributes
                .downcast_ref::<Unknown>()
                .expect("Must be of Unknown type")
                .name
                .as_str();
            span_name.starts_with("Unknown")
                && name == "test_child_span"
                && *deserialized_trace_id == trace_id
                && *parent_id == root_span_id
        } else {
            false
        }
    }));
    assert!(span_ends.iter().any(|r| {
        if let SpanEndInfo {
            trace_id: deserialized_trace_id,
            span_name,
            parent_span_id: Some(parent_id),
            attributes,
            ..
        } = r
        {
            let name = attributes
                .downcast_ref::<Unknown>()
                .expect("Must be of Unknown type")
                .name
                .as_str();
            span_name.starts_with("Unknown")
                && name == "test_child_span"
                && *deserialized_trace_id == trace_id
                && *parent_id == root_span_id
        } else {
            false
        }
    }));

    // Test log records are present
    assert!(log_records.iter().any(|r| matches!(
        r,
        LogRecordInfo {
            trace_id: deserialized_trace_id,
            span_name: Some(span_name),
            body,
            span_id: Some(span_id),
            ..
        } if *deserialized_trace_id == trace_id && span_name.starts_with("Unknown") && body == "Log message in root span" && *span_id == root_span_id
    )));

    assert!(log_records.iter().any(|r| matches!(
        r,
        LogRecordInfo {
            trace_id: deserialized_trace_id,
            span_name: Some(span_name),
            body,
            span_id: Some(span_id),
            ..
        } if *deserialized_trace_id == trace_id && span_name.starts_with("Unknown") && body == "Log message in child span" && *span_id != root_span_id
    )));
}

#[test]
fn test_tracing_log_record_poisoning() {
    use std::thread;

    struct SharedLayer;

    impl TelemetryConsumer for SharedLayer {
        fn on_log_record(&self, record: &LogRecordInfo, _: &DataProvider<'_>) {
            assert_eq!(
                record.body,
                format!("event from thread {:?}", thread::current().id()),
            );
        }
    }

    // Initialize tracing with a custom layer to capture events
    let trace_id = rand::random::<u128>();

    // Init telemetry using internal API allowing to set thread local subscriber.
    // This avoids collisions with other unit tests
    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::empty(),
            std::iter::once(Box::new(SharedLayer) as ConsumerLayer),
        ),
    );

    let subscriber = Arc::new(subscriber);

    tracing::subscriber::with_default(subscriber.clone(), || {
        let shared_span = tracing::info_span!("test_root_span");
        let shared_span_clone = shared_span.clone();

        // Thread 1
        let subscriber1 = subscriber.clone();
        let t1 = thread::spawn(move || {
            tracing::subscriber::with_default(subscriber1, || {
                let _g = shared_span.entered();
                emit_tracing_event!(
                    LogMessage {
                        ..Default::default()
                    }
                    .into(),
                    "event from thread {:?}",
                    thread::current().id()
                );
            })
        });

        // Thread 2
        let subscriber2 = subscriber.clone();
        let t2 = thread::spawn(move || {
            tracing::subscriber::with_default(subscriber2, || {
                let _g = shared_span_clone.entered();
                emit_tracing_event!(
                    LogMessage {
                        ..Default::default()
                    }
                    .into(),
                    "event from thread {:?}",
                    thread::current().id()
                );
            })
        });

        t1.join().unwrap();
        t2.join().unwrap();
    });
}

#[test]
fn test_emit_macros() {
    // Initialize tracing with a custom layer to capture events
    let trace_id = rand::random::<u128>();

    let (test_layer, span_starts, span_ends, log_records) = TestLayer::new();

    // Init telemetry using internal API allowing to set thread local subscriber.
    // This avoids collisions with other unit tests
    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::empty(),
            std::iter::once(Box::new(test_layer) as ConsumerLayer),
        ),
    );

    // Create different test attributes for each call
    let mut root_attrs: TelemetryAttributes = CallTrace {
        name: "root_span".to_string(),
        file: None,
        line: None,
        extra: BTreeMap::new(),
    }
    .into();

    let mut child_attrs: TelemetryAttributes = CallTrace {
        name: "child_span".to_string(),
        file: None,
        line: None,
        extra: BTreeMap::new(),
    }
    .into();

    let mut event1_attrs: TelemetryAttributes = LogMessage {
        code: Some(300),
        dbt_core_event_code: Some("event1_code".to_string()),
        original_severity_number: SeverityNumber::Warn as i32,
        original_severity_text: "WARN".to_string(),
        unique_id: None,
        phase: None,
        file: None,
        line: None,
    }
    .into();

    let mut event2_attrs: TelemetryAttributes = LogMessage {
        code: Some(400),
        dbt_core_event_code: Some("event2_code".to_string()),
        original_severity_number: SeverityNumber::Error as i32,
        original_severity_text: "ERROR".to_string(),
        unique_id: None,
        phase: None,
        file: None,
        line: None,
    }
    .into();

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

    // Get captured data
    let span_starts = {
        let ss = span_starts.lock().expect("Should have no locks");
        ss.clone()
    };
    let span_ends = {
        let se = span_ends.lock().expect("Should have no locks");
        se.clone()
    };
    let log_records = {
        let lr = log_records.lock().expect("Should have no locks");
        lr.clone()
    };

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
    root_attrs
        .inner_mut()
        .with_code_location(expected_root_location);
    assert_eq!(root_span_start.attributes, root_attrs);

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
    child_attrs
        .inner_mut()
        .with_code_location(expected_child_location);
    assert_eq!(child_span_start.attributes, child_attrs);

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
    event1_attrs
        .inner_mut()
        .with_code_location(expected_event1_location);

    assert_eq!(event1.attributes, event1_attrs);

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
    event2_attrs
        .inner_mut()
        .with_code_location(expected_event2_location);

    assert_eq!(event2.attributes, event2_attrs);
}
