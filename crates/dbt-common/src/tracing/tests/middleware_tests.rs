use crate::tracing::{
    init::create_tracing_subcriber_with_layer,
    layer::{ConsumerLayer, MiddlewareLayer},
    layers::data_layer::TelemetryDataLayer,
    metrics::{MetricKey, get_metric},
};
use crate::{create_info_span, create_root_info_span, emit_tracing_event};

use super::mocks::{MockDynLogEvent, MockDynSpanEvent, MockMiddleware, TestLayer};
use dbt_telemetry::TelemetryOutputFlags;
use tracing::level_filters::LevelFilter;

#[test]
fn middleware_modifies_drops_and_updates_metrics() {
    let trace_id = rand::random::<u128>();
    let (test_layer, span_starts, span_ends, log_records) = TestLayer::new();

    let middleware = MockMiddleware::new()
        .with_span_start(|mut span, metrics| {
            if span.span_name.ends_with("drop-me") {
                return None;
            }

            if span.span_name.ends_with("child") {
                metrics.increment_metric(MetricKey::TotalWarnings, 1);

                span.attributes = MockDynSpanEvent {
                    name: "mutated-child".to_string(),
                    flags: TelemetryOutputFlags::ALL,
                    has_sensitive: false,
                    was_scrubbed: true,
                }
                .into();
                span.span_name = "Mock Dyn Span Event: mutated-child".to_string();
            }

            Some(span)
        })
        .with_log_record(|record, _| {
            if record
                .attributes
                .downcast_ref::<MockDynLogEvent>()
                .is_some_and(|event| matches!(event.code, 2 | 3))
            {
                None
            } else {
                Some(record)
            }
        });

    let middlewares: Vec<MiddlewareLayer> = vec![Box::new(middleware)];
    let consumers: Vec<ConsumerLayer> = vec![Box::new(test_layer)];

    let mut data_layer = TelemetryDataLayer::new(
        trace_id,
        false,
        middlewares.into_iter(),
        consumers.into_iter(),
    );
    data_layer.with_sequential_ids();

    let subscriber = create_tracing_subcriber_with_layer(LevelFilter::TRACE, data_layer);

    let recorded_metric = tracing::subscriber::with_default(subscriber, || {
        let _root_guard = create_root_info_span!(
            MockDynSpanEvent {
                name: "root".to_string(),
                flags: TelemetryOutputFlags::ALL,
                ..Default::default()
            }
            .into()
        )
        .entered();

        create_info_span!(
            MockDynSpanEvent {
                name: "child".to_string(),
                flags: TelemetryOutputFlags::ALL,
                ..Default::default()
            }
            .into()
        )
        .in_scope(|| {
            emit_tracing_event!(
                MockDynLogEvent {
                    code: 1,
                    flags: TelemetryOutputFlags::ALL,
                    ..Default::default()
                }
                .into(),
                "keep me"
            );
            emit_tracing_event!(
                MockDynLogEvent {
                    code: 2,
                    flags: TelemetryOutputFlags::ALL,
                    ..Default::default()
                }
                .into(),
                "drop me"
            );
        });

        create_info_span!(
            MockDynSpanEvent {
                name: "drop-me".to_string(),
                flags: TelemetryOutputFlags::ALL,
                ..Default::default()
            }
            .into()
        )
        .in_scope(|| {
            emit_tracing_event!(
                MockDynLogEvent {
                    code: 3,
                    flags: TelemetryOutputFlags::ALL,
                    ..Default::default()
                }
                .into(),
                "should vanish"
            );
        });

        get_metric(MetricKey::TotalWarnings)
    });

    assert_eq!(recorded_metric, 1, "middleware should increment metric");

    let captured_span_starts = {
        let guard = span_starts.lock().expect("span starts mutex poisoned");
        guard.clone()
    };
    let captured_span_ends = {
        let guard = span_ends.lock().expect("span ends mutex poisoned");
        guard.clone()
    };
    let captured_log_records = {
        let guard = log_records.lock().expect("log records mutex poisoned");
        guard.clone()
    };

    let log_codes: Vec<Option<i32>> = captured_log_records
        .iter()
        .map(|record| {
            record
                .attributes
                .downcast_ref::<MockDynLogEvent>()
                .map(|event| event.code)
        })
        .collect();

    assert_eq!(
        captured_span_starts.len(),
        2,
        "dropped span should not be recorded"
    );
    assert_eq!(
        captured_span_ends.len(),
        2,
        "dropped span should not emit end record"
    );
    assert_eq!(
        log_codes,
        vec![Some(1)],
        "only log with code 1 should remain"
    );
    assert_eq!(
        captured_log_records.len(),
        1,
        "dropped log should be filtered out"
    );
    assert_eq!(captured_log_records[0].body, "keep me");

    let mutated_span = captured_span_starts
        .iter()
        .find(|span| span.span_name == "Mock Dyn Span Event: mutated-child")
        .expect("mutated span should be present");
    let mutated_end = captured_span_ends
        .iter()
        .find(|span| span.span_name == "Mock Dyn Span Event: mutated-child")
        .expect("mutated span end should be present");

    let attrs = mutated_span
        .attributes
        .downcast_ref::<MockDynSpanEvent>()
        .expect("attributes should be the mutated span event");
    assert_eq!(attrs.name, "mutated-child");
    assert!(attrs.was_scrubbed, "middleware should update attributes");

    assert_eq!(mutated_span.span_id, mutated_end.span_id);
}
