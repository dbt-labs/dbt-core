use super::super::{
    init::create_tracing_subcriber_with_layer,
    layer::{ConsumerLayer, MiddlewareLayer},
    layers::{data_layer::TelemetryDataLayer, metric_aggregator::TelemetryMetricAggregator},
    metrics::{MetricKey, get_metric},
    span_info::record_current_span_status_from_attrs,
    tests::mocks::{MockDynSpanEvent, TestLayer},
};
use dbt_telemetry::{ExecutionPhase, NodeEvaluated, NodeOutcome, NodeType, TelemetryOutputFlags};

#[test]
fn node_outcome_metrics_increment_on_span_close() {
    let trace_id = rand::random::<u128>();

    let (test_layer, ..) = TestLayer::new();

    // Init telemetry using internal API allowing to set thread local subscriber.
    // This avoids collisions with other unit tests
    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::once(Box::new(TelemetryMetricAggregator) as MiddlewareLayer),
            std::iter::once(Box::new(test_layer) as ConsumerLayer),
        ),
    );

    let test_node_outcome = NodeOutcome::Success;
    let test_metric_key =
        MetricKey::NodeOutcomeCounts(test_node_outcome, Default::default(), Default::default());

    tracing::subscriber::with_default(subscriber, || {
        let root_span_guard = create_root_info_span!(
            MockDynSpanEvent {
                name: "root".to_string(),
                flags: TelemetryOutputFlags::empty(),
                ..Default::default()
            }
            .into()
        )
        .entered();

        // Metric is 0 at the start
        assert_eq!(get_metric(test_metric_key), 0);

        {
            let _node_span_guard = create_info_span!(
                NodeEvaluated {
                    unique_id: "model.test.node".to_string(),
                    node_type: NodeType::Model as i32,
                    phase: ExecutionPhase::Run as i32,
                    ..Default::default()
                }
                .into()
            )
            .entered();

            // Metric is still zero until span is closed
            assert_eq!(get_metric(test_metric_key), 0);

            record_current_span_status_from_attrs(|attrs| {
                if let Some(node) = attrs.downcast_mut::<NodeEvaluated>() {
                    node.set_node_outcome(test_node_outcome);
                }
            });

            // We only record on span close, so metric should still be 0
            assert_eq!(get_metric(test_metric_key), 0);
        }

        // Span closed, recorded status should trigger a count
        assert_eq!(get_metric(test_metric_key), 1);

        drop(root_span_guard);

        // Dropping root, drops all metrics
        assert_eq!(get_metric(test_metric_key), 0);
    });

    assert_eq!(get_metric(test_metric_key), 0);
}

#[test]
fn warning_logs_increment_warning_metric() {
    let trace_id = rand::random::<u128>();

    let (test_layer, ..) = TestLayer::new();

    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::once(Box::new(TelemetryMetricAggregator) as MiddlewareLayer),
            std::iter::once(Box::new(test_layer) as ConsumerLayer),
        ),
    );

    let test_metric_key = MetricKey::TotalWarnings;

    tracing::subscriber::with_default(subscriber, || {
        let root_span_guard = create_root_info_span!(
            MockDynSpanEvent {
                name: "root".to_string(),
                flags: TelemetryOutputFlags::empty(),
                ..Default::default()
            }
            .into()
        )
        .entered();

        assert_eq!(get_metric(test_metric_key), 0);

        tracing::warn!("test warning");

        assert_eq!(get_metric(test_metric_key), 1);

        drop(root_span_guard);
    });

    assert_eq!(get_metric(test_metric_key), 0);
}
