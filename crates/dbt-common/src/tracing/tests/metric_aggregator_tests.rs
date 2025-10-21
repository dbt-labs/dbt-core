use crate::tracing::metrics::InvocationMetricKey;

use super::super::{
    emit::create_root_info_span,
    init::create_tracing_subcriber_with_layer,
    layer::{ConsumerLayer, MiddlewareLayer},
    layers::data_layer::TelemetryDataLayer,
    metrics::{MetricKey, get_metric},
    middlewares::metric_aggregator::TelemetryMetricAggregator,
    tests::mocks::{MockDynSpanEvent, TestLayer},
};
use dbt_telemetry::TelemetryOutputFlags;

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

    let test_metric_key = MetricKey::InvocationMetric(InvocationMetricKey::TotalWarnings);

    tracing::subscriber::with_default(subscriber, || {
        let root_span_guard = create_root_info_span(MockDynSpanEvent {
            name: "root".to_string(),
            flags: TelemetryOutputFlags::empty(),
            ..Default::default()
        })
        .entered();

        assert_eq!(get_metric(test_metric_key), 0);

        tracing::warn!("test warning");

        assert_eq!(get_metric(test_metric_key), 1);

        drop(root_span_guard);
    });

    assert_eq!(get_metric(test_metric_key), 0);
}
