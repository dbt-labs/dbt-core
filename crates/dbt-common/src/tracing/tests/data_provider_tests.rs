use super::super::{
    init::create_tracing_subcriber_with_layer,
    layer::{ConsumerLayer, MiddlewareLayer},
    layers::data_layer::TelemetryDataLayer,
    metrics::{MetricKey, get_metric},
    tests::mocks::{MockDynSpanEvent, MockMiddleware, TestLayer},
};
use crate::{create_info_span, create_root_info_span};
use dbt_telemetry::TelemetryOutputFlags;

#[derive(Debug, Clone, PartialEq)]
struct TestExtension {
    value: u64,
}

#[test]
fn data_provider_isolates_roots_and_shares_within_tree() {
    let trace_id = rand::random::<u128>();
    let (test_layer, ..) = TestLayer::new();

    let test_metric = MetricKey::TotalWarnings;

    // Middleware will increment metric and store extension on each span
    let middleware = MockMiddleware::new().with_span_start(move |span, data_provider| {
        let value = if span.span_name.contains("root1") {
            10
        } else if span.span_name.contains("root2") {
            20
        } else {
            0
        };

        if value > 0 {
            data_provider.increment_metric(test_metric, value);
            let _ = data_provider.init(|| TestExtension { value });
        }

        Some(span)
    });

    let test_layer = test_layer.with_span_start(move |span, data_provider| {
        let expected_value = if span.span_name.ends_with("1") {
            10
        } else {
            20
        };

        // Check metric is as expected
        let metric = data_provider.get_metric(test_metric);
        assert_eq!(
            metric, expected_value,
            "{} should see metric={}",
            span.span_name, expected_value
        );
        // Check extension is as expected
        data_provider.with(|ext: &TestExtension| {
            assert_eq!(
                ext.value, expected_value,
                "{} should see extension value={}",
                span.span_name, expected_value
            );
        });
    });

    let subscriber = create_tracing_subcriber_with_layer(
        tracing::level_filters::LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::once(Box::new(middleware) as MiddlewareLayer),
            std::iter::once(Box::new(test_layer) as ConsumerLayer),
        ),
    );

    tracing::subscriber::with_default(subscriber, || {
        // First root span tree: root1 -> child1 -> grandchild1
        let root1_guard = create_root_info_span!(
            MockDynSpanEvent {
                name: "root1".to_string(),
                flags: TelemetryOutputFlags::empty(),
                ..Default::default()
            }
            .into()
        )
        .entered();

        let metric1_after_root = get_metric(test_metric);
        assert_eq!(metric1_after_root, 10, "root1 should have metric=10");

        create_info_span!(
            MockDynSpanEvent {
                name: "child1".to_string(),
                flags: TelemetryOutputFlags::empty(),
                ..Default::default()
            }
            .into()
        )
        .in_scope(|| {
            let metric1_in_child = get_metric(test_metric);
            assert_eq!(metric1_in_child, 10, "child1 should see root1 metric=10");

            create_info_span!(
                MockDynSpanEvent {
                    name: "grandchild1".to_string(),
                    flags: TelemetryOutputFlags::empty(),
                    ..Default::default()
                }
                .into()
            )
            .in_scope(|| {
                let metric1_in_grandchild = get_metric(test_metric);
                assert_eq!(
                    metric1_in_grandchild, 10,
                    "grandchild1 should see root1 metric=10"
                );
            });
        });

        drop(root1_guard);

        // Second root span tree: root2 -> child2 -> grandchild2
        let _root2_guard = create_root_info_span!(
            MockDynSpanEvent {
                name: "root2".to_string(),
                flags: TelemetryOutputFlags::empty(),
                ..Default::default()
            }
            .into()
        )
        .entered();

        let metric2_after_root = get_metric(test_metric);
        assert_eq!(metric2_after_root, 20, "root2 should have metric=20");

        create_info_span!(
            MockDynSpanEvent {
                name: "child2".to_string(),
                flags: TelemetryOutputFlags::empty(),
                ..Default::default()
            }
            .into()
        )
        .in_scope(|| {
            let metric2_in_child = get_metric(test_metric);
            assert_eq!(metric2_in_child, 20, "child2 should see root2 metric=20");

            create_info_span!(
                MockDynSpanEvent {
                    name: "grandchild2".to_string(),
                    flags: TelemetryOutputFlags::empty(),
                    ..Default::default()
                }
                .into()
            )
            .in_scope(|| {
                let metric2_in_grandchild = get_metric(test_metric);
                assert_eq!(
                    metric2_in_grandchild, 20,
                    "grandchild2 should see root2 metric=20"
                );
            });
        });
    });
}
