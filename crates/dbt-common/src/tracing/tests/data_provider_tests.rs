use super::super::{
    emit::{create_info_span, create_root_info_span},
    init::create_tracing_subcriber_with_layer,
    layer::{ConsumerLayer, MiddlewareLayer},
    layers::data_layer::TelemetryDataLayer,
    metrics::{InvocationMetricKey, MetricKey, get_metric},
    tests::mocks::{MockDynSpanEvent, MockMiddleware, TestLayer},
};
use dbt_telemetry::TelemetryOutputFlags;

#[derive(Debug, Clone, PartialEq)]
struct TestExtension {
    value: u64,
}

#[test]
fn data_provider_isolates_roots_and_shares_within_tree() {
    let trace_id = rand::random::<u128>();
    let (test_layer, ..) = TestLayer::new();

    let test_metric = MetricKey::InvocationMetric(InvocationMetricKey::TotalWarnings);

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
            let _ = data_provider.init(TestExtension { value });
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
        let root1_guard = create_root_info_span(MockDynSpanEvent {
            name: "root1".to_string(),
            flags: TelemetryOutputFlags::empty(),
            ..Default::default()
        })
        .entered();

        let metric1_after_root = get_metric(test_metric);
        assert_eq!(metric1_after_root, 10, "root1 should have metric=10");

        create_info_span(MockDynSpanEvent {
            name: "child1".to_string(),
            flags: TelemetryOutputFlags::empty(),
            ..Default::default()
        })
        .in_scope(|| {
            let metric1_in_child = get_metric(test_metric);
            assert_eq!(metric1_in_child, 10, "child1 should see root1 metric=10");

            create_info_span(MockDynSpanEvent {
                name: "grandchild1".to_string(),
                flags: TelemetryOutputFlags::empty(),
                ..Default::default()
            })
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
        let _root2_guard = create_root_info_span(MockDynSpanEvent {
            name: "root2".to_string(),
            flags: TelemetryOutputFlags::empty(),
            ..Default::default()
        })
        .entered();

        let metric2_after_root = get_metric(test_metric);
        assert_eq!(metric2_after_root, 20, "root2 should have metric=20");

        create_info_span(MockDynSpanEvent {
            name: "child2".to_string(),
            flags: TelemetryOutputFlags::empty(),
            ..Default::default()
        })
        .in_scope(|| {
            let metric2_in_child = get_metric(test_metric);
            assert_eq!(metric2_in_child, 20, "child2 should see root2 metric=20");

            create_info_span(MockDynSpanEvent {
                name: "grandchild2".to_string(),
                flags: TelemetryOutputFlags::empty(),
                ..Default::default()
            })
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

#[derive(Debug, Clone, PartialEq)]
struct CustomExtension {
    counter: u64,
    message: String,
}

#[test]
fn data_provider_with_and_with_mut() {
    let trace_id = rand::random::<u128>();
    let (test_layer, ..) = TestLayer::new();

    // Middleware will insert custom extension on root span and verify final value on end
    let middleware = MockMiddleware::new()
        .with_span_start(move |span, data_provider| {
            if span.span_name.contains("root") {
                // Test insert
                data_provider.init(CustomExtension {
                    counter: 0,
                    message: "initial".to_string(),
                });
            }

            Some(span)
        })
        .with_span_end(move |span, data_provider| {
            if span.span_name.contains("root") {
                // Verify we can read the final value
                data_provider.with(|ext: &CustomExtension| {
                    assert_eq!(ext.counter, 2);
                    assert_eq!(ext.message, "updated 2");
                });
            }

            Some(span)
        });

    // Consumer will mutate the extension and verify
    let test_layer = test_layer.with_span_start(move |span, data_provider| {
        if span.span_name.ends_with("child1") {
            // Verify we can read the initial value
            data_provider.with(|ext: &CustomExtension| {
                assert_eq!(ext.counter, 0);
                assert_eq!(ext.message, "initial");
            });

            // Test with_mut by updating the extension
            data_provider.with_mut(|ext: &mut CustomExtension| {
                ext.counter += 1;
                ext.message = format!("updated {}", ext.counter);
            });
        }

        if span.span_name.ends_with("child2") {
            // Verify we can read the initial value
            data_provider.with(|ext: &CustomExtension| {
                assert_eq!(ext.counter, 1);
                assert_eq!(ext.message, "updated 1");
            });

            // Test get_mut
            data_provider.with_mut(|ext: &mut CustomExtension| {
                ext.counter += 1;
                ext.message = format!("updated {}", ext.counter);
            });
        }
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
        let _root_guard = create_root_info_span(MockDynSpanEvent {
            name: "root".to_string(),
            flags: TelemetryOutputFlags::empty(),
            ..Default::default()
        })
        .entered();

        // First child should increment counter to 1
        create_info_span(MockDynSpanEvent {
            name: "child1".to_string(),
            flags: TelemetryOutputFlags::empty(),
            ..Default::default()
        });

        // Second child should increment counter to 2
        create_info_span(MockDynSpanEvent {
            name: "child2".to_string(),
            flags: TelemetryOutputFlags::empty(),
            ..Default::default()
        });
    });
}

#[test]
fn data_provider_init_replaces_existing() {
    let trace_id = rand::random::<u128>();
    let (test_layer, ..) = TestLayer::new();

    let middleware = MockMiddleware::new().with_span_start(move |span, data_provider| {
        if span.span_name.contains("root") {
            // Insert first value
            let old = data_provider.init(CustomExtension {
                counter: 1,
                message: "first".to_string(),
            });
            assert!(old.is_none(), "First insert should return None");

            // Insert second value, should replace first
            let old = data_provider.init(CustomExtension {
                counter: 2,
                message: "second".to_string(),
            });
            assert!(old.is_some(), "Second insert should return Some");
            assert_eq!(old.unwrap().counter, 1);

            // Verify current value
            data_provider.with(|ext: &CustomExtension| {
                assert_eq!(ext.counter, 2);
                assert_eq!(ext.message, "second");
            });
        }

        Some(span)
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
        let _root_guard = create_root_info_span(MockDynSpanEvent {
            name: "root".to_string(),
            flags: TelemetryOutputFlags::empty(),
            ..Default::default()
        })
        .entered();
    });
}
