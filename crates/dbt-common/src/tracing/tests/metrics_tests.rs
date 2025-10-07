use crate::tracing::{
    data_provider::{DataProvider, DataProviderMut},
    init::create_tracing_subcriber_with_layer,
    layer::ConsumerLayer,
    layers::data_layer::TelemetryDataLayer,
    metrics::{InvocationMetricKey, MetricKey, get_metric, increment_metric},
};
use dbt_telemetry::TelemetryOutputFlags;
use tracing_subscriber::{Registry, registry::LookupSpan};

use super::mocks::{MockDynSpanEvent, TestLayer};

#[test]
fn metrics_are_scoped_to_root_span() {
    let trace_id = rand::random::<u128>();

    let (test_layer, ..) = TestLayer::new();

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
        let first_root = create_root_info_span!(
            MockDynSpanEvent {
                name: "first_root".to_string(),
                flags: TelemetryOutputFlags::empty(),
                ..Default::default()
            }
            .into()
        );
        {
            let _root_scope = first_root.enter();

            let first_child = create_info_span!(
                MockDynSpanEvent {
                    name: "first_child".to_string(),
                    flags: TelemetryOutputFlags::empty(),
                    ..Default::default()
                }
                .into()
            );
            {
                let _child_scope = first_child.enter();
                increment_metric(
                    MetricKey::InvocationMetric(InvocationMetricKey::TotalErrors),
                    3,
                );

                let first_child_id = first_child.id().expect("child span must have id");
                tracing::dispatcher::get_default(|dispatch| {
                    let registry = dispatch
                        .downcast_ref::<Registry>()
                        .expect("active subscriber must be backed by a registry");
                    let span_ref = registry
                        .span(&first_child_id)
                        .expect("child span must exist in registry");
                    let root_span = span_ref.scope().from_root().next().unwrap();

                    assert_eq!(first_root.id().expect("must exist"), root_span.id());

                    DataProviderMut::new(&root_span).increment_metric(
                        MetricKey::InvocationMetric(InvocationMetricKey::TotalWarnings),
                        2,
                    );
                });

                assert_eq!(
                    get_metric(MetricKey::InvocationMetric(
                        InvocationMetricKey::TotalErrors
                    )),
                    3
                );
                assert_eq!(
                    get_metric(MetricKey::InvocationMetric(
                        InvocationMetricKey::TotalWarnings
                    )),
                    2
                );
                assert_eq!(
                    get_metric(MetricKey::InvocationMetric(
                        InvocationMetricKey::AutoFixSuggestions
                    )),
                    0
                );

                tracing::dispatcher::get_default(|dispatch| {
                    let registry = dispatch
                        .downcast_ref::<Registry>()
                        .expect("active subscriber must be backed by a registry");
                    let span_ref = registry
                        .span(&first_child_id)
                        .expect("child span must exist in registry");
                    let root_span = span_ref.scope().from_root().next().unwrap();
                    assert_eq!(
                        DataProvider::new(&root_span).get_metric(MetricKey::InvocationMetric(
                            InvocationMetricKey::TotalWarnings
                        )),
                        2
                    );
                });
            }
        }
        drop(first_root);

        assert_eq!(
            get_metric(MetricKey::InvocationMetric(
                InvocationMetricKey::TotalErrors
            )),
            0
        );
        assert_eq!(
            get_metric(MetricKey::InvocationMetric(
                InvocationMetricKey::TotalWarnings
            )),
            0
        );
        assert_eq!(
            get_metric(MetricKey::InvocationMetric(
                InvocationMetricKey::AutoFixSuggestions
            )),
            0
        );

        let second_root = create_root_info_span!(
            MockDynSpanEvent {
                name: "second_root".to_string(),
                flags: TelemetryOutputFlags::empty(),
                ..Default::default()
            }
            .into()
        );
        {
            let _root_scope = second_root.enter();

            let second_child = create_info_span!(
                MockDynSpanEvent {
                    name: "second_child".to_string(),
                    flags: TelemetryOutputFlags::empty(),
                    ..Default::default()
                }
                .into()
            );
            {
                let _child_scope = second_child.enter();
                increment_metric(
                    MetricKey::InvocationMetric(InvocationMetricKey::TotalErrors),
                    7,
                );

                assert_eq!(
                    get_metric(MetricKey::InvocationMetric(
                        InvocationMetricKey::TotalErrors
                    )),
                    7
                );
                assert_eq!(
                    get_metric(MetricKey::InvocationMetric(
                        InvocationMetricKey::AutoFixSuggestions
                    )),
                    0
                );
            }
        }
        drop(second_root);

        assert_eq!(
            get_metric(MetricKey::InvocationMetric(
                InvocationMetricKey::TotalErrors
            )),
            0
        );
        assert_eq!(
            get_metric(MetricKey::InvocationMetric(
                InvocationMetricKey::TotalWarnings
            )),
            0
        );
        assert_eq!(
            get_metric(MetricKey::InvocationMetric(
                InvocationMetricKey::AutoFixSuggestions
            )),
            0
        );
    });

    assert_eq!(
        get_metric(MetricKey::InvocationMetric(
            InvocationMetricKey::TotalErrors
        )),
        0
    );
    assert_eq!(
        get_metric(MetricKey::InvocationMetric(
            InvocationMetricKey::TotalWarnings
        )),
        0
    );
    assert_eq!(
        get_metric(MetricKey::InvocationMetric(
            InvocationMetricKey::AutoFixSuggestions
        )),
        0
    );
}
