use crate::tracing::{
    dbt_emit::emit_warn_log_message,
    dbt_metrics::{FusionMetricKey, InvocationMetricKey},
    layer::{ConsumerLayer, MiddlewareLayer},
    metrics::get_metric,
    middlewares::{
        metric_aggregator::TelemetryMetricAggregator,
        warn_error_options::TelemetryWarnErrorOptionsMiddleware,
    },
};
use dbt_tracing::emit::create_root_info_span;
use dbt_tracing::init::create_tracing_subcriber_with_layer;
use dbt_tracing::test_support::mocks::{MockDynSpanEvent, TestLayer, test_data_layer};

use crate::ErrorCode;
use crate::warn_error_options::{SupportedLegacyWarnError, WarnErrorOptionValue, WarnErrorOptions};
use dbt_tracing::{SeverityNumber, TelemetryOutputFlags};
use tracing::level_filters::LevelFilter;

#[test]
fn warn_error_options_middleware_updates_runtime_decisions() {
    let trace_id = rand::random::<u128>();
    let (test_layer, _span_starts, _span_ends, log_records) = TestLayer::new();
    let (warn_error_options_middleware, options_handle) =
        TelemetryWarnErrorOptionsMiddleware::new(WarnErrorOptions::default());

    let middlewares: Vec<MiddlewareLayer> = vec![
        Box::new(warn_error_options_middleware),
        Box::new(TelemetryMetricAggregator),
    ];
    let consumers: Vec<ConsumerLayer> = vec![Box::new(test_layer)];

    let mut data_layer = test_data_layer(
        trace_id,
        None,
        false,
        middlewares.into_iter(),
        consumers.into_iter(),
    );
    data_layer.with_sequential_ids();

    let subscriber = create_tracing_subcriber_with_layer(LevelFilter::TRACE, data_layer);

    let (error_count, warning_count) = tracing::subscriber::with_default(subscriber, || {
        let _root_guard = create_root_info_span(MockDynSpanEvent {
            name: "root".to_string(),
            flags: TelemetryOutputFlags::ALL,
            ..Default::default()
        })
        .entered();

        emit_warn_log_message(ErrorCode::NoNodesSelected, "warn", None);

        *options_handle
            .write()
            .expect("warn_error_options lock should not be poisoned") = WarnErrorOptions {
            error: vec![WarnErrorOptionValue::FusionCode(
                ErrorCode::NoNodesSelected as u16,
            )],
            ..Default::default()
        };
        emit_warn_log_message(ErrorCode::NoNodesSelected, "error", None);

        *options_handle
            .write()
            .expect("warn_error_options lock should not be poisoned") = WarnErrorOptions {
            silence: vec![WarnErrorOptionValue::SupportedLegacy(
                SupportedLegacyWarnError::NothingToDo,
            )],
            ..Default::default()
        };
        emit_warn_log_message(ErrorCode::NoNodesSelected, "silence", None);

        (
            get_metric(FusionMetricKey::InvocationMetric(
                InvocationMetricKey::TotalErrors,
            )),
            get_metric(FusionMetricKey::InvocationMetric(
                InvocationMetricKey::TotalWarnings,
            )),
        )
    });

    let captured_log_records = log_records
        .lock()
        .expect("log records mutex poisoned")
        .clone();

    assert_eq!(captured_log_records.len(), 2);
    assert_eq!(
        captured_log_records[0].severity_number,
        SeverityNumber::Warn
    );
    assert_eq!(
        captured_log_records[1].severity_number,
        SeverityNumber::Error
    );
    assert_eq!(warning_count, 1);
    assert_eq!(error_count, 1);
}
