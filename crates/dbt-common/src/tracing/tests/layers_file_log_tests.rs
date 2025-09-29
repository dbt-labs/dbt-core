use crate::{
    io_args::EvalArgs,
    tracing::{
        create_invocation_attributes,
        init::create_tracing_subcriber_with_layer,
        layers::{
            data_layer::TelemetryDataLayer,
            file_log_layer::build_file_log_layer_with_background_writer,
        },
    },
};
use dbt_telemetry::{LogMessage, SeverityNumber};
use rand::random;
use std::fs;
use tracing::level_filters::LevelFilter;

#[test]
fn file_log_layer_creates_invocation_and_log_stub() {
    let invocation_id = uuid::Uuid::now_v7();
    let trace_id = invocation_id.as_u128();
    let temp_file_path =
        std::env::temp_dir().join(format!("file-log-layer-{}.log", random::<u64>()));

    let (file_log_layer, mut shutdown_handle) = build_file_log_layer_with_background_writer(
        fs::File::create(&temp_file_path).expect("Failed to create temporary log file"),
        LevelFilter::DEBUG,
    );

    // Init telemetry using internal API allowing to set thread local subscriber.
    // This avoids collisions with other unit tests
    let subscriber = create_tracing_subcriber_with_layer(
        LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            false,
            std::iter::empty(),
            std::iter::once(file_log_layer),
        ),
    );

    let mut eval_args = EvalArgs::default();
    eval_args.io.invocation_id = invocation_id;
    let temp_path_string = temp_file_path.to_string_lossy().to_string();

    tracing::subscriber::with_default(subscriber, || {
        let invocation_span =
            create_root_info_span!(create_invocation_attributes("dbt-test", &eval_args).into());

        invocation_span.in_scope(|| {
            let log_message = LogMessage {
                code: None,
                dbt_core_event_code: None,
                original_severity_number: SeverityNumber::Info as i32,
                original_severity_text: "INFO".to_string(),
                unique_id: None,
                file: Some(temp_path_string.clone()),
                line: None,
                phase: None,
            };

            emit_tracing_event!(log_message.into(), "file log layer stub log");
        });
    });

    // Shutdown telemetry to ensure all data is flushed to the file
    shutdown_handle
        .shutdown()
        .expect("Failed to shutdown telemetry");

    let _ = fs::remove_file(&temp_file_path);
}
