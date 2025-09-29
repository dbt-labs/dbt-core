use crate::{
    io_args::EvalArgs,
    logging::LogFormat,
    tracing::{
        background_writer::BackgroundWriter,
        create_invocation_attributes,
        init::create_tracing_subcriber_with_layer,
        layers::{data_layer::TelemetryDataLayer, tui_layer::build_tui_layer},
        shutdown::TelemetryShutdown as _,
    },
};
use dbt_telemetry::{LogMessage, SeverityNumber};
use rand::random;
use std::fs::File;
use tracing::level_filters::LevelFilter;

#[test]
fn tui_layer_creates_invocation_and_log_stub() {
    let trace_id = random::<u128>();
    let temp_dir = std::env::temp_dir();
    let temp_stdout_path = temp_dir.join(format!("tui-test-stdout-{}.log", random::<u64>()));
    let temp_stderr_path = temp_dir.join(format!("tui-test-stderr-{}.log", random::<u64>()));

    let (stdout_writer, mut stdout_handle) = BackgroundWriter::new(
        File::create(&temp_stdout_path).expect("create temp file for stdout"),
    );
    let (stderr_writer, mut stderr_handle) = BackgroundWriter::new(
        File::create(&temp_stderr_path).expect("create temp file for stderr"),
    );

    let tui_layer = build_tui_layer(
        stdout_writer,
        stderr_writer,
        LevelFilter::INFO,
        LogFormat::Default,
    );

    // Init telemetry using internal API allowing to set thread local subscriber.
    // This avoids collisions with other unit tests
    let subscriber = create_tracing_subcriber_with_layer(
        LevelFilter::TRACE,
        TelemetryDataLayer::new(
            trace_id,
            true, // strip code location in tests
            std::iter::empty(),
            std::iter::once(tui_layer),
        ),
    );

    let mut eval_args = EvalArgs::default();
    eval_args.io.invocation_id = uuid::Uuid::new_v4();

    tracing::subscriber::with_default(subscriber, || {
        let invocation_span =
            create_root_info_span!(create_invocation_attributes("dbt-test", &eval_args).into());

        invocation_span.in_scope(|| {
            let log_message = LogMessage {
                code: Some(1),
                dbt_core_event_code: Some("T042".to_string()),
                original_severity_number: SeverityNumber::Info as i32,
                original_severity_text: "INFO".to_string(),
                ..Default::default()
            };

            emit_tracing_event!(log_message.into(), "tui layer stub log");
        });
    });

    // Shutdown telemetry to ensure all data is flushed to the file
    stdout_handle
        .shutdown()
        .expect("Failed to shutdown telemetry");
    stderr_handle
        .shutdown()
        .expect("Failed to shutdown telemetry");

    let _ = std::fs::remove_file(&temp_stdout_path);
    let _ = std::fs::remove_file(&temp_stderr_path);
}
