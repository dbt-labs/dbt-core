use dbt_clap_core::{CliParserFactory as _, from_main};
use dbt_common::tracing::{FsTraceConfig, dbt_init::init_tracing};
use dbt_features::cli::DefaultCliParserFactory;
use dbt_features::feature_stack::FeatureStack;
use dbt_features::tracing::TracingFeature;
use dbt_lib::print_trimmed_error;

use std::process::ExitCode;
use std::sync::Arc;

// test: copybara reverse-sync end-to-end probe — remove before merge
fn main() -> ExitCode {
    let cli_parser = DefaultCliParserFactory.create("dbt-core");
    let cli = dbt_lib::prepare_cli_or_exit(&cli_parser);

    let mut arg = from_main(&cli);

    let (telemetry_handle, tracing_config_provider) = match init_tracing(
        FsTraceConfig::new_from_io_args(
            arg.command,
            cli.project_dir().as_ref(),
            cli.target_path().as_ref(),
            &arg.io,
            Some(&cli.common_args().get_cli_warn_error_options()),
            "dbt",
        )
        .with_command_name(cli_parser.command_name()),
    ) {
        Ok(handle) => handle,
        Err(e) => {
            let msg = e.to_string();
            print_trimmed_error(msg);
            std::process::exit(1);
        }
    };

    let tracing = TracingFeature::default()
        .with_config_provider(tracing_config_provider)
        .with_shutdown_handle(telemetry_handle);

    if let Some(resolved_file_log_path) = tracing.config_provider.get_file_log_path() {
        arg.io.log_path = Some(resolved_file_log_path.to_path_buf());
    }

    let feature_stack: Arc<FeatureStack> =
        dbt_features::feature_stack_builder::FeatureStackBuilder::new(tracing)
            .send_anonymous_usage_stats(arg.io.send_anonymous_usage_stats)
            .build()
            .into();

    dbt_lib::run_cli(cli, arg, feature_stack)
}
