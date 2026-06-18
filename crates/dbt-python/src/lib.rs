use dbt_clap_core::{CliParserFactory as _, from_lib};
use dbt_common::tracing::{FsTraceConfig, dbt_init::init_tracing};
use dbt_features::cli::DefaultCliParserFactory;
use dbt_features::feature_stack_builder::FeatureStackBuilder;
use dbt_features::tracing::TracingFeature;
use pyo3::prelude::*;
use std::sync::{Arc, OnceLock};

// Tracing can only be globally initialized once per process.
static TRACING_INITIALIZED: OnceLock<()> = OnceLock::new();

#[pyclass(get_all)]
struct DbtRunnerResult {
    success: bool,
    returncode: u8,
}

#[pymethods]
impl DbtRunnerResult {
    fn __repr__(&self) -> String {
        format!(
            "DbtRunnerResult(success={}, returncode={})",
            self.success, self.returncode
        )
    }
}

/// In-process dbt runner. invoke() drives the full Rust engine without forking
/// a subprocess — parse, compile, and execute all happen in this process.
#[pyclass]
struct DbtRunner;

#[pymethods]
impl DbtRunner {
    #[new]
    fn new() -> Self {
        DbtRunner
    }

    /// Invoke dbt with the given CLI args, e.g. `["run", "--select", "my_model"]`.
    ///
    /// Releases the GIL so other Python threads can run during execution.
    /// Stdout/stderr flow through to the terminal.
    #[allow(deprecated)] // allow_threads → detach rename in pyo3 ≥0.27
    fn invoke(&self, py: Python<'_>, args: Vec<String>) -> PyResult<DbtRunnerResult> {
        let mut argv = vec!["dbt".to_string()];
        argv.extend(args);

        let returncode = py.allow_threads(|| invoke_inner(argv))?;
        Ok(DbtRunnerResult {
            success: returncode == 0,
            returncode,
        })
    }
}

fn invoke_inner(argv: Vec<String>) -> PyResult<u8> {
    let version = env!("CARGO_PKG_VERSION");
    let cli_parser = DefaultCliParserFactory.create("dbt-core", version);

    let cli = cli_parser
        .try_parse_from(argv)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    let mut arg = from_lib(&cli);

    // init_tracing sets a global tracing subscriber and panics/errors if called twice.
    // Guard it with OnceLock so subsequent invoke() calls skip re-initialization.
    let tracing = if TRACING_INITIALIZED.get().is_none() {
        let (telemetry_handle, tracing_config_provider) = init_tracing(
            FsTraceConfig::new_from_io_args(
                arg.command,
                cli.project_dir().as_ref(),
                cli.target_path().as_ref(),
                &arg.io,
                Some(&cli.common_args().get_cli_warn_error_options()),
                "dbt",
            )
            .with_command_name(cli_parser.command_name()),
        )
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        TRACING_INITIALIZED.get_or_init(|| ());
        TracingFeature::default()
            .with_config_provider(tracing_config_provider)
            .with_shutdown_handle(telemetry_handle)
    } else {
        TracingFeature::default()
    };

    if let Some(log_path) = tracing.config_provider.get_file_log_path() {
        arg.io.log_path = Some(log_path.to_path_buf());
    }

    let feature_stack = Arc::from(
        FeatureStackBuilder::new(tracing)
            .send_anonymous_usage_stats(arg.io.send_anonymous_usage_stats)
            .build(),
    );

    // Use a non-cancellable token — Ctrl+C handling is left to the Python caller.
    let token = dbt_base::cancel::never_cancels();

    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    // Call execute_fs_and_shutdown directly to avoid run_cli's process::exit paths
    // and to get a FsResult back instead of an opaque ExitCode.
    let result = tokio_rt.block_on(dbt_lib::dbt_lib::execute_fs_and_shutdown(
        arg,
        cli,
        true,
        feature_stack,
        token,
    ));

    let code: u8 = match result {
        Ok(()) => 0,
        Err(e) => e.exit_status().unwrap_or(1) as u8,
    };
    Ok(code)
}

#[pymodule]
#[pyo3(name = "_core")]
fn dbt_core_pyo3(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DbtRunner>()?;
    m.add_class::<DbtRunnerResult>()?;
    Ok(())
}
