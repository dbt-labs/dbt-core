use dbt_clap_core::commands::{Command, CoreCommand};
use dbt_clap_core::{CliParserFactory as _, from_lib, from_main};
use dbt_common::tracing::{FsTraceConfig, dbt_init::init_tracing};
use dbt_features::cli::DefaultCliParserFactory;
use dbt_features::feature_stack::FeatureStack;
use dbt_features::feature_stack_builder::FeatureStackBuilder;
use dbt_features::tracing::TracingFeature;
use dbt_main::dbt_lib::CommandExecutionResult;
use dbt_main::{print_trimmed_error, run_cli_with_code};
use pyo3::prelude::*;
use std::sync::{Arc, OnceLock};

mod contracts;

// Tracing inits once per process.
static TRACING_INITIALIZED: OnceLock<()> = OnceLock::new();

/// Low-level engine result. The dbt-core-compatible `.success`/`.result`/
/// `.exception` object is the Python wrapper in `dbt/cli/main.py`.
#[pyclass(get_all)]
struct DbtRunnerResult {
    success: bool,
    exit_code: u8,
    /// Command-specific artifact (`Manifest` / `RunResultsArtifact` / ...), or
    /// `None` for commands that produce no in-memory artifact.
    result: Option<Py<PyAny>>,
    /// Engine error message when the command errored out, else `None`. A
    /// handled failure (e.g. a failing test) captures artifacts and reports
    /// `success=False` with no message; only real errors set this.
    exception: Option<String>,
}

#[pymethods]
impl DbtRunnerResult {
    fn __repr__(&self) -> String {
        format!(
            "DbtRunnerResult(success={}, exit_code={})",
            self.success, self.exit_code
        )
    }
}

/// Pick the artifact that best represents a command's result and wrap it as a
/// contract pyclass. Mirrors dbt-core: run/build/test → run_results, parse →
/// manifest, with catalog as a fallback. `None` when nothing was captured.
fn build_result_object(
    py: Python<'_>,
    exec: CommandExecutionResult,
) -> PyResult<Option<Py<PyAny>>> {
    if let Some(rr) = exec.run_results {
        Ok(Some(
            Py::new(py, contracts::RunResultsArtifact::from_inner(rr))?.into_any(),
        ))
    } else if let Some(m) = exec.manifest {
        Ok(Some(
            Py::new(py, contracts::Manifest::from_inner(m))?.into_any(),
        ))
    } else if let Some(c) = exec.catalog {
        Ok(Some(
            Py::new(py, contracts::CatalogArtifact::from_inner(c))?.into_any(),
        ))
    } else {
        Ok(None)
    }
}

/// Runs dbt in-process — no subprocess fork.
#[pyclass]
struct DbtRunner;

#[pymethods]
impl DbtRunner {
    #[new]
    fn new() -> Self {
        DbtRunner
    }

    /// Run dbt with CLI args, e.g. `["run", "--select", "my_model"]`.
    /// Drops the GIL for the duration so other Python threads keep running.
    fn invoke(&self, py: Python<'_>, args: Vec<String>) -> PyResult<DbtRunnerResult> {
        let mut argv = vec!["dbt".to_string()];
        argv.extend(args);

        // invoke_inner is pure Rust (no Python refs), so run it with the GIL
        // released; build the result pyobject afterwards, back under the GIL.
        let (exit_code, exec, exception) = py.detach(|| invoke_inner(argv))?;
        let result = match exec {
            Some(exec) => build_result_object(py, exec)?,
            None => None,
        };
        Ok(DbtRunnerResult {
            success: exit_code == 0,
            exit_code,
            result,
            exception,
        })
    }
}

fn invoke_inner(
    argv: Vec<String>,
) -> PyResult<(u8, Option<CommandExecutionResult>, Option<String>)> {
    let version = env!("CARGO_PKG_VERSION");
    let cli_parser = DefaultCliParserFactory.create("dbt-core", version);

    let cli = cli_parser
        .try_parse_from(argv)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    let mut arg = from_lib(&cli);

    // init_tracing installs a global subscriber and errors if called twice, so
    // only the first invoke runs it. Known limitation: that first call also pins
    // the log-file path and level for the whole process, so a later invoke on a
    // different project logs into the first project's logs/dbt.log. Legacy
    // dbt-core re-set up logging every invoke; matching that needs a tracing
    // reload handle (deferred — rare in practice, fine for same-project use).
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

    let feature_stack: Arc<FeatureStack> = Arc::from(
        FeatureStackBuilder::new(tracing)
            .send_anonymous_usage_stats(arg.io.send_anonymous_usage_stats)
            .build(),
    );

    // Apply the global ANTLR parser configuration from the common args, the same
    // way run_cli does (main_impl.rs). Skipping it makes in-process parsing
    // diverge from the CLI for flags that drive the parser.
    feature_stack
        .antlr_parser
        .config
        .apply_configuration(&cli.common_args());

    // Ctrl+C is Python's job, so the engine gets a token that never cancels.
    let token = dbt_base::cancel::never_cancels();

    // Big stack for the recursive parser/compiler; blocking-thread headroom for adapters.
    let tokio_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .max_blocking_threads(512)
        .build()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    // Straight to execute_fs_and_shutdown, not run_cli: run_cli calls
    // process::exit on panic, which would take the interpreter down with it.
    // shutdown=false leaves the Vortex telemetry producer (a process global)
    // alive — tearing it down on the first invoke breaks logging on every
    // later one. The runtime built above still drops when this call returns.
    let result = tokio_rt.block_on(dbt_main::dbt_lib::execute_fs_and_shutdown(
        arg,
        cli,
        false,
        true,
        feature_stack,
        token,
    ));

    match result {
        Ok(exec) => Ok((0, Some(exec), None)),
        // A real engine error: preserve the exit code and hand the message back
        // as `exception` rather than dropping it (a bare success=False tells the
        // caller nothing about what went wrong).
        Err(e) => Ok((
            e.exit_status().unwrap_or(1) as u8,
            None,
            Some(e.to_string()),
        )),
    }
}

/// Console-script entrypoint (the `dbt` command shipped by this wheel). Runs dbt
/// as a CLI in the current process and exits — it never returns to Python.
///
/// Unlike [`DbtRunner::invoke`] (the library path, which hands errors back on
/// the result object), this mirrors the standalone `dbt` binary: clap prints
/// help/version/usage with the right exit code, and the engine drives the
/// process exit code.
#[pyfunction]
fn run_cli(py: Python<'_>, argv: Vec<String>) -> PyResult<()> {
    let code = py.detach(|| run_cli_inner(argv));
    std::process::exit(code as i32);
}

fn run_cli_inner(argv: Vec<String>) -> u8 {
    let version = env!("CARGO_PKG_VERSION");
    let cli_parser = DefaultCliParserFactory.create("dbt-core", version);

    // argv is Python's sys.argv — we parse it explicitly rather than reading
    // std::env like the binary, since the process is the interpreter, not `dbt`.
    let cli = match cli_parser.try_parse_from(argv) {
        Ok(cli) => cli,
        // clap prints help/version/usage to the right stream with the right
        // code; honor it — this is the CLI process, so exiting is correct.
        Err(e) => e.exit(),
    };

    // Handle completions before any runtime setup, mirroring prepare_cli_or_exit.
    if let Command::Core(CoreCommand::Completions(args)) = &cli.command {
        cli_parser.write_completions(args.shell, &mut std::io::stdout());
        std::process::exit(0);
    }

    let mut arg = from_main(&cli);

    // init_tracing installs a global subscriber and errors if called twice, so
    // the OnceLock guard keeps it to the first call (shared with invoke_inner).
    let tracing = if TRACING_INITIALIZED.get().is_none() {
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
                print_trimmed_error(e.to_string());
                std::process::exit(1);
            }
        };
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

    let feature_stack: Arc<FeatureStack> = Arc::from(
        FeatureStackBuilder::new(tracing)
            .send_anonymous_usage_stats(arg.io.send_anonymous_usage_stats)
            .build(),
    );

    run_cli_with_code(cli, arg, feature_stack)
}

#[pymodule]
#[pyo3(name = "_core")]
fn dbt_core_pyo3(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DbtRunner>()?;
    m.add_class::<DbtRunnerResult>()?;
    m.add_function(wrap_pyfunction!(run_cli, m)?)?;
    contracts::register(m)?;
    Ok(())
}
