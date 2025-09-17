use std::path::PathBuf;

use super::convert::log_level_filter_to_tracing;
use crate::{
    constants::{DBT_LOG_DIR_NAME, DBT_METADATA_DIR_NAME, DBT_PROJECT_YML, DBT_TARGET_DIR_NAME},
    io_args::IoArgs,
    io_utils::determine_project_dir,
    logging::LogFormat,
};
use tracing::level_filters::LevelFilter;

/// Configuration for tracing.
///
/// This struct defines where trace data should be written for both debug
/// and production scenarios, and defines metadata necessary for top-level span
/// and trace correlation.
#[derive(Clone, Debug)]
pub struct FsTraceConfig {
    /// Name of the package emitting the telemetry, e.g. `dbt-cli` or `dbt-lsp`
    pub(super) package: &'static str,
    /// Tracing level filter, which specifies maximum verbosity (inverse
    /// of log level)
    pub(super) max_log_verbosity: LevelFilter,
    /// Fully resolved path for production telemetry output (JSONL format).
    ///
    /// If Some(), enables corresponding output layer.
    pub(super) otm_file_path: Option<PathBuf>,
    /// Fully resolved path for production telemetry output (Parquet format)
    ///
    /// If Some(), enables corresponding output layer.
    pub(super) otm_parquet_file_path: Option<PathBuf>,
    /// Invocation ID. Currently used as trace ID for correlation
    pub(super) invocation_id: uuid::Uuid,
    /// If True, traces will be forwarded to OTLP endpoints, if any
    /// are set via OTEL environment variables. See `OTLPExporterLayer::new`
    pub(super) export_to_otlp: bool,
    /// If True, progress bar layer will be enabled
    pub(super) enable_progress: bool,
    /// The log format being used. As of today (while old logging infra exists) - this is used to
    /// enable jsonl output on stdout if needed.
    pub(super) log_format: LogFormat,
}

impl Default for FsTraceConfig {
    fn default() -> Self {
        Self {
            package: "unknown",
            max_log_verbosity: LevelFilter::INFO,
            otm_file_path: None,
            otm_parquet_file_path: None,
            invocation_id: uuid::Uuid::new_v4(),
            enable_progress: false,
            export_to_otlp: false,
            log_format: LogFormat::Default,
        }
    }
}

/// Helper function to calculate in_dir and out_dir for tracing configuration.
/// This implements the same logic as execute_setup_and_all_phases but without canonicalization.
/// Unlike the project setup logic, this function never fails - it falls back to using the current
/// working directory if no project directory can be determined.
fn calculate_trace_dirs(
    project_dir: Option<&PathBuf>,
    target_path: Option<&PathBuf>,
) -> (PathBuf, PathBuf) {
    let in_dir = project_dir.cloned().unwrap_or_else(|| {
        // If no project directory is provided, try to determine it
        // Fallback to empty path if not found
        determine_project_dir(&[], DBT_PROJECT_YML).unwrap_or_else(|_| PathBuf::new())
    });

    // If no target path is provided, determine the output directory
    let out_dir = target_path
        .cloned()
        .unwrap_or_else(|| in_dir.join(DBT_TARGET_DIR_NAME));

    (in_dir, out_dir)
}

impl FsTraceConfig {
    /// Creates a new FsTraceConfig with explicit parameter control.
    ///
    /// This constructor provides full control over all tracing configuration options
    /// and handles path resolution for trace output files.
    ///
    /// # Arguments
    ///
    /// * `package` - Static string identifying the package emitting telemetry (e.g., "dbt", "dbt-lsp")
    /// * `project_dir` - Optional path to the dbt project directory. If None, attempts to auto-detect
    ///   from current working directory using `dbt_project.yml` as a marker
    /// * `target_path` - Optional path to the target directory for outputs. If None, defaults to
    ///   `{project_dir}/target`. Target path is used for Parquet trace output.
    /// * `log_path` - Optional custom path for log files. If None, uses `{project_dir}/logs`.
    ///   If relative, resolved relative to `project_dir`
    /// * `max_log_verbosity` - Maximum tracing level filter (higher = more verbose tracing output)
    /// * `otel_file_name` - Optional filename for JSONL trace output. If provided,
    ///   creates trace file at `{log_path}/{otel_file_name}`
    /// * `otel_parquet_file_name` - Optional filename for OpenTelemetry Parquet trace output.
    ///   If provided, creates trace file at `{target_path}/metadata/{otel_parquet_file_name}`
    /// * `invocation_id` - Unique identifier for this execution, used as trace ID for correlation
    /// * `export_to_otlp` - If true, enables forwarding traces to OTLP endpoints configured
    ///   via OTEL environment variables
    /// * `enable_progress` - If true, enables the progress bar layer (console only)
    /// * `log_format` - The log format being used. As of today (while old logging infra exists) - this is used to
    ///   enable jsonl output on stdout if needed.
    ///
    /// # Path Resolution
    ///
    /// The method resolves paths as follows:
    /// - `project_dir`: Auto-detected if None, fallback to current working directory
    /// - `target_path`: Defaults to `{project_dir}/target` if None
    /// - Log files: `{log_path or project_dir/logs}/{otel_file_name}`
    /// - Parquet files: `{target_path}/metadata/{otel_parquet_file_name}`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tracing::level_filters::LevelFilter;
    /// use uuid::Uuid;
    /// use std::path::PathBuf;
    ///
    /// let config = FsTraceConfig::new(
    ///     "dbt-cli",
    ///     Some(PathBuf::from("/path/to/project")),
    ///     None, // Use default target path
    ///     None, // Use default log path
    ///     LevelFilter::INFO,
    ///     Some("dbt-tm.jsonl".to_string()),
    ///     Some("dbt-tm.parquet".to_string()),
    ///     Uuid::new_v4(),
    ///     false, // Don't export to OTLP
    ///     true,  // Enable progress bar
    /// );
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        package: &'static str,
        project_dir: Option<&PathBuf>,
        target_path: Option<&PathBuf>,
        log_path: Option<&PathBuf>,
        max_log_verbosity: LevelFilter,
        otm_file_name: Option<&str>,
        otm_parquet_file_name: Option<&str>,
        invocation_id: uuid::Uuid,
        export_to_otlp: bool,
        enable_progress: bool,
        log_format: LogFormat,
    ) -> Self {
        let (in_dir, out_dir) = calculate_trace_dirs(project_dir, target_path);

        Self {
            package,
            max_log_verbosity,
            otm_file_path: otm_file_name.map(|file_name| {
                log_path.map_or_else(
                    || in_dir.join(DBT_LOG_DIR_NAME).join(file_name),
                    |log_path| {
                        if log_path.is_relative() {
                            // If the path is relative, join it with the current working directory
                            in_dir.join(log_path).join(file_name)
                        } else {
                            log_path.join(file_name)
                        }
                    },
                )
            }),
            otm_parquet_file_path: otm_parquet_file_name
                .map(|file_name| out_dir.join(DBT_METADATA_DIR_NAME).join(file_name)),
            invocation_id,
            enable_progress,
            export_to_otlp,
            log_format,
        }
    }

    /// Creates a new FsTraceConfig with proper path resolution.
    /// This method never fails - it uses fallback logic for directory resolution.
    pub fn new_from_io_args(
        project_dir: Option<&PathBuf>,
        target_path: Option<&PathBuf>,
        io_args: &IoArgs,
        package: &'static str,
    ) -> Self {
        let max_log_verbosity = io_args
            .log_level
            .map(|lf| log_level_filter_to_tracing(&lf))
            .unwrap_or_else(|| {
                if cfg!(debug_assertions) {
                    LevelFilter::TRACE
                } else {
                    LevelFilter::INFO
                }
            });

        Self::new(
            package,
            project_dir,
            target_path,
            io_args.log_path.as_ref(),
            max_log_verbosity,
            io_args.otel_file_name.as_deref(),
            io_args.otel_parquet_file_name.as_deref(),
            io_args.invocation_id,
            io_args.export_to_otlp,
            io_args.log_format == LogFormat::Default,
            io_args.log_format,
        )
    }
}
