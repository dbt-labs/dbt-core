use std::{collections::HashSet, path::PathBuf};

use super::{
    convert::log_level_filter_to_tracing,
    layer::{ConsumerLayer, MiddlewareLayer},
    layers::{
        file_log_layer::build_file_log_layer_with_background_writer,
        json_compat_layer::{
            build_json_compat_layer, build_json_compat_layer_with_background_writer,
        },
        jsonl_writer::{build_jsonl_layer, build_jsonl_layer_with_background_writer},
        otlp::build_otlp_layer,
        parquet_writer::build_parquet_writer_layer,
        query_log::build_query_log_layer_with_background_writer,
        tui_layer::build_tui_layer,
    },
    middlewares::metric_aggregator::TelemetryMetricAggregator,
    shutdown::TelemetryShutdownItem,
};
use crate::{
    constants::{
        DBT_DEAFULT_LOG_FILE_NAME, DBT_DEAFULT_QUERY_LOG_FILE_NAME, DBT_LOG_DIR_NAME,
        DBT_METADATA_DIR_NAME, DBT_PROJECT_YML, DBT_TARGET_DIR_NAME,
    },
    io_args::{IoArgs, ShowOptions},
    io_utils::determine_project_dir,
    logging::LogFormat,
    tracing::middlewares::parse_error_filter::TelemetryParsingErrorFilter,
};
use dbt_error::{ErrorCode, FsResult};
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
    /// of log level) for tui & jsonl log sinks.
    pub(super) max_log_verbosity: LevelFilter,
    /// Maximum verbosity for the file log sink.
    pub(super) max_file_log_verbosity: LevelFilter,
    /// Fully resolved path for production telemetry output (JSONL format).
    ///
    /// If Some(), enables corresponding output layer.
    pub(super) otel_file_path: Option<PathBuf>,
    /// Fully resolved path for production telemetry output (Parquet format)
    ///
    /// If Some(), enables corresponding output layer.
    pub(super) otel_parquet_file_path: Option<PathBuf>,
    /// Fully resolved path to the directory where log-related files
    /// (e.g. dbt.log, query log) should be written.
    pub(super) log_path: PathBuf,
    /// Optional custom name for the log file. If None, defaults to `dbt.log`.
    pub(super) log_file_name: Option<String>,
    /// Invocation ID. Used as trace ID for correlation
    pub(super) invocation_id: uuid::Uuid,
    /// If True, traces will be forwarded to OTLP endpoints, if any
    /// are set via OTEL environment variables. See `OTLPExporterLayer::new`
    pub(super) export_to_otlp: bool,
    /// The log format being used
    pub(super) log_format: LogFormat,
    /// If True, enables separate query log file output
    pub(super) enable_query_log: bool,
    /// Show options controlling terminal/file output visibility
    pub(super) show_options: HashSet<ShowOptions>,
    /// Show all deprecations warnings/errors instead of one per package
    pub(super) show_all_deprecations: bool,
    /// If True, disables stdout/console output even when using Text/Default format.
    /// Useful for long-running services like LSP that only want file logging.
    pub(super) disable_console_output: bool,
}

impl Default for FsTraceConfig {
    fn default() -> Self {
        Self {
            package: "unknown",
            max_log_verbosity: LevelFilter::INFO,
            max_file_log_verbosity: LevelFilter::DEBUG,
            otel_file_path: None,
            otel_parquet_file_path: None,
            log_path: PathBuf::new(),
            log_file_name: None,
            invocation_id: uuid::Uuid::now_v7(),
            export_to_otlp: false,
            log_format: LogFormat::Default,
            enable_query_log: false,
            show_options: HashSet::new(),
            show_all_deprecations: false,
            disable_console_output: false,
        }
    }
}

/// Helper function to calculate in_dir and out_dir for tracing configuration.
/// This implements the same logic as `execute_setup_and_all_phases` but without canonicalization.
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
    /// * `max_log_verbosity` - Maximum tracing level filter (higher = more verbose tracing output).
    ///   This controls verbosity for TUI and JSONL log output
    /// * `max_file_log_verbosity` - Maximum tracing level filter for file log output
    /// * `otel_file_name` - Optional filename for JSONL trace output. If provided,
    ///   creates trace file at `{log_path}/{otel_file_name}`
    /// * `otel_parquet_file_name` - Optional filename for OpenTelemetry Parquet trace output.
    ///   If provided, creates trace file at `{target_path}/metadata/{otel_parquet_file_name}`
    /// * `invocation_id` - Unique identifier for this execution, used as trace ID for correlation
    /// * `export_to_otlp` - If true, enables forwarding traces to OTLP endpoints configured
    ///   via OTEL environment variables
    /// * `log_format` - The log format being used
    /// * `enable_query_log` - If true, enables writing a separate query log file
    /// * `show_options` - Set of ShowOptions controlling terminal/file output visibility
    /// * `show_all_deprecations` - If true, show all deprecation warnings/errors instead of one per package
    /// * `log_file_name` - Optional custom name for the log file. If None, defaults to `dbt.log`.
    ///   If Some, creates log file at `{log_path}/{log_file_name}`
    /// * `disable_console_output` - If true, disables stdout/console output even when using Text/Default format
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
    /// use std::{collections::HashSet, path::PathBuf};
    ///
    /// let config = FsTraceConfig::new(
    ///     "dbt-cli",
    ///     Some(PathBuf::from("/path/to/project")),
    ///     None, // Use default target path
    ///     None, // Use default log path
    ///     LevelFilter::INFO,
    ///     LevelFilter::DEBUG,
    ///     Some("otel.jsonl".to_string()),
    ///     Some("otel.parquet".to_string()),
    ///     Uuid::new_v4(),
    ///     false, // Don't export to OTLP
    ///     LogFormat::Default, // Use default log format
    ///     true,  // Enable query log
    ///     HashSet::new(),
    /// );
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        package: &'static str,
        project_dir: Option<&PathBuf>,
        target_path: Option<&PathBuf>,
        log_path: Option<&PathBuf>,
        max_log_verbosity: LevelFilter,
        max_file_log_verbosity: LevelFilter,
        otel_file_name: Option<&str>,
        otel_parquet_file_name: Option<&str>,
        invocation_id: uuid::Uuid,
        export_to_otlp: bool,
        log_format: LogFormat,
        enable_query_log: bool,
        show_options: HashSet<ShowOptions>,
        show_all_deprecations: bool,
        log_file_name: Option<&str>,
        disable_console_output: bool,
    ) -> Self {
        let (in_dir, out_dir) = calculate_trace_dirs(project_dir, target_path);

        // Resolve log directory path (base directory for auxiliary log files)
        let log_dir_path = log_path.map_or_else(
            || in_dir.join(DBT_LOG_DIR_NAME),
            |log_path| {
                if log_path.is_relative() {
                    in_dir.join(log_path)
                } else {
                    log_path.clone()
                }
            },
        );

        Self {
            package,
            max_log_verbosity,
            max_file_log_verbosity,
            otel_file_path: otel_file_name.map(|file_name| log_dir_path.join(file_name)),
            otel_parquet_file_path: otel_parquet_file_name
                .map(|file_name| out_dir.join(DBT_METADATA_DIR_NAME).join(file_name)),
            log_path: log_dir_path,
            log_file_name: log_file_name.map(|s| s.to_string()),
            invocation_id,
            export_to_otlp,
            log_format,
            enable_query_log,
            show_options,
            show_all_deprecations,
            disable_console_output,
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
            .unwrap_or(LevelFilter::INFO);

        let max_file_log_verbosity = io_args
            .log_level_file
            .map(|lf| log_level_filter_to_tracing(&lf))
            .unwrap_or(LevelFilter::DEBUG);

        Self::new(
            package,
            project_dir,
            target_path,
            io_args.log_path.as_ref(),
            max_log_verbosity,
            max_file_log_verbosity,
            io_args.otel_file_name.as_deref(),
            io_args.otel_parquet_file_name.as_deref(),
            io_args.invocation_id,
            io_args.export_to_otlp,
            io_args.log_format,
            true, // Always enable query log for now
            io_args.show.clone(),
            io_args.show_all_deprecations,
            None,  // log_file_name - use default dbt.log
            false, // disable_console_output defaults to false for CLI
        )
    }

    /// Builds the configured tracing layers and corresponding shutdown items.
    /// This method handles all path creation and file opening as needed.
    /// If no layers are configured, returns an empty layer and no shutdown items.
    pub fn build_layers(
        &self,
    ) -> FsResult<(
        Vec<MiddlewareLayer>,
        Vec<ConsumerLayer>,
        Vec<TelemetryShutdownItem>,
    )> {
        let mut shutdown_items = Vec::new();
        let mut consumer_layers = Vec::new();

        // Create jsonl writer layer if file path provided
        if let Some(file_path) = &self.otel_file_path {
            // Ensure log directory exists
            if let Some(log_dir) = file_path.parent() {
                crate::stdfs::create_dir_all(log_dir)?;
            }

            // Open file in append mode to avoid overwriting existing telemetry
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(file_path)
                .map_err(|e| {
                    fs_err!(
                        ErrorCode::IoError,
                        "Failed to open telemetry jsonl file for append: {}",
                        e
                    )
                })?;

            let (layer, handle) =
                build_jsonl_layer_with_background_writer(file, self.max_file_log_verbosity);

            // Keep a handle for shutdown
            shutdown_items.push(handle);

            // Create layer and apply user specified filtering
            consumer_layers.push(layer)
        };

        // Create parquet writer layer if file path provided
        if let Some(file_path) = &self.otel_parquet_file_path {
            // Create the file and initialize the Parquet layer
            let file_dir = file_path.parent().ok_or_else(|| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to get parent directory for file path"
                )
            })?;

            crate::stdfs::create_dir_all(file_dir)?;

            let file = std::fs::File::create(file_path)
                .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to create parquet file: {}", e))?;

            let (parquet_layer, writer_handle) = build_parquet_writer_layer(file)?;

            // Keep a handle for shutdown
            shutdown_items.push(writer_handle);

            // Create layer. User specified filtering is not applied here
            consumer_layers.push(parquet_layer)
        };

        // Create console layer based on log format (unless disabled)
        if !self.disable_console_output {
            match self.log_format {
                LogFormat::Default | LogFormat::Text => {
                    // Create layer and apply user specified filtering
                    consumer_layers.push(build_tui_layer(
                        self.max_log_verbosity,
                        self.log_format,
                        self.show_options.clone(),
                    ))
                }
                LogFormat::Json => {
                    // Create layer and apply user specified filtering
                    consumer_layers.push(build_json_compat_layer(
                        std::io::stdout(),
                        self.max_log_verbosity,
                        self.invocation_id,
                    ))
                }
                LogFormat::Otel => {
                    // Create jsonl writer layer on stdout if log format is OTEL
                    // No shutdown logic as we flushing to stdout as we write anyway
                    consumer_layers
                        .push(build_jsonl_layer(std::io::stdout(), self.max_log_verbosity));
                }
            }
        };

        // If any of the file logs are enabled - create the log directory
        if self.enable_query_log || self.max_file_log_verbosity != LevelFilter::OFF {
            // Ensure log directory exists
            crate::stdfs::create_dir_all(&self.log_path)?;
        }

        if self.max_file_log_verbosity != LevelFilter::OFF {
            let log_file_name = self
                .log_file_name
                .as_deref()
                .unwrap_or(DBT_DEAFULT_LOG_FILE_NAME);
            let file_log_path = self.log_path.join(log_file_name);

            // Open file in append mode, same as dbt core.
            // NOTE: legacy logger based onfra also opens this file with `truncate` as of today.
            // This is only working because we hold 2 implicit assumptions until full migration to tracing:
            // 1. We only write end of run summary to the log file, so it comes last anyway
            // 2. logger based infra flushes on each write, so we shouldn't have interleaved writes
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(file_log_path)
                .map_err(|e| {
                    fs_err!(
                        ErrorCode::IoError,
                        "Failed to open telemetry log file for append: {}",
                        e
                    )
                })?;

            if let Some((file_log_layer, writer_handle)) = match self.log_format {
                LogFormat::Default | LogFormat::Text => Some(
                    build_file_log_layer_with_background_writer(file, self.max_file_log_verbosity),
                ),
                LogFormat::Json => Some(build_json_compat_layer_with_background_writer(
                    file,
                    self.max_file_log_verbosity,
                    self.invocation_id,
                )),
                LogFormat::Otel => None,
            } {
                // Keep a handle for shutdown
                shutdown_items.push(writer_handle);

                // Create layer. User specified filtering is not applied here
                consumer_layers.push(file_log_layer)
            }
        };

        // Create query log writer layer (always enabled; internal-only event sink)
        if self.enable_query_log {
            let file_path = self.log_path.join(DBT_DEAFULT_QUERY_LOG_FILE_NAME);

            // Create or truncate existing file
            let file = crate::stdfs::File::create(&file_path)?;

            let (layer, handle) = build_query_log_layer_with_background_writer(file);
            shutdown_items.push(handle);
            consumer_layers.push(layer)
        };

        // Create OTLP layer - if enabled and endpoint is set via env vars
        if self.export_to_otlp
            && let Some((otlp_layer, mut handles)) = build_otlp_layer()
        {
            shutdown_items.append(&mut handles);
            consumer_layers.push(otlp_layer)
        };

        Ok((
            vec![
                // Order important! First handle parsing errors, which may filter, only then aggregate metrics
                Box::new(TelemetryParsingErrorFilter::new(self.show_all_deprecations)),
                Box::new(TelemetryMetricAggregator),
            ],
            consumer_layers,
            shutdown_items,
        ))
    }
}
