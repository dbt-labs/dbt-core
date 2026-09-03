use std::{
    borrow::Cow,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use super::{
    dbt_convert::log_level_filter_to_tracing,
    dbt_init::init_tracing_with_layers,
    fs_error_log::get_log_message,
    layers::{
        file_log_layer::build_file_log_layer_with_background_writer,
        json_compat_layer::{
            build_json_compat_layer, build_json_compat_layer_with_background_writer,
        },
        query_log::build_query_log_layer_with_background_writer,
        tui_layer::build_tui_layer,
    },
    middlewares::markdown_log_filter::TelemetryMarkdownLogFilter,
    middlewares::metric_aggregator::TelemetryMetricAggregator,
    middlewares::node_warn_outcome::TelemetryNodeWarnOutcome,
    middlewares::warn_error_options::TelemetryWarnErrorOptionsMiddleware,
    tracing_feature_handles::TracingConfigProvider,
};
use crate::{
    collections::HashSet, tracing::tracing_feature_handles::create_tracing_config_provider,
};
use crate::{
    constants::{
        DBT_DEFAULT_LOG_FILE_BACKUP_COUNT, DBT_DEFAULT_LOG_FILE_MAX_BYTES,
        DBT_DEFAULT_LOG_FILE_NAME, DBT_DEFAULT_QUERY_LOG_FILE_NAME, DBT_FUSION, DBT_LOG_DIR_NAME,
        DBT_PROJECT_YML, DBT_TARGET_DIR_NAME,
    },
    io_args::{FsCommand, IoArgs, LogFormat, ShowOptions},
    io_utils::determine_project_dir,
    tracing::middlewares::parse_error_filter::TelemetryParsingErrorFilter,
    warn_error_options::WarnErrorOptions,
};
use dbt_error::{ErrorCode, FsError, FsResult};
use dbt_telemetry::TelemetryEventTypeRegistry;
use dbt_tracing::{
    LogRecordInfo,
    layer::{ConsumerLayer, MiddlewareLayer},
    layers::{
        jsonl_writer::{build_jsonl_layer, build_jsonl_layer_with_background_writer},
        otlp::{OtlpResourceConfig, build_otlp_layer},
        parquet_writer::build_parquet_writer_layer,
    },
    rotating_file_writer::RotatingFileWriter,
    shutdown::TelemetryShutdownItem,
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
    /// The command being executed, e.g. "run", "compile", "list"
    pub(super) command: FsCommand,
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
    /// Max size in bytes for rotating file logs. `0` means no limit.
    pub(super) log_file_max_bytes: u64,
    /// Invocation ID. Used as trace ID for correlation
    pub(super) invocation_id: uuid::Uuid,
    /// Optional parent span ID for OpenTelemetry trace correlation.
    /// Used as fallback when creating root spans.
    pub(super) parent_span_id: Option<u64>,
    /// If True, traces will be forwarded to OTLP endpoints, if any
    /// are set via OTEL environment variables. See `OTLPExporterLayer::new`
    pub(super) export_to_otlp: bool,
    /// The log format being used
    pub(super) log_format: LogFormat,
    /// File-specific log format (`--log-format-file`), overriding `log_format` for the on-disk log sink only.
    pub(super) file_log_format: Option<LogFormat>,
    /// If True, enables separate query log file output
    pub(super) enable_query_log: bool,
    /// Show options controlling terminal/file output visibility
    pub(super) show_options: HashSet<ShowOptions>,
    /// Show all deprecations warnings/errors instead of one per package
    pub(super) show_all_deprecations: bool,
    /// The initial warn-error options loaded from CLI/env before project flags are resolved.
    pub(super) warn_error_options: WarnErrorOptions,
    /// Withholds upgrades of warnings with no dbt-core counterpart; set while replaying.
    pub(super) skip_fusion_only_upgrades: bool,
    /// User-facing CLI brand name shown in the version banner and JSON log lines.
    pub(super) command_name: &'static str,
}

impl Default for FsTraceConfig {
    fn default() -> Self {
        Self {
            package: "unknown",
            command: FsCommand::Unset,
            max_log_verbosity: LevelFilter::INFO,
            max_file_log_verbosity: LevelFilter::DEBUG,
            otel_file_path: None,
            otel_parquet_file_path: None,
            log_path: PathBuf::new(),
            log_file_name: None,
            log_file_max_bytes: DBT_DEFAULT_LOG_FILE_MAX_BYTES,
            invocation_id: uuid::Uuid::now_v7(),
            parent_span_id: None,
            export_to_otlp: false,
            log_format: LogFormat::Default,
            file_log_format: None,
            enable_query_log: false,
            show_options: HashSet::default(),
            show_all_deprecations: false,
            warn_error_options: WarnErrorOptions::default(),
            skip_fusion_only_upgrades: false,
            command_name: DBT_FUSION,
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

fn dbt_log_preprocessor_hook(record: &LogRecordInfo) -> Cow<'_, LogRecordInfo> {
    if get_log_message(&record.attributes).is_none() {
        return Cow::Borrowed(record);
    }

    match console::strip_ansi_codes(record.body.as_str()) {
        Cow::Owned(stripped) => Cow::Owned(LogRecordInfo {
            body: stripped,
            ..record.clone()
        }),
        Cow::Borrowed(_) => Cow::Borrowed(record),
    }
}

/// Builds the middleware pipeline shared by dbt tracing configurations.
pub fn build_shared_middleware_layers(
    show_all_deprecations: bool,
    warn_error_options: WarnErrorOptions,
    skip_fusion_only_upgrades: bool,
) -> (Vec<MiddlewareLayer>, Arc<RwLock<WarnErrorOptions>>) {
    let (warn_error_options_middleware, warn_error_options) =
        TelemetryWarnErrorOptionsMiddleware::new(warn_error_options, skip_fusion_only_upgrades);

    (
        vec![
            Box::new(TelemetryMarkdownLogFilter),
            Box::new(TelemetryParsingErrorFilter::new(show_all_deprecations)),
            Box::new(warn_error_options_middleware),
            Box::new(TelemetryNodeWarnOutcome),
            Box::new(TelemetryMetricAggregator),
        ],
        warn_error_options,
    )
}

/// Builds a JSONL file consumer with dbt log preprocessing.
pub fn build_jsonl_file_consumer(
    file_path: &std::path::Path,
    max_verbosity: LevelFilter,
) -> FsResult<(ConsumerLayer, TelemetryShutdownItem)> {
    if let Some(log_dir) = file_path.parent() {
        crate::stdfs::create_dir_all(log_dir)?;
    }

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)
        .map_err(|error| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to open telemetry jsonl file for append: {}",
                error
            )
        })?;

    Ok(build_jsonl_layer_with_background_writer(
        file,
        max_verbosity,
        Some(dbt_log_preprocessor_hook),
    ))
}

/// Builds an unstructured rotating file-log consumer.
#[allow(clippy::too_many_arguments)]
pub fn build_file_log_consumer(
    max_file_log_verbosity: LevelFilter,
    log_path: &std::path::Path,
    log_file_name: Option<&str>,
    log_file_max_bytes: u64,
    log_format: LogFormat,
    invocation_id: uuid::Uuid,
    command: FsCommand,
    command_name: &'static str,
) -> FsResult<(
    Option<ConsumerLayer>,
    Vec<TelemetryShutdownItem>,
    Option<PathBuf>,
)> {
    if max_file_log_verbosity == LevelFilter::OFF {
        return Ok((None, Vec::new(), None));
    }

    crate::stdfs::create_dir_all(log_path)?;
    let log_file_name = log_file_name.unwrap_or(DBT_DEFAULT_LOG_FILE_NAME);
    let file_log_path = log_path.join(log_file_name);
    let file = RotatingFileWriter::new(
        &file_log_path,
        log_file_max_bytes,
        DBT_DEFAULT_LOG_FILE_BACKUP_COUNT,
    )
    .map_err(|error| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to open log file for append: {}",
            error
        )
    })?;

    let Some((consumer, shutdown_item)) = (match log_format {
        LogFormat::Default | LogFormat::Text => Some(build_file_log_layer_with_background_writer(
            file,
            max_file_log_verbosity,
        )),
        LogFormat::Json => Some(build_json_compat_layer_with_background_writer(
            file,
            max_file_log_verbosity,
            invocation_id,
            command,
            command_name,
        )),
        LogFormat::Otel => None,
    }) else {
        return Ok((None, Vec::new(), Some(file_log_path)));
    };

    Ok((Some(consumer), vec![shutdown_item], Some(file_log_path)))
}

pub fn build_tracing_config_provider(
    warn_error_options: Arc<RwLock<WarnErrorOptions>>,
    file_log_path: Option<PathBuf>,
    command_name: &'static str,
) -> Box<dyn TracingConfigProvider> {
    create_tracing_config_provider(warn_error_options, file_log_path, command_name)
}

pub struct FsTraceLayers {
    middleware_layers: Vec<MiddlewareLayer>,
    consumer_layers: Vec<ConsumerLayer>,
    shutdown_items: Vec<TelemetryShutdownItem>,
    tracing_config_provider: Box<dyn TracingConfigProvider>,
}

impl FsTraceLayers {
    pub fn into_parts(
        self,
    ) -> (
        Vec<MiddlewareLayer>,
        Vec<ConsumerLayer>,
        Vec<TelemetryShutdownItem>,
        Box<dyn TracingConfigProvider>,
    ) {
        (
            self.middleware_layers,
            self.consumer_layers,
            self.shutdown_items,
            self.tracing_config_provider,
        )
    }
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
    /// * `command` - Command being executed (e.g., "run", "compile", "list")
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
    /// * `parent_span_id` - Optional parent span ID for OpenTelemetry trace correlation
    /// * `export_to_otlp` - If true, enables forwarding traces to OTLP endpoints configured
    ///   via OTEL environment variables
    /// * `log_format` - The log format being used
    /// * `enable_query_log` - If true, enables writing a separate query log file
    /// * `show_options` - Set of ShowOptions controlling terminal/file output visibility
    /// * `show_all_deprecations` - If true, show all deprecation warnings/errors instead of one per package
    /// * `warn_error_options` - Initial warn-error options from CLI/env before project flags are resolved
    /// * `skip_fusion_only_upgrades` - Withholds upgrades of warnings with no dbt-core counterpart; set while replaying
    /// * `log_file_name` - Optional custom name for the log file. If None, defaults to `dbt.log`.
    ///   If Some, creates log file at `{log_path}/{log_file_name}`
    /// * `log_file_max_bytes` - Max size for rotating file logs in bytes.
    ///   `0` means no size limit.
    ///
    /// # Path Resolution
    ///
    /// The method resolves paths as follows:
    /// - `project_dir`: Auto-detected if None, fallback to current working directory
    /// - `target_path`: Defaults to `{project_dir}/target` if None
    /// - Log files: `{log_path or project_dir/logs}/{otel_file_name}`
    /// - Parquet files: `{target_path}/metadata/{otel_parquet_file_name}`
    ///
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        package: &'static str,
        command: FsCommand,
        project_dir: Option<&PathBuf>,
        target_path: Option<&PathBuf>,
        log_path: Option<&PathBuf>,
        max_log_verbosity: LevelFilter,
        max_file_log_verbosity: LevelFilter,
        otel_file_name: Option<&str>,
        otel_parquet_file_name: Option<&str>,
        invocation_id: uuid::Uuid,
        parent_span_id: Option<u64>,
        export_to_otlp: bool,
        log_format: LogFormat,
        enable_query_log: bool,
        show_options: HashSet<ShowOptions>,
        show_all_deprecations: bool,
        warn_error_options: WarnErrorOptions,
        skip_fusion_only_upgrades: bool,
        log_file_name: Option<&str>,
        log_file_max_bytes: u64,
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
            command,
            max_log_verbosity,
            max_file_log_verbosity,
            otel_file_path: otel_file_name.map(|file_name| log_dir_path.join(file_name)),
            otel_parquet_file_path: otel_parquet_file_name
                .map(|file_name| crate::constants::default_metadata_dir(out_dir).join(file_name)),
            log_path: log_dir_path,
            log_file_name: log_file_name.map(|s| s.to_string()),
            log_file_max_bytes,
            invocation_id,
            parent_span_id,
            export_to_otlp,
            log_format,
            file_log_format: None,
            enable_query_log,
            show_options,
            show_all_deprecations,
            warn_error_options,
            skip_fusion_only_upgrades,
            command_name: DBT_FUSION,
        }
    }

    /// Override the user-facing CLI brand name shown in the version banner and
    /// JSON log lines.
    pub fn with_command_name(mut self, command_name: &'static str) -> Self {
        self.command_name = command_name;
        self
    }

    /// Override the file-specific log format. When `None` fall back to `log_format`.
    pub fn with_file_log_format(mut self, file_log_format: Option<LogFormat>) -> Self {
        self.file_log_format = file_log_format;
        self
    }

    /// Creates a new FsTraceConfig with proper path resolution.
    /// This method never fails - it uses fallback logic for directory resolution.
    ///
    /// OTel parquet tracing is only enabled when explicitly requested via `--otel-parquet-file-name`.
    pub fn new_from_io_args(
        command: FsCommand,
        project_dir: Option<&PathBuf>,
        target_path: Option<&PathBuf>,
        io_args: &IoArgs,
        warn_error_options: Option<&WarnErrorOptions>,
        skip_fusion_only_upgrades: bool,
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

        // OTel parquet tracing is only enabled when explicitly requested via
        // --otel-parquet-file-name. The write_metadata flag no longer auto-enables it.
        let otel_parquet_file_name = io_args.otel_parquet_file_name.as_deref();

        Self::new(
            package,
            command,
            project_dir,
            target_path,
            io_args.log_path.as_ref(),
            max_log_verbosity,
            max_file_log_verbosity,
            io_args.otel_file_name.as_deref(),
            otel_parquet_file_name,
            io_args.invocation_id,
            io_args.otel_parent_span_id,
            io_args.export_to_otlp,
            io_args.log_format,
            true, // Always enable query log for now
            io_args.show.clone(),
            io_args.show_all_deprecations,
            warn_error_options.cloned().unwrap_or_default(),
            skip_fusion_only_upgrades,
            None, // log_file_name - use default dbt.log
            io_args.log_file_max_bytes,
        )
        .with_file_log_format(io_args.log_format_file)
    }

    /// Initializes tracing with the consumers configured for this CLI invocation.
    pub fn init(
        self,
    ) -> FsResult<(
        dbt_tracing::init::TelemetryHandle,
        Box<dyn TracingConfigProvider>,
    )> {
        let package = self.package;
        let fallback_trace_id = self.invocation_id.as_u128();
        let fallback_parent_span_id = self.parent_span_id;
        let max_log_verbosity = std::cmp::max(self.max_log_verbosity, self.max_file_log_verbosity);
        let (middlewares, consumer_layers, shutdown_items, feature_handle) =
            self.build_layers()?.into_parts();

        init_tracing_with_layers(
            package,
            fallback_trace_id,
            fallback_parent_span_id,
            max_log_verbosity,
            middlewares,
            consumer_layers,
            shutdown_items,
            feature_handle,
        )
    }

    /// Builds the configured tracing layers and corresponding shutdown items.
    /// This method handles all path creation and file opening as needed.
    /// If no layers are configured, returns an empty layer and no shutdown items.
    pub fn build_layers(&self) -> FsResult<FsTraceLayers> {
        let mut shutdown_items = Vec::new();
        let mut consumer_layers = Vec::new();
        let (middleware_layers, warn_error_options) = build_shared_middleware_layers(
            self.show_all_deprecations,
            self.warn_error_options.clone(),
            self.skip_fusion_only_upgrades,
        );

        // Create jsonl writer layer if file path provided
        if let Some(file_path) = &self.otel_file_path {
            let (layer, handle) =
                build_jsonl_file_consumer(file_path, self.max_file_log_verbosity)?;

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

            let (parquet_layer, writer_handle) =
                build_parquet_writer_layer::<_, TelemetryEventTypeRegistry>(file)
                    .map_err(FsError::from)?;

            // Keep a handle for shutdown
            shutdown_items.push(writer_handle);

            // Create layer. User specified filtering is not applied here
            consumer_layers.push(parquet_layer)
        };

        // Create console layer based on log format.
        match self.log_format {
            LogFormat::Default | LogFormat::Text => {
                // Create layer and apply user specified filtering
                consumer_layers.push(build_tui_layer(
                    self.max_log_verbosity,
                    self.log_format,
                    self.show_options.clone(),
                    self.command,
                ))
            }
            LogFormat::Json => {
                // Create layer and apply user specified filtering
                consumer_layers.push(build_json_compat_layer(
                    std::io::stdout(),
                    self.max_log_verbosity,
                    self.invocation_id,
                    self.command,
                    self.command_name,
                ))
            }
            LogFormat::Otel => {
                // Create jsonl writer layer on stdout if log format is OTEL
                // No shutdown logic as we flushing to stdout as we write anyway
                consumer_layers.push(build_jsonl_layer(
                    std::io::stdout(),
                    self.max_log_verbosity,
                    Some(dbt_log_preprocessor_hook),
                ));
            }
        };

        // If any of the file logs are enabled - create the log directory
        if self.enable_query_log || self.max_file_log_verbosity != LevelFilter::OFF {
            // Ensure log directory exists
            crate::stdfs::create_dir_all(&self.log_path)?;
        }

        // File sink honours `--log-format-file` when set, otherwise uses `log_format`
        let (file_log_layer, mut file_log_shutdown_items, file_log_path) = build_file_log_consumer(
            self.max_file_log_verbosity,
            &self.log_path,
            self.log_file_name.as_deref(),
            self.log_file_max_bytes,
            self.file_log_format.unwrap_or(self.log_format),
            self.invocation_id,
            self.command,
            self.command_name,
        )?;
        if let Some(file_log_layer) = file_log_layer {
            consumer_layers.push(file_log_layer);
        }
        shutdown_items.append(&mut file_log_shutdown_items);

        let tracing_config_provider =
            build_tracing_config_provider(warn_error_options, file_log_path, self.command_name);

        // Create query log writer layer (always enabled; internal-only event sink)
        if self.enable_query_log {
            let file_path = self.log_path.join(DBT_DEFAULT_QUERY_LOG_FILE_NAME);
            // Keep query_log.sql scoped to the current invocation.
            let file = crate::stdfs::File::create(&file_path)
                .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to open query log file: {}", e))?;

            let (layer, handle) = build_query_log_layer_with_background_writer(file);
            shutdown_items.push(handle);
            consumer_layers.push(layer)
        };

        // Create OTLP layer - if enabled and endpoint is set via env vars
        if self.export_to_otlp
            && let Some((otlp_layer, mut handles)) = build_otlp_layer(
                OtlpResourceConfig::new(self.command_name, env!("CARGO_PKG_VERSION")),
                Some(dbt_log_preprocessor_hook),
            )
        {
            shutdown_items.append(&mut handles);
            consumer_layers.push(otlp_layer)
        };

        Ok(FsTraceLayers {
            middleware_layers,
            consumer_layers,
            shutdown_items,
            tracing_config_provider,
        })
    }
}
