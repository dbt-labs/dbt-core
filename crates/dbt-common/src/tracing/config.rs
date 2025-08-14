use std::path::PathBuf;

use super::convert::log_level_filter_to_tracing;
use crate::{
    FsResult,
    constants::{DBT_METADATA_DIR_NAME, DBT_PROJECT_YML, DBT_TARGET_DIR_NAME},
    io_args::IoArgs,
    io_utils::determine_project_dir,
    logging::LogFormat,
};

/// Configuration for tracing.
///
/// This struct defines where trace data should be written for both debug
/// and production scenarios, and defines metadata necessary for top-level span
/// and trace correlation.
#[derive(Clone, Debug)]
pub struct FsTraceConfig {
    /// Tracing level filter, which specifies maximum verbosity (inverse
    /// of log level)
    pub(super) max_log_verbosity: tracing::level_filters::LevelFilter,
    /// Path for production telemetry output (JSONL format)
    pub(super) otm_file_path: Option<PathBuf>,
    /// Path for production telemetry output (Parquet format)
    pub(super) otm_parquet_file_path: Option<PathBuf>,
    /// Invocation ID used as trace ID for correlation
    pub(super) invocation_id: uuid::Uuid,
    /// If True, traces will be forwarded to OTLP endpoints, if any
    /// are set via OTEL environment variables. See `OTLPExporterLayer::new`
    pub(super) export_to_otlp: bool,
    /// If True, progress bar layer will be enabled
    pub(super) enable_progress: bool,
}

impl Default for FsTraceConfig {
    fn default() -> Self {
        Self {
            max_log_verbosity: tracing::level_filters::LevelFilter::INFO,
            otm_file_path: None,
            otm_parquet_file_path: None,
            invocation_id: uuid::Uuid::new_v4(),
            enable_progress: false,
            export_to_otlp: false,
        }
    }
}

/// Helper function to calculate in_dir and out_dir for tracing configuration.
/// This implements the same logic as execute_setup_and_all_phases but without canonicalization.
fn calculate_trace_dirs(
    project_dir: Option<PathBuf>,
    target_path: Option<PathBuf>,
) -> FsResult<(PathBuf, PathBuf)> {
    let in_dir = if let Some(project_dir) = project_dir {
        project_dir
    } else {
        determine_project_dir(&[], DBT_PROJECT_YML)?
    };

    let out_dir = target_path.unwrap_or_else(|| in_dir.join(DBT_TARGET_DIR_NAME));

    Ok((in_dir, out_dir))
}

impl FsTraceConfig {
    /// Creates a new FsTraceConfig with proper path resolution.
    pub fn new(
        project_dir: Option<PathBuf>,
        target_path: Option<PathBuf>,
        io_args: &IoArgs,
    ) -> FsResult<Self> {
        let (in_dir, out_dir) = calculate_trace_dirs(project_dir, target_path)?;

        Ok(Self {
            max_log_verbosity: io_args
                .log_level
                .map(|lf| log_level_filter_to_tracing(&lf))
                .unwrap_or_else(|| {
                    if cfg!(debug_assertions) {
                        tracing::level_filters::LevelFilter::TRACE
                    } else {
                        tracing::level_filters::LevelFilter::INFO
                    }
                }),
            otm_file_path: io_args.otm_file_name.as_ref().map(|file_name| {
                io_args.log_path.as_ref().map_or_else(
                    || {
                        if out_dir.starts_with(&in_dir) {
                            in_dir.join("logs").join(file_name)
                        } else {
                            // This is because when we do test we do not want to modify in_dir
                            out_dir.join(file_name)
                        }
                    },
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
            otm_parquet_file_path: io_args
                .otm_parquet_file_name
                .as_ref()
                .map(|file_name| out_dir.join(DBT_METADATA_DIR_NAME).join(file_name)),
            invocation_id: io_args.invocation_id,
            // TODO. For now never print to stdout. Maybe remove with the debug layer?
            enable_progress: io_args.log_format == LogFormat::Fancy,
            export_to_otlp: io_args.export_to_otlp,
        })
    }
}
