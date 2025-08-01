use std::path::PathBuf;

use super::convert::log_level_filter_to_tracing;
use crate::io_args::IoArgs;

/// Configuration for tracing.
///
/// This struct defines where trace data should be written for both debug
/// and production scenarios, and defines metadata necessary for top-level span
/// and trace correlation.
#[derive(Clone, Debug)]
pub struct FsTraceConfig {
    pub(super) max_log_level: tracing::level_filters::LevelFilter,
    /// Path for production telemetry output (JSONL format)
    pub(super) otm_file_path: Option<PathBuf>,
    /// Invocation ID used as trace ID for correlation
    pub(super) invocation_id: uuid::Uuid,
    pub(super) print_to_stdout: bool,
    #[cfg(all(debug_assertions, feature = "otlp"))]
    pub(super) export_to_otlp: bool,
}

impl Default for FsTraceConfig {
    fn default() -> Self {
        Self {
            max_log_level: tracing::level_filters::LevelFilter::INFO,
            otm_file_path: None,
            invocation_id: uuid::Uuid::new_v4(),
            print_to_stdout: false,
            #[cfg(all(debug_assertions, feature = "otlp"))]
            export_to_otlp: false,
        }
    }
}

impl From<&IoArgs> for FsTraceConfig {
    fn from(args: &IoArgs) -> Self {
        Self {
            max_log_level: args
                .log_level
                .map(|lf| log_level_filter_to_tracing(&lf))
                .unwrap_or_else(|| {
                    if cfg!(debug_assertions) {
                        tracing::level_filters::LevelFilter::TRACE
                    } else {
                        tracing::level_filters::LevelFilter::INFO
                    }
                }),
            otm_file_path: args.otm_file_name.as_ref().map(|file_name| {
                args.log_path.as_ref().map_or_else(
                    || {
                        if args.out_dir.starts_with(&args.in_dir) {
                            args.in_dir.join("logs").join(file_name)
                        } else {
                            // This is because when we do test we do not want to modify in_dir
                            args.out_dir.join(file_name)
                        }
                    },
                    |log_path| {
                        if log_path.is_relative() {
                            // If the path is relative, join it with the current working directory
                            args.in_dir.join(log_path).join(file_name)
                        } else {
                            log_path.join(file_name)
                        }
                    },
                )
            }),
            invocation_id: args.invocation_id,
            // TODO. For now never print to stdout. Maybe remove with the debug layer?
            print_to_stdout: false,
            #[cfg(all(debug_assertions, feature = "otlp"))]
            export_to_otlp: args.export_to_otlp,
        }
    }
}
