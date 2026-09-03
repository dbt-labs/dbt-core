use crate::io_args::{FsCommand, IoArgs, LogFormat};
use crate::tracing::FsTraceConfig;
use std::path::PathBuf;

fn config_from_formats(log_format: LogFormat, log_format_file: Option<LogFormat>) -> FsTraceConfig {
    // Any path works; nothing here resolves it.
    let project_dir = PathBuf::from("/dbt-log-format-file-tests");
    let io_args = IoArgs {
        log_format,
        log_format_file,
        ..Default::default()
    };

    FsTraceConfig::new_from_io_args(
        FsCommand::Unset,
        Some(&project_dir),
        None,
        &io_args,
        None,
        false,
        "dbt-test",
    )
}

/// Regression test for dbt-core#15685: `new_from_io_args` copies
/// `IoArgs::log_format_file` onto the config.
#[test]
fn new_from_io_args_carries_log_format_file() {
    let config = config_from_formats(LogFormat::Text, Some(LogFormat::Json));

    assert_eq!(config.file_log_format, Some(LogFormat::Json));
    assert_eq!(config.log_format, LogFormat::Text);
}

#[test]
fn new_from_io_args_leaves_file_log_format_unset_when_not_given() {
    let config = config_from_formats(LogFormat::Json, None);

    // `None` means "fall back to log_format", resolved later in `build_layers`.
    assert_eq!(config.file_log_format, None);
}
