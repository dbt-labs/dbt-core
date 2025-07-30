use crate::FsResult;
use crate::io_args::IoArgs;

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use tracing::subscriber::{NoSubscriber, set_global_default};
use tracing_subscriber::{
    EnvFilter,
    fmt::{format::FmtSpan, writer::BoxMakeWriter},
};

pub struct FsTraceConfig {
    pub file_path: Option<PathBuf>,
}

impl From<IoArgs> for FsTraceConfig {
    fn from(args: IoArgs) -> Self {
        Self {
            file_path: args
                .trace_path
                .map(|p| Some(p.join("dbt.trace")))
                .unwrap_or_else(|| None),
        }
    }
}

#[allow(unused_variables)]
pub fn init_tracing(config: FsTraceConfig) -> FsResult<()> {
    if cfg!(debug_assertions)
        && let Some(file_path) = config.file_path
    {
        // Set up file-based tracing in debug builds when path is provided
        let file = Arc::new(File::create(file_path)?);

        let make_writer = BoxMakeWriter::new({
            move || {
                let file_clone = file.try_clone().expect("Failed to clone file");
                Box::new(file_clone) as Box<dyn std::io::Write + Send>
            }
        });

        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("trace"));

        let subscriber = tracing_subscriber::fmt()
            // set filter for the events
            .with_env_filter(env_filter)
            // set the writer, i.e., place to write to
            .with_writer(make_writer)
            // include span events for #include
            .with_span_events(FmtSpan::FULL)
            // avoid issues with colors, etc.
            .with_ansi(false)
            // format as json
            .json()
            // finish it
            .finish();

        set_global_default(subscriber).expect("setting default subscriber failed");
    } else {
        // Always set up a no-op subscriber to prevent tracing from falling back to stdout

        // Try to set a no-op subscriber, but don't panic if it fails
        let _ = set_global_default(NoSubscriber::default());
    }

    Ok(())
}
