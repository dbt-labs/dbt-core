use crate::io_args::IoArgs;
use crate::FsResult;

#[cfg(debug_assertions)]
use std::fs::File;
use std::path::PathBuf;
#[cfg(debug_assertions)]
use std::sync::Arc;

#[cfg(debug_assertions)]
use tracing::subscriber::set_global_default;
#[cfg(debug_assertions)]
use tracing_subscriber::fmt::format::FmtSpan;
#[cfg(debug_assertions)]
use tracing_subscriber::{fmt::writer::BoxMakeWriter, EnvFilter};

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
    // We ignore the entire setup in non-debug builds
    #[cfg(debug_assertions)]
    if let Some(file_path) = config.file_path {
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
    }

    Ok(())
}
