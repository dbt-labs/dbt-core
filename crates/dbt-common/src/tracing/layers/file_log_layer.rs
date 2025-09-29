use tracing::level_filters::LevelFilter;

use super::super::{
    background_writer::BackgroundWriter,
    layer::{ConsumerLayer, TelemetryConsumer},
    shared_writer::SharedWriter,
    shutdown::TelemetryShutdownItem,
};

/// Build file log layer with a background writer. This is preferred for writing to
/// slow IO sinks like files.
pub fn build_file_log_layer_with_background_writer<W: std::io::Write + Send + 'static>(
    writer: W,
    max_log_verbosity: LevelFilter,
) -> (ConsumerLayer, TelemetryShutdownItem) {
    let (writer, handle) = BackgroundWriter::new(writer);

    (
        Box::new(FileLogLayer::new(writer).with_filter(max_log_verbosity)),
        Box::new(handle),
    )
}

/// Placeholder layer for future file logging support.
pub struct FileLogLayer {
    #[allow(unused)]
    writer: Box<dyn SharedWriter>,
}

impl FileLogLayer {
    pub fn new<W: SharedWriter + 'static>(writer: W) -> Self {
        Self {
            writer: Box::new(writer),
        }
    }
}

impl TelemetryConsumer for FileLogLayer {}
