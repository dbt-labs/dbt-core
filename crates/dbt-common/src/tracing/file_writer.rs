use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, mpsc};
use std::thread::{self, JoinHandle};

use crate::{ErrorCode, FsResult};

use super::TelemetryShutdown;

/// Channel-based writer that handles telemetry data in a separate thread
pub struct TelemetryFileWriter {
    sender: mpsc::Sender<TelemetryMessage>,
    writer_thread: Option<JoinHandle<()>>,
    shutdown_flag: Arc<AtomicBool>,
}

/// Messages sent to the writer thread
enum TelemetryMessage {
    Write(String),
    Shutdown,
}

impl TelemetryFileWriter {
    pub fn new(mut writer: Box<dyn std::io::Write + Send>) -> Self {
        let (sender, receiver) = mpsc::channel::<TelemetryMessage>();
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_clone = shutdown_flag.clone();

        let writer_thread = thread::spawn(move || {
            while let Ok(message) = receiver.recv() {
                match message {
                    TelemetryMessage::Write(line) => {
                        // Write the line with newline
                        if let Err(e) = writeln!(writer, "{line}") {
                            eprintln!("Failed to write telemetry record: {e}");
                            continue;
                        }

                        // Immediately flush to ensure data hits disk
                        if let Err(e) = writer.flush() {
                            eprintln!("Failed to flush telemetry writer: {e}");
                        }
                    }
                    TelemetryMessage::Shutdown => {
                        // Process any remaining messages in the channel
                        while let Ok(TelemetryMessage::Write(line)) = receiver.try_recv() {
                            let _ = writeln!(writer, "{line}");
                        }

                        // Final flush before shutdown
                        let _ = writer.flush();
                        shutdown_flag_clone.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }

            // Mark shutdown complete even if we exited due to channel closure
            shutdown_flag_clone.store(true, Ordering::Relaxed);
        });

        Self {
            sender,
            writer_thread: Some(writer_thread),
            shutdown_flag,
        }
    }

    /// Send a telemetry record to be written
    pub fn write(&self, json: String) {
        if self.sender.send(TelemetryMessage::Write(json)).is_err() {
            // Channel closed, writer thread probably shut down
            eprintln!("Failed to send telemetry record: writer thread not available");
        }
    }

    /// Gracefully shutdown the writer thread
    pub fn shutdown(&mut self) -> thread::Result<()> {
        // Send shutdown message
        let _ = self.sender.send(TelemetryMessage::Shutdown);

        // Wait for the writer thread to finish
        if let Some(handle) = self.writer_thread.take() {
            handle.join()?;
        }

        // Wait a bit to ensure shutdown completed
        let start = std::time::Instant::now();
        while !self.shutdown_flag.load(Ordering::Relaxed) && start.elapsed().as_secs() < 5 {
            thread::sleep(std::time::Duration::from_millis(10));
        }

        Ok(())
    }
}

impl TelemetryShutdown for Arc<RwLock<TelemetryFileWriter>> {
    fn shutdown(&mut self) -> FsResult<()> {
        // Arc doesn't implement DerefMut, so we need to use try_unwrap or clone
        // Since we're shutting down, we should have the only reference to this Arc
        let mut writer = self
            .try_write()
            .map_err(|_| fs_err!(ErrorCode::IoError, "Failed to acquire write lock"))?;

        TelemetryFileWriter::shutdown(&mut writer).map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to close telemetry file writer: {e:?}"
            )
        })
    }
}
