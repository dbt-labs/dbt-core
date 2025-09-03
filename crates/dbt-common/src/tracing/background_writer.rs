use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};

use crate::{ErrorCode, FsResult};

use super::{TelemetryShutdown, shared_writer::SharedWriter};

/// Channel-based non-blocking writer that performs writes on a separate thread.
///
/// Note that it will flush after every write to ensure data is committed.
///
/// It will stop accepting new writes after first write or flush error occurs,
/// and will return an error on subsequent write attempts.
///
/// The error will be available via the shutdown handle after shutdown.
pub struct BackgroundWriter {
    sender: mpsc::Sender<TelemetryMessage>,
    /// Flag used to avoid repeated error messages in case of early shutdown
    /// due to panics in writer thread (e.g. disk full)
    shutdown_flag: Arc<AtomicBool>,
}

pub struct BackgroundWriterShutdownHandle {
    sender: mpsc::Sender<TelemetryMessage>,
    writer_thread: Option<JoinHandle<()>>,
    /// Flag used to avoid repeated error messages in case of early shutdown
    /// due to panics in writer thread (e.g. disk full)
    shutdown_flag: Arc<AtomicBool>,
    shutdown_err: Arc<Mutex<Option<io::Error>>>,
}

/// Messages sent to the writer thread
enum TelemetryMessage {
    Write(Vec<u8>),
    Shutdown,
}

impl BackgroundWriter {
    pub fn new<W>(mut writer: W) -> (Self, BackgroundWriterShutdownHandle)
    where
        W: io::Write + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel::<TelemetryMessage>();
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_clone = shutdown_flag.clone();
        let shutdown_err = Arc::new(Mutex::new(None));
        let shutdown_err_clone = shutdown_err.clone();

        let writer_thread = thread::spawn(move || {
            while let Ok(message) = receiver.recv() {
                match message {
                    TelemetryMessage::Write(data) => {
                        // Write the data
                        if let Err(e) = writer.write_all(&data) {
                            // Save the error for later reporting
                            let mut err_lock = shutdown_err_clone.lock().expect("Mutex poisoned");
                            *err_lock = Some(e);

                            // Avoid further attempts to write, assume fatal
                            break;
                        }

                        // Immediately flush to ensure data is committed
                        if let Err(e) = writer.flush() {
                            // Save the error for later reporting
                            let mut err_lock = shutdown_err_clone.lock().expect("Mutex poisoned");
                            *err_lock = Some(e);

                            // Avoid further attempts to write, assume fatal
                            break;
                        }
                    }
                    TelemetryMessage::Shutdown => {
                        // Process any remaining messages in the channel
                        while let Ok(TelemetryMessage::Write(data)) = receiver.try_recv() {
                            let _ = writer.write_all(&data);
                        }

                        // Final flush before shutdown
                        let _ = writer.flush();
                        break;
                    }
                }
            }

            // Mark shutdown complete for whatever reason we exited the loop
            shutdown_flag_clone.store(true, Ordering::Release);
        });

        (
            Self {
                sender: sender.clone(),
                shutdown_flag: shutdown_flag.clone(),
            },
            BackgroundWriterShutdownHandle {
                sender,
                writer_thread: Some(writer_thread),
                shutdown_flag,
                shutdown_err,
            },
        )
    }

    /// Send data to be written
    pub fn write_bytes(&self, data: &[u8]) -> FsResult<()> {
        if self.shutdown_flag.load(Ordering::Acquire) {
            // Writer thread has shut down
            return err!(
                ErrorCode::IoError,
                "Telemetry writer thread has terminated unexpectedly",
            );
        }

        self.sender
            .send(TelemetryMessage::Write(data.to_vec()))
            .map_err(|_| {
                // Channel is disconnected, mark as shut down
                self.shutdown_flag.store(true, Ordering::Release);
                fs_err!(
                    ErrorCode::IoError,
                    "Telemetry writer thread has terminated unexpectedly",
                )
            })
    }
}

impl SharedWriter for BackgroundWriter {
    fn write(&self, data: &str) -> io::Result<()> {
        self.write_bytes(data.as_bytes())
            .map_err(|e| io::Error::other(e.to_string()))
    }
}

impl TelemetryShutdown for BackgroundWriterShutdownHandle {
    fn shutdown(&mut self) -> FsResult<()> {
        if !self.shutdown_flag.swap(true, Ordering::AcqRel) {
            // Send shutdown message
            let _ = self.sender.send(TelemetryMessage::Shutdown);
        }

        // Wait for the writer thread to finish
        if let Some(handle) = self.writer_thread.take() {
            handle.join().map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to close telemetry file writer: {e:?}"
                )
            })?;
        }

        // Check if there was an error during writing
        let err_lock = self.shutdown_err.lock().expect("Mutex poisoned");

        if let Some(e) = err_lock.as_ref() {
            return Err(fs_err!(
                ErrorCode::IoError,
                "Telemetry writer encountered an error: {}. Some telemetry data may have been lost.",
                e
            ));
        }

        Ok(())
    }
}

/// Ensure shutdown is called on drop
impl Drop for BackgroundWriterShutdownHandle {
    fn drop(&mut self) {
        // Discard any error, as we can't return it from drop
        let _ = self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    /// Mock writer that captures output
    struct MockWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
        msg_buffer: Arc<Mutex<Vec<u8>>>,
        fail_after_message_count: Option<usize>,
        fail_on_flush: bool,
        message_count: Arc<Mutex<usize>>,
    }

    impl MockWriter {
        fn new() -> Self {
            Self {
                buffer: Arc::new(Mutex::new(Vec::new())),
                msg_buffer: Arc::new(Mutex::new(Vec::new())),
                fail_after_message_count: None,
                fail_on_flush: false,
                message_count: Arc::new(Mutex::new(0)),
            }
        }

        fn with_fail_after_message(mut self, count: usize) -> Self {
            self.fail_after_message_count = Some(count);
            self
        }

        fn with_fail_on_flush(mut self) -> Self {
            self.fail_on_flush = true;
            self
        }
    }

    impl Write for MockWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            // Check if this is a complete message
            if buf.ends_with(b"\n") {
                let count = {
                    let mut c = self.message_count.lock().unwrap();
                    *c += 1;
                    *c
                };

                // Fail after writing N messages
                if let Some(fail_after) = self.fail_after_message_count {
                    if count > fail_after {
                        // Don't write to buffer on failure
                        return Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "Mock write error",
                        ));
                    }
                }

                let mut full_msg = self.msg_buffer.lock().unwrap();

                // Write to main buffer
                self.buffer
                    .lock()
                    .unwrap()
                    .extend_from_slice(full_msg.as_slice());
                self.buffer.lock().unwrap().extend_from_slice(buf);

                // Clear message buffer
                full_msg.clear();
            } else {
                // Partial line, accumulate
                self.msg_buffer.lock().unwrap().extend_from_slice(buf);
            }

            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            if self.fail_on_flush {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "Mock flush error",
                ))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn test_normal_write_and_shutdown() {
        let mock = MockWriter::new();
        let buffer = mock.buffer.clone();
        let (writer, mut handle) = BackgroundWriter::new(Box::new(mock));

        // Write two messages
        assert!(writer.write_bytes(b"message1\n").is_ok());
        assert!(writer.write_bytes(b"message2\n").is_ok());

        // Shutdown and verify exact buffer contents
        handle.shutdown().unwrap();

        let buffer_contents = buffer.lock().unwrap();
        let output = String::from_utf8_lossy(&buffer_contents);
        assert_eq!(output, "message1\nmessage2\n");
    }

    #[test]
    fn test_write_failure_stops_writer() {
        // Create a writer that fails after 1 message
        let mock = MockWriter::new().with_fail_after_message(1);
        let buffer = mock.buffer.clone();
        let counter = mock.message_count.clone();
        let (writer, mut handle) = BackgroundWriter::new(Box::new(mock));

        // First message should succeed
        assert!(writer.write_bytes(b"message1\n").is_ok());
        // Second a second message. Write will succeed, but writer thread should fail
        assert!(writer.write_bytes(b"message2\n").is_ok());

        // Lock until we reach count of 1
        {
            let count = counter.lock().unwrap();
            if *count < 2 {
                drop(count);
                // Wait a bit for the writer thread to process
                thread::sleep(std::time::Duration::from_millis(10));
            }

            let count = counter.lock().unwrap();
            assert!(*count > 1);
        };

        // The next write should fail
        assert!(writer.write_bytes(b"message3\n").is_err());

        // Shutdown - should return error due to write failure
        let Err(error) = handle.shutdown() else {
            panic!("Expected shutdown to return error due to write failure");
        };

        assert_eq!(error.code, ErrorCode::IoError);
        assert!(error.to_string().contains("Mock write error"));

        // Verify only first message was written (writer failed after 1)
        let buffer_contents = buffer.lock().unwrap();
        let output = String::from_utf8_lossy(&buffer_contents);
        assert_eq!(output, "message1\n");
    }

    #[test]
    fn test_flush_failure() {
        let mock = MockWriter::new().with_fail_on_flush();
        let buffer = mock.buffer.clone();
        let (writer, mut handle) = BackgroundWriter::new(Box::new(mock));

        // Write will succeed but flush will fail
        assert!(writer.write_bytes(b"message1\n").is_ok());

        // Shutdown - should return error due to write failure
        let Err(error) = handle.shutdown() else {
            panic!("Expected shutdown to return error due to write failure");
        };

        assert_eq!(error.code, ErrorCode::IoError);
        assert!(error.to_string().contains("Mock flush error"));

        // After shutdown, writes should fail
        assert!(writer.write_bytes(b"message2\n").is_err());

        // First message was written to buffer (before flush failed)
        let buffer_contents = buffer.lock().unwrap();
        let output = String::from_utf8_lossy(&buffer_contents);
        assert_eq!(output, "message1\n");
    }

    #[test]
    fn test_shutdown_idempotency() {
        let mock = MockWriter::new();
        let (writer, mut handle) = BackgroundWriter::new(Box::new(mock));

        assert!(writer.write_bytes(b"message1\n").is_ok());

        // Multiple shutdowns should be safe
        assert!(handle.shutdown().is_ok());
        assert!(handle.shutdown().is_ok());
        assert!(handle.shutdown().is_ok());
    }

    #[test]
    fn test_write_after_shutdown() {
        let mock = MockWriter::new();
        let buffer = mock.buffer.clone();
        let (writer, mut handle) = BackgroundWriter::new(Box::new(mock));

        assert!(writer.write_bytes(b"message1\n").is_ok());

        handle.shutdown().unwrap();

        // Writes after shutdown should return error
        assert!(writer.write_bytes(b"after_shutdown\n").is_err());

        let buffer_contents = buffer.lock().unwrap();
        let output = String::from_utf8_lossy(&buffer_contents);
        assert_eq!(output, "message1\n");
    }
}
