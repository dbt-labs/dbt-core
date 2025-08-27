use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};

use arrow::datatypes::FieldRef;
use dbt_telemetry::{
    TelemetryRecord,
    serialize::arrow::{create_arrow_schema, serialize_to_arrow},
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use tracing::{Subscriber, span};
use tracing_subscriber::{Layer, layer::Context};

use super::super::{event_info::with_current_thread_event_data, init::TelemetryShutdown};
use crate::{ErrorCode, FsResult};

/// Buffer size for parquet record batching - write when this many records are collected
const PARQUET_WRITER_BUF_SIZE: usize = 1000;

/// A tracing layer that batches telemetry data and writes it as Parquet files.
///
/// This layer collects telemetry records in batches and writes them to Parquet
/// format using Arrow serialization in a separate worker thread. It filters records to only include SpanEnd
/// records and valid log records, skipping SpanStart, DevInternal, Unknown, and LegacyLog records.
pub struct TelemetryParquetWriterLayer {
    sender: mpsc::Sender<ParquetMessage>,
}

/// Messages sent to the parquet writer thread
enum ParquetMessage {
    Write(Box<TelemetryRecord>),
    Shutdown,
}

/// Internal parquet writer that handles batching and file operations
struct ParquetWriter {
    buffer: Vec<TelemetryRecord>,
    arrow_schema: (Vec<FieldRef>, Vec<FieldRef>),
    parquet_writer: Option<ArrowWriter<std::fs::File>>,
}

impl ParquetWriter {
    fn new(file_path: std::path::PathBuf) -> FsResult<Self> {
        let arrow_schema = create_arrow_schema()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to create Arrow schema: {}", e))?;

        // Create the file and initialize the Parquet writer
        let file_dir = file_path.parent().ok_or_else(|| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to get parent directory for file path"
            )
        })?;

        crate::stdfs::create_dir_all(file_dir)?;

        let file = std::fs::File::create(&file_path)
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to create parquet file: {}", e))?;

        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let parquet_writer = ArrowWriter::try_new(
            file,
            arrow::datatypes::Schema::new(arrow_schema.1.clone()).into(),
            Some(writer_properties),
        )
        .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to create Parquet writer: {}", e))?;

        Ok(Self {
            buffer: Vec::with_capacity(PARQUET_WRITER_BUF_SIZE),
            arrow_schema,
            parquet_writer: Some(parquet_writer),
        })
    }

    fn write_record(&mut self, record: TelemetryRecord) -> FsResult<()> {
        // Write batch if buffer is full
        if self.buffer.len() >= PARQUET_WRITER_BUF_SIZE {
            self.flush_batch()?;
        }

        // Add to buffer
        self.buffer.push(record);

        Ok(())
    }

    fn flush_batch(&mut self) -> FsResult<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Serialize records to Arrow RecordBatch
        let record_batch =
            serialize_to_arrow(&self.buffer, &self.arrow_schema.0, &self.arrow_schema.1)
                .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to serialize to Arrow: {}", e))?;

        // Write the batch
        if let Some(ref mut writer) = self.parquet_writer {
            writer
                .write(&record_batch)
                .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to write Parquet batch: {}", e))?;
        }

        // Clear buffer for reuse (truncate avoids reallocation)
        self.buffer.truncate(0);

        Ok(())
    }

    fn finalize(&mut self) -> FsResult<()> {
        // Flush any remaining records
        self.flush_batch()?;

        // Close the parquet writer
        if let Some(writer) = self.parquet_writer.take() {
            writer.close().map_err(|e| {
                fs_err!(ErrorCode::IoError, "Failed to close Parquet writer: {}", e)
            })?;
        }

        Ok(())
    }
}

impl TelemetryParquetWriterLayer {
    pub fn new(file_path: std::path::PathBuf) -> FsResult<(Self, TelemetryParquetWriterHandle)> {
        let (sender, receiver) = mpsc::channel::<ParquetMessage>();
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_clone = shutdown_flag.clone();

        let mut writer = ParquetWriter::new(file_path)?;

        let writer_thread = thread::spawn(move || {
            while let Ok(message) = receiver.recv() {
                match message {
                    ParquetMessage::Write(record) => {
                        if let Err(e) = writer.write_record(*record) {
                            eprintln!("Failed to write parquet record: {e}");
                            continue;
                        }
                    }
                    ParquetMessage::Shutdown => {
                        // Process any remaining messages in the channel
                        while let Ok(ParquetMessage::Write(record)) = receiver.try_recv() {
                            let _ = writer.write_record(*record);
                        }

                        // Finalize and close the parquet writer
                        if let Err(e) = writer.finalize() {
                            eprintln!("Failed to finalize parquet writer: {e}");
                        }

                        shutdown_flag_clone.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }

            // Mark shutdown complete even if we exited due to channel closure
            shutdown_flag_clone.store(true, Ordering::Relaxed);
        });

        let layer = Self { sender };

        let handle = TelemetryParquetWriterHandle {
            sender: layer.sender.clone(),
            writer_thread: Some(writer_thread),
            shutdown_flag,
        };

        Ok((layer, handle))
    }

    /// Send a telemetry record to be written
    pub fn write_record(&self, record: TelemetryRecord) {
        if self
            .sender
            .send(ParquetMessage::Write(Box::new(record)))
            .is_err()
        {
            // Channel closed, writer thread probably shut down
            eprintln!("Failed to send parquet record: writer thread not available");
        }
    }

    fn should_include_record(&self, record: &TelemetryRecord) -> bool {
        match record {
            // Only include SpanEnd records, not SpanStart
            TelemetryRecord::SpanStart(_) => false,
            // Theoretically we should skip DevInternal and Unknown spans,
            // but this may cause some logs to have span_ids pointing to
            // non-existing spans, in parquet. Implementing a proper
            // drill up to the closest non-filtered span is chore, but
            // would also cause the data to differ for parquet and other
            // consumers, so we skip filtering here for now.
            TelemetryRecord::SpanEnd(_) => true,
            // TelemetryRecord::SpanEnd(span_end) => {
            //     // Skip DevInternal and Unknown span types
            //     !matches!(
            //         span_end.attributes,
            //         dbt_telemetry::TelemetryAttributes::DevInternal { .. }
            //             | dbt_telemetry::TelemetryAttributes::Unknown { .. }
            //     )
            // }
            TelemetryRecord::LogRecord(log_record) => {
                // Skip LegacyLog records - they are temporary and should not be used
                // by any downstream consumers
                // Skip InlineCompiledCode - this is the first case of a potentially
                // PII sensitive log that should not be stored, hence we skip it here.
                !matches!(
                    log_record.attributes,
                    dbt_telemetry::TelemetryAttributes::LegacyLog(_)
                        | dbt_telemetry::TelemetryAttributes::InlineCompiledCode(_)
                )
            }
        }
    }
}

impl<S> Layer<S> for TelemetryParquetWriterLayer
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let span = ctx
            .span(&id)
            .expect("Span must exist for id in the current context");

        // Get the TelemetryRecord from extensions
        if let Some(record) = span.extensions().get::<dbt_telemetry::SpanEndInfo>() {
            let telemetry_record = TelemetryRecord::SpanEnd(record.clone());

            // Filter
            if !self.should_include_record(&telemetry_record) {
                return;
            }

            self.write_record(telemetry_record);
        }
    }

    fn on_event(&self, _event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        with_current_thread_event_data(|log_record| {
            let telemetry_record = TelemetryRecord::LogRecord(log_record.clone());

            // Filter
            if !self.should_include_record(&telemetry_record) {
                return;
            }

            self.write_record(telemetry_record);
        });
    }
}

/// Handle for shutdown handling  
pub struct TelemetryParquetWriterHandle {
    sender: mpsc::Sender<ParquetMessage>,
    writer_thread: Option<JoinHandle<()>>,
    shutdown_flag: Arc<AtomicBool>,
}

impl TelemetryParquetWriterHandle {
    /// Gracefully shutdown the writer thread
    fn shutdown_internal(&mut self) -> thread::Result<()> {
        // Send shutdown message
        let _ = self.sender.send(ParquetMessage::Shutdown);

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

impl TelemetryShutdown for TelemetryParquetWriterHandle {
    fn shutdown(&mut self) -> FsResult<()> {
        self.shutdown_internal().map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to close telemetry parquet writer: {e:?}"
            )
        })
    }
}
