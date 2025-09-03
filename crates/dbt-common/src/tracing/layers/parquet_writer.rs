use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
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

/// Buffer size for parquet record batching. This is the buffer in our part of the code
/// used to reduce the number of rust struct -> RecordBatch conversions.
/// The ArrowWriter itself also has an internal buffer. See the memory limit const below.
const PARQUET_WRITER_BUF_SIZE: usize = 1024;

/// Maximum memory usage for the ArrowWriter internal buffer.
const PARQUET_WRITER_MEMORY_LIMIT: usize = 128 * 1024 * 1024; // 128 MB

impl<W> ParquetWriter<W>
where
    W: Write + Send + 'static,
{
    fn new(writer: W) -> FsResult<Self> {
        let arrow_schema = create_arrow_schema()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to create Arrow schema: {}", e))?;

        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let parquet_writer = ArrowWriter::try_new(
            writer,
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
        let Some(ref mut writer) = self.parquet_writer else {
            // Should not be possible, since we ensure that parquet_writer is Some in new()
            // and we only take it in finalize() after flushing
            unreachable!("Parquet writer is not initialized");
        };

        writer
            .write(&record_batch)
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to write Parquet batch: {}", e))?;

        // Flush if we are over memory limit
        if writer.memory_size() >= PARQUET_WRITER_MEMORY_LIMIT {
            writer.flush().map_err(|e| {
                fs_err!(ErrorCode::IoError, "Failed to flush Parquet writer: {}", e)
            })?;
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

/// A tracing layer that batches telemetry data and writes it as Parquet files.
///
/// This layer collects telemetry records in batches and writes them to Parquet
/// format using Arrow serialization in a separate worker thread. It filters records to only include SpanEnd
/// records and valid log records, skipping SpanStart, DevInternal, Unknown, and LegacyLog records.
pub struct TelemetryParquetWriterLayer {
    sender: mpsc::Sender<ParquetMessage>,
    /// Flag used to avoid repeated error messages in case of early shutdown
    /// due to panics in writer thread (e.g. disk full)
    shutdown_flag: Arc<AtomicBool>,
}

/// Messages sent to the parquet writer thread
enum ParquetMessage {
    Write(Box<TelemetryRecord>),
    Shutdown,
}

/// Internal parquet writer that handles batching and file operations
struct ParquetWriter<W>
where
    W: Write + Send + 'static,
{
    buffer: Vec<TelemetryRecord>,
    arrow_schema: (Vec<FieldRef>, Vec<FieldRef>),
    parquet_writer: Option<ArrowWriter<W>>,
}

impl TelemetryParquetWriterLayer {
    pub fn new<W>(writer: W) -> FsResult<(Self, TelemetryParquetWriterHandle)>
    where
        W: Write + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel::<ParquetMessage>();
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_clone = shutdown_flag.clone();
        let shutdown_err = Arc::new(Mutex::new(None));
        let shutdown_err_clone = shutdown_err.clone();

        let mut parquet_writer = ParquetWriter::new(writer)?;

        let writer_thread = thread::spawn(move || {
            while let Ok(message) = receiver.recv() {
                match message {
                    ParquetMessage::Write(record) => {
                        if let Err(e) = parquet_writer.write_record(*record) {
                            // Save the error for later reporting
                            let mut err_lock = shutdown_err_clone.lock().expect("Mutex poisoned");
                            *err_lock = Some(io::Error::other(e.to_string()));

                            // Avoid further attempts to write, assume fatal
                            break;
                        }
                    }
                    ParquetMessage::Shutdown => {
                        // Process any remaining messages in the channel
                        while let Ok(ParquetMessage::Write(record)) = receiver.try_recv() {
                            let _ = parquet_writer.write_record(*record);
                        }

                        // Finalize and close the parquet writer
                        if let Err(e) = parquet_writer.finalize() {
                            // Save the error for later reporting
                            let mut err_lock = shutdown_err_clone.lock().expect("Mutex poisoned");
                            *err_lock = Some(io::Error::other(e.to_string()));
                        }

                        break;
                    }
                }
            }

            // Mark shutdown complete for whatever reason we exited the loop
            shutdown_flag_clone.store(true, Ordering::Release);
        });

        let layer = Self {
            sender: sender.clone(),
            shutdown_flag: shutdown_flag.clone(),
        };

        let handle = TelemetryParquetWriterHandle {
            sender,
            writer_thread: Some(writer_thread),
            shutdown_flag,
            shutdown_err,
        };

        Ok((layer, handle))
    }

    /// Send a telemetry record to be written
    pub fn write_record(&self, record: TelemetryRecord) -> FsResult<()> {
        if self.shutdown_flag.load(Ordering::Acquire) {
            // Writer thread has shut down
            return err!(
                ErrorCode::IoError,
                "Telemetry parquet writer thread has terminated unexpectedly",
            );
        }

        self.sender
            .send(ParquetMessage::Write(Box::new(record)))
            .map_err(|_| {
                // Channel is disconnected, mark as shut down
                self.shutdown_flag.store(true, Ordering::Release);
                fs_err!(
                    ErrorCode::IoError,
                    "Telemetry parquet writer thread has terminated unexpectedly",
                )
            })
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

            // Ignore error if write fails - we don't want to panic in a layer
            let _ = self.write_record(telemetry_record);
        }
    }

    fn on_event(&self, _event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        with_current_thread_event_data(|log_record| {
            let telemetry_record = TelemetryRecord::LogRecord(log_record.clone());

            // Filter
            if !self.should_include_record(&telemetry_record) {
                return;
            }

            // Ignore error if write fails - we don't want to panic in a layer
            let _ = self.write_record(telemetry_record);
        });
    }
}

/// Handle for shutdown handling
pub struct TelemetryParquetWriterHandle {
    sender: mpsc::Sender<ParquetMessage>,
    writer_thread: Option<JoinHandle<()>>,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_err: Arc<Mutex<Option<io::Error>>>,
}

impl TelemetryShutdown for TelemetryParquetWriterHandle {
    fn shutdown(&mut self) -> FsResult<()> {
        if !self.shutdown_flag.swap(true, Ordering::AcqRel) {
            // Send shutdown message
            let _ = self.sender.send(ParquetMessage::Shutdown);
        }

        // Wait for the writer thread to finish
        if let Some(handle) = self.writer_thread.take() {
            handle.join().map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "Failed to close telemetry parquet writer: {e:?}"
                )
            })?;
        }

        // Check if there was an error during writing
        let err_lock = self.shutdown_err.lock().expect("Mutex poisoned");

        if let Some(e) = err_lock.as_ref() {
            return Err(fs_err!(
                ErrorCode::IoError,
                "Telemetry parquet writer encountered an error: {}. Some telemetry data may have been lost.",
                e
            ));
        }

        Ok(())
    }
}

/// Ensure shutdown is called on drop
impl Drop for TelemetryParquetWriterHandle {
    fn drop(&mut self) {
        // Discard any error, as we can't return it from drop
        let _ = self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Schema;
    use dbt_telemetry::{
        LogEventInfo, LogRecordInfo, RecordCodeLocation, SeverityNumber, SpanEndInfo,
        TelemetryAttributes, TelemetryRecord, UnknownInfo,
        serialize::arrow::deserialize_from_arrow,
    };
    use std::io::{self, Cursor, Write};
    use std::sync::{Arc, Mutex};
    use std::time::SystemTime;
    use tracing_subscriber::{Registry, layer::SubscriberExt};

    /// Mock writer that uses an in-memory buffer
    struct MockWriter {
        buffer: Arc<Mutex<Cursor<Vec<u8>>>>,
        fail_after_bytes: Option<usize>,
        fail_on_close: bool,
    }

    impl MockWriter {
        fn new() -> (Self, Arc<Mutex<Cursor<Vec<u8>>>>) {
            let buffer = Arc::new(Mutex::new(Cursor::new(Vec::new())));
            (
                Self {
                    buffer: buffer.clone(),
                    fail_after_bytes: None,
                    fail_on_close: false,
                },
                buffer,
            )
        }

        fn with_fail_after_bytes(mut self, bytes: usize) -> Self {
            self.fail_after_bytes = Some(bytes);
            self
        }

        #[allow(dead_code)]
        fn with_fail_on_close(mut self) -> Self {
            self.fail_on_close = true;
            self
        }
    }

    impl Write for MockWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut cursor = self.buffer.lock().unwrap();

            // Check if we should fail
            if let Some(fail_after) = self.fail_after_bytes {
                let current_pos = cursor.position() as usize;
                if current_pos + buf.len() > fail_after {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "Mock write error",
                    ));
                }
            }

            cursor.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            if self.fail_on_close {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "Mock flush error",
                ))
            } else {
                Ok(())
            }
        }
    }

    fn get_test_log_record(i: u64) -> TelemetryRecord {
        TelemetryRecord::LogRecord(LogRecordInfo {
            trace_id: 12345,
            span_id: Some(i),
            span_name: Some(format!("test_span_{i}")),
            time_unix_nano: SystemTime::now(),
            body: format!("Test message {i}"),
            severity_number: SeverityNumber::Info,
            severity_text: "INFO".to_string(),
            attributes: TelemetryAttributes::Log(LogEventInfo {
                code: Some(i as u32),
                dbt_core_code: Some(format!("test_code_{i}")),
                original_severity_number: SeverityNumber::Info,
                original_severity_text: "INFO".to_string(),
                location: RecordCodeLocation::none(),
            }),
        })
    }

    fn deserialize_parquet(buffer: &[u8]) -> Vec<TelemetryRecord> {
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};

        let bytes = Bytes::from_owner(buffer.to_vec());
        let (serialisable_schema, schema_with_timestamps) = create_arrow_schema().unwrap();
        let schema_ref = Arc::new(Schema::new(schema_with_timestamps));

        let arrow_reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
            bytes,
            ArrowReaderOptions::new().with_schema(schema_ref),
        )
        .unwrap()
        .build()
        .unwrap();

        let mut records = Vec::new();
        for batch in arrow_reader {
            records.extend(deserialize_from_arrow(&batch.unwrap(), &serialisable_schema).unwrap());
        }

        records
    }

    #[test]
    fn test_normal_write_and_shutdown_idempotency() {
        let (mock, buffer) = MockWriter::new();
        let (layer, mut handle) = TelemetryParquetWriterLayer::new(mock).unwrap();

        // Create some test records
        let record1 = get_test_log_record(1);
        let record2 = get_test_log_record(2);

        // Write records
        assert!(layer.write_record(record1.clone()).is_ok());
        assert!(layer.write_record(record2.clone()).is_ok());

        // Multiple shutdowns should be safe
        assert!(handle.shutdown().is_ok());
        assert!(handle.shutdown().is_ok());

        // Verify data was written (parquet format will have headers/footers)
        let buffer_contents = buffer.lock().unwrap();
        let records = deserialize_parquet(buffer_contents.get_ref());
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], record1);
        assert_eq!(records[1], record2);
    }

    #[test]
    fn test_write_failure_stops_writer() {
        // Create a writer that fails after 1 byte
        let (mock, buffer) = MockWriter::new();
        let mock = mock.with_fail_after_bytes(1);
        let (layer, mut handle) = TelemetryParquetWriterLayer::new(mock).unwrap();

        // Write itself should succeed (the error will occur in the writer thread)
        assert!(layer.write_record(get_test_log_record(1)).is_ok());

        // Shutdown - should return error due to write failure
        let Err(error) = handle.shutdown() else {
            panic!("Expected shutdown to return error due to write failure");
        };

        assert_eq!(error.code, ErrorCode::IoError);
        assert!(
            // Due to internal parquet buffering, our mock writer will only
            // be really hit on finalize, NOT on the initial write
            error.to_string().contains("Failed to close Parquet writer"),
            "{}",
            error.to_string()
        );

        // Verify that no complete parquet file was written (the buffer should be empty or have incomplete data)
        let buffer_contents = buffer.lock().unwrap();
        let buf = buffer_contents.get_ref();

        // If any data was written, it would be incomplete and not parseable as valid parquet
        if !buf.is_empty() {
            // Attempting to deserialize should fail since the parquet file is incomplete
            let result = std::panic::catch_unwind(|| deserialize_parquet(buf));
            assert!(
                result.is_err(),
                "Should not be able to deserialize incomplete parquet data"
            );
        }
    }

    #[test]
    fn test_write_after_shutdown() {
        let (mock, buffer) = MockWriter::new();
        let (layer, mut handle) = TelemetryParquetWriterLayer::new(mock).unwrap();

        let record1 = get_test_log_record(1);
        let record2 = get_test_log_record(2);

        // Write first record before shutdown
        assert!(layer.write_record(record1.clone()).is_ok());

        handle.shutdown().unwrap();

        // Writes after shutdown should return error
        assert!(layer.write_record(record2).is_err());

        // Verify first record was written
        let buffer_contents = buffer.lock().unwrap();
        let records = deserialize_parquet(buffer_contents.get_ref());
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], record1);
    }

    #[test]
    fn test_layer_with_tracing_registry() {
        use crate::tracing::layers::data_layer::TelemetryDataLayer;

        let (mock, buffer) = MockWriter::new();
        let (parquet_layer, mut handle) = TelemetryParquetWriterLayer::new(mock).unwrap();

        // We need the data layer to populate span extensions
        let trace_id = uuid::Uuid::new_v4().as_u128();
        let data_layer = TelemetryDataLayer::new(trace_id, false);

        // Create a Registry-based subscriber with both layers
        let subscriber = Registry::default().with(data_layer).with(parquet_layer);

        tracing::subscriber::with_default(subscriber, || {
            // Create nested spans
            let root_span = tracing::info_span!("root_span");
            let _root_guard = root_span.enter();

            {
                let child_span = tracing::info_span!("child_span");
                let _child_guard = child_span.enter();
                // Child span closes here
            }
            // Root span closes here
        });

        // Shutdown
        handle.shutdown().unwrap();

        // Verify the span records were written to parquet
        let buffer_contents = buffer.lock().unwrap();
        let records = deserialize_parquet(buffer_contents.get_ref());

        assert_eq!(records.len(), 2, "Should have 2 span end records");

        // Check records for correct span names and parent-child relationship
        for record in &records {
            if let TelemetryRecord::SpanEnd(SpanEndInfo {
                trace_id: deserialized_trace_id,
                span_name: span_type,
                parent_span_id: parent_id,
                attributes: TelemetryAttributes::Unknown(UnknownInfo { name, .. }),
                ..
            }) = record
            {
                assert_eq!(deserialized_trace_id, &trace_id);
                assert_eq!(span_type, "Unknown");

                if name == "child_span" {
                    // Child span should have root span as parent
                    assert!(parent_id.is_some());
                } else if name == "root_span" {
                    // Root span should have no parent
                    assert!(parent_id.is_none());
                } else {
                    panic!("Unexpected span name: {name}");
                }
            } else {
                panic!("Unexpected record: {record:?}")
            }
        }
    }
}
