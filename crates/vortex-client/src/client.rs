//! Client for sending messages to Vortex.
//!
//! The behavior of the producer can be controlled by setting environment variables:
//! - `VORTEX_BASE_URL` (default `http://127.0.0.1:8083`): The base URL of the Vortex API.
//! - `VORTEX_INGEST_ENDPOINT` (default `/internal/v1/ingest/protobuf`): The ingest endpoint of
//!   the Vortex API.
//! - `VORTEX_DEV_MODE` (default `false`): If true, the producer will write to a file instead of
//!   sending messages to an API endpoint.
//! - `VORTEX_DEV_MODE_OUTPUT_PATH` (default `/tmp/vortex_dev_mode_output.jsonl`): The path to the
//!   file where the producer will write the messages.  Note that this file will be appended to, not
//!   overwritten, so it can grow quite large!
use std::fs::OpenOptions;
use std::io::{self, BufWriter, Write};

use std::ops::DerefMut;
use std::path::PathBuf;
#[cfg(not(test))]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
use mock_instant::global::{SystemTime, UNIX_EPOCH};

use dbt_env::env::InternalEnv;
use http::HeaderValue;
use pbjson_types::Timestamp;
use prost::Message;
use proto_rust::v1::events::vortex::{VortexMessage, VortexMessageBatch};
use reqwest::StatusCode;
use std::sync::{self, Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::{JoinError, JoinHandle};
use uuid::Uuid;

#[cfg(not(test))]
use log::debug;

#[cfg(test)]
use std::println as debug;

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_MAX_BATCH_SIZE_BYTES: usize = 1024; // 1kb batches
const DEFAULT_QUEUE_CAPACITY: usize = 10000;

// Global singleton instance
static PRODUCER: LazyLock<Arc<VortexProducerClient>> =
    LazyLock::new(|| Arc::new(VortexProducerClient::default()));

/// Main entrypoint for logging messages to Vortex.
#[inline(always)]
pub async fn log_proto<T: Message + prost::Name + serde::Serialize>(
    message: T,
    error_mode: ErrorMode,
) -> Result<()> {
    PRODUCER.log_proto(message, error_mode).await
}

pub async fn shutdown() -> Result<()> {
    PRODUCER.shutdown().await
}

pub type Result<T> = std::result::Result<T, ProducerError>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ErrorMode {
    LogAndContinue,
    LogAndRaise,
}

#[derive(Debug, Clone)]
pub enum ProducerError {
    ValidationError(String),
    SendError(String),
    BadRequestError(String),
    ShutdownError(String),
    UnknownError(String),
}

impl std::fmt::Display for ProducerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProducerError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
            ProducerError::SendError(msg) => write!(f, "Send error: {}", msg),
            ProducerError::BadRequestError(msg) => write!(f, "Bad request error: {}", msg),
            ProducerError::ShutdownError(msg) => write!(f, "Shutdown error: {}", msg),
            ProducerError::UnknownError(msg) => write!(f, "Unknown error: {}", msg),
        }
    }
}

impl std::error::Error for ProducerError {}

impl From<reqwest::Error> for ProducerError {
    fn from(e: reqwest::Error) -> Self {
        ProducerError::SendError(format!("Failed to send message: {}", e))
    }
}

impl From<std::time::SystemTimeError> for ProducerError {
    fn from(e: std::time::SystemTimeError) -> Self {
        ProducerError::UnknownError(format!("Failed to generate a timestamp! {}", e))
    }
}

#[cfg(test)]
impl From<mock_instant::SystemTimeError> for ProducerError {
    fn from(e: mock_instant::SystemTimeError) -> Self {
        ProducerError::UnknownError(format!("Failed to generate a timestamp! {}", e))
    }
}

impl From<JoinError> for ProducerError {
    fn from(e: JoinError) -> Self {
        ProducerError::ShutdownError(format!("Failed to join worker task! {}", e))
    }
}

impl From<io::Error> for ProducerError {
    fn from(e: io::Error) -> Self {
        ProducerError::UnknownError(format!("IO error occurred: {}", e))
    }
}

#[derive(Debug, Clone, serde::Serialize)]
struct VortexDevModeMessage {
    type_url: String,
    message: serde_json::Value,
}

struct VortexProducerClient {
    sender: mpsc::Sender<VortexMessage>,
    shutdown_tx: Option<Mutex<watch::Sender<bool>>>,
    future: Option<Mutex<JoinHandle<()>>>,
    /// Path to the file where messages will be written in dev mode.
    ///
    /// Only set in development mode. MUST be `None` in production.
    dev_mode_output_path: Option<PathBuf>,
    /// Dev-mode output writer, used to write messages to a file in development mode.
    dev_mode_output_writer: sync::Mutex<Result<BufWriter<std::fs::File>>>,
}

impl Default for VortexProducerClient {
    fn default() -> Self {
        let env = InternalEnv::global();
        let vortex_config = env.vortex_config();
        let endpoint = {
            let base_url = vortex_config.base_url.clone();
            let ingest_endpoint = vortex_config.ingest_endpoint.clone();
            let full_url = format!("{}{}", base_url, ingest_endpoint);
            url::Url::parse(&full_url).expect("Failed to parse Vortex endpoint URL")
        };
        let vortex_client_platform = {
            // Construct the X-Vortex-Client-Platform header with service, client, and proto library
            // information. Format:
            //
            //     {service}/{version} {client}/{version} {proto_library}/{version}
            //
            // This helps identify the client platform and its components for monitoring and debugging.
            let service_name = "fusion";
            let service_version = env.invocation_config().dbt_version.clone();
            // TODO: Change this to the actual version of the proto-rust library.
            let proto_version = "unknown";
            let header_value_string = format!(
                "{}/{} {}/{} {}/{}",
                service_name,
                service_version,
                "vortex-client-rust",
                env!("CARGO_PKG_VERSION"),
                "proto-rust",
                proto_version
            );
            HeaderValue::from_str(&header_value_string)
                .expect("Failed to create X-Vortex-Client-Platform header value")
        };
        let dev_mode_output_path = {
            if vortex_config.dev_mode == "true" {
                let path = PathBuf::from(&vortex_config.dev_mode_output_path);
                Some(path)
            } else {
                None
            }
        };
        Self::new(
            DEFAULT_QUEUE_CAPACITY,
            DEFAULT_MAX_BATCH_SIZE_BYTES,
            DEFAULT_FLUSH_INTERVAL,
            endpoint,
            vortex_client_platform,
            dev_mode_output_path,
        )
    }
}

impl VortexProducerClient {
    fn new(
        queue_capacity: usize,
        max_batch_size: usize,
        flush_interval: Duration,
        endpoint: url::Url,
        vortex_client_platform: HeaderValue,
        dev_mode_output_path: Option<PathBuf>,
    ) -> Self {
        let dev_mode_output_writer = if let Some(path) = &dev_mode_output_path {
            match OpenOptions::new().append(true).create(true).open(path) {
                Ok(file) => {
                    let writer = BufWriter::new(file);
                    sync::Mutex::new(Ok(writer))
                }
                Err(e) => sync::Mutex::new(Err(e.into())),
            }
        } else {
            let err = ProducerError::UnknownError(
                "Trying to write JSON, but client is not in dev-mode.".to_string(),
            );
            sync::Mutex::new(Err(err))
        };

        let (sender, mut receiver) = mpsc::channel(queue_capacity);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let mut client = Self {
            sender,
            shutdown_tx: Some(Mutex::new(shutdown_tx)),
            future: None,
            dev_mode_output_path,
            dev_mode_output_writer,
        };

        // Spawn worker task
        client.future = Some(Mutex::new(tokio::spawn(async move {
            let mut vortex_messages: Vec<VortexMessage> = Vec::new();
            let mut bytes_buffered = 0;

            loop {
                let should_flush = tokio::select! {
                    _ = tokio::time::sleep(flush_interval) => {
                        true
                    },
                    _ = shutdown_rx.changed() => {
                        break;
                    },
                    msg = receiver.recv() => {
                        match msg {
                            Some(msg) => {
                                vortex_messages.push(msg);
                                bytes_buffered >= max_batch_size
                            }
                            None => {
                                debug!("Channel has been closed, exiting worker!");
                                break;
                            }
                        }
                    }
                };

                if should_flush && !vortex_messages.is_empty() {
                    debug!("Flushing {} telemetry messages", vortex_messages.len());
                    match Self::send_batch(
                        endpoint.clone(),
                        vortex_client_platform.clone(),
                        &mut vortex_messages,
                    )
                    .await
                    {
                        Ok(_) => {
                            vortex_messages.clear();
                            bytes_buffered = 0;
                        }
                        Err(ProducerError::BadRequestError(e)) => {
                            debug!("Failed to send telemetry batch: {}", e);
                            vortex_messages.clear();
                            bytes_buffered = 0;
                        }
                        Err(e) => {
                            // Skip clearing the messages so they can be included in the next batch.
                            debug!("Failed to send telemetry batch: {}", e);
                        }
                    }
                }
            }

            receiver.close();
        })));

        client
    }

    pub fn is_in_dev_mode(&self) -> bool {
        self.dev_mode_output_path.is_some()
    }

    async fn send_batch(
        endpoint: url::Url,
        vortex_client_platform: HeaderValue,
        messages: &mut Vec<VortexMessage>,
    ) -> Result<()> {
        // Override the client sent timestamp for each message that goes into the batch.
        let now = Self::current_timestamp();
        messages.iter_mut().for_each(|msg| {
            msg.vortex_client_sent_at = Some(now);
        });

        let body = {
            let mut batch = VortexMessageBatch {
                request_id: Uuid::new_v4().to_string(),
                payload: Vec::new(),
            };
            // Consume these messages into a batch's payload and then swap them back to
            // the original messages vector to enable retries from the caller if needed.
            std::mem::swap(messages, &mut batch.payload);
            let body = batch.encode_to_vec();
            std::mem::swap(messages, &mut batch.payload);
            body
        };

        let client = reqwest::Client::new();
        let response = client
            .post(endpoint)
            .header("Content-Type", "application/vnd.google.protobuf")
            .header("X-Vortex-Client-Platform", vortex_client_platform)
            .body(body)
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;

        if status.is_success() {
            debug!("Successfully sent telemetry batch.");
            Ok(())
        } else {
            debug!("Failed to send message: {} {}", status, text);
            let err = if status == StatusCode::BAD_REQUEST {
                let message = format!("Failed to send message: {} {}", status, text);
                ProducerError::BadRequestError(message)
            } else {
                ProducerError::SendError(format!("Failed to send message: {} {}", status, text))
            };
            Err(err)
        }
    }

    fn current_timestamp() -> Timestamp {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        Timestamp {
            seconds: now.as_secs() as i64,
            nanos: now.subsec_nanos() as i32,
        }
    }

    pub async fn log_proto<T: Message + prost::Name + serde::Serialize>(
        &self,
        message: T,
        error_mode: ErrorMode,
    ) -> Result<()> {
        if self.is_in_dev_mode() {
            // This code should never run in production, so using unwrap_or_default to avoid
            // panics.
            let json_value = serde_json::to_value(&message).unwrap_or_default();
            self.do_log_proto_in_dev(T::PACKAGE, T::NAME, json_value, error_mode)
                .unwrap_or_default();
            Ok(())
        } else {
            self.do_log_proto_in_prod(T::PACKAGE, T::NAME, message.encode_to_vec(), error_mode)
                .await
        }
    }

    /// Logs a protobuf message to Vortex in development mode.
    ///
    /// This private function is type-erased so it does not have to be specialized
    /// for different message types, thus saving on binary size and compilation time
    /// for every `log_proto` callsite.
    #[inline(never)]
    fn do_log_proto_in_dev(
        &self,
        package: &'static str,
        name: &'static str,
        json_message: serde_json::Value,
        error_mode: ErrorMode,
    ) -> Result<()> {
        // Dummy up sending error
        if self.sender.is_closed() {
            match error_mode {
                ErrorMode::LogAndContinue => {
                    return Ok(());
                }
                ErrorMode::LogAndRaise => {
                    return Err(ProducerError::SendError(
                        "Failed to send message".to_string(),
                    ));
                }
            }
        }

        let dev_mode_msg = VortexDevModeMessage {
            type_url: format!("/{}.{}", package, name),
            message: json_message,
        };
        let json_payload = serde_json::to_string(&dev_mode_msg).unwrap_or_default();

        let mut writer_res_lock_guard = self.dev_mode_output_writer.lock().unwrap();
        match writer_res_lock_guard.deref_mut() {
            Ok(writer) => {
                writer.write_all(json_payload.as_bytes())?;
                writeln!(writer)?; // carriage return after
                writer.flush()?;
                Ok(())
            }
            Err(e) => Err(e.clone()),
        }
    }

    /// Logs a protobuf message to Vortex in production mode.
    ///
    /// This private function is type-erased so it does not have to be specialized
    /// for different message types, thus saving on binary size and compilation time
    /// for every `log_proto` callsite.
    #[inline(never)]
    async fn do_log_proto_in_prod(
        &self,
        package: &'static str,
        name: &'static str,
        serialized_value: Vec<u8>,
        error_mode: ErrorMode,
    ) -> Result<()> {
        let any = pbjson_types::Any {
            type_url: format!("/{}.{}", package, name),
            value: serialized_value.into(),
        };
        let vortex_msg = VortexMessage {
            any: Some(any),
            vortex_event_created_at: Some(Self::current_timestamp()),
            vortex_client_sent_at: None,
            vortex_backend_received_at: None,
            vortex_backend_processed_at: None,
        };

        match self.sender.send(vortex_msg).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let err = ProducerError::SendError(format!("Failed to send message: {}", e));
                match error_mode {
                    ErrorMode::LogAndContinue => {
                        debug!("{}", err);
                        Ok(())
                    }
                    ErrorMode::LogAndRaise => Err(err),
                }
            }
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        const MAX_SHUTDOWN_DELAY_MS: u64 = 100;

        if let Some(shutdown_tx_mutex) = &self.shutdown_tx {
            let shutdown_tx = shutdown_tx_mutex.lock().await;
            if !shutdown_tx.is_closed() {
                shutdown_tx.send(true).map_err(|_| {
                    ProducerError::ShutdownError("Failed to send shutdown signal!".to_string())
                })?;
            }
        }

        if let Some(future_mutex) = &self.future {
            let future = future_mutex.lock().await;

            let shutdown_start = Instant::now();

            loop {
                if future.is_finished() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
                if shutdown_start.elapsed() > Duration::from_millis(MAX_SHUTDOWN_DELAY_MS) {
                    return Err(ProducerError::ShutdownError(
                        "Failed to shut down background thread in time, moving on!".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    // Create a test message
    #[derive(prost::Message, Clone, serde::Serialize)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        field: String,
    }

    impl prost::Name for TestMessage {
        const NAME: &'static str = "TestMessage";
        const PACKAGE: &'static str = "test";
    }

    #[tokio::test]
    async fn test_log_proto_non_blocking_on_unreachable_server() {
        // Create a client with an unreachable URL
        let client = VortexProducerClient::default();

        let message = TestMessage {
            field: "test".to_string(),
        };

        let result = timeout(
            Duration::from_millis(100),
            client.log_proto(message, ErrorMode::LogAndContinue),
        )
        .await;

        // Should complete before timeout fires
        assert!(result.is_ok(), "log_proto should not block");

        // Cleanup
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_log_proto_non_blocking_after_shutdown() {
        // Create a client
        let client = VortexProducerClient::default();

        let message = TestMessage {
            field: "test".to_string(),
        };

        client.shutdown().await.unwrap();

        // Attempt to log one more message with a timeout
        let result = timeout(
            Duration::from_millis(100),
            client.log_proto(message, ErrorMode::LogAndContinue),
        )
        .await;

        // Should complete within timeout (non-blocking)
        assert!(result.is_ok(), "log_proto should not block");

        // Cleanup
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_log_proto_with_error_mode() {
        let client = VortexProducerClient::default();

        let message = TestMessage {
            field: "test".to_string(),
        };

        tokio::time::sleep(Duration::from_secs(0)).await;

        client.shutdown().await.unwrap();

        let result = client
            .log_proto(message.clone(), ErrorMode::LogAndContinue)
            .await;
        assert!(result.is_ok());

        // This can't actually be tested now, because VORTEX_DEV_MODE is on
        // and sending messages won't fail.
        let _result = client.log_proto(message, ErrorMode::LogAndRaise).await;

        // Cleanup
        client.shutdown().await.unwrap();
    }
}
