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
use std::io::{BufWriter, Write};

#[cfg(not(test))]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
use mock_instant::global::{SystemTime, UNIX_EPOCH};

use dbt_env::env::InternalEnv;
use pbjson_types::Timestamp;
use prost::Message;
use proto_rust::v1::events::vortex::{VortexMessage, VortexMessageBatch};
use reqwest::StatusCode;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::sleep;
use uuid::Uuid;

#[cfg(not(test))]
use log::debug;

#[cfg(test)]
use std::println as debug;

const FLUSH_INTERVAL_MS: u64 = 500; // 500ms
const MAX_BATCH_SIZE_BYTES: usize = 1024; // 1kb batches
const MAX_QUEUE_SIZE: usize = 10000;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ErrorMode {
    LogAndContinue,
    LogAndRaise,
}

#[derive(Debug)]
pub enum ProducerError {
    ValidationError(String),
    SendError(String),
    BadRequestError(String),
    ShutdownError(String),
    UnknownError(String),
}

pub type Result<T> = std::result::Result<T, ProducerError>;

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

type MessageSender = mpsc::Sender<VortexMessage>;

struct VortexProducerClient {
    sender: MessageSender,
    shutdown_tx: Option<Mutex<watch::Sender<bool>>>,
    future: Option<Mutex<JoinHandle<()>>>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct VortexDevModeMessage {
    type_url: String,
    message: serde_json::Value,
}

impl VortexProducerClient {
    pub fn new() -> Self {
        let (sender, mut receiver) = mpsc::channel(MAX_QUEUE_SIZE);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let mut client = Self {
            sender,
            shutdown_tx: Some(Mutex::new(shutdown_tx)),
            future: None,
        };

        // Spawn worker task
        client.future = Some(Mutex::new(tokio::spawn(async move {
            let mut vortex_messages: Vec<VortexMessage> = Vec::new();
            let mut bytes_buffered = 0;

            loop {
                let timeout = Duration::from_millis(FLUSH_INTERVAL_MS);
                let should_flush = tokio::select! {
                    _ = sleep(timeout) => {
                        true
                    },
                    _ = shutdown_rx.changed() => {
                        break;
                    },
                    msg = receiver.recv() => {
                        match msg {
                            Some(msg) => {
                                vortex_messages.push(msg);
                                bytes_buffered >= MAX_BATCH_SIZE_BYTES
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
                    match Self::send_batch(&vortex_messages).await {
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
                            debug!("Failed to send telemetry batch: {}", e);
                        }
                    }
                }
            }

            receiver.close();
        })));

        client
    }

    async fn send_batch(messages: &[VortexMessage]) -> Result<()> {
        let env = InternalEnv::global();

        // Set the client sent timestamp for each message in the batch
        let now = Self::get_timestamp()?;
        let mut messages_rewritten: Vec<VortexMessage> = Vec::new();

        for msg in messages {
            let mut msg_clone = msg.clone();
            msg_clone.vortex_client_sent_at = Some(now);
            messages_rewritten.push(msg_clone);
        }

        let batch = VortexMessageBatch {
            request_id: Uuid::new_v4().to_string(),
            payload: messages_rewritten.to_vec(),
        };

        let base_url = env.vortex_config().base_url.clone();
        let ingest_endpoint = env.vortex_config().ingest_endpoint.clone();
        let service_name = "fusion";
        let service_version = env.invocation_config().dbt_version.clone();
        // TODO: Change this to the actual version of the proto-rust library.
        let proto_version = "unknown";

        let endpoint = format!("{}{}", base_url, ingest_endpoint);
        let client = reqwest::Client::new();
        let response = client
            .post(endpoint)
            .header("Content-Type", "application/vnd.google.protobuf")
            // Construct the X-Vortex-Client-Platform header with service, client, and proto library
            // information Format: {service}/{version} {client}/{version}
            // {proto_library}/{version} This helps identify the client platform and its
            // components for monitoring and debugging
            .header(
                "X-Vortex-Client-Platform",
                format!(
                    "{}/{} {}/{} {}/{}",
                    service_name,
                    service_version,
                    "vortex-client-rust",
                    env!("CARGO_PKG_VERSION"),
                    "proto-rust",
                    proto_version
                ),
            )
            .body(batch.encode_to_vec())
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;

        if !status.is_success() {
            debug!("Failed to send message: {} {}", status, text);
            if status == StatusCode::BAD_REQUEST {
                return Err(ProducerError::BadRequestError(format!(
                    "Failed to send message: {} {}",
                    status, text
                )));
            }
            return Err(ProducerError::SendError(format!(
                "Failed to send message: {} {}",
                status, text
            )));
        } else {
            debug!("Successfully sent telemetry batch.");
        }

        Ok(())
    }

    fn get_timestamp() -> Result<Timestamp> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let seconds = now.as_secs();
        let nanos = now.subsec_nanos();

        Ok(Timestamp {
            seconds: seconds as i64,
            nanos: nanos as i32,
        })
    }

    pub async fn log_proto<T: Message + prost::Name + serde::Serialize>(
        &self,
        message: T,
        error_mode: ErrorMode,
    ) -> Result<()> {
        let env = InternalEnv::global();

        let any = pbjson_types::Any {
            type_url: format!("/{}", T::full_name()),
            value: message.encode_to_vec().into(),
        };

        if env.vortex_config().dev_mode == "true" {
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

            let path = env.vortex_config().dev_mode_output_path.clone();
            let maybe_file = OpenOptions::new().append(true).open(path);
            let file = match maybe_file {
                Ok(file) => file,
                Err(_) => {
                    return Ok(());
                }
            };

            let dev_mode_msg = VortexDevModeMessage {
                type_url: any.type_url,
                message: serde_json::to_value(&message).unwrap_or_default(),
            };

            // This code should never run in production, so using unwrap_or_default
            // to avoid panics.
            let mut writer = BufWriter::new(file);
            writer
                .write_all(
                    serde_json::to_string(&dev_mode_msg)
                        .unwrap_or_default()
                        .as_bytes(),
                )
                .unwrap_or_default();
            writeln!(writer).unwrap_or_default(); // carriage return after

            Ok(())
        } else {
            let vortex_msg = VortexMessage {
                any: Some(any),
                vortex_event_created_at: Some(Self::get_timestamp()?),
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

impl Default for VortexProducerClient {
    fn default() -> Self {
        Self::new()
    }
}

// Global singleton instance
lazy_static::lazy_static! {
    static ref PRODUCER: Arc<VortexProducerClient> = Arc::new(VortexProducerClient::new());
}

/// Main entrypoint for logging messages to Vortex.
pub async fn log_proto<T: Message + prost::Name + serde::Serialize>(
    message: T,
    error_mode: ErrorMode,
) -> Result<()> {
    PRODUCER.log_proto(message, error_mode).await
}

pub async fn shutdown() -> Result<()> {
    PRODUCER.shutdown().await
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
        let client = VortexProducerClient::new();

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
        let client = VortexProducerClient::new();

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
        let client = VortexProducerClient::new();

        let message = TestMessage {
            field: "test".to_string(),
        };

        sleep(Duration::from_secs(0)).await;

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
