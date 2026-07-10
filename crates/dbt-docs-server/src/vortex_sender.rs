//! Vortex telemetry sender for dbt-docs-server.
//!
//! Adapted from `crates/dbt-index/src/vortex_sender.rs` (itself adapted from
//! `fs/sa/crates/vortex-client/src/client.rs`) with all internal dependencies
//! (dbt-env, proto-rust, pbjson-types, dbt-adbc) removed. Self-contained: the
//! only deps are existing workspace crates (prost, ureq, http, uuid, rand).
//!
//! Environment variables:
//! - `VORTEX_BASE_URL` (default `https://p.vx.dbt.com`)
//! - `VORTEX_INGEST_ENDPOINT` (default `/v1/ingest/protobuf`)
//! - `VORTEX_DEV_MODE` (if `true`, writes to a file instead of the API)
//! - `VORTEX_DEV_MODE_OUTPUT_PATH` (default `/tmp/vortex_dev_mode_output.jsonl`)

use std::fs;
use std::io::{self, Write as _};
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use http::HeaderValue;
use prost::Message;

#[cfg(test)]
macro_rules! trace {
    ($($arg:tt)*) => { println!($($arg)*) };
}
#[cfg(not(test))]
macro_rules! trace {
    ($($arg:tt)*) => {};
}

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_TARGET_BATCH_SIZE_BYTES: usize = 1024; // 1kb batches
const DEFAULT_BODY_SIZE_LIMIT_BYTES: usize = 4 * 1024 * 1024; // 4mb body size limit
const MAX_ENCODED_MESSAGE_SIZE_BYTES: usize = 2 * 1024 * 1024; // 2mb
const MIN_BACKOFF_MILLIS: u64 = 200;
const MAX_BACKOFF_MILLIS: u64 = 30_000;
const TIMEOUT_PER_CALL: Duration = Duration::from_secs(60);

#[cfg(debug_assertions)]
const LOG_PROTO_SHUTDOWN_MESSAGE: &str = "You're trying to log a message via \
Vortex, but the client is already shut down. This should be fixed, but on release \
builds the message will simply be dropped.";

/// Create a [`ureq::Agent`] that does **not** treat HTTP error status codes as
/// Rust errors. TLS uses ureq's platform-verifier default (OS cert store); no
/// dependency on the proprietary `dbt-adbc` TLS config, keeping this crate
/// source-available clean.
fn agent_no_http_error() -> ureq::Agent {
    let config = ureq::config::Config::builder()
        .http_status_as_error(false)
        .timeout_per_call(Some(TIMEOUT_PER_CALL))
        .build();
    ureq::Agent::new_with_config(config)
}

// ---------------------------------------------------------------------------
// Wire-type proto structs (hand-rolled, no proto-rust or pbjson-types dep)
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Timestamp {
    #[prost(int64, tag = "1")]
    pub(crate) seconds: i64,
    #[prost(int32, tag = "2")]
    pub(crate) nanos: i32,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Any {
    #[prost(string, tag = "1")]
    pub(crate) type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    pub(crate) value: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct VortexMessage {
    #[prost(message, optional, tag = "1")]
    pub(crate) any: Option<Any>,
    #[prost(message, optional, tag = "2")]
    pub(crate) vortex_event_created_at: Option<Timestamp>,
    #[prost(message, optional, tag = "3")]
    pub(crate) vortex_client_sent_at: Option<Timestamp>,
    #[prost(message, optional, tag = "4")]
    pub(crate) vortex_backend_received_at: Option<Timestamp>,
    #[prost(message, optional, tag = "5")]
    pub(crate) vortex_backend_processed_at: Option<Timestamp>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct VortexMessageBatch {
    #[prost(string, tag = "1")]
    pub(crate) request_id: String,
    #[prost(message, repeated, tag = "2")]
    pub(crate) payload: Vec<VortexMessage>,
}

// ---------------------------------------------------------------------------
// Statics
// ---------------------------------------------------------------------------

static WORKER_THREAD: Mutex<Option<JoinHandle<Result<(), ureq::Error>>>> = Mutex::new(None);

static PRODUCER: LazyLock<VortexProducerClient> = LazyLock::new(|| {
    let mut client = VortexProducerClient::default();
    let handle = client.take_thread_handle();
    debug_assert!(
        client.is_in_dev_mode() || handle.is_some(),
        "Worker thread must be spawned by VortexProducerClient::new()"
    );
    let mut lock = WORKER_THREAD.lock().unwrap();
    *lock = handle;
    client
});

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub enum ProducerError {
    /// The client is in dev mode and cannot write messages to the log file.
    DevModeError(io::Error),
    /// Communication error with the Vortex HTTP endpoint.
    SendError(ureq::Error),
    /// Failed to join the worker thread during shutdown.
    ShutdownError(Box<dyn std::any::Any + Send + 'static>),
}

impl std::fmt::Display for ProducerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProducerError::DevModeError(e) => write!(f, "Vortex: dev mode error: {e}"),
            ProducerError::SendError(e) => write!(f, "Vortex: send error: {e}"),
            ProducerError::ShutdownError(e) => write!(f, "Vortex: shutdown error: {e:?}"),
        }
    }
}

impl std::error::Error for ProducerError {}

/// Main entrypoint for logging messages to Vortex.
///
/// Caller should ignore the return error. This function is non-blocking in production
/// and only returns an error when the client is in dev-mode logging to a file.
#[inline(always)]
pub fn log_proto<T: Message + prost::Name + serde::Serialize>(
    message: T,
) -> Result<(), ProducerError> {
    PRODUCER
        .log_proto(message, false) // can only fail in dev mode
        .map_err(ProducerError::DevModeError)
}

/// Logs the last message to Vortex and shuts down the client.
#[allow(dead_code)]
pub fn log_proto_and_shutdown<T: Message + prost::Name + serde::Serialize>(
    shutdown_message: T,
) -> Result<(), ProducerError> {
    PRODUCER
        .log_proto(shutdown_message, true) // can only fail in dev mode
        .map_err(ProducerError::DevModeError)?;
    // Wait for the worker thread to finish processing messages. Since a
    // shutdown message has been sent, it will NOT block indefinitely.
    let handle = {
        let mut lock = WORKER_THREAD.lock().unwrap();
        lock.take()
    };
    match handle {
        Some(h) => match h.join() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(ProducerError::SendError(e)),
            Err(e) => Err(ProducerError::ShutdownError(e)),
        },
        None => Ok(()), // dev-mode or already shut down
    }
}

#[allow(dead_code)]
pub fn vortex_producer_is_running() -> bool {
    let lock = WORKER_THREAD.lock().unwrap();
    lock.is_some()
}

// ---------------------------------------------------------------------------
// Batch
// ---------------------------------------------------------------------------

struct Batch {
    // configuration and the RNG
    target_batch_size: usize,
    body_size_limit: usize,
    flush_interval: Duration,
    rng: rand::rngs::ThreadRng,
    // mutable state
    messages: Vec<VortexMessage>,
    bytes_buffered: usize,
    last_error: Option<ureq::Error>,
    deadline: Option<Instant>,
    num_attempts: usize,
}

impl Batch {
    fn new(target_batch_size: usize, body_size_limit: usize, flush_interval: Duration) -> Self {
        debug_assert!(
            target_batch_size > 0,
            "target_batch_size must be greater than 0"
        );
        debug_assert!(
            body_size_limit >= 2 * target_batch_size,
            "body_size_limit must be much larger than target_batch_size"
        );
        Self {
            target_batch_size,
            body_size_limit,
            flush_interval,
            rng: rand::rng(),
            messages: Vec::new(),
            bytes_buffered: 0,
            last_error: None,
            deadline: None, // INVARIANT: deadline.is_none() implies is_empty()
            num_attempts: 0,
        }
    }

    fn for_agent(agent: &dyn SenderAgent) -> Self {
        Self::new(
            agent.target_batch_size(),
            agent.body_size_limit(),
            agent.flush_interval(),
        )
    }

    fn push(&mut self, message: Box<VortexMessage>) {
        let encoded_len = message.encoded_len();
        if encoded_len > self.body_size_limit.min(MAX_ENCODED_MESSAGE_SIZE_BYTES) {
            debug_assert!(
                false,
                "{} message is too large ({} bytes).",
                message
                    .any
                    .as_ref()
                    .map_or_else(|| "unknown", |a| a.type_url.as_str()),
                encoded_len
            );
            return; // silently drop the large message in release builds
        }
        if self.deadline.is_none() {
            debug_assert!(
                self.messages.is_empty(),
                "If deadline is None, messages must be empty"
            );
            // Set a deadline when the first message is added to the batch.
            self.deadline = Some(Instant::now() + self.flush_interval);
        }
        self.bytes_buffered += encoded_len;
        self.messages.push(*message);
    }

    fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    fn is_full(&self) -> bool {
        self.bytes_buffered >= self.target_batch_size
    }

    fn is_overdue(&self) -> bool {
        let overdue = self
            .deadline
            .map(|deadline| Instant::now() >= deadline)
            .unwrap_or(false);
        overdue || {
            // Even if the batch is not overdue for sending, we can still
            // send if it's full and we are not retrying with backoff.
            self.is_full() && self.num_attempts == 0
        }
    }

    fn clear(&mut self) -> Result<(), ureq::Error> {
        self.messages.clear();
        self.bytes_buffered = 0;
        let res = match self.last_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        };
        self.deadline = None;
        self.num_attempts = 0;
        res
    }

    fn clear_after_success(&mut self) {
        let _ = self.clear();
    }

    /// Adjust the deadline for the next retry with exponential backoff.
    fn backoff(&mut self) {
        use rand::Rng as _;
        // backoff() is called after a failed send operation, so
        // we should have at least one message in the batch.
        debug_assert!(!self.is_empty());
        // Exponential backoff with jitter: MIN_BACKOFF_MILLIS * 2**num_attempts + jitter.
        let backoff_millis = MIN_BACKOFF_MILLIS
            .saturating_mul(1u64 << self.num_attempts.min(63))
            .saturating_add(self.rng.random_range(0..=MIN_BACKOFF_MILLIS))
            .min(MAX_BACKOFF_MILLIS);
        let backoff = Duration::from_millis(backoff_millis);
        self.deadline = Some(Instant::now() + backoff);
        self.num_attempts += 1;
    }

    fn prune(&mut self) {
        debug_assert!(
            self.messages.len() >= 2,
            "prune: batch must have at least 2 messages to prune."
        );
        let mut i = 0;
        let mut new_bytes_buffered = self.bytes_buffered;
        while i < self.messages.len() {
            new_bytes_buffered -= self.messages[i].encoded_len();
            if new_bytes_buffered < self.body_size_limit {
                self.messages.drain(0..=i);
                self.bytes_buffered = new_bytes_buffered;
                break;
            } else {
                i += 1;
            }
        }
        // This is a consequence of not letting messages larger than the
        // body_size_limit into the batch in the first place.
        debug_assert!(!self.is_empty(), "batch must not be empty after pruning.");
    }

    fn encode_for_sending(&mut self) -> Vec<u8> {
        debug_assert!(!self.is_empty(), "send_batch: batch must be non-empty.");
        if self.bytes_buffered > self.body_size_limit {
            self.prune();
        }
        // Override the client sent timestamp for each message that goes into the batch.
        let now = VortexProducerClient::current_timestamp();
        self.messages.iter_mut().for_each(|msg| {
            msg.vortex_client_sent_at = Some(now.clone());
        });

        let mut message_batch = VortexMessageBatch {
            request_id: uuid::Uuid::new_v4().to_string(),
            payload: Vec::new(),
        };
        // Consume these messages into a batch's payload and then swap them back to
        // the original messages vector to enable retries from the caller if needed.
        std::mem::swap(&mut self.messages, &mut message_batch.payload);
        let body = message_batch.encode_to_vec();
        std::mem::swap(&mut self.messages, &mut message_batch.payload);
        body
    }

    fn on_error(&mut self, error: ureq::Error) {
        #[allow(clippy::single_match)]
        match error {
            ureq::Error::StatusCode(_) => {
                // Status codes should be handled in `on_response`
                // because `http_status_as_error` is not enabled.
                unreachable!("`http_status_as_error` is not enabled.")
            }
            _ => (),
        }
        self.last_error = Some(error);
        self.backoff();
    }

    #[allow(unused_variables)]
    fn on_response(&mut self, status: http::StatusCode, text: String) {
        if status.is_success() {
            self.clear_after_success();
            trace!("Successfully sent telemetry batch.");
        } else {
            self.backoff();
            trace!("Failed to send batch of messages: {status}: {text}");
            self.last_error = Some(ureq::Error::StatusCode(status.as_u16()));
        }
    }
}

// ---------------------------------------------------------------------------
// SenderAgent trait
// ---------------------------------------------------------------------------

/// Abstract interface for the HTTP agent that sends batches of messages to the Vortex endpoint.
trait SenderAgent: Send {
    fn target_batch_size(&self) -> usize;
    fn body_size_limit(&self) -> usize;
    fn flush_interval(&self) -> Duration;
    /// Send a batch of messages to the Vortex endpoint.
    ///
    /// PRECONDITION: !batch.is_empty()
    fn send_batch(&self, batch: &mut Batch) -> bool;
}

struct BatchSenderAgentImpl {
    agent: ureq::Agent,
    endpoint: http::Uri,
    vortex_client_platform: HeaderValue,
}

impl BatchSenderAgentImpl {
    pub fn new(endpoint: http::Uri, vortex_client_platform: HeaderValue) -> Self {
        let agent = agent_no_http_error();
        Self {
            agent,
            endpoint,
            vortex_client_platform,
        }
    }
}

impl SenderAgent for BatchSenderAgentImpl {
    fn target_batch_size(&self) -> usize {
        DEFAULT_TARGET_BATCH_SIZE_BYTES
    }

    fn body_size_limit(&self) -> usize {
        DEFAULT_BODY_SIZE_LIMIT_BYTES
    }

    fn flush_interval(&self) -> Duration {
        DEFAULT_FLUSH_INTERVAL
    }

    fn send_batch(&self, batch: &mut Batch) -> bool {
        let body = batch.encode_for_sending();
        let result = self
            .agent
            .post(self.endpoint.clone())
            .header("Content-Type", "application/vnd.google.protobuf")
            .header(
                "X-Vortex-Client-Platform",
                self.vortex_client_platform.clone(),
            )
            .send(body);

        match result {
            Ok(response) => {
                let status = response.status();
                let text = response.into_body().read_to_string().unwrap_or_default();
                batch.on_response(status, text);
                true
            }
            Err(e) => {
                batch.on_error(e);
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// VortexProducerClient
// ---------------------------------------------------------------------------

struct VortexProducerClient {
    sender: mpsc::Sender<(Box<VortexMessage>, bool)>,
    thread_handle: Option<JoinHandle<Result<(), ureq::Error>>>,
    /// Path to the file where messages will be written in dev mode.
    ///
    /// Only set in development mode. MUST be `None` in production.
    dev_mode_output_path: Option<PathBuf>,
    /// Dev-mode output writer, used to write messages to a file in development mode.
    dev_mode_output_writer: Mutex<Result<io::BufWriter<fs::File>, io::Error>>,
}

impl Default for VortexProducerClient {
    fn default() -> Self {
        let base_url =
            std::env::var("VORTEX_BASE_URL").unwrap_or_else(|_| "https://p.vx.dbt.com".to_string());
        let ingest_endpoint = std::env::var("VORTEX_INGEST_ENDPOINT")
            .unwrap_or_else(|_| "/v1/ingest/protobuf".to_string());
        let dev_mode =
            std::env::var("VORTEX_DEV_MODE").is_ok_and(|v| v.eq_ignore_ascii_case("true"));
        let dev_mode_output_path_str = std::env::var("VORTEX_DEV_MODE_OUTPUT_PATH")
            .unwrap_or_else(|_| "/tmp/vortex_dev_mode_output.jsonl".to_string());

        let endpoint = {
            let full_url = format!("{base_url}{ingest_endpoint}");
            full_url
                .parse::<http::Uri>()
                .expect("Failed to parse Vortex endpoint URL")
        };
        let vortex_client_platform = {
            // Construct the X-Vortex-Client-Platform header with service, client, and proto library
            // information. Format:
            //
            //     {service}/{version} {client}/{version} {proto_library}/{version}
            //
            // This helps identify the client platform and its components for monitoring and debugging.
            let service_name = "dbt-docs-server";
            let service_version = env!("CARGO_PKG_VERSION");
            let proto_version = "unknown";
            #[allow(clippy::uninlined_format_args)]
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
        let dev_mode_output_path = if dev_mode {
            Some(PathBuf::from(&dev_mode_output_path_str))
        } else {
            None
        };
        let agent = BatchSenderAgentImpl::new(endpoint, vortex_client_platform);
        let agent: Box<dyn SenderAgent> = Box::new(agent);
        Self::new(agent, dev_mode_output_path)
    }
}

impl VortexProducerClient {
    fn new(agent: Box<dyn SenderAgent>, dev_mode_output_path: Option<PathBuf>) -> Self {
        let dev_mode_output_writer = if let Some(path) = &dev_mode_output_path {
            match fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
            {
                Ok(file) => {
                    let writer = io::BufWriter::new(file);
                    Mutex::new(Ok(writer))
                }
                Err(e) => Mutex::new(Err(e)),
            }
        } else {
            let e = io::Error::other(
                "Trying to write JSON, but client is not in dev-mode.".to_string(),
            );
            Mutex::new(Err(e))
        };

        let (sender, receiver) = mpsc::channel();

        let mut client = Self {
            sender,
            thread_handle: None,
            dev_mode_output_path,
            dev_mode_output_writer,
        };
        client.thread_handle = if client.is_in_dev_mode() {
            None
        } else {
            Some(thread::spawn(move || worker_thread_loop(agent, receiver)))
        };
        client
    }

    fn take_thread_handle(&mut self) -> Option<JoinHandle<Result<(), ureq::Error>>> {
        debug_assert!(
            self.is_in_dev_mode() || self.thread_handle.is_some(),
            "take_thread_handle() must be called only once."
        );
        self.thread_handle.take()
    }

    pub fn is_in_dev_mode(&self) -> bool {
        self.dev_mode_output_path.is_some()
    }

    #[cfg(not(test))]
    fn current_timestamp() -> Timestamp {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        Timestamp {
            seconds: now.as_secs() as i64,
            nanos: now.subsec_nanos() as i32,
        }
    }

    #[cfg(test)]
    fn current_timestamp() -> Timestamp {
        Timestamp {
            seconds: 0,
            nanos: 0,
        }
    }

    fn log_proto<T: Message + prost::Name + serde::Serialize>(
        &self,
        message: T,
        is_shutdown: bool,
    ) -> Result<(), io::Error> {
        if self.is_in_dev_mode() {
            // This code should never run in prod, so using unwrap_or_default to avoid panics.
            let json_value = serde_json::to_value(&message).unwrap_or_default();
            self.do_log_proto_in_dev(T::PACKAGE, T::NAME, json_value)
        } else {
            self.do_log_proto_in_prod(T::PACKAGE, T::NAME, message.encode_to_vec(), is_shutdown);
            Ok(())
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
    ) -> Result<(), io::Error> {
        /// A message that will be written to the dev-mode output file as JSON.
        #[derive(Debug, Clone, serde::Serialize)]
        struct VortexDevModeMessage {
            type_url: String,
            message: serde_json::Value,
        }
        let dev_mode_msg = VortexDevModeMessage {
            type_url: format!("/{package}.{name}"),
            message: json_message,
        };
        let json_payload = serde_json::to_string(&dev_mode_msg).unwrap_or_default();

        let mut writer_res_lock_guard = self.dev_mode_output_writer.lock().unwrap();
        match writer_res_lock_guard.deref_mut() {
            Ok(writer) => {
                writer.write_all(json_payload.as_bytes())?;
                writeln!(writer)?; // carriage return after
                writer.flush()
            }
            Err(e) => {
                // we can't clone io::Error, so we create a custom one that carries the kind
                let e = io::Error::new(e.kind(), e.to_string());
                Err(e)
            }
        }
    }

    /// Logs a protobuf message to Vortex in production mode.
    ///
    /// This private function is type-erased so it does not have to be specialized
    /// for different message types, thus saving on binary size and compilation time
    /// for every `log_proto` callsite.
    #[inline(never)]
    fn do_log_proto_in_prod(
        &self,
        package: &'static str,
        name: &'static str,
        serialized_value: Vec<u8>,
        is_shutdown: bool,
    ) {
        let any = Any {
            type_url: format!("/{package}.{name}"),
            value: serialized_value,
        };
        let msg = box_any_message(any);
        #[cfg(debug_assertions)]
        if self.sender.send((msg, is_shutdown)).is_err() {
            eprintln!("{LOG_PROTO_SHUTDOWN_MESSAGE} type_url=/{package}.{name}");
        }
        #[cfg(not(debug_assertions))]
        let _ = self.sender.send((msg, is_shutdown));
    }
}

fn box_any_message(any: Any) -> Box<VortexMessage> {
    Box::new(VortexMessage {
        any: Some(any),
        vortex_event_created_at: Some(VortexProducerClient::current_timestamp()),
        vortex_client_sent_at: None,
        vortex_backend_received_at: None,
        vortex_backend_processed_at: None,
    })
}

// ---------------------------------------------------------------------------
// Worker thread
// ---------------------------------------------------------------------------

/// Worker thread loop for processing messages from the channel and sending them in batches.
/// If messages stay in a partial batch for a while, they will be sent even before the
/// full batch is completed to ensure timely delivery. If the batch has messages that are
/// being retried after a failure, a deadline will be honored even if the batch is full.
/// If and only if the batch grows too large during a backoff period, it will be pruned
/// to the HTTP body size limit accepted by the Vortex endpoint.
///
/// IMPORTANT: messages are only dropped if two anomalies occur at the same time:
/// 1) the backend is not responsive for a while and the client is forced to backoff;
/// 2) the number of events logged during the backoff is so large that the batch
///    grows larger than the HTTP body size limit set by the Vortex endpoint. This
///    limit is much higher than the target batch size.
fn worker_thread_loop(
    agent: Box<dyn SenderAgent>,
    receiver: mpsc::Receiver<(Box<VortexMessage>, bool)>,
) -> Result<(), ureq::Error> {
    enum State {
        Processing,
        Sending,
        WaitForEvent,
        Terminated,
    }
    let next_state = |event: (Option<Box<VortexMessage>>, bool),
                      shutdown_flag: &mut bool,
                      batch: &mut Batch|
     -> State {
        match event {
            (Some(msg), is_shutdown) => {
                *shutdown_flag |= is_shutdown;
                batch.push(msg);
                if batch.is_overdue() {
                    State::Sending
                } else {
                    State::Processing
                }
            }
            (None, is_shutdown) => {
                *shutdown_flag |= is_shutdown;
                if *shutdown_flag {
                    if !batch.is_empty() {
                        State::Sending // Send the *last* batch.
                    } else {
                        State::Terminated // Batch and queue are empty.
                    }
                } else {
                    // Not shutting down, so we take deadline into account.
                    if batch.is_overdue() {
                        State::Sending // Send the *full* or *overdue* batch.
                    } else {
                        // Enter the "WaitForEvent" state: batch is partially filled
                        // and the queue was empty, so we wait for more messages.
                        State::WaitForEvent
                    }
                }
            }
        }
    };

    let mut state = State::Processing;
    let mut shutdown_flag = false;
    let mut batch = Batch::for_agent(agent.as_ref());
    loop {
        match state {
            // "Processing" state: peek the queue to see if there are any messages.
            State::Processing => {
                let event = peek_queue(&receiver);
                state = next_state(event, &mut shutdown_flag, &mut batch);
            }
            State::Sending => {
                let sent = agent.send_batch(&mut batch);
                state = if shutdown_flag && !sent {
                    State::Terminated // If shutting down, give up on first send failure.
                } else {
                    State::Processing // Come back to "Processing" state after "Sending".
                };
            }
            // "WaitForEvent" state: wait for an event or timeout.
            State::WaitForEvent => {
                state = match wait_for_event(&receiver, batch.deadline) {
                    WaitForEvent::Event(event) => next_state(event, &mut shutdown_flag, &mut batch),
                    WaitForEvent::Timeout => {
                        // Go back to "Processing" state: re-consider sending the partial batch.
                        State::Processing
                    }
                };
            }
            State::Terminated => break,
        }
    }
    // Take the last error, if any, from the batch.
    batch.clear()
}

/// Returns a message and the shutdown flag peeked from the queue without blocking.
///
/// When the sender is dropped, and all messages have been processed,
/// the shutdown flag is returned as `true`.
fn peek_queue(
    receiver: &mpsc::Receiver<(Box<VortexMessage>, bool)>,
) -> (Option<Box<VortexMessage>>, bool) {
    match receiver.try_recv() {
        Ok((msg, is_shutdown)) => (Some(msg), is_shutdown),
        Err(mpsc::TryRecvError::Empty) => (None, false),
        Err(mpsc::TryRecvError::Disconnected) => (None, true),
    }
}

enum WaitForEvent {
    /// Message and shutdown flag received from the channel.
    ///
    /// The shutdown message comes with the shutdown flag set to `true`.
    /// If the sender has disconnected, `wait_for_event` will return no
    /// message and the shutdown flag set to `true`.
    Event((Option<Box<VortexMessage>>, bool)),
    /// No message received, but the channel is still open.
    Timeout,
}

/// `recv_timeout` or `recv` from the `mpsc::Receiver` of events.
///
/// This function only blocks indefinitely when the `deadline` is `None`. And
/// that's only the case when the batch of messages has no messages in it, so
/// waking up only when there are new messages in the queue is the right thing
/// to do. This guarantees retries, timely delivery, and progress in the worker
/// thread loop with the minimal number of wakeups.
fn wait_for_event(
    receiver: &mpsc::Receiver<(Box<VortexMessage>, bool)>,
    deadline: Option<Instant>,
) -> WaitForEvent {
    if let Some(deadline) = deadline {
        let timeout = deadline.saturating_duration_since(Instant::now());
        match receiver.recv_timeout(timeout) {
            Ok((message, is_shutdown)) => WaitForEvent::Event((Some(message), is_shutdown)),
            Err(mpsc::RecvTimeoutError::Timeout) => WaitForEvent::Timeout,
            Err(mpsc::RecvTimeoutError::Disconnected) => WaitForEvent::Event((None, true)),
        }
    } else {
        match receiver.recv() {
            Ok((message, is_shutdown)) => WaitForEvent::Event((Some(message), is_shutdown)),
            Err(mpsc::RecvError) => WaitForEvent::Event((None, true)),
        }
    }
}
