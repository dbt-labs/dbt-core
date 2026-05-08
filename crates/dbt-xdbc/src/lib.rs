#![cfg_attr(docsrs, feature(doc_auto_cfg, doc_cfg))]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/docs/source/_static/favicon.ico",
    html_favicon_url = "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/docs/source/_static/favicon.ico"
)]
#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))]
#![allow(clippy::cognitive_complexity)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::if_same_then_else)]
#![allow(clippy::let_and_return)]
#![allow(clippy::needless_bool)]
#![allow(clippy::should_implement_trait)]

use dbt_base::cancel::{Cancellable, CancellationToken, CancelledError};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinError;
use tracing::Span;
use tracy_client::span;

use std::ffi::c_char;
use std::future::Future;
use std::panic;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

#[cfg(debug_assertions)]
pub(crate) mod env_var;

pub mod driver;
pub use driver::Backend;
pub use driver::Driver;
pub use driver::LoadStrategy;

pub mod database;
pub use database::Database;

pub mod connection;
pub use connection::Connection;

pub mod statement;
pub use statement::Statement;

pub mod query_ctx;
pub use query_ctx::QueryCtx;

pub mod semaphore;

#[cfg(feature = "odbc")]
pub(crate) mod odbc;
#[cfg(feature = "odbc")]
pub(crate) mod odbc_api;

pub(crate) mod builder;
pub(crate) mod checksums;
pub mod driver_manager;
pub mod duration;
pub mod install;

// Constants for different backends
pub mod athena;
pub mod bigquery;
pub mod databricks;
pub mod redshift;
pub mod salesforce;
pub mod snowflake;
pub mod spark;

// REPL for ADBC drivers
#[cfg(feature = "repl")]
pub mod repl;

/// Interpret the SQLSTATE [1] 5-char ASCII string as a Rust string.
///
/// [1] https://en.wikipedia.org/wiki/SQLSTATE
pub fn str_from_sqlstate(sqlstate: &[c_char; 5]) -> &str {
    // This is safe because the range of the byte values is validated by str::from_utf8 below.
    // It would be unnecessary if Rust ADBC used u8 for [`Error::sqlstate`] [1] instead of i8.
    //
    // [1] https://github.com/apache/arrow-adbc/pull/1725#discussion_r1567531539
    let unsigned: &[u8; 5] = unsafe { std::mem::transmute(sqlstate) };
    let res = std::str::from_utf8(unsigned);
    debug_assert!(res.is_ok(), "SQLSTATE is not valid ASCII: {sqlstate:?}");
    res.unwrap_or("")
}

pub const SNOWFLAKE_DRIVER_VERSION: &str = "0.21.0.dev+dbt0.21.10";
pub const BIGQUERY_DRIVER_VERSION: &str = "0.21.0.dev+dbt0.21.10";
pub const POSTGRES_DRIVER_VERSION: &str = "0.21.0+dbt0.21.0";
pub const DATABRICKS_DRIVER_VERSION: &str = "0.21.0+dbt0.21.7";
pub const REDSHIFT_DRIVER_VERSION: &str = "0.18.0+dbt0.18.4";
pub const DUCKDB_EXTENDED_DRIVER_VERSION: &str = "0.21.0.dev+dbt0.0.18";
pub const SALESFORCE_DRIVER_VERSION: &str = "0.21.0.dev+dbt0.22.0";
pub const SPARK_DRIVER_VERSION: &str = "0.21.0.dev+dbt0.1.2";
pub const MSSQLSERVER_DRIVER_VERSION: &str = "1.3.1";
pub const EXASOL_DRIVER_VERSION: &str = "0.9.0";

pub use install::pre_install_all_drivers;
pub use install::pre_install_driver;

/// Encapsulates connection creation, recycling, and concurrency limits.
pub trait ConnectionFactory: Send + Sync {
    type Error;

    /// Create or recycle a connection. `node_id` identifies the node
    /// requesting the connection (used by some adapters for recycling affinity).
    fn new_connection(&self, node_id: Option<&str>) -> Result<Box<dyn Connection>, Self::Error>;

    /// Return a connection for potential reuse.
    fn recycle_connection(&self, conn: Box<dyn Connection>);

    /// Maximum number of concurrent connections this factory allows.
    fn connection_limit(&self) -> u32;
}

/// A function that maps a key to a computed value using a [Connection].
type MapF<Key, Value> = Box<dyn Fn(&'_ mut dyn Connection, &Key) -> Value + Send + Sync>;

/// A function that reduces a computed value into an accumulator.
type ReduceF<Acc, Key, Value, Error> =
    Box<dyn Fn(&mut Acc, Key, Value) -> Result<(), Error> + Send + Sync>;

struct MapReduceInner<Key, Value, Acc, Error>
where
    Key: Sized + Send,
    Value: Sized + Send + 'static,
    Acc: Sized + Default + Send + 'static,
    Error: Send,
{
    /// Connection factory for creating, recycling, and limiting connections.
    connection_factory: Box<dyn ConnectionFactory<Error = Cancellable<Error>>>,
    /// Node ID forwarded to the connection factory on each new_connection call.
    node_id: Option<String>,
    /// Function to map a key to a computed value using a [Connection].
    map_f: MapF<Key, Value>,
    /// Function to reduce a computed value into the accumulator.
    reduce_f: ReduceF<Acc, Key, Value, Cancellable<Error>>,

    /// The next key to be processed by any of the workers.
    key_counter: AtomicUsize,
    /// Total time spent in `task_count` tasks.
    total_task_time_us: AtomicU64,
    task_count: AtomicU64,
    /// Total time spent in `conn_count` connections.
    total_conn_time_us: AtomicU64,
    conn_count: AtomicU64,
}

impl<K, V, Acc, E> MapReduceInner<K, V, Acc, E>
where
    K: Sized + Send,
    V: Sized + Send + 'static,
    Acc: Sized + Default + Send + 'static,
    E: Send + 'static,
{
    #[inline(never)]
    fn new_connection(&self) -> Result<Box<dyn Connection>, Cancellable<E>> {
        let _span = span!("MapReduceInner::new_connection");
        let start = std::time::Instant::now();
        let res = self
            .connection_factory
            .new_connection(self.node_id.as_deref());
        if res.is_ok() {
            let elapsed = start.elapsed();
            self.conn_count.fetch_add(1, Ordering::SeqCst);
            self.total_conn_time_us
                .fetch_add(elapsed.as_micros() as u64, Ordering::SeqCst);
        }
        res
    }

    fn recycle_connection(&self, conn: Box<dyn Connection>) {
        self.connection_factory.recycle_connection(conn);
    }

    fn map(&self, conn: &'_ mut dyn Connection, key: &K) -> V {
        let _span = span!("MapReduceInner::map");
        let start = std::time::Instant::now();
        let res = (self.map_f)(conn, key);
        let elapsed = start.elapsed();
        self.task_count.fetch_add(1, Ordering::SeqCst);
        self.total_task_time_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::SeqCst);
        res
    }

    fn avg_conn_time_us(&self) -> f64 {
        let conn_count = self.conn_count.load(Ordering::SeqCst);
        self.total_conn_time_us.load(Ordering::SeqCst) as f64 / conn_count.max(1) as f64
    }

    fn avg_task_time_us(&self) -> f64 {
        // if an older task_count or total_task_time_us is loaded, the
        // average will be incorrect, but the error will be small
        let task_count = self.task_count.load(Ordering::SeqCst);
        self.total_task_time_us.load(Ordering::SeqCst) as f64 / task_count.max(1) as f64
    }
}

/// Run parallel Key-to-Value tasks in parallel with a bounded number of
/// connections and reduce the results into an accumulator.
///
/// Connection creation, recycling, and concurrency limits are managed by a
/// [`ConnectionFactory`] implementation passed at construction time.
pub struct MapReduce<Key, Value, Acc, Error>
where
    Key: Sized + Clone + Send + Sync + 'static,
    Value: Sized + Send + 'static,
    Acc: Sized + Default + Send + 'static,
    Error: Send + 'static,
{
    inner: Arc<MapReduceInner<Key, Value, Acc, Error>>,
    max_connections: u32,
}

impl<K, V, Acc, E> MapReduce<K, V, Acc, E>
where
    K: Sized + Clone + Send + Sync + 'static,
    V: Sized + Send + 'static,
    Acc: Sized + Default + Send + 'static,
    E: Send + 'static,
{
    pub fn new(
        connection_factory: Box<dyn ConnectionFactory<Error = Cancellable<E>>>,
        map_f: MapF<K, V>,
        reduce_f: ReduceF<Acc, K, V, Cancellable<E>>,
        node_id: Option<String>,
    ) -> Self {
        let max_connections = connection_factory.connection_limit().max(2);
        let inner = MapReduceInner {
            connection_factory,
            node_id,
            map_f,
            reduce_f,
            key_counter: AtomicUsize::new(0),
            total_task_time_us: AtomicU64::new(0),
            task_count: AtomicU64::new(0),
            total_conn_time_us: AtomicU64::new(0),
            conn_count: AtomicU64::new(0),
        };
        Self {
            inner: Arc::new(inner),
            max_connections,
        }
    }

    #[inline(never)]
    #[allow(clippy::type_complexity)]
    pub fn new_connection(
        &self,
        cur_span: Span,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn Connection>, Cancellable<E>>> + Send>> {
        let inner = Arc::clone(&self.inner); // clone needed to move it into lambda
        let future = async move {
            match tokio::task::spawn_blocking(move || {
                let _sp = cur_span.entered();
                inner.new_connection()
            })
            .await
            {
                Ok(res) => res,
                Err(join_err) => Err(cancellable_from_join_error(join_err)),
            }
        };
        Box::pin(future)
    }

    #[inline(never)]
    #[allow(clippy::type_complexity)]
    fn worker(
        &self,
        conn: Box<dyn Connection>,
        tx: mpsc::UnboundedSender<(K, V)>,
        keys: Arc<Vec<K>>,
        token: &CancellationToken,
        cur_span: Span,
    ) -> Pin<Box<dyn Future<Output = Result<(), CancelledError>> + Send>> {
        let inner = Arc::clone(&self.inner); // clone needed to move it into lambda
        let token = token.clone(); // clone needed to move it into lambda
        let future = async move {
            let mut conn = conn;
            loop {
                let inner = Arc::clone(&inner);
                let keys_for_task = keys.clone();
                let i = inner.key_counter.fetch_add(1, Ordering::SeqCst);
                if i >= keys.len() {
                    // No more keys to process, recycle connection and exit.
                    let _ = tokio::task::spawn_blocking(move || {
                        let _sp = cur_span.entered();
                        inner.recycle_connection(conn);
                    })
                    .await;
                    return Ok(());
                }
                let cur_span = cur_span.clone();
                let handle = tokio::task::spawn_blocking(move || {
                    let _sp = cur_span.entered();
                    let key = &keys_for_task[i];
                    let value = inner.map(&mut *conn, key);
                    (conn, value)
                });
                // unwrap() fails only when the task code above panics, so calling
                // it makes the code no more panic-prone than it alerady is
                let conn_value = match handle.await {
                    Ok(conn_value) => conn_value,
                    Err(join_error) => {
                        // can't recover connection after a join error
                        let err = cancelled_from_join_error(join_error);
                        return Err(err);
                    }
                };
                conn = conn_value.0;
                let value = conn_value.1;

                let key = keys[i].clone();
                match tx.send((key, value)) {
                    Ok(()) => (),
                    Err(SendError(_)) => {
                        // The receiver has been dropped (due to cancellation),
                        // so we fail with a CancelledError. We also don't worry
                        // about recycling the connection.
                        return Err(CancelledError);
                    }
                }

                if token.is_cancelled() {
                    return Err(CancelledError);
                    // And don't worry about recycling the connection since we're shutting down.
                }
            }
        };
        Box::pin(future)
    }

    /// Reduce a computed value into an accumulator.
    fn reduce(&self, acc: &mut Acc, key: K, value: V) -> Result<(), Cancellable<E>> {
        (self.inner.reduce_f)(acc, key, value)
    }

    /// Run all tasks in parallel with at most `max_connections` connections.
    async fn do_run(
        self,
        keys: Arc<Vec<K>>,
        token: CancellationToken,
    ) -> Result<Acc, Cancellable<E>> {
        let mut acc = Acc::default();
        if keys.is_empty() {
            return Ok(acc);
        }

        let mut recv_buffer = Vec::new();
        let (tx, mut rx) = mpsc::unbounded_channel::<(K, V)>();

        let max_conns = keys.len().min(self.max_connections as usize);
        let mut conn_futures = FuturesUnordered::new();
        let mut workers = FuturesUnordered::new();

        let cur_span = Span::current();

        let mut n_conns = {
            conn_futures.push(self.new_connection(cur_span.clone()));
            if max_conns > 1 {
                // If we have more than one task, we can start a second
                // connection before knowing how long the tasks will take.
                conn_futures.push(self.new_connection(cur_span.clone()));
                2
            } else {
                1
            }
        };
        // To start, ensure there is at least one connection open and one task enqueued.
        // Even if all the other connections fail, we can still keep making progress by
        // reusing the first connection.
        let conn = conn_futures.next().await.unwrap()?;
        let worker =
            tokio::spawn(self.worker(conn, tx.clone(), keys.clone(), &token, cur_span.clone()));
        workers.push(worker);

        while self.inner.key_counter.load(Ordering::SeqCst) < keys.len() {
            if let Some(Ok(conn)) = conn_futures.next().await {
                let worker = tokio::spawn(self.worker(
                    conn,
                    tx.clone(),
                    keys.clone(),
                    &token,
                    cur_span.clone(),
                ));
                workers.push(worker);
            }
            if n_conns < max_conns {
                let remaining_keys = {
                    let key_counter = self.inner.key_counter.load(Ordering::SeqCst);
                    if key_counter < keys.len() {
                        keys.len() - key_counter
                    } else {
                        0
                    }
                };

                const K: f64 = 1.5; // sensitivity factor
                if (remaining_keys as f64 * self.inner.avg_task_time_us()) / (n_conns as f64)
                    > (self.inner.avg_conn_time_us() * K)
                {
                    conn_futures.push(self.new_connection(cur_span.clone()));
                    n_conns += 1;
                    continue;
                }
            }

            if !rx.is_empty() {
                let n = rx.recv_many(&mut recv_buffer, n_conns).await;
                debug_assert!(recv_buffer.len() == n);
                for _ in 0..n {
                    let (key, value) = recv_buffer.pop().unwrap();
                    self.reduce(&mut acc, key, value)?;
                }
            } else if self.inner.key_counter.load(Ordering::SeqCst) < keys.len() {
                let us = self.inner.avg_conn_time_us().floor() as u64;
                let duration = Duration::from_micros(us).min(Duration::from_secs(1));
                tokio::time::sleep(duration).await;
            }

            token.check_cancellation()?;
        }
        drop(tx);

        // Wait for all the workers to finish...
        while let Some(res) = workers.next().await {
            match res {
                Ok(Ok(())) => (),
                Ok(Err(CancelledError)) => {
                    return Err(CancelledError.into());
                }
                Err(join_error) => {
                    return Err(cancellable_from_join_error(join_error));
                }
            }
            token.check_cancellation()?;
        }
        // ...and reduce their results.
        loop {
            let n = rx.recv_many(&mut recv_buffer, n_conns).await;
            if n == 0 {
                break;
            }
            for _ in 0..n {
                let (key, value) = recv_buffer.pop().unwrap();
                self.reduce(&mut acc, key, value)?;
            }
            token.check_cancellation()?;
        }

        Ok(acc)
    }

    pub fn run(
        self,
        keys: Arc<Vec<K>>,
        token: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<Acc, Cancellable<E>>> + Send>> {
        let future = self.do_run(keys, token);
        Box::pin(future)
    }
}

fn cancelled_from_join_error(err: JoinError) -> CancelledError {
    if err.is_cancelled() {
        CancelledError
    } else if err.is_panic() {
        panic::resume_unwind(err.into_panic());
    } else {
        unreachable!("JoinError's are either due to cancellation or panic");
    }
}

fn cancellable_from_join_error<T>(err: JoinError) -> Cancellable<T> {
    cancelled_from_join_error(err).into()
}
