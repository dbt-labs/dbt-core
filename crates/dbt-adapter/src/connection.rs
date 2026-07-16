use std::cell::RefCell;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicIsize, AtomicU64, AtomicUsize, Ordering};
use std::task::{Poll, Waker};
use std::time::Instant;

use dbt_adapter_core::AdapterType;
use dbt_adbc::{Connection, ConnectionFactory};
use dbt_common::AdapterResult;
use dbt_common::cancellation::Cancellable;
use dbt_telemetry::{AdapterConnectionClose, ConnectionLimitWait};
use dbt_tracing::emit::{create_debug_span, emit_trace_event, is_trace_enabled};
use minijinja::State;

use crossbeam_skiplist::SkipMap;
use crossbeam_utils::CachePadded;

use tracing::Span;

use crate::AdapterEngine;
use crate::errors::AdapterError;

/// Global atomic for generating unique connection IDs.
static CONN_SEQ_NUM: AtomicU64 = AtomicU64::new(0);

// Thread-local connection.
//
// This implementation provides an efficient connection management strategy:
// 1. Each thread maintains its own connection instance
// 2. Connections are reused across multiple operations within the same thread
// 3. This approach ensures proper transaction management within a DAG node
// 4. The ConnectionGuard wrapper ensures connections are returned to the thread-local
thread_local! {
    static CONNECTION: pri::TlsConnectionContainer = pri::TlsConnectionContainer::new();
}
static RECYCLING_POOL: LazyLock<pri::ConnectionRecyclingPool> =
    LazyLock::new(pri::ConnectionRecyclingPool::new);

/// High water mark when none is explicitly configured or a higher one is requested.
fn default_high_water_mark(adapter_type: AdapterType) -> u32 {
    use AdapterType::*;
    match adapter_type {
        Snowflake => 48,
        Redshift => 4,
        _ => 48,
    }
}

/// Derive a connection limit from the `threads` configuration option.
///
/// Used by both [`ConnectionBackpressure`] and [`AdapterConnectionFactory`] so
/// that the MapReduce parallel metadata queries respect the same bound as the
/// node-execution backpressure mechanism.
fn connection_limit_from_threads(adapter_type: AdapterType, threads: Option<usize>) -> u32 {
    let default = default_high_water_mark(adapter_type);
    let hwm = threads
        .map(|t| t.min(u32::MAX as usize) as u32)
        .unwrap_or(default)
        .clamp(2, default);
    hwm
}

/// Reads the atomic counter of active connections.
pub fn num_active_connections() -> isize {
    BACKPRESSURE_STATE.num_active_connections()
}

/// Returns current active nodes and connections if trace level logging is enabled
fn backpressure_counts_for_trace() -> (Option<u32>, Option<u32>) {
    if !is_trace_enabled() {
        return (None, None);
    }

    (
        Some(BACKPRESSURE_STATE.num_active_nodes().min(u32::MAX as usize) as u32),
        Some(
            BACKPRESSURE_STATE
                .num_active_connections()
                .clamp(0, u32::MAX as isize) as u32,
        ),
    )
}

/// Function that must be called when a node/operation finishes executing.
///
/// This allows the connection used by that node to be recycled and made available
/// for reuse by other nodes in other threads. The task execution system should
/// guarantee that:
///
///    (Property I) A node starts executing in a thread and stays in that thread
///    until it finishes executing.
///
/// This will ensure, together with Property II, that arbitrary jinja code
/// executed in the context of a node (e.g. in macros or hooks) will always
/// use the same connection instance.
///
///     (Property II) While holding a connection, a node execution task will not
///     attempt to borrow from the thread-local again.
///
/// Violations of Property II are detected at runtime when
/// [pri::too_many_tlocal_connections] is called.
///
/// This must be called from the same thread that borrowed the connection. For
/// blocking task bodies, prefer [`ThreadLocalConnectionRecycleGuard`] so cleanup
/// also runs while unwinding from a panic.
pub fn recycle_thread_local_connection() {
    let conn = CONNECTION.with(|c| c.take());
    if let Some(conn) = conn {
        sort_for_recycling(conn)
    }
}

/// Drop this thread's cached connection instead of recycling it, so a connection
/// left in the wrong scope by a failed `RESET USE` / warehouse restore can't be
/// handed to another node.
pub fn drop_thread_local_connection() {
    let conn = CONNECTION.with(|c| c.take());
    drop(conn);
}

/// Drop guard that recycles this thread's cached adapter connection.
///
/// Create this at the start of a blocking task body that may borrow an adapter
/// connection. Because it runs in [`Drop`], the connection is moved from the
/// blocking worker's thread-local slot to the shared recycling pool on both
/// normal return and panic unwinding.
#[must_use = "connection recycling only happens when the guard is dropped"]
pub struct ThreadLocalConnectionRecycleGuard;

impl Default for ThreadLocalConnectionRecycleGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadLocalConnectionRecycleGuard {
    pub fn new() -> Self {
        Self
    }
}

impl Drop for ThreadLocalConnectionRecycleGuard {
    fn drop(&mut self) {
        recycle_thread_local_connection();
    }
}

/// Send a connection for reuse by other nodes/threads.
pub fn sort_for_recycling(conn: Box<dyn Connection>) {
    let _ = RECYCLING_POOL.sort_for_recycling(conn);
}

/// Try to get a recycled connection for reuse.
pub fn recycle_connection(node_id: Option<&String>) -> Option<Box<dyn Connection>> {
    RECYCLING_POOL.recycle().map(|mut conn| {
        conn.update_node_id(node_id.cloned());
        conn
    })
}

/// Clears the global connection recycling pool.
#[allow(dead_code)]
pub(crate) fn drain_recycling_pool() {
    while RECYCLING_POOL.recycle().is_some() {}
}

static BACKPRESSURE_STATE: pri::BackpressureState = pri::BackpressureState::new();

/// Borrow the current thread-local connection or create one if it's not set yet.
///
/// A guard is returned. When destroyed, the guard returns the connection to
/// the thread-local variable. If another connection became the thread-local
/// in the mean time, that connection is dropped and the return proceeds as
/// normal.
#[tracing::instrument(skip(engine, state), level = "trace")]
pub(crate) fn borrow_tlocal_connection<'a>(
    engine: &dyn AdapterEngine,
    state: Option<&State>,
    node_id: Option<String>,
) -> AdapterResult<ConnectionGuard<'a>> {
    borrow_tlocal_connection_impl(
        engine.adapter_type(),
        state,
        node_id,
        engine.fingerprint(),
        |state, node_id| engine.new_connection(state, node_id),
    )
}

pub(crate) fn borrow_tlocal_connection_impl<'a>(
    adapter_type: AdapterType,
    state: Option<&State>,
    node_id: Option<String>,
    engine_fingerprint: u64,
    new_connection_fn: impl Fn(Option<&State>, Option<String>) -> AdapterResult<Box<dyn Connection>>,
) -> AdapterResult<ConnectionGuard<'a>> {
    let conn = match CONNECTION.with(|c| c.take()) {
        None => {
            // No connection in thread-local, try to get one from the recycling pool.
            // Only reuse a pooled connection whose config fingerprint matches this
            // engine's; otherwise create a new one. The pool is shared across
            // engines, so a non-matching connection belongs to an engine with a
            // different connection configuration.
            match recycle_connection(node_id.as_ref()) {
                Some(conn) if conn.fingerprint() == engine_fingerprint => conn,
                _ => new_connection_fn(state, node_id)?,
            }
        }
        Some(mut c) => {
            // Discard a cached connection whose config fingerprint doesn't match
            // the current engine, so connections are reused only among identical
            // connection configurations.
            if c.fingerprint() != engine_fingerprint {
                new_connection_fn(state, node_id)?
            } else {
                c.update_node_id(node_id);
                c
            }
        }
    };
    let mut guard = ConnectionGuard::new(conn);
    // DuckDB connection are cheap to create, but if long-lived, prevent other processes (not
    // other threads!) from connecting to the same database file. Set the guard to not persist in
    // this case, so the connection is dropped immediately after use instead of being returned
    // to the thread-local. This gives other processes a chance to acquire a connection to the
    // same database file.
    guard.persist = adapter_type != AdapterType::DuckDB;
    Ok(guard)
}

/// A connection wrapper that automatically returns the connection to the thread local when dropped.
/// This ensures that for a single thread, a connection is reused across multiple operations.
pub struct ConnectionGuard<'a> {
    conn: Option<Box<dyn Connection>>,
    /// Whether to return the connection to the thread-local on drop.
    persist: bool,
    _phantom: PhantomData<&'a ()>,
}

impl ConnectionGuard<'_> {
    fn new(conn: Box<dyn Connection>) -> Self {
        BACKPRESSURE_STATE.will_activate_connection();
        Self {
            conn: Some(conn),
            persist: true,
            _phantom: PhantomData,
        }
    }
}
impl Deref for ConnectionGuard<'_> {
    type Target = Box<dyn Connection>;

    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().unwrap()
    }
}
impl DerefMut for ConnectionGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().unwrap()
    }
}
impl Drop for ConnectionGuard<'_> {
    fn drop(&mut self) {
        if self.persist {
            let conn = self.conn.take();
            CONNECTION.with(|c| c.replace(conn));
        } else if let Some(conn) = self.conn.as_ref() {
            emit_trace_event(|| {
                (
                    AdapterConnectionClose {
                        repr: format!("{conn:?}"),
                    }
                    .into(),
                    None,
                )
            });
        }
        BACKPRESSURE_STATE.did_deactivate_connection();
    }
}

/// [Future] that stays in [Pending](Poll::Pending) mode until DB connection
/// capacity is available.
///
/// This follows Rust's [Future] polling pattern:
/// - check capacity
/// - register wakeup
/// - return pending until notified
///
/// Capacity is metered by the number of admitted nodes (live
/// [`NextBackpressureWakerGuard`] instances), not by the number of actively
/// borrowed connections. Counting at gate entry closes the race where tasks
/// could pass the readiness check before any of them had a chance to borrow a
/// connection and update the active connection counter.
pub struct ConnectionBackpressure {
    high_water_mark: u32,
    span: Option<Span>,
    /// Key assigned on first registration into [`wakers`](pri::BackpressureState::wakers).
    /// Reused across re-polls so the task keeps its original queue position.
    key: Option<(Instant, u64)>,
}

impl ConnectionBackpressure {
    /// Create a new backpressure [Future] with the given high water mark.
    ///
    /// `high_water_mark` is the number of active connections that should trigger
    /// backpressure to the node scheduler when reached.
    pub fn new(high_water_mark: u32) -> Self {
        Self {
            high_water_mark,
            span: None,
            key: None,
        }
    }

    /// Create a new backpressure [Future] based on the given adapter type and `threads`
    /// configuration option.
    pub fn from_config(adapter_type: AdapterType, threads: Option<usize>) -> Self {
        let high_water_mark = connection_limit_from_threads(adapter_type, threads);
        Self::new(high_water_mark)
    }

    /// Establish or retrieve the ordered key for this backpressure future.
    ///
    /// The key is assigned on first registration into `wakers` and reused
    /// across re-polls so the task keeps its original queue position. This
    /// prevents priority inversion [1]. The lower the key, the earlier the task
    /// is in the `wakers` queue and the sooner it will be woken when capacity is
    /// available.
    fn ordered_key(&mut self) -> (Instant, u64) {
        *self.key.get_or_insert_with(|| {
            let seq = BACKPRESSURE_STATE.fresh_waker_seq();
            (Instant::now(), seq)
        })
    }

    fn ensure_wait_span(&mut self) {
        if self.span.is_none() {
            let (active_nodes, active_connections) = backpressure_counts_for_trace();
            self.span = Some(create_debug_span(ConnectionLimitWait {
                active_nodes,
                active_connections,
            }));
        }
    }

    /// Creates new NextBackpressureWakerGuard and updates current span data if trace is enabled
    fn ready_guard(&self) -> Option<NextBackpressureWakerGuard> {
        let guard = NextBackpressureWakerGuard::try_new(self.high_water_mark)?;

        if !is_trace_enabled() {
            return Some(guard);
        }

        let (active_nodes, active_connections) = backpressure_counts_for_trace();
        if let Some(span) = &self.span {
            dbt_common::tracing::span_info::update_span_attrs(
                span,
                |attrs: &mut ConnectionLimitWait| {
                    attrs.active_nodes = active_nodes;
                    attrs.active_connections = active_connections;
                },
            );
        }

        Some(guard)
    }
}

impl Future for ConnectionBackpressure {
    type Output = NextBackpressureWakerGuard;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        use Poll::{Pending, Ready};
        let this = self.get_mut();

        let check_readiness_condition =
            |this: &ConnectionBackpressure| this.ready_guard().map_or(Pending, Ready);

        if let Ready(guard) = check_readiness_condition(this) {
            return Ready(guard);
        }

        this.ensure_wait_span();

        // Register the waker BEFORE checking the condition again to avoid a race
        // where a node slot is released between the check and the registration
        // (which would cause us to miss the wake-up and sleep forever). Once a
        // waker is registered, the next node slot release will wake a waiter.
        BACKPRESSURE_STATE.register_waker(this.ordered_key(), cx.waker().clone());

        // Check again after registering waker
        check_readiness_condition(this)
    }
}

impl Drop for ConnectionBackpressure {
    fn drop(&mut self) {
        if let Some(key) = self.key {
            BACKPRESSURE_STATE.unregister_waker(key);
        }
    }
}

/// Guard returned by [`ConnectionBackpressure`] for a node admitted past the
/// backpressure gate.
///
/// Owning this guard represents one active node slot. The slot is acquired
/// atomically before the guard is created and released when the guard is
/// dropped, at which point the next queued waiter is woken.
///
/// https://en.wikipedia.org/wiki/Semaphore_(programming)#Passing_the_baton_pattern
pub struct NextBackpressureWakerGuard;

impl NextBackpressureWakerGuard {
    fn try_new(high_water_mark: u32) -> Option<Self> {
        if BACKPRESSURE_STATE.try_acquire_active_node(high_water_mark as usize) {
            Some(Self)
        } else {
            None
        }
    }
}

impl Drop for NextBackpressureWakerGuard {
    fn drop(&mut self) {
        BACKPRESSURE_STATE.release_active_node();
        BACKPRESSURE_STATE.wake_next_backpressure_waiter();
    }
}

mod pri {
    use super::*;

    // XXX: don't put anything in this struct that prevents it from being a const-constructible
    #[derive(Debug)]
    pub(super) struct BackpressureState {
        /// Atomic counting of active/borrowed connections.
        active_connections: CachePadded<AtomicIsize>,
        /// Monotonically increasing key for `wakers` entries.
        waker_seq: CachePadded<AtomicU64>,
        /// Count of active [`NextBackpressureWakerGuard`] instances.
        ///
        /// This tracks nodes admitted past the [`ConnectionBackpressure`] gate,
        /// including nodes that have not borrowed a connection yet.
        active_nodes: CachePadded<AtomicUsize>,
        /// Wakers registered by [`ConnectionBackpressure`] futures waiting for capacity.
        wakers: LazyLock<SkipMap<(Instant, u64), Waker>>,
    }

    impl BackpressureState {
        pub const fn new() -> Self {
            Self {
                active_connections: CachePadded::new(AtomicIsize::new(0)),
                active_nodes: CachePadded::new(AtomicUsize::new(0)),
                waker_seq: CachePadded::new(AtomicU64::new(0)),
                wakers: LazyLock::new(SkipMap::new),
            }
        }

        pub fn try_acquire_active_node(&self, high_water_mark: usize) -> bool {
            let mut current = self.active_nodes.load(Ordering::Acquire);
            loop {
                if current >= high_water_mark {
                    return false;
                }
                match self.active_nodes.compare_exchange_weak(
                    current,
                    current + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return true,
                    Err(actual) => current = actual,
                }
            }
        }

        pub fn release_active_node(&self) -> usize {
            let prev = self.active_nodes.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(prev > 0, "active_nodes counter underflow");
            prev - 1
        }

        pub fn num_active_nodes(&self) -> usize {
            self.active_nodes.load(Ordering::Acquire)
        }

        pub fn will_activate_connection(&self) {
            self.active_connections.fetch_add(1, Ordering::AcqRel);
        }

        pub fn did_deactivate_connection(&self) {
            let prev = self.active_connections.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(prev > 0, "ACTIVE_CONNECTIONS counter underflow");
        }

        /// Wake the longest-waiting backpressure waiter, if any.
        ///
        /// The [`SkipMap`] is sorted by insertion key, so `pop_front` removes the
        /// entry with the smallest key — the oldest registered waker.
        pub fn wake_next_backpressure_waiter(&self) {
            if let Some(entry) = self.wakers.pop_front() {
                entry.value().wake_by_ref();
            }
        }

        pub fn num_active_connections(&self) -> isize {
            self.active_connections.load(Ordering::Acquire)
        }

        pub fn fresh_waker_seq(&self) -> u64 {
            self.waker_seq.fetch_add(1, Ordering::AcqRel)
        }

        pub fn register_waker(&self, key: (Instant, u64), waker: Waker) {
            self.wakers.insert(key, waker);
        }

        pub fn unregister_waker(&self, key: (Instant, u64)) {
            #[allow(clippy::used_underscore_binding)]
            let _removed = self.wakers.remove(&key).is_some();
        }
    }

    /// A wrapper around a [Connection] stored in thread-local storage
    ///
    /// The point of this struct is to avoid calling the `Drop` destructor on
    /// the wrapped [Connection] during process exit, which dead locks on
    /// Windows.
    pub(super) struct TlsConnectionContainer(RefCell<Option<Box<dyn Connection>>>);

    impl TlsConnectionContainer {
        pub(super) fn new() -> Self {
            TlsConnectionContainer(RefCell::new(None))
        }

        pub(super) fn replace(&self, conn: Option<Box<dyn Connection>>) {
            let prev = self.take();
            *self.0.borrow_mut() = conn;
            if let Some(prev_conn) = prev {
                // We should avoid nested borrows because they mean we are creating more
                // than one connection when one would be sufficient. But if we reached
                // this branch, we did exactly that (!).
                //
                //     {
                //       let outer_guard = adapter.borrow_tlocal_connection()?;
                //       f(outer_guard.as_mut());  // Pass the conn as ref. GOOD.
                //       {
                //         // We tried to borrow, but a new connection had to
                //         // be created. BAD.
                //         let inner_guard = adapter.borrow_tlocal_connection()?;
                //         ...
                //       }  // Connection from inner_guard returns to CONNECTION.
                //     }  // Connection from outer_guard is returning to CONNECTION,
                //        // but one was already there -- the one from inner_guard.
                //
                // We hope to not reach this branch, but if we do, just return the
                // previous connection to the recycling pool and move on.
                let _ = RECYCLING_POOL.sort_for_recycling(prev_conn);
                // An assert could be added here to help finding code that creates
                // a connection instead of taking one as a parameter so that the
                // outermost caller can pass the thread-local one by reference.
                too_many_tlocal_connections();
            }
        }

        pub(super) fn take(&self) -> Option<Box<dyn Connection>> {
            self.0.borrow_mut().take()
        }
    }

    impl Drop for TlsConnectionContainer {
        fn drop(&mut self) {
            std::mem::forget(self.take());
        }
    }

    #[inline(never)]
    fn too_many_tlocal_connections() {
        // set a breakpoint on this function to find where nested connections guards are created
        debug_assert!(false, "nested connection guards detected");
    }

    /// A wrapper around a [Connection] (non-Sync) that never lets the inner connection escape.
    ///
    /// This is used to store connections in the recycling pool, which is shared across threads
    /// and needs to be [Sync]. By never letting the inner connection escape, we can safely
    /// implement [Sync] for this wrapper even though [Connection] itself is not [Sync].
    struct StoredConnection {
        conn: Box<dyn Connection>,
    }

    impl StoredConnection {
        fn new(conn: Box<dyn Connection>) -> Self {
            Self { conn }
        }

        fn into_inner(self) -> Box<dyn Connection> {
            self.conn
        }
    }

    // SAFETY: See [StoredConnection].
    unsafe impl Sync for StoredConnection {}

    pub(super) struct ConnectionRecyclingPool {
        connection_set: scc::HashMap<u64, StoredConnection>,
    }

    impl ConnectionRecyclingPool {
        pub(super) fn new() -> Self {
            Self {
                connection_set: scc::HashMap::new(),
            }
        }

        pub(super) fn recycle(&self) -> Option<Box<dyn Connection>> {
            let mut any_id: Option<u64> = None;
            loop {
                let found = !self.connection_set.iter_sync(|id, _| {
                    any_id = Some(*id);
                    false
                });
                if found {
                    if let Some(id) = any_id {
                        match self
                            .connection_set
                            .remove_sync(&id)
                            .map(|(_, c)| c.into_inner())
                        {
                            Some(conn) => return Some(conn),
                            None => continue, // the connection was taken by another thread, try again
                        }
                    } else {
                        unreachable!("any_id should be Some if found is true");
                    }
                } else {
                    return None; // the set is empty, stop trying
                }
            }
        }

        pub(super) fn sort_for_recycling(&self, conn: Box<dyn Connection>) -> Result<(), ()> {
            let conn_id = CONN_SEQ_NUM.fetch_add(1, Ordering::Acquire);
            self.connection_set
                .insert_sync(conn_id, StoredConnection::new(conn))
                .map_err(|_| ())
        }
    }
}

/// Connection factory that creates connections via an [`AdapterEngine`],
/// with recycling through the global connection pool.
///
/// The connection limit is derived from the `threads` configuration using
/// [`connection_limit_from_threads`], the same logic used by
/// [`ConnectionBackpressure`].
pub struct AdapterConnectionFactory {
    engine: Arc<dyn AdapterEngine>,
    max_connections: u32,
}

impl AdapterConnectionFactory {
    pub fn new(engine: Arc<dyn AdapterEngine>, threads: Option<usize>) -> Self {
        let adapter_type = engine.adapter_type();
        Self {
            engine,
            max_connections: connection_limit_from_threads(adapter_type, threads),
        }
    }
}

impl ConnectionFactory for AdapterConnectionFactory {
    type Error = Cancellable<AdapterError>;

    fn new_connection(&self, node_id: Option<&str>) -> Result<Box<dyn Connection>, Self::Error> {
        let node_id_string = node_id.map(|s| s.to_string());
        if let Some(conn) = recycle_connection(node_id_string.as_ref()) {
            Ok(conn)
        } else {
            self.engine
                .new_connection(None, node_id_string)
                .map_err(Cancellable::Error)
        }
    }

    fn recycle_connection(&self, conn: Box<dyn Connection>) {
        sort_for_recycling(conn);
    }

    /// Dynamic limit queried by [MapReduce] when deciding to create more connections for tasks.
    fn connection_limit(&self) -> u32 {
        // NOTE(felipecrv): this implementation is racy: the number of active connections could
        // have changed by the time we return. I don't want to introduce a Mutex now cause it
        // would create a lot of undesirablae contention, but I also don't want to implement
        // the subtle double-checked locking pattern [1] just yet.
        //
        // [1]: https://en.wikipedia.org/wiki/Double-checked_locking
        let num_active = BACKPRESSURE_STATE.num_active_connections().max(0) as u32;
        self.max_connections.saturating_sub(num_active)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::NoopConnection;

    fn make_conn() -> Box<dyn Connection> {
        Box::new(NoopConnection)
    }

    fn _assert_sync<T: Sync>() {}

    #[test]
    fn recycling_pool_is_sync() {
        _assert_sync::<pri::ConnectionRecyclingPool>();
    }

    #[test]
    fn tls_container_stores_tlocal_connections() {
        let c = pri::TlsConnectionContainer::new();
        assert!(c.take().is_none());
        c.replace(Some(make_conn())); // replace
        assert!(c.take().is_some()); // take
        assert!(c.take().is_none());
    }

    #[test]
    fn multiple_connections_all_recycled() {
        let pool = pri::ConnectionRecyclingPool::new();
        pool.sort_for_recycling(make_conn()).unwrap();
        pool.sort_for_recycling(make_conn()).unwrap();
        pool.sort_for_recycling(make_conn()).unwrap();
        assert!(pool.recycle().is_some());
        assert!(pool.recycle().is_some());
        assert!(pool.recycle().is_some());
        assert!(pool.recycle().is_none()); // none
    }

    // Tests that touch the global thread-locals (CONNECTION / RECYCLING_POOL)
    // run on a fresh thread to avoid TLS-destruction ordering issues with
    // nextest's test harness.
    fn run_on_fresh_thread(f: impl FnOnce() + Send + 'static) {
        std::thread::spawn(f).join().unwrap();
    }

    #[test]
    fn tlocal_connection_lifecycle() {
        run_on_fresh_thread(|| {
            // Ensure thread-local starts empty
            CONNECTION.with(|c| {
                let conn = c.take();
                assert!(conn.is_none());
            });

            // Ensure recycling pool starts empty
            assert!(RECYCLING_POOL.recycle().is_none());

            let new_connection_calls = AtomicU64::new(0);
            let new_connection_fn = |_: Option<&State>, _: Option<String>| {
                new_connection_calls.fetch_add(1, Ordering::Relaxed);
                Ok(make_conn())
            };
            let guard = borrow_tlocal_connection_impl(
                AdapterType::Snowflake,
                None,
                None,
                0,
                new_connection_fn,
            );
            assert_eq!(new_connection_calls.load(Ordering::Relaxed), 1);
            CONNECTION.with(|c| {
                // Connection is taken, so thread-local should be empty
                assert!(c.take().is_none());
            });
            drop(guard);
            CONNECTION.with(|c| {
                // Connection should be returned to thread-local after guard is dropped
                let conn = c.take();
                assert!(conn.is_some());
                c.replace(conn); // put it back for the next test
            });
            recycle_thread_local_connection();
            CONNECTION.with(|c| {
                // Connection is not in the thread-local anymore
                assert!(c.take().is_none());
            });
            // Connection should be in the recycling pool after node execution finishes
            let conn = RECYCLING_POOL.recycle();
            assert!(conn.is_some());
            CONNECTION.with(|c| {
                c.replace(conn); // put it back for the next test
            });
            recycle_thread_local_connection();
            assert_eq!(new_connection_calls.load(Ordering::Relaxed), 1); // ensure this is still 1
            let _guard = borrow_tlocal_connection_impl(
                AdapterType::Snowflake,
                None,
                None,
                0,
                new_connection_fn,
            );
            assert_eq!(
                new_connection_calls.load(Ordering::Relaxed),
                1,
                "connection should be reused from the recycling pool"
            );
        });
    }

    #[test]
    fn drop_thread_local_connection_drops_and_does_not_recycle() {
        run_on_fresh_thread(|| {
            // Put a connection in the thread-local
            CONNECTION.with(|c| {
                c.replace(Some(make_conn()));
            });

            // Ensure the connection is in the thread-local
            CONNECTION.with(|c| {
                let conn = c.take();
                assert!(conn.is_some());
                c.replace(conn); // put it back
            });

            drop_thread_local_connection();

            // Ensure thread-local is empty (connection was dropped)
            CONNECTION.with(|c| {
                let conn = c.take();
                assert!(conn.is_none());
            });

            // Ensure the connection was dropped, not recycled into the pool
            assert!(RECYCLING_POOL.recycle().is_none());
        });
    }
}
