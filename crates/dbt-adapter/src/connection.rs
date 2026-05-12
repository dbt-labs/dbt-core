use std::cell::RefCell;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicIsize, AtomicU64, AtomicUsize, Ordering};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

use dbt_adapter_core::AdapterType;
use dbt_common::AdapterResult;
use dbt_common::cancellation::Cancellable;
use dbt_xdbc::{Connection, ConnectionFactory};
use minijinja::State;
use tracy_client::span;

use crossbeam_skiplist::SkipMap;
use crossbeam_utils::CachePadded;

use rand::Rng;

use crate::AdapterEngine;
use crate::errors::AdapterError;

/// When more than half connections are active, wait a little bit before
/// letting a task through to make sure current nodes are not about to
/// pick up thread-local connections they have just released. This reduces
/// the chances of going above the high water mark because of unlucky timing.
const GRACE_PERIOD: Duration = Duration::from_millis(500);

/// Return a random duration in `[d/2, d)` to spread out concurrent deadline expirations.
///
/// This avoids the thundering herd problem [1] where a large number of tasks are
/// waiting for capacity and all hit their deadline at the same time.
///
/// [1]: https://en.wikipedia.org/wiki/Thundering_herd_problem
fn jittered(d: Duration) -> Duration {
    let half = d / 2;
    let half_ms = half.as_secs() * 1000 + half.subsec_millis() as u64;
    let jitter = Duration::from_millis(rand::rng().random_range(0..half_ms));
    half + jitter
}

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

/// Function that must be called when a node execution tasks finishes executing.
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
#[allow(clippy::result_unit_err)]
pub fn on_node_execution_finished(_node_id: &str) {
    let conn = CONNECTION.with(|c| c.take());
    if let Some(conn) = conn {
        sort_for_recycling(conn)
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
pub(crate) fn borrow_tlocal_connection<'a>(
    engine: &dyn AdapterEngine,
    state: Option<&State>,
    node_id: Option<String>,
) -> AdapterResult<ConnectionGuard<'a>> {
    let _span = span!("borrow_thread_local_connection");
    borrow_tlocal_connection_impl(
        engine.adapter_type(),
        state,
        node_id,
        engine.generation(),
        |state, node_id| engine.new_connection(state, node_id),
    )
}

pub(crate) fn borrow_tlocal_connection_impl<'a>(
    adapter_type: AdapterType,
    state: Option<&State>,
    node_id: Option<String>,
    engine_generation: u64,
    new_connection_fn: impl Fn(Option<&State>, Option<String>) -> AdapterResult<Box<dyn Connection>>,
) -> AdapterResult<ConnectionGuard<'a>> {
    let conn = match CONNECTION.with(|c| c.take()) {
        None => {
            // No connection in thread-local, try to get one from the recycling pool.
            // If the pool is empty, create a new one.
            match recycle_connection(node_id.as_ref()) {
                Some(conn) => conn,
                None => new_connection_fn(state, node_id)?,
            }
        }
        Some(mut c) => {
            // Discard cached connections whose generation doesn't match the
            // current engine. This prevents stale connections from being
            // reused across sequential runs with different configurations.
            if c.generation() != engine_generation {
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
/// This is intentionally a soft controller: delays scheduling based on current load.
/// Bursts can still overshoot the configured threshold.
pub struct ConnectionBackpressure {
    high_water_mark: u32,
    /// Key assigned on first registration into [`wakers`](pri::BackpressureState::wakers).
    /// Reused across re-polls so the task keeps its original queue position.
    key: Option<(Instant, u64)>,
    /// Set on first `Pending` return.
    ///
    /// Allows introducing jittered deadlines to reduce the chances of thundering herd wakeups.
    deadline: Option<Instant>,
}

impl ConnectionBackpressure {
    /// Create a new backpressure [Future] with the given high water mark.
    ///
    /// `high_water_mark` is the number of active connections that should trigger
    /// backpressure to the node scheduler when reached.
    pub fn new(high_water_mark: u32) -> Self {
        Self {
            high_water_mark,
            key: None,
            deadline: None,
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
            let deadline = self.deadline.unwrap_or_else(Instant::now);
            let seq = BACKPRESSURE_STATE.fresh_waker_seq();
            (deadline, seq)
        })
    }
}

impl Future for ConnectionBackpressure {
    type Output = NextBackpressureWakerGuard;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        use Poll::{Pending, Ready};
        let this = self.get_mut();

        let mut lazy_now = None;
        let mut now = || *lazy_now.get_or_insert_with(Instant::now);

        let mut check_readiness_condition = |this: &mut ConnectionBackpressure| {
            let num_active = BACKPRESSURE_STATE.num_active_connections();
            let high_water_mark = this.high_water_mark as isize;
            match this.deadline {
                // less than half the connections are active, ignore the deadline
                None if num_active < (high_water_mark / 2) => {
                    Ready(NextBackpressureWakerGuard::new())
                }
                None => {
                    // still under the high water mark, add a small jittered wait
                    if num_active < high_water_mark {
                        this.deadline = Some(now() + jittered(GRACE_PERIOD));
                        Pending
                    } else {
                        Ready(NextBackpressureWakerGuard::new())
                    }
                }
                Some(deadline) => {
                    if num_active < high_water_mark && now() >= deadline {
                        Ready(NextBackpressureWakerGuard::new())
                    } else {
                        Pending
                    }
                }
            }
        };

        if let Ready(guard) = check_readiness_condition(this) {
            return Ready(guard);
        }

        // Register the waker BEFORE checking the condition again to avoid a race
        // where a connection is released between the check and the registration
        // (which would cause us to miss the wake-up and sleep forever). Once a
        // waker is registered, the task is guaranteed to be woken if we ensure
        // all wakers are eventually called.
        BACKPRESSURE_STATE.register_waker(this.ordered_key(), cx.waker().clone());

        // Check again after registering waker
        let result = check_readiness_condition(this);

        // Liveness check: if nothing is running (no active guards) and we would
        // return Pending, return Ready anyway to avoid deadlock. This ensures
        // at least one task can make progress.
        if result.is_pending() && BACKPRESSURE_STATE.num_active_guards() == 0 {
            return Ready(NextBackpressureWakerGuard::new());
        }

        result
    }
}

impl Drop for ConnectionBackpressure {
    fn drop(&mut self) {
        if let Some(key) = self.key {
            BACKPRESSURE_STATE.unregister_waker(key);
        }
    }
}

/// Guard returned by [`ConnectionBackpressure`] that ensures the notification chain
/// is never broken. When dropped, it wakes the next waiter so that tasks which
/// complete without ever acquiring a connection don't stall the queue.
///
/// https://en.wikipedia.org/wiki/Semaphore_(programming)#Passing_the_baton_pattern
pub struct NextBackpressureWakerGuard;

impl NextBackpressureWakerGuard {
    fn new() -> Self {
        BACKPRESSURE_STATE.increment_active_guards();
        Self
    }
}

impl Drop for NextBackpressureWakerGuard {
    fn drop(&mut self) {
        BACKPRESSURE_STATE.decrement_active_guards();
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
        /// Used to detect liveness issues: if no guards are active and no capacity
        /// is available, we must allow progress to avoid deadlock.
        active_guards: CachePadded<AtomicUsize>,
        /// Wakers registered by [`ConnectionBackpressure`] futures waiting for capacity.
        wakers: LazyLock<SkipMap<(Instant, u64), Waker>>,
    }

    impl BackpressureState {
        pub const fn new() -> Self {
            Self {
                active_connections: CachePadded::new(AtomicIsize::new(0)),
                active_guards: CachePadded::new(AtomicUsize::new(0)),
                waker_seq: CachePadded::new(AtomicU64::new(0)),
                wakers: LazyLock::new(SkipMap::new),
            }
        }

        pub fn increment_active_guards(&self) -> usize {
            self.active_guards.fetch_add(1, Ordering::AcqRel) + 1
        }

        pub fn decrement_active_guards(&self) -> usize {
            let prev = self.active_guards.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(prev > 0, "active_guards counter underflow");
            prev - 1
        }

        pub fn num_active_guards(&self) -> usize {
            self.active_guards.load(Ordering::Acquire)
        }

        pub fn will_activate_connection(&self) {
            self.active_connections.fetch_add(1, Ordering::AcqRel);
        }

        pub fn did_deactivate_connection(&self) {
            let prev = self.active_connections.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(prev > 0, "ACTIVE_CONNECTIONS counter underflow");
            self.wake_next_backpressure_waiter();
        }

        /// Wake the longest-waiting backpressure waiter, if any.
        ///
        /// The [`SkipMap`] is sorted by insertion key, so `pop_front` removes the
        /// entry with the smallest key — the oldest registered waker. This is called
        /// from both [`did_deactivate_connection`] and [`NextBackpressureWakerGuard::drop`].
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
            on_node_execution_finished("node1");
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
            on_node_execution_finished("node2");
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
}
