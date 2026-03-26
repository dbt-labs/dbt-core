use dbt_adapter_core::AdapterType;
use dbt_common::AdapterResult;
use dbt_xdbc::Connection;
use minijinja::State;
use tracy_client::span;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicIsize, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::task::{Poll, Waker};

use crate::AdapterEngine;

/// Global atomic for generating unique connection IDs.
static CONN_SEQ_NUM: AtomicU64 = AtomicU64::new(0);

/// Global atomic counting active/borrowed connections.
static ACTIVE_CONNECTIONS: AtomicIsize = AtomicIsize::new(0);
/// Wakers registered by [`ConnectionBackpressure`] futures waiting for capacity.
static BACKPRESSURE_WAKERS: Mutex<VecDeque<Waker>> = Mutex::new(VecDeque::new());

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
pub(crate) fn drain_recycling_pool() {
    while RECYCLING_POOL.recycle().is_some() {}
}

fn will_activate_connection() {
    ACTIVE_CONNECTIONS.fetch_add(1, Ordering::AcqRel);
}

fn did_deactivate_connection() {
    let prev = ACTIVE_CONNECTIONS.fetch_sub(1, Ordering::AcqRel);
    debug_assert!(prev > 0, "ACTIVE_CONNECTIONS counter underflow");
    // Wake one waiter — the freed slot can only be used by one task.
    // If that task finds capacity is gone, it re-registers on its next poll.
    if let Some(waker) = BACKPRESSURE_WAKERS.lock().unwrap().pop_front() {
        waker.wake();
    }
}

fn num_active_connections() -> isize {
    ACTIVE_CONNECTIONS.load(Ordering::Acquire)
}

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
) -> Result<ConnectionGuard<'a>, minijinja::Error> {
    let _span = span!("borrow_thread_local_connection");
    borrow_tlocal_connection_impl(
        engine.adapter_type(),
        state,
        node_id,
        engine.recordings_dir(),
        |state, node_id| engine.new_connection(state, node_id),
    )
}

pub(crate) fn borrow_tlocal_connection_impl<'a>(
    adapter_type: AdapterType,
    state: Option<&State>,
    node_id: Option<String>,
    recordings_dir: Option<&Path>,
    new_connection_fn: impl Fn(Option<&State>, Option<String>) -> AdapterResult<Box<dyn Connection>>,
) -> Result<ConnectionGuard<'a>, minijinja::Error> {
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
            // Discard cached connections whose recordings path doesn't match
            // the current engine. This prevents stale record/replay connections
            // from writing to the wrong directory across sequential runs.
            if c.recordings_path() != recordings_dir {
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
        will_activate_connection();
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
        did_deactivate_connection();
    }
}

/// [Future] that stays in [Pending](Poll::Pending) mode until DB connection capacity is available.
///
/// This follows Rust's [Future] polling pattern:
/// - check capacity
/// - register wakeup
/// - return pending until notified
///
/// This is intentionally a soft controller: delays scheduling based on current load.
/// Bursts can still overshoot the configured threshold.
pub struct ConnectionBackpressure {
    max_water_mark: Option<usize>,
}

impl ConnectionBackpressure {
    pub fn new(max_water_mark: Option<usize>) -> Self {
        Self { max_water_mark }
    }

    pub fn from_config(adapter_type: AdapterType, max_threads: Option<usize>) -> Self {
        use AdapterType::*;
        let max_water_mark = match (adapter_type, max_threads) {
            (Redshift, _) => max_threads,
            // no backpressure for non-Redshift adapters for now, but this can be extended in the future
            (_, _) => None,
        };
        Self::new(max_water_mark)
    }
}

impl Future for ConnectionBackpressure {
    type Output = NextBackpressureWakerGuard;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let max_water_mark = match self.max_water_mark {
            // No max water mark means no backpressure, so we can immediately return ready.
            None => return Poll::Ready(NextBackpressureWakerGuard),
            Some(max_water_mark) => max_water_mark.min(isize::MAX as usize) as isize,
        };

        if num_active_connections() < max_water_mark {
            return Poll::Ready(NextBackpressureWakerGuard);
        }

        // Register the waker BEFORE checking the condition again to avoid a race where
        // a connection is released between the check and the registration (which would
        // cause us to miss the wake-up and sleep forever).
        let mut wakers = BACKPRESSURE_WAKERS.lock().unwrap();
        wakers.push_back(cx.waker().clone());

        if num_active_connections() < max_water_mark {
            Poll::Ready(NextBackpressureWakerGuard)
        } else {
            Poll::Pending
        }
    }
}

/// Guard returned by [`ConnectionBackpressure`] that ensures the notification chain
/// is never broken. When dropped, it wakes the next waiter so that tasks which
/// complete without ever acquiring a connection don't stall the queue.
///
/// https://en.wikipedia.org/wiki/Semaphore_(programming)#Passing_the_baton_pattern
pub struct NextBackpressureWakerGuard;

impl Drop for NextBackpressureWakerGuard {
    fn drop(&mut self) {
        if let Some(waker) = BACKPRESSURE_WAKERS.lock().unwrap().pop_front() {
            waker.wake();
        }
    }
}

mod pri {
    use super::*;

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
                None,
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
                None,
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
