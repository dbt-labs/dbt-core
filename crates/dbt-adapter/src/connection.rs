use dbt_common::adapter::AdapterType;
use dbt_xdbc::Connection;
use minijinja::State;
use tracy_client::span;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::task::{Poll, Waker};

use crate::AdapterEngine;

/// The node ID used for ad-hoc connections that are not associated with any particular DAG node.
///
/// This is used to reserve a separate connection for operations that can't
/// provide a node ID.
pub const ADHOC_CONNECTION_NODE_ID: &str = "dbt-fusion.adhoc_conn";

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
    static CONNECTION: pri::ConnectionContainer = pri::ConnectionContainer::new();
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
    node_id: String,
) -> Result<ConnectionGuard<'a>, minijinja::Error> {
    let _span = span!("borrow_thread_local_connection");
    let conn = match CONNECTION.with(|c| c.take()) {
        None => engine.new_connection(state, node_id)?,
        Some(mut c) => {
            // Discard cached connections whose recordings path doesn't match
            // the current engine. This prevents stale record/replay connections
            // from writing to the wrong directory across sequential runs.
            if c.recordings_path() != engine.recordings_dir() {
                engine.new_connection(state, node_id)?
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
    guard.persist = engine.adapter_type() != AdapterType::DuckDB;
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

    /// A wrapper around a [Connection] that never drops the connection when the wrapper is
    /// dropped.
    ///
    /// The point of this struct is to avoid calling the `Drop` destructor on
    /// the wrapped [Connection] during process exit, which dead locks on
    /// Windows if the connection is a part of a thread-local/static variable.
    pub(super) struct ConnectionContainer(RefCell<Option<Box<dyn Connection>>>);

    impl ConnectionContainer {
        pub(super) fn new() -> Self {
            ConnectionContainer(RefCell::new(None))
        }

        pub(super) fn replace(&self, conn: Option<Box<dyn Connection>>) {
            let prev = self.take();
            *self.0.borrow_mut() = conn;
            if prev.is_some() {
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
                // The right choice is to simply drop the innermost connection.
                drop(prev);
                // An assert could be added here to help finding code that creates
                // a connection instead of taking one as a parameter so that the
                // outermost caller can pass the thread-local one by reference.
            }
        }

        pub(super) fn take(&self) -> Option<Box<dyn Connection>> {
            self.0.borrow_mut().take()
        }
    }

    impl Drop for ConnectionContainer {
        fn drop(&mut self) {
            std::mem::forget(self.take());
        }
    }
}
