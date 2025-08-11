use core::fmt;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use adbc_core::PartitionedResult;
use adbc_core::error::Result;
use adbc_core::options::{OptionStatement, OptionValue};
use arrow::array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use crossbeam_skiplist::SkipMap;
use dbt_xdbc::semaphore::AcquireAllSemaphore;
use dbt_xdbc::{QueryCtx, Statement};

/// Generate a unique statement ID for each [TrackedStatement]
/// by incrementing this global atomic counter.
static NEXT_STMT_ID: AtomicU64 = AtomicU64::new(0);

/// A semaphore to ensure that during `cancel_all_tracked_statements`,
/// no other thread can drop the inner [Statement] of a [TrackedStatement].
static TRACKED_STMTS_SEMAPHORE: AcquireAllSemaphore = AcquireAllSemaphore::new(u32::MAX / 2 + 1);

/// A global map that tracks all [TrackedStatement]s created by the application.
///
/// The map is sorted (based on a lock-free skip list). This means iteration starts
/// from the oldest statement and goes to the newest one, including the statements
/// being created concurrently if any.
static TRACKED_STMTS: LazyLock<SkipMap<u64, ErasedFatPtr>> = LazyLock::new(SkipMap::new);

type MutStmtPtr = &'static mut (dyn Statement + 'static);

/// A type-erased fat pointer [1][2] that can hold a `dyn Statement` pointer.
///
/// This is a workaround before the stabilization of `ptr_metadata` in Rust [3].
///
/// [1] Also known as "wide pointer".
/// [2] https://doc.rust-lang.org/nomicon/exotic-sizes.html#dynamically-sized-types-dsts
/// [3] https://github.com/rust-lang/rust/issues/81513
#[repr(C)]
#[derive(Copy, Clone)]
struct ErasedFatPtr {
    data: usize,
    meta: usize,
}

impl ErasedFatPtr {
    #[inline(never)]
    unsafe fn new(ptr: MutStmtPtr) -> Self {
        debug_assert!(size_of::<MutStmtPtr>() == 2 * size_of::<usize>());
        // SAFETY: relying on the (arguably shaky) guarantee that fat pointers
        // are represented as a pair of machine words in memory.
        let (data, vtable): (usize, usize) = unsafe { std::mem::transmute(ptr) };
        ErasedFatPtr { data, meta: vtable }
    }

    /// Convert the fat pointer to a raw pointer.
    #[inline(never)]
    unsafe fn as_raw_ptr(&mut self) -> MutStmtPtr {
        // SAFETY: this is the reverse of `new`, which ensures that `data` and `meta`
        // are valid uintptrs to the data and vtable (fat-pointer metadata) respectively.
        unsafe { std::mem::transmute((self.data, self.meta)) }
    }
}

fn register_stmt(id: u64, stmt: Box<dyn Statement>) -> &'static mut (dyn Statement + 'static) {
    // Leak the Box to get a 'static pointer and associate its
    // lifetime with the global static `TRACKED_STMTS` map.
    let ptr = Box::leak::<'static>(stmt);
    // SAFETY: the `ptr` is now leaked, we track its provenance in `ErasedFatPtr`,
    // and drop it manually when `unregister_stmt` is called from the destructor of
    // [TrackedStatement].
    let mut erased_ptr = unsafe { ErasedFatPtr::new(ptr) };
    TRACKED_STMTS.insert(id, erased_ptr);
    // SAFETY: we return a mutable reference to the pointer we received, now
    // we have a mutable alias to the original `Box<dyn Statement>`, but this
    // is safe because we are careful in how use access the `TRACKED_STMTS` map
    // such that we are never accessing the object through its mutable references
    // from more than one thread at a time.
    unsafe { erased_ptr.as_raw_ptr() }
}

pub struct StmtCancellationReport {
    pub stmt_count: usize,
    pub fail_count: usize,
    pub next_stmt_id: u64,
}

/// Iterate over all tracked statements and cancel them.
pub fn cancel_all_tracked_statements(from_stmt_id: u64) -> StmtCancellationReport {
    let mut stmt_count = 0;
    let mut fail_count = 0;
    let mut next_stmt_id = from_stmt_id;

    if !TRACKED_STMTS.is_empty() {
        let _all_permits = TRACKED_STMTS_SEMAPHORE.acquire_all();
        for entry in TRACKED_STMTS.iter() {
            let stmt_id = *entry.key();
            if stmt_id < from_stmt_id {
                continue;
            }
            let mut erased_ptr = *entry.value();
            // SAFETY: all Drop handlers are blocked by the semaphore, so we
            // can dereference pointers extracted from `TRACKED_STMTS`.
            let stmt = unsafe { erased_ptr.as_raw_ptr() };
            // There is a RISK here though! `Statement::cancel()` can be called
            // from the main-thread (the caller of `cancel_all_tracked_statements`)
            // concurrently with other operations running on the thread that the
            // [Statement] is confined to. Only the Drop handler is blocked by the
            // semaphore. This is acceptable because:
            //
            // 1) when `cancel_all_tracked_statements` is called we are tearing down
            //    everything, but need to tell the database servers to cancel
            //    potentially expensive long-running queries.
            // 2) most implementations of `Statement::cancel()` are just forwarding
            //    calls to the underlying database driver, which is expected to
            //    handle concurrent cancellations gracefully.
            let res = stmt.cancel();
            stmt_count += 1;
            if res.is_err() {
                fail_count += 1;
            }
            next_stmt_id = stmt_id + 1;
        }
    }
    StmtCancellationReport {
        stmt_count,
        fail_count,
        next_stmt_id,
    }
}

/// De-registers a statement from the global `TRACKED_STMTS` map and drops it.
///
/// IMPORTANT: must be called from the destructor of [TrackedStatement] which,
/// other than `TRACKED_STMTS`, is the only owner of the [Box<dyn Statement>]
/// alieased in `TRACKED_STMTS` at the destructor call time. [Statament]s are
/// [Send] but not [Sync], so this is always called from the thread to which
/// the [Statement] is currently confined to.
fn unregister_stmt(id: u64) {
    let _permit = TRACKED_STMTS_SEMAPHORE.acquire();
    if let Some(entry) = TRACKED_STMTS.remove(&id) {
        let mut erased_ptr = *entry.value();
        // SAFETY: the drop handler is called by the thread to which the
        // [Statement] is confined to and the semaphore ensures that
        // `cancel_all_tracked_statements` is not reading the `TRACKED_STMTS`
        // map. And if the pointer is still in the map, it means that the
        // statement is still alive.
        let stmt = unsafe {
            let ptr = erased_ptr.as_raw_ptr();
            Box::from_raw(ptr)
        };
        drop(stmt);
    }
}

#[allow(dead_code)]
pub struct TrackedStatement {
    stmt_id: u64,
    inner_ptr: &'static mut dyn Statement,
}

impl Drop for TrackedStatement {
    fn drop(&mut self) {
        unregister_stmt(self.stmt_id);
    }
}

impl TrackedStatement {
    pub fn new(stmt: Box<dyn Statement>) -> Self {
        let stmt_id = NEXT_STMT_ID.fetch_add(1, Ordering::SeqCst);
        let ptr = register_stmt(stmt_id, stmt);
        Self {
            inner_ptr: ptr,
            stmt_id,
        }
    }

    #[inline]
    fn inner(&self) -> &dyn Statement {
        self.inner_ptr
    }

    #[inline]
    fn inner_mut(&mut self) -> &mut dyn Statement {
        self.inner_ptr
    }
}

impl Statement for TrackedStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner_mut().bind(batch)
    }
    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        self.inner_mut().bind_stream(reader)
    }
    fn execute<'a>(&'a mut self) -> Result<Box<dyn RecordBatchReader + Send + 'a>> {
        self.inner_mut().execute()
    }
    fn execute_update(&mut self) -> Result<Option<i64>> {
        self.inner_mut().execute_update()
    }
    fn execute_schema(&mut self) -> Result<Schema> {
        self.inner_mut().execute_schema()
    }
    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        self.inner_mut().execute_partitions()
    }
    fn get_parameter_schema(&self) -> Result<Schema> {
        self.inner().get_parameter_schema()
    }
    fn prepare(&mut self) -> Result<()> {
        self.inner_mut().prepare()
    }
    fn set_sql_query(&mut self, query: &QueryCtx) -> Result<()> {
        self.inner_mut().set_sql_query(query)
    }
    fn set_substrait_plan(&mut self, plan: &[u8]) -> Result<()> {
        self.inner_mut().set_substrait_plan(plan)
    }
    fn cancel(&mut self) -> Result<()> {
        self.inner_mut().cancel()
    }

    // adbc_core::Optionable<Option = OptionStatement> functions -----------------------------

    fn set_option(&mut self, key: OptionStatement, value: OptionValue) -> Result<()> {
        self.inner_mut().set_option(key, value)
    }
    fn get_option_string(&self, key: OptionStatement) -> Result<String> {
        self.inner().get_option_string(key)
    }
    fn get_option_bytes(&self, key: OptionStatement) -> Result<Vec<u8>> {
        self.inner().get_option_bytes(key)
    }
    fn get_option_int(&self, key: OptionStatement) -> Result<i64> {
        self.inner().get_option_int(key)
    }
    fn get_option_double(&self, key: OptionStatement) -> Result<f64> {
        self.inner().get_option_double(key)
    }

    fn debug_fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner().debug_fmt(f)
    }
}
