use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use dbt_runtime::builder::Builder;

#[dbt_runtime::test]
async fn test_macro_provides_dbt_runtime_handle() {
    let handle = dbt_runtime::Handle::try_current().expect("handle should be set");
    let result = handle.spawn_blocking(|| 6 * 7).await.unwrap();
    assert_eq!(result, 42);
}

#[dbt_runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_macro_multi_thread() {
    let handle = dbt_runtime::Handle::try_current().expect("handle should be set");
    let result = handle.spawn_blocking(|| 6 * 7).await.unwrap();
    assert_eq!(result, 42);
}

#[test]
fn runs_a_blocking_task_end_to_end() {
    let rt = Builder::new().max_blocking_threads(2).build();
    let handle = rt.handle();

    let h = handle.spawn_blocking(|| 6 * 7);
    let out = futures::executor::block_on(h).expect("task should complete");
    assert_eq!(out, 42);

    drop(rt);
}

#[test]
fn runs_many_tasks() {
    let rt = Builder::new().max_blocking_threads(4).build();
    let handle = rt.handle();
    let counter = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..64)
        .map(|i| {
            let counter = Arc::clone(&counter);
            handle.spawn_blocking(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                i * 2
            })
        })
        .collect();

    let mut sum = 0;
    for h in handles {
        sum += futures::executor::block_on(h).unwrap();
    }

    assert_eq!(counter.load(Ordering::Relaxed), 64);
    assert_eq!(sum, (0..64).map(|i| i * 2).sum::<i32>());
    drop(rt);
}

#[test]
fn a_panicking_task_reports_a_join_error() {
    let rt = Builder::new().build();
    let handle = rt.handle();

    let h = handle.spawn_blocking(|| panic!("boom"));
    let err = futures::executor::block_on(h).expect_err("should be a JoinError");
    assert!(err.is_panic());
    drop(rt);
}

#[test]
fn worker_threads_are_marked_as_pool_workers() {
    let rt = Builder::new().max_blocking_threads(2).build();
    let seen = Arc::new(AtomicBool::new(false));
    let seen2 = Arc::clone(&seen);

    let h = rt.handle().spawn_blocking(move || {
        seen2.store(dbt_runtime::is_pool_worker(), Ordering::Relaxed);
    });
    futures::executor::block_on(h).unwrap();
    assert!(
        seen.load(Ordering::Relaxed),
        "worker thread should report is_pool_worker() == true"
    );
    drop(rt);
}
