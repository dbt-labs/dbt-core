use dbt_common::{ErrorCode, FsResult, fs_err};
use std::future::Future;
use tracing::Instrument as _;

/// Returns the effective parallelism level from an optional user-specified thread count.
/// If `num_threads` is `None` or `0`, returns the number of available CPUs (minimum 1).
pub fn effective_parallelism(num_threads: Option<usize>) -> usize {
    num_threads
        .filter(|&n| n != 0)
        .unwrap_or_else(|| std::cmp::max(1, num_cpus::get()))
}

/// Execute items sequentially or in parallel via `tokio::spawn`.
///
/// When `parallel` is `true`, each item is spawned as a tokio task with tracing span
/// propagation. When `false`, items are processed sequentially in a loop.
pub async fn dispatch_maybe_parallel<I, R, F, Fut>(
    items: Vec<I>,
    parallel: bool,
    process: F,
) -> FsResult<Vec<R>>
where
    I: Send + 'static,
    R: Send + 'static,
    F: Fn(I) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = FsResult<R>> + Send + 'static,
{
    if !parallel {
        let mut results = Vec::with_capacity(items.len());
        for item in items {
            results.push(process(item).await?);
        }
        Ok(results)
    } else {
        let mut handles = Vec::with_capacity(items.len());
        for item in items {
            let process = process.clone();
            handles.push(tokio::spawn(
                async move { process(item).await.map_err(|e| *e) }.in_current_span(),
            ));
        }
        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(Ok(r)) => results.push(r),
                Ok(Err(e)) => return Err(Box::new(e)),
                Err(e) => return Err(fs_err!(ErrorCode::Unexpected, "Join error: {}", e)),
            }
        }
        Ok(results)
    }
}
