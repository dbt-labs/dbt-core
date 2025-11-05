//! Async tracing helpers for propagating span context across task boundaries.

use tracing::Instrument as _;

/// Helper to spawn async tasks while preserving tracing span context.
/// This ensures all spawned work inherits the current span.
///
/// # Example
/// ```ignore
/// spawn_traced(async {
///     // This task inherits the current span
///     tracing::info!("This log will be in the parent span");
/// });
/// ```
pub fn spawn_traced<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future.in_current_span())
}

/// Helper to spawn blocking tasks while preserving tracing span context.
///
/// # Example
/// ```ignore
/// spawn_blocking_traced(|| {
///     // This blocking task inherits the current span
///     tracing::info!("This log will be in the parent span");
///     expensive_computation()
/// });
/// ```
pub fn spawn_blocking_traced<F, R>(f: F) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || {
        let _guard = span.enter();
        f()
    })
}
