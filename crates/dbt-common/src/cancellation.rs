use std::error::Error;
use std::panic;
use tokio::task::JoinError;

// re-export symbols that used to live here, but are now in dbt-cancel
pub use dbt_cancel::{
    Cancellable, CancellationToken, CancellationTokenSource, CancelledError, never_cancels,
};

pub fn cancellable_from_join_error<T: Error>(err: JoinError) -> Cancellable<T> {
    if err.is_cancelled() {
        Cancellable::Cancelled
    } else if err.is_panic() {
        panic::resume_unwind(err.into_panic());
    } else {
        unreachable!("JoinError's are either due to cancellation or panic");
    }
}
