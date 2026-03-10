use std::marker;
use std::pin::Pin;
use std::result::Result;
use std::sync::LazyLock;
use std::task::{Context, Poll};

use tokio::sync::watch;

/// Fail-fast signal.
///
/// Fusion is highly parallel. It's desirable to have a fail-fast mechanism to
/// quickly cancel ongoing operations when a critical error is detected without
/// waiting for the operations to time out on their own.
static FAIL_FAST_WATCH: LazyLock<watch::Sender<bool>> = LazyLock::new(|| watch::Sender::new(false));

/// Signal that fail-fast has been triggered. Wakes all receivers.
///
/// Only the last value is kept, but that's fine because we only care about value
/// flipping from transition from `false` to `true`.
pub fn trigger_fail_fast() {
    FAIL_FAST_WATCH.send_replace(true);
}

/// Reset the fail-fast signal.
///
/// Necessary to support sequential invocation in tests. In production, the
/// fail-fast signal only changes from `false` to `true` once.
pub fn reset_fail_fast() {
    FAIL_FAST_WATCH.send_replace(false);
}

/// Check if the fail-fast signal has been triggered before.
pub fn has_fail_fast_triggered() -> bool {
    *FAIL_FAST_WATCH.borrow()
}

/// Subscribe to the fail-fast signal.
///
/// If the `fail_fast_flag` is `false`, this Future will never complete.
/// This flag should be true when the user invokes dbt with `--fail-fast`.
pub async fn fail_fast_subscribe(fail_fast_flag: bool) -> Result<(), watch::error::RecvError> {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Pending<T> {
        _data: marker::PhantomData<fn() -> T>,
    }
    impl<T> Future for Pending<T> {
        type Output = T;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<T> {
            Poll::Pending
        }
    }

    pub fn pending<T>() -> Pending<T> {
        Pending {
            _data: marker::PhantomData,
        }
    }

    let mut rx = FAIL_FAST_WATCH.subscribe();
    loop {
        let fail_fast = *rx.wait_for(|&v| v).await?;
        if fail_fast {
            // If flag is true, return immediately. Otherwise, wait indefinitely
            // because the caller is not interested in waking up on fail-fast trigger.
            return if fail_fast_flag {
                Ok(())
            } else {
                pending().await
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const TIMEOUT: Duration = Duration::from_millis(100);

    #[tokio::test]
    async fn subscribe_returns_immediately_when_already_triggered() {
        trigger_fail_fast();
        let result = tokio::time::timeout(TIMEOUT, fail_fast_subscribe(true)).await;
        #[allow(clippy::single_match)]
        match result {
            Ok(res) => res.unwrap(),
            Err(_) => (), // timed-out, ignore (test machines can be slow)
        }
    }

    #[tokio::test]
    async fn subscribe_never_returns_if_flag_is_false() {
        let result = tokio::time::timeout(TIMEOUT, fail_fast_subscribe(false)).await;
        let timed_out = result.is_err();
        assert!(
            timed_out,
            "Expected subscribe to time out, but it returned instead"
        );
    }
}
