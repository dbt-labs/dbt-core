use std::result::Result;
use std::sync::LazyLock;

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
    let mut rx = FAIL_FAST_WATCH.subscribe();
    loop {
        let fail_fast = *rx.wait_for(|&v| v).await?;
        if fail_fast && fail_fast_flag {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const TIMEOUT: Duration = Duration::from_millis(10);

    #[test]
    fn signal_lifecycle() {
        reset_fail_fast();
        assert!(!has_fail_fast_triggered());
        trigger_fail_fast();
        assert!(has_fail_fast_triggered());
    }

    #[test]
    fn reset_clears_flag() {
        reset_fail_fast();
        trigger_fail_fast();
        assert!(has_fail_fast_triggered());
        reset_fail_fast();
        assert!(!has_fail_fast_triggered());
    }

    #[tokio::test]
    async fn subscribe_returns_immediately_when_already_triggered() {
        reset_fail_fast();
        trigger_fail_fast();
        let result = tokio::time::timeout(TIMEOUT, fail_fast_subscribe(true)).await;
        assert!(result.is_ok(), "subscriber should return immediately");
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn subscribe_wakes_on_trigger() {
        reset_fail_fast();
        let handle = tokio::spawn(async { fail_fast_subscribe(true).await });
        // Give the subscriber time to start waiting.
        tokio::time::sleep(TIMEOUT).await;
        trigger_fail_fast();
        let result = tokio::time::timeout(TIMEOUT, handle).await;
        assert!(result.is_ok(), "subscriber should wake up after trigger");
        assert!(result.unwrap().unwrap().is_ok());
    }

    #[tokio::test]
    async fn subscribe_false_never_completes_even_when_triggered() {
        reset_fail_fast();
        trigger_fail_fast();
        let result = tokio::time::timeout(TIMEOUT, fail_fast_subscribe(false)).await;
        assert!(
            result.is_err(),
            "subscriber with fail_fast_flag=false should never return"
        );
    }
}
