use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};

/// General-case semaphore implementation.
///
/// Typical Dijkstra Semaphore algorithm over atomics, wait and notify functions.
///
/// The `atomic-wait` crate by Mara Bos is used as it provides the atomic wait and wake
/// functionality that exists in C++20's `std::atomic<T>::wait` and `std::atomic<T>::notify_all`
/// but is not yet available in stable Rust.
struct AtomicSemaphoreBase {
    /// The atomic counter representing the number of available permits.
    ///
    /// `u32` was chosen because that is the atomic that Linux uses for futexes,
    /// and as such, the type chosen byt the `atomic-wait` crate.
    a: AtomicU32,
}

impl AtomicSemaphoreBase {
    pub fn new(count: u32) -> Self {
        let a = AtomicU32::new(count);
        Self { a }
    }

    #[inline]
    pub fn release_impl(&self, update: u32) {
        let old = self.a.fetch_add(update, Ordering::Release);
        debug_assert!(
            update <= u32::MAX - old,
            "update is greater than the expected value"
        );
        if old == 0u32 {
            atomic_wait::wake_all(&self.a);
        }
    }

    pub fn release(&self) {
        self.release_impl(1);
    }

    // Try to acquire a permit without blocking.
    #[inline]
    fn try_acquire_impl(&self, old: u32) -> bool {
        old > 0
            && self
                .a
                .compare_exchange_weak(old, old - 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
    }

    pub fn acquire(&self) {
        loop {
            // wait until the value is not 0 anymore
            atomic_wait::wait(&self.a, 0);
            let old = self.a.load(Ordering::Relaxed);
            if self.try_acquire_impl(old) {
                break;
            }
        }
    }

    pub fn try_acquire(&self) -> bool {
        let old = self.a.load(Ordering::Acquire);
        self.try_acquire_impl(old)
    }
}

/// Counting semaphore implementation.
pub struct Semaphore {
    /// The maximum number of permits the semaphore can hold.
    ///
    /// NOTE: If release() gets called more than this number, it will not
    /// panic, but will simply increase the count of available permits.
    max: u32,
    base: AtomicSemaphoreBase,
}

impl Semaphore {
    pub fn new(count: u32) -> Self {
        debug_assert!(count > 0, "Semaphore must allow for at least one permit");
        Self {
            max: count,
            base: AtomicSemaphoreBase::new(count),
        }
    }

    /// Get the number of available permits the semaphore started with.
    pub fn max(&self) -> u32 {
        self.max
    }

    /// Release a permit, incrementing the count of available permits.
    pub fn release(&self) {
        self.base.release();
    }

    /// Acquire a permit, blocking until one is available.
    pub fn acquire(&self) {
        self.base.acquire();
    }

    /// Try to acquire a permit without blocking.
    pub fn try_acquire(&self) -> bool {
        self.base.try_acquire()
    }
}

impl fmt::Debug for Semaphore {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Semaphore")
            .field("max", &self.max)
            .field("available", &self.base.a.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::mpsc::channel;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_semaphore_basic_acquire_release() {
        let semaphore = Semaphore::new(2);
        assert_eq!(semaphore.max(), 2);

        semaphore.acquire();
        semaphore.acquire();

        semaphore.release();
        semaphore.acquire();
        assert!(!semaphore.try_acquire());
    }

    #[test]
    fn test_semaphore_release_more_than_initial() {
        let semaphore = Semaphore::new(1);
        // releasing without acquiring first
        semaphore.release();

        assert!(semaphore.try_acquire());
        // The semaphore handles the case where more permits are released than
        // initially available by expanding the count of available permits.
        // Any other strategy would be too complicated and error-prone.
        assert!(semaphore.try_acquire());
        assert!(!semaphore.try_acquire());
    }

    #[test]
    fn test_semaphore_basic_concurrent_access() {
        let semaphore = Arc::new(Semaphore::new(2));
        let mut handles = vec![];

        for i in 0..3 {
            let sem = semaphore.clone();
            handles.push(thread::spawn(move || {
                sem.acquire();
                thread::sleep(Duration::from_millis(100));
                sem.release();
                i
            }));
        }
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results, vec![0, 1, 2]);
    }

    #[test]
    fn test_semaphore_wait_signals() {
        let main: Arc<Semaphore> = Arc::new(Semaphore::new(1));
        let child_1 = main.clone();
        let child_2 = main.clone();
        let (tx, rx) = channel();

        let tx = Arc::new(tx);

        let tx_1 = tx.clone();
        let _ = thread::spawn(move || {
            child_1.acquire();
            tx_1.send(()).unwrap();
        });

        let _ = thread::spawn(move || {
            child_2.acquire();
            tx.send(()).unwrap();
        });

        // if main doesn't release one of the children will get stuck
        main.release();
        let _ = rx.recv();
    }

    #[test]
    #[should_panic]
    fn test_semaphore_zero_permits() {
        let _ = Semaphore::new(0);
    }
}
