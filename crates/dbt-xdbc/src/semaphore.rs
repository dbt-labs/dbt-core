use std::sync::{Condvar, Mutex};

/// A semaphore implementation
#[derive(Debug)]
pub struct Semaphore {
    permits: Mutex<usize>,
    initial_permits: usize,
    condvar: Condvar,
}

impl Semaphore {
    pub fn new(permits: usize) -> Self {
        debug_assert!(permits > 0, "Semaphore must have at least one permit");
        Self {
            permits: Mutex::new(permits),
            initial_permits: permits,
            condvar: Condvar::new(),
        }
    }

    pub fn acquire(&self) {
        let mut permits = self.permits.lock().unwrap();

        permits = self.condvar.wait_while(permits, |p| *p == 0).unwrap();
        *permits -= 1;
    }

    pub fn release(&self) {
        let mut permits = self.permits.lock().unwrap();
        if *permits < self.initial_permits {
            *permits += 1;
            self.condvar.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_semaphore_basic_acquire_release() {
        let semaphore = Semaphore::new(2);

        semaphore.acquire();
        semaphore.acquire();

        semaphore.release();
        semaphore.acquire();
    }

    #[test]
    fn test_semaphore_release_more_than_initial() {
        let semaphore = Semaphore::new(1);

        semaphore.release();
        semaphore.release();
        assert_eq!(*semaphore.permits.lock().unwrap(), 1);
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
