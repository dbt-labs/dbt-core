// This module keep track of error and warning counters.
// It uses `DashMap` for concurrent access
// and `AtomicUsize` for atomic counter increments.

// To allow for running many fs calls in parallel,
// we use dbt's invocation_id as key for the counters.

use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::LazyLock;

pub static ERROR_COUNTERS: LazyLock<DashMap<String, AtomicUsize>> = LazyLock::new(DashMap::new);
pub static WARNING_COUNTERS: LazyLock<DashMap<String, AtomicUsize>> = LazyLock::new(DashMap::new);
pub static AUTOFIX_SUGGESTION_COUNTERS: LazyLock<DashMap<String, AtomicUsize>> =
    LazyLock::new(DashMap::new);

/// Increments the error counter for the given key.
pub fn increment_error_counter(key: &str) {
    let counter = ERROR_COUNTERS
        .entry(key.to_string())
        .or_insert_with(|| AtomicUsize::new(0));
    counter.fetch_add(1, Ordering::SeqCst);
}
/// Increments the error counter for the given key.
pub fn increment_warning_counter(key: &str) {
    let counter = WARNING_COUNTERS
        .entry(key.to_string())
        .or_insert_with(|| AtomicUsize::new(0));
    counter.fetch_add(1, Ordering::SeqCst);
}

/// Increments the autofix suggestion counter for the given key.
pub fn increment_autofix_suggestion_counter(key: &str) {
    let counter = AUTOFIX_SUGGESTION_COUNTERS
        .entry(key.to_string())
        .or_insert_with(|| AtomicUsize::new(0));
    counter.fetch_add(1, Ordering::SeqCst);
}

/// Returns the error counter for the given key.
pub fn get_error_counter(key: &str) -> usize {
    ERROR_COUNTERS
        .get(key)
        .map(|counter| counter.load(Ordering::SeqCst))
        .unwrap_or(0)
}
/// Returns the warning counter for the given key.
pub fn get_warning_counter(key: &str) -> usize {
    WARNING_COUNTERS
        .get(key)
        .map(|counter| counter.load(Ordering::SeqCst))
        .unwrap_or(0)
}

pub fn get_autofix_suggestion_counter(key: &str) -> usize {
    AUTOFIX_SUGGESTION_COUNTERS
        .get(key)
        .map(|counter| counter.load(Ordering::SeqCst))
        .unwrap_or(0)
}
