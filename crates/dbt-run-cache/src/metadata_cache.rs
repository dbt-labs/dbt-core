//! Short-lived warehouse metadata cache for dbt State service request assembly.
//!
//! The cache is keyed by rendered relation name and stores only metadata that is
//! expensive or redundant to fetch while constructing service payloads. Failed
//! metadata lookups are deliberately not cached so callers can fail open and
//! retry later in the same invocation.

use std::fmt::Display;
use std::future::Future;
use std::time::{Duration, Instant};

use dashmap::DashMap;

#[derive(Debug, Default)]
pub struct RunCacheMetadataCache {
    ttl: Option<Duration>,
    relation_exists: DashMap<String, TimedEntry<bool>>,
    last_modified_epochs: DashMap<String, TimedEntry<Option<i64>>>,
    lookup_errors: DashMap<String, TimedEntry<String>>,
}

#[derive(Clone, Debug)]
struct TimedEntry<T> {
    value: T,
    fetched_at: Instant,
}

impl<T> TimedEntry<T> {
    fn new(value: T) -> Self {
        Self {
            value,
            fetched_at: Instant::now(),
        }
    }

    fn is_expired(&self, ttl: Option<Duration>) -> bool {
        ttl.is_some_and(|ttl| self.fetched_at.elapsed() > ttl)
    }
}

impl RunCacheMetadataCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl: Some(ttl),
            ..Self::default()
        }
    }

    pub fn with_ttl_seconds(ttl_seconds: i64) -> Self {
        if ttl_seconds <= 0 {
            Self::new()
        } else {
            Self::with_ttl(Duration::from_secs(ttl_seconds as u64))
        }
    }

    pub fn relation_exists(&self, relation: &str) -> Option<bool> {
        get_cached(&self.relation_exists, relation, self.ttl)
    }

    pub fn last_modified_epoch(&self, relation: &str) -> Option<Option<i64>> {
        get_cached(&self.last_modified_epochs, relation, self.ttl)
    }

    pub fn lookup_error(&self, lookup: &str) -> Option<String> {
        get_cached(&self.lookup_errors, lookup, self.ttl)
    }

    pub fn insert_relation_exists(&self, relation: impl Into<String>, exists: bool) {
        let relation = relation.into();
        self.remove_lookup_error(&lookup_error_key("relation_exists", &relation));
        self.relation_exists
            .insert(relation, TimedEntry::new(exists));
    }

    pub fn insert_last_modified_epoch(&self, relation: impl Into<String>, epoch: Option<i64>) {
        let relation = relation.into();
        self.remove_lookup_error(&lookup_error_key("last_modified_epoch", &relation));
        self.last_modified_epochs
            .insert(relation, TimedEntry::new(epoch));
    }

    pub fn remove_last_modified_epoch(&self, relation: &str) {
        self.last_modified_epochs.remove(relation);
    }

    pub fn insert_lookup_error(&self, lookup: impl Into<String>, error: impl Into<String>) {
        self.lookup_errors
            .insert(lookup.into(), TimedEntry::new(error.into()));
    }

    pub fn remove_lookup_error(&self, lookup: &str) {
        self.lookup_errors.remove(lookup);
    }

    pub fn invalidate_relation_metadata(&self, relation: &str) {
        self.relation_exists.remove(relation);
        self.last_modified_epochs.remove(relation);
        self.lookup_errors
            .remove(&format!("relation_exists:{relation}"));
        self.lookup_errors
            .remove(&format!("last_modified_epoch:{relation}"));
    }

    pub async fn get_or_try_insert_relation_exists<E, F, Fut>(
        &self,
        relation: &str,
        fetch: F,
    ) -> Result<bool, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<bool, E>>,
        E: Display,
    {
        get_or_try_insert(
            &self.relation_exists,
            &self.lookup_errors,
            self.ttl,
            "relation_exists",
            relation,
            fetch,
        )
        .await
    }

    pub async fn get_or_try_insert_last_modified_epoch<E, F, Fut>(
        &self,
        relation: &str,
        fetch: F,
    ) -> Result<Option<i64>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Option<i64>, E>>,
        E: Display,
    {
        get_or_try_insert(
            &self.last_modified_epochs,
            &self.lookup_errors,
            self.ttl,
            "last_modified_epoch",
            relation,
            fetch,
        )
        .await
    }

    pub fn clear(&self) {
        self.relation_exists.clear();
        self.last_modified_epochs.clear();
        self.lookup_errors.clear();
    }
}

async fn get_or_try_insert<T, E, F, Fut>(
    map: &DashMap<String, TimedEntry<T>>,
    errors: &DashMap<String, TimedEntry<String>>,
    ttl: Option<Duration>,
    lookup_kind: &str,
    key: &str,
    fetch: F,
) -> Result<T, E>
where
    T: Clone,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Display,
{
    if let Some(value) = get_cached(map, key, ttl) {
        return Ok(value);
    }

    let value = match fetch().await {
        Ok(value) => value,
        Err(error) => {
            errors.insert(
                lookup_error_key(lookup_kind, key),
                TimedEntry::new(error.to_string()),
            );
            return Err(error);
        }
    };
    errors.remove(&lookup_error_key(lookup_kind, key));
    map.insert(key.to_string(), TimedEntry::new(value.clone()));
    Ok(value)
}

fn get_cached<T: Clone>(
    map: &DashMap<String, TimedEntry<T>>,
    key: &str,
    ttl: Option<Duration>,
) -> Option<T> {
    if let Some(value) = map.get(key) {
        if value.is_expired(ttl) {
            let fetched_at = value.fetched_at;
            drop(value);
            map.remove_if(key, |_, value| value.fetched_at == fetched_at);
            None
        } else {
            Some(value.value.clone())
        }
    } else {
        None
    }
}

fn lookup_error_key(kind: &str, relation: &str) -> String {
    format!("{kind}:{relation}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration as TokioDuration, sleep};

    #[tokio::test]
    async fn relation_exists_lookup_caches_success() {
        let cache = RunCacheMetadataCache::new();
        let calls = Arc::new(AtomicUsize::new(0));

        let first = cache
            .get_or_try_insert_relation_exists("analytics.orders", {
                let calls = Arc::clone(&calls);
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, &'static str>(true)
                }
            })
            .await
            .unwrap();
        let second = cache
            .get_or_try_insert_relation_exists("analytics.orders", || async {
                Ok::<_, &'static str>(false)
            })
            .await
            .unwrap();

        assert!(first);
        assert!(second);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_lookup_is_not_cached_for_fail_open_callers() {
        let cache = RunCacheMetadataCache::new();

        let err = cache
            .get_or_try_insert_last_modified_epoch("analytics.orders", || async {
                Err::<Option<i64>, _>("warehouse metadata unavailable")
            })
            .await
            .unwrap_err();

        assert_eq!(err, "warehouse metadata unavailable");
        assert_eq!(cache.last_modified_epoch("analytics.orders"), None);
        assert_eq!(
            cache.lookup_error("last_modified_epoch:analytics.orders"),
            Some("warehouse metadata unavailable".to_string())
        );

        let epoch = cache
            .get_or_try_insert_last_modified_epoch("analytics.orders", || async {
                Ok::<_, &'static str>(Some(123))
            })
            .await
            .unwrap();

        assert_eq!(epoch, Some(123));
        assert_eq!(
            cache.last_modified_epoch("analytics.orders"),
            Some(Some(123))
        );
        assert_eq!(
            cache.lookup_error("last_modified_epoch:analytics.orders"),
            None
        );
    }

    #[tokio::test]
    async fn ttl_expiry_refreshes_cached_values() {
        let cache = RunCacheMetadataCache::with_ttl(Duration::from_millis(5));
        let calls = Arc::new(AtomicUsize::new(0));

        let first = cache
            .get_or_try_insert_relation_exists("analytics.orders", {
                let calls = Arc::clone(&calls);
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, &'static str>(true)
                }
            })
            .await
            .unwrap();
        assert!(first);

        sleep(TokioDuration::from_millis(10)).await;

        let second = cache
            .get_or_try_insert_relation_exists("analytics.orders", {
                let calls = Arc::clone(&calls);
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, &'static str>(false)
                }
            })
            .await
            .unwrap();

        assert!(!second);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn ttl_expiry_refreshes_lookup_errors() {
        let cache = RunCacheMetadataCache::with_ttl(Duration::from_millis(5));

        let err = cache
            .get_or_try_insert_relation_exists("analytics.orders", || async {
                Err::<bool, _>("metadata unavailable")
            })
            .await
            .unwrap_err();
        assert_eq!(err, "metadata unavailable");
        assert_eq!(
            cache.lookup_error("relation_exists:analytics.orders"),
            Some("metadata unavailable".to_string())
        );

        sleep(TokioDuration::from_millis(10)).await;

        assert_eq!(cache.lookup_error("relation_exists:analytics.orders"), None);
    }

    #[test]
    fn lookup_errors_can_be_cleared_after_later_success() {
        let cache = RunCacheMetadataCache::new();

        cache.insert_lookup_error("custom_lookup:analytics.raw.orders", "metadata unavailable");
        assert_eq!(
            cache.lookup_error("custom_lookup:analytics.raw.orders"),
            Some("metadata unavailable".to_string())
        );

        cache.remove_lookup_error("custom_lookup:analytics.raw.orders");
        assert_eq!(
            cache.lookup_error("custom_lookup:analytics.raw.orders"),
            None
        );
    }

    #[test]
    fn direct_success_inserts_clear_lookup_errors() {
        let cache = RunCacheMetadataCache::new();

        cache.insert_lookup_error("relation_exists:analytics.orders", "metadata unavailable");
        cache.insert_relation_exists("analytics.orders", true);
        assert_eq!(cache.lookup_error("relation_exists:analytics.orders"), None);

        cache.insert_lookup_error(
            "last_modified_epoch:analytics.orders",
            "metadata unavailable",
        );
        cache.insert_last_modified_epoch("analytics.orders", Some(123));
        assert_eq!(
            cache.lookup_error("last_modified_epoch:analytics.orders"),
            None
        );
    }

    #[test]
    fn relation_metadata_can_be_invalidated_after_relation_changes() {
        let cache = RunCacheMetadataCache::new();

        cache.insert_relation_exists("analytics.orders", false);
        cache.insert_last_modified_epoch("analytics.orders", None);
        cache.insert_lookup_error("relation_exists:analytics.orders", "missing");
        cache.insert_lookup_error("last_modified_epoch:analytics.orders", "missing");

        cache.invalidate_relation_metadata("analytics.orders");

        assert_eq!(cache.relation_exists("analytics.orders"), None);
        assert_eq!(cache.last_modified_epoch("analytics.orders"), None);
        assert_eq!(cache.lookup_error("relation_exists:analytics.orders"), None);
        assert_eq!(
            cache.lookup_error("last_modified_epoch:analytics.orders"),
            None
        );
    }
}
