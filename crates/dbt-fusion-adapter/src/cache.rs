use std::sync::Arc;

use dashmap::DashMap;
use dbt_schemas::schemas::relations::{base::BaseRelation, relation_configs::BaseRelationConfig};

use crate::metadata::{CatalogAndSchema, RelationVec};

type RelationCacheKey = String;
/// Represents a [BaseRelation] and any associated [BaseRelationConfig] if available
/// This struct represents any value inside a [RelationCache]
#[derive(Debug, Clone)]
pub struct RelationCacheEntry {
    /// Timestamp in milliseconds when this cache entry was created
    created_at: u128,
    relation: Arc<dyn BaseRelation>,
    relation_config: Option<Arc<dyn BaseRelationConfig>>,
}

impl RelationCacheEntry {
    /// Used to create a new [RelationCacheEntry] with the given [Arc<dyn BaseRelation>] and [Arc<dyn BaseRelationConfig>]
    pub fn new(
        relation: Arc<dyn BaseRelation>,
        relation_config: Option<Arc<dyn BaseRelationConfig>>,
    ) -> Self {
        let created_at = std::time::UNIX_EPOCH
            .elapsed()
            .map(|d| d.as_millis())
            .unwrap_or(0);
        Self {
            created_at,
            relation,
            relation_config,
        }
    }

    /// Gets a reference to the [BaseRelation]
    pub fn relation(&self) -> Arc<dyn BaseRelation> {
        self.relation.clone()
    }

    /// Gets a reference to the [BaseRelationConfig], if available
    pub fn relation_config(&self) -> Option<Arc<dyn BaseRelationConfig>> {
        self.relation_config.clone()
    }
}

#[derive(Debug, Clone, Default)]
struct SchemaEntry {
    relations: DashMap<RelationCacheKey, RelationCacheEntry>,
    // Tracks whether or not we have complete knowledge of this schema
    is_complete: bool,
    // Timestamp when this schema was cached (for complete schemas)
    cached_at: u128,
}

/// A dialect agnostic cache of [RelationCacheEntry]
///
/// # Example
/// ```rust
/// use std::sync::Arc;
/// use dbt_schemas::schemas::relations::base::BaseRelation;
/// use dbt_fusion_adapter::cache::{RelationCache, RelationCacheEntry};
///
/// let cache = RelationCache::new();
/// let relation: Arc<dyn BaseRelation> = // ... some relation
///
/// // Insert relation into cache
/// cache.insert_relation(relation.clone());
///
/// // Retrieve cached relation
/// let cached: Option<RelationCacheEntry> = cache.get_relation(relation);
/// ```
#[derive(Debug, Clone, Default)]
pub struct RelationCache {
    // This structure loosely represents remote warehouse state
    // Outer key represents a database schema
    //
    // Schema Key -> SchemaeEntry
    //               Relation Key -> Cache Entry (Relation + Relation Config)
    // The inner key is a unique key generated from a relation's fully qualified name
    // We also differentiate using [SchemaEntry] to see what information we actually know about that schema
    schemas_and_relations: DashMap<String, SchemaEntry>,
}

impl RelationCache {
    /// Retrieves a cached entry by relation
    pub fn get_relation(&self, relation: &Arc<dyn BaseRelation>) -> Option<RelationCacheEntry> {
        let (schema_key, relation_key) = Self::get_relation_cache_keys(relation);
        if let Some(schema) = self.schemas_and_relations.get(&schema_key) {
            schema
                .relations
                .get(&relation_key)
                .map(|r| r.value().clone())
        } else {
            None
        }
    }

    /// Inserts a relation of [Arc<dyn BaseRelation] along with an optional <Arc<dyn BaseRelationConfig>> if applicable
    pub fn insert_relation(
        &self,
        relation: Arc<dyn BaseRelation>,
        relation_config: Option<Arc<dyn BaseRelationConfig>>,
    ) -> Option<RelationCacheEntry> {
        let (schema_key, relation_key) = Self::get_relation_cache_keys(&relation);
        let entry = RelationCacheEntry::new(relation, relation_config);
        self.schemas_and_relations
            .entry(schema_key)
            .or_default()
            .relations
            .insert(relation_key, entry)
    }

    /// Removes and returns a cached entry by key
    fn evict(&self, schema_key: &str, key: &str) -> Option<RelationCacheEntry> {
        // Schema Read Guard -> Inner Delete
        // We do not need to lock reads to the schema as the read guard is sufficient
        if let Some(relations) = self.schemas_and_relations.get(schema_key) {
            relations
                .value()
                .relations
                .remove(key)
                .map(|(_key, value)| value)
        } else {
            None
        }
    }

    /// Removes and returns a cached entry by relation
    pub fn evict_relation(&self, relation: &Arc<dyn BaseRelation>) -> Option<RelationCacheEntry> {
        let (schema_key, relation_key) = Self::get_relation_cache_keys(relation);
        self.evict(&schema_key, &relation_key)
    }

    /// Inserts a schema and its relations into the cache
    pub fn insert_schema(&self, schema: CatalogAndSchema, relations: RelationVec) {
        let cached_relations: DashMap<_, _> = relations
            .iter()
            .map(|r| {
                (
                    Self::get_relation_cache_key_from_relation(r),
                    RelationCacheEntry::new(r.clone(), None),
                )
            })
            .collect();

        let cached_at = std::time::UNIX_EPOCH
            .elapsed()
            .map(|d| d.as_millis())
            .unwrap_or(0);

        self.schemas_and_relations.insert(
            schema.to_string(),
            SchemaEntry {
                relations: cached_relations,
                is_complete: true,
                cached_at,
            },
        );
    }

    /// Drops an entire schema
    pub fn evict_schema_for_relation(&self, relation: &Arc<dyn BaseRelation>) {
        let schema_key = Self::get_schema_cache_key_from_relation(relation);
        self.schemas_and_relations.remove(&schema_key);
    }

    /// Checks if the entire schema was cached
    ///
    /// If relation provided does not contain catalog/database and schema information
    /// this function will always return false
    pub fn contains_full_schema_for_relation(&self, relation: &Arc<dyn BaseRelation>) -> bool {
        self.schemas_and_relations
            .get(&Self::get_schema_cache_key_from_relation(relation))
            .map(|entry| entry.is_complete)
            .unwrap_or(false)
    }

    /// Checks if the entire schema was cached
    pub fn contains_full_schema(&self, schema: &CatalogAndSchema) -> bool {
        self.schemas_and_relations
            .get(&schema.to_string())
            .map(|entry| entry.is_complete)
            .unwrap_or(false)
    }

    /// Checks if a relation exists in the cache
    pub fn contains_relation(&self, relation: &Arc<dyn BaseRelation>) -> bool {
        let (schema_key, relation_key) = Self::get_relation_cache_keys(relation);
        if let Some(relation_cache) = self.schemas_and_relations.get(&schema_key) {
            relation_cache.value().relations.contains_key(&relation_key)
        } else {
            false
        }
    }

    /// Renames a relation by updating its key while preserving its configuration
    /// Returns the new entry that was inserted
    pub fn rename_relation(
        &self,
        old: &Arc<dyn BaseRelation>,
        new: Arc<dyn BaseRelation>,
    ) -> Option<RelationCacheEntry> {
        if let Some(original_entry) = self.evict_relation(old) {
            self.insert_relation(new, original_entry.relation_config)
        } else {
            None
        }
    }

    /// Removes all entries from the cache
    pub fn clear(&self) {
        self.schemas_and_relations.clear();
    }

    /// Number of total relations cached
    pub fn num_relations(&self) -> usize {
        self.schemas_and_relations
            .iter()
            .map(|entry| entry.value().relations.len())
            .sum()
    }

    /// Number of total schemas cached
    pub fn num_schemas(&self) -> usize {
        self.schemas_and_relations
            .iter()
            .filter(|entry| entry.value().is_complete)
            .count()
    }

    /// Helper: Generates cache key pairs from a [BaseRelation]
    fn get_relation_cache_keys(relation: &Arc<dyn BaseRelation>) -> (String, String) {
        (
            Self::get_schema_cache_key_from_relation(relation),
            Self::get_relation_cache_key_from_relation(relation),
        )
    }

    /// Helper: Generates a relation cache key from a [BaseRelation]
    fn get_relation_cache_key_from_relation(relation: &Arc<dyn BaseRelation>) -> String {
        relation.semantic_fqn()
    }

    /// Helper: Generates a schema cache key from a [BaseRelation]
    fn get_schema_cache_key_from_relation(relation: &Arc<dyn BaseRelation>) -> String {
        CatalogAndSchema::from(relation).to_string()
    }

    fn log_final_state(&self) {
        use dbt_common::constants::CACHE_LOG;
        use std::fmt::Write;

        let total_schemas = self.schemas_and_relations.len();
        if total_schemas == 0 {
            return;
        }

        let mut buf = String::new();

        writeln!(&mut buf).unwrap();
        writeln!(&mut buf, "=== RelationCache Final State ===").unwrap();
        writeln!(&mut buf, "Total Schemas: {total_schemas}").unwrap();
        writeln!(&mut buf).unwrap();

        // Collect and sort schema entries for consistent output
        let mut schema_entries: Vec<_> = self.schemas_and_relations.iter().collect();
        schema_entries.sort_by(|a, b| a.key().cmp(b.key()));

        let mut complete_schemas = 0;
        let mut partial_schemas = 0;
        let mut total_relations = 0;

        for (idx, schema_entry) in schema_entries.iter().enumerate() {
            let schema_name = schema_entry.key();
            let entry = schema_entry.value();
            let relation_count = entry.relations.len();
            total_relations += relation_count;

            if entry.is_complete {
                complete_schemas += 1;

                writeln!(&mut buf, "[COMPLETE] SCHEMA").unwrap();
                writeln!(&mut buf, "   ╭─ Name: {schema_name}").unwrap();
                writeln!(&mut buf, "   ├─ Relations: {relation_count}").unwrap();

                if entry.cached_at > 0 {
                    writeln!(&mut buf, "   ├─ Cached: {}ms", entry.cached_at).unwrap();
                } else {
                    writeln!(&mut buf, "   ├─ Cached: unknown").unwrap();
                }
                writeln!(&mut buf, "   ╰─ Status: All relations cached as a batch").unwrap();
            } else {
                partial_schemas += 1;

                writeln!(&mut buf, "[PARTIAL]  SCHEMA").unwrap();
                writeln!(&mut buf, "   ╭─ Name: {schema_name}").unwrap();
                writeln!(
                    &mut buf,
                    "   ├─ Relations: {relation_count} (individually cached)"
                )
                .unwrap();
                writeln!(&mut buf, "   ├─ Individual Relations:").unwrap();

                let mut relations: Vec<_> = entry.relations.iter().collect();
                relations.sort_by(|a, b| a.key().cmp(b.key()));

                for (rel_idx, relation) in relations.iter().enumerate() {
                    let relation_key = relation.key();
                    let created_at = relation.value().created_at;
                    let is_last = rel_idx == relations.len() - 1;
                    let connector = if is_last { "╰" } else { "├" };

                    if created_at > 0 {
                        writeln!(
                            &mut buf,
                            "   │  {connector} • {relation_key} → {created_at}ms"
                        )
                        .unwrap();
                    } else {
                        writeln!(
                            &mut buf,
                            "   │  {connector} • {relation_key} → no timestamp"
                        )
                        .unwrap();
                    }
                }
                writeln!(&mut buf, "   ╰─ Status: Relations cached individually").unwrap();
            }

            if idx < schema_entries.len() - 1 {
                writeln!(&mut buf).unwrap();
            }
        }

        writeln!(&mut buf).unwrap();
        writeln!(&mut buf, "=== Summary ===").unwrap();
        writeln!(&mut buf, "Complete Schemas: {complete_schemas}").unwrap();
        writeln!(&mut buf, "Partial Schemas: {partial_schemas}").unwrap();
        writeln!(&mut buf, "Total Relations: {total_relations}").unwrap();

        log::debug!(target: CACHE_LOG, name = "CacheState"; "{buf}");
    }
}

impl Drop for RelationCache {
    fn drop(&mut self) {
        self.log_final_state();
    }
}

#[cfg(test)]
mod tests {
    use crate::AdapterType;

    use super::*;
    use dbt_schemas::schemas::{common::ResolvedQuoting, relations::DEFAULT_RESOLVED_QUOTING};

    #[test]
    fn test_different_key_creation() {
        use crate::relation_object::create_relation;

        let cache = RelationCache::default();

        // Create relations with different combinations of database, schema, identifier
        let relation1 = create_relation(
            AdapterType::Postgres,
            "db1".to_string(),
            "schema1".to_string(),
            Some("table1".to_string()),
            None,
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let relation2 = create_relation(
            AdapterType::Postgres,
            "db2".to_string(),
            "schema1".to_string(),
            Some("table1".to_string()),
            None,
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let relation3 = create_relation(
            AdapterType::Postgres,
            "db1".to_string(),
            "schema2".to_string(),
            Some("table1".to_string()),
            None,
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let relation4 = create_relation(
            AdapterType::Postgres,
            "db1".to_string(),
            "schema1".to_string(),
            Some("table2".to_string()),
            None,
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        let relation1_dup = create_relation(
            AdapterType::Postgres,
            "db1".to_string(),
            "schema1".to_string(),
            Some("table1".to_string()),
            None,
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        // Insert relations into cache
        cache.insert_relation(relation1.clone(), None);
        cache.insert_relation(relation2.clone(), None);
        cache.insert_relation(relation3.clone(), None);
        cache.insert_relation(relation4.clone(), None);
        cache.insert_relation(relation1_dup.clone(), None);

        // Verify all different relations are cached separately
        assert!(cache.contains_relation(&relation1));
        assert!(cache.contains_relation(&relation2));
        assert!(cache.contains_relation(&relation3));
        assert!(cache.contains_relation(&relation4));

        // Verify cache keys are different
        let key1 = RelationCache::get_relation_cache_key_from_relation(&relation1);
        let key2 = RelationCache::get_relation_cache_key_from_relation(&relation2);
        let key3 = RelationCache::get_relation_cache_key_from_relation(&relation3);
        let key4 = RelationCache::get_relation_cache_key_from_relation(&relation4);
        let key5 = RelationCache::get_relation_cache_key_from_relation(&relation1_dup);

        // Different relations should have different keys
        assert_ne!(key1, key2);
        assert_ne!(key1, key3);
        assert_ne!(key1, key4);
        assert_ne!(key2, key3);
        assert_ne!(key2, key4);
        assert_ne!(key3, key4);

        // Same relation should have same key
        assert_eq!(key1, key5);
    }

    #[test]
    fn test_quoting_policy_affects_cache_keys() {
        use crate::relation_object::create_relation;

        let cache = RelationCache::default();

        // With DEFAULT_RESOLVED_QUOTING
        let relation_quoted = create_relation(
            AdapterType::Postgres,
            "MyDB".to_string(),
            "MySchema".to_string(),
            Some("MyTable".to_string()),
            None,
            DEFAULT_RESOLVED_QUOTING,
        )
        .unwrap();

        // With no quoting
        let relation_unquoted = create_relation(
            AdapterType::Postgres,
            "MyDB".to_string(),
            "MySchema".to_string(),
            Some("MyTable".to_string()),
            None,
            ResolvedQuoting {
                database: false,
                schema: false,
                identifier: false,
            },
        )
        .unwrap();

        let key_quoted = RelationCache::get_relation_cache_key_from_relation(&relation_quoted);
        let key_unquoted = RelationCache::get_relation_cache_key_from_relation(&relation_unquoted);

        // Cache keys should be different due to quoting policy affecting normalization
        // This is intentional! Quoting enforces different semantics within dialects
        // Relations created with identical quoting configs should result in cache hits
        assert_ne!(key_quoted, key_unquoted);

        cache.insert_relation(relation_quoted.clone(), None);
        cache.insert_relation(relation_unquoted.clone(), None);

        // Both should exist as separate entries
        assert!(cache.contains_relation(&relation_quoted));
        assert!(cache.contains_relation(&relation_unquoted));

        // Test that we find the unquoted relation when searching with unquoted policy
        let search_relation_unquoted = create_relation(
            AdapterType::Postgres,
            "MyDB".to_string(),
            "MySchema".to_string(),
            Some("MyTable".to_string()),
            None,
            ResolvedQuoting {
                database: false,
                schema: false,
                identifier: false,
            },
        )
        .unwrap();

        let found_unquoted_entry = cache.get_relation(&search_relation_unquoted);
        assert!(found_unquoted_entry.is_some());
    }

    #[test]
    fn test_concurrent_mixed_operations_no_race_condition() {
        use crate::metadata::CatalogAndSchema;
        use crate::relation_object::create_relation;
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(RelationCache::default());
        let num_threads = 8;
        let operations_per_thread = 50;

        let relations: Vec<_> = (0..operations_per_thread)
            .flat_map(|i| {
                // Create relations in 3 different schemas
                (0..3).map(move |schema_id| {
                    create_relation(
                        AdapterType::Postgres,
                        "test_db".to_string(),
                        format!("schema_{schema_id}"),
                        Some(format!("table_{schema_id}_{i}")),
                        None,
                        DEFAULT_RESOLVED_QUOTING,
                    )
                    .unwrap()
                })
            })
            .collect();

        let relations = Arc::new(relations);

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let cache = cache.clone();
                let relations = relations.clone();

                thread::spawn(move || {
                    for i in 0..operations_per_thread {
                        let relation_idx =
                            (thread_id * operations_per_thread + i) % relations.len();
                        let relation = &relations[relation_idx];

                        match i % 7 {
                            0 => {
                                // Individual relation insert
                                cache.insert_relation(relation.clone(), None);
                            }
                            1 => {
                                // Individual relation evict
                                cache.evict_relation(relation);
                            }
                            2 => {
                                // Schema hydration
                                let schema = CatalogAndSchema::from(relation);
                                let schema_relations: Vec<_> = relations
                                    .iter()
                                    .filter(|r| CatalogAndSchema::from(*r) == schema)
                                    .cloned()
                                    .collect();
                                cache.insert_schema(schema, schema_relations);
                            }
                            3 => {
                                // Schema eviction
                                cache.evict_schema_for_relation(relation);
                            }
                            4 => {
                                // Read operations (most common in real usage)
                                cache.contains_relation(relation);
                                cache.get_relation(relation);
                            }
                            5 => {
                                // Schema checks
                                cache.contains_full_schema_for_relation(relation);
                            }
                            6 => {
                                // Rename operations (less common but important)
                                let new_relation = create_relation(
                                    AdapterType::Postgres,
                                    "test_db".to_string(),
                                    format!("schema_{}", thread_id % 3),
                                    Some(format!("renamed_table_{thread_id}_{i}")),
                                    None,
                                    DEFAULT_RESOLVED_QUOTING,
                                )
                                .unwrap();
                                cache.rename_relation(relation, new_relation);
                            }
                            _ => unreachable!(),
                        }

                        // Occasionally clear everything (stress test)
                        if i % 25 == 0 && thread_id == 0 {
                            cache.clear();
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify consistency after all operations
        for relation in relations.iter() {
            let contains = cache.contains_relation(relation);
            let get_result = cache.get_relation(relation);

            // consistency check: if contains says it exists, it must actually exist!
            if contains {
                assert!(
                    get_result.is_some(),
                    "Cache inconsistency: contains_relation=true but get_relation=None for relation: {:?}",
                    relation.semantic_fqn()
                );
            }

            // Schema-level consistency
            let schema_exists = cache.contains_full_schema_for_relation(relation);
            if schema_exists && contains {
                assert!(
                    get_result.is_some(),
                    "Schema exists and relation exists, but get_relation failed"
                );
            }
        }
        // Test survived all concurrent operations without panicking or corrupting
    }
}
