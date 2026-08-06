use std::path::PathBuf;

use crate::relation_registry::RelationRegistry;

static REGISTRY: RelationRegistry<PathBuf> = RelationRegistry::new();

pub fn register(relation: &str, path: PathBuf) {
    REGISTRY.register(relation, path);
}

pub fn lookup(relation: &str) -> Option<PathBuf> {
    REGISTRY.lookup(relation)
}
