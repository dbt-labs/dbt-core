use crate::dashmap::DashMap;
use std::sync::OnceLock;

pub struct RelationRegistry<V> {
    map: OnceLock<DashMap<String, V>>,
}

impl<V> RelationRegistry<V> {
    pub const fn new() -> Self {
        Self {
            map: OnceLock::new(),
        }
    }

    fn map(&self) -> &DashMap<String, V> {
        self.map.get_or_init(crate::dashmap::new)
    }

    pub fn register(&self, relation: &str, value: V) {
        self.map().insert(
            dbt_frontend_common::utils::canonicalize_relation_name(relation),
            value,
        );
    }

    pub fn lookup(&self, relation: &str) -> Option<V>
    where
        V: Clone,
    {
        self.map()
            .get(&dbt_frontend_common::utils::canonicalize_relation_name(
                relation,
            ))
            .map(|entry| entry.value().clone())
    }
}
