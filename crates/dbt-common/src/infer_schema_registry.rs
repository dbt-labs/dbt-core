use crate::relation_registry::RelationRegistry;

#[derive(Clone, Debug)]
pub struct InferredSchema {
    pub columns: Vec<String>,
    pub closed: bool,
    pub open_tail: Vec<String>,
}

static REGISTRY: RelationRegistry<InferredSchema> = RelationRegistry::new();

pub fn register(relation: &str, columns: &[String], closed: bool, open_tail: &[String]) {
    REGISTRY.register(
        relation,
        InferredSchema {
            columns: columns.to_vec(),
            closed,
            open_tail: open_tail.to_vec(),
        },
    );
}

pub fn lookup(relation: &str) -> Option<InferredSchema> {
    REGISTRY.lookup(relation)
}
