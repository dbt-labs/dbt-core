use std::sync::Arc;

use arrow_schema::Schema;

/// Schema registry access interface.
pub trait SchemaRegistry: Send + Sync {
    /// Get the schema of a table by its unique identifier.
    fn get_schema(&self, unique_id: &str) -> Option<Arc<Schema>>;

    /// Get the schema of a table by its fully-qualified name (FQN).
    fn get_schema_by_fqn(&self, fqn: &str) -> Option<Arc<Schema>>;
}
