/// Name of the [Statement] option that carries the dbt node unique ID.
pub const DBT_NODE_ID: &str = "dbt.node_id";
/// Name of the [Statement] option that carries the dbt execution phase.
pub const DBT_EXECUTION_PHASE: &str = "dbt.execution_phase";
/// Name of the [Statement] option that carries whether the query is for metadata fetch (schema hydration).
pub const DBT_METADATA: &str = "dbt.metadata";
/// Name of the [Statement] option that carries whether the caller expects results
/// (`fetch=true` means a read/SELECT; `fetch=false` typically means DDL/DML).
pub const DBT_FETCH: &str = "dbt.fetch";
