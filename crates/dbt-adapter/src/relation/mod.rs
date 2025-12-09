//! Relation and RelationConfig implementations for different data warehouses.
//!
mod config;
pub use config::{BaseRelationChangeSet, BaseRelationConfig, ComponentConfig, RelationChangeSet};

// Relation and RelationConfig for different data warehouses
pub mod bigquery;
pub mod databricks;
pub mod parse;
pub mod postgres;
pub mod redshift;
pub mod salesforce;
pub mod snowflake;

mod relation_object;
pub use relation_object::{
    RelationObject, StaticBaseRelation, StaticBaseRelationObject, create_relation,
    create_relation_from_node, create_relation_internal,
};
