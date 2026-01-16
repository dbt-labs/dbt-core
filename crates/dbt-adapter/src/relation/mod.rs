//! Relation and RelationConfig implementations for different data warehouses.

pub(crate) mod config;
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
    create_relation_from_node, do_create_relation,
};

pub(crate) mod config_v2;

#[cfg(test)]
pub(crate) mod test_helpers;
