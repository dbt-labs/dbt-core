pub mod base;
pub mod column_comments;
pub mod column_tags;
pub mod comment;
pub mod constraints;
pub mod liquid_clustering;
pub mod partitioning;
pub mod query;
pub mod refresh;
pub mod tags;
pub mod tblproperties;
pub mod typed_constraint;

pub(crate) mod configs;
pub use configs::*;

// NOTE: this is currently being tested, eventually we'll flip the actual code to use these
#[cfg(test)]
pub mod config_v2;

mod relation;
pub use relation::{DEFAULT_DATABRICKS_DATABASE, INFORMATION_SCHEMA_SCHEMA, SYSTEM_DATABASE};
pub use relation::{DatabricksRelation, DatabricksRelationType};

pub mod relation_api;
