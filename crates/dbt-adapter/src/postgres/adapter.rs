use crate::adapter_engine::AdapterEngine;
use crate::base_adapter::AdapterTyping;
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use crate::metadata::*;
use crate::typed_adapter::TypedBaseAdapter;
use dbt_schemas::schemas::common::{ConstraintSupport, ConstraintType};

use minijinja::Value;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

/// An adapter for Postgres.
#[derive(Clone)]
pub struct PostgresAdapter {
    engine: Arc<AdapterEngine>,
}

impl PostgresAdapter {
    pub fn new(engine: Arc<AdapterEngine>) -> Self {
        Self { engine }
    }
}

impl AdapterTyping for PostgresAdapter {
    fn as_metadata_adapter(&self) -> Option<&dyn MetadataAdapter> {
        None // TODO: implement metadata_adapter() for PostgresAdapter
    }

    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        self
    }

    fn engine(&self) -> &Arc<AdapterEngine> {
        &self.engine
    }
}

impl Debug for PostgresAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.adapter_type())
    }
}

impl TypedBaseAdapter for PostgresAdapter {
    fn verify_database(&self, database: String) -> AdapterResult<Value> {
        let configured_database = self.engine.get_configured_database_name();
        if let Some(configured_database) = configured_database {
            if database != configured_database {
                Err(AdapterError::new(
                    AdapterErrorKind::UnexpectedDbReference,
                    format!(
                        "Cross-db references not allowed in the {} adapter ({} vs {})",
                        self.adapter_type(),
                        database,
                        configured_database
                    ),
                ))
            } else {
                Ok(Value::from(()))
            }
        } else {
            // Replay engine does not have a configured database
            Ok(Value::from(()))
        }
    }

    fn get_constraint_support(&self, ct: ConstraintType) -> ConstraintSupport {
        match ct {
            ConstraintType::Check => ConstraintSupport::NotSupported,
            ConstraintType::NotNull | ConstraintType::ForeignKey => ConstraintSupport::Enforced,
            ConstraintType::Unique | ConstraintType::PrimaryKey => ConstraintSupport::NotEnforced,
            _ => ConstraintSupport::NotSupported,
        }
    }
}
