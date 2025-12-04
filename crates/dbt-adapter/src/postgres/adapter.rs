use crate::adapter_engine::AdapterEngine;
use crate::base_adapter::{AdapterType, AdapterTyping};
use crate::column::Column;
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use crate::funcs::execute_macro;
use crate::metadata::*;
use crate::relation_object::RelationObject;
use crate::typed_adapter::TypedBaseAdapter;
use dbt_schemas::schemas::common::{ConstraintSupport, ConstraintType};
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_xdbc::{Connection, QueryCtx};

use minijinja::{State, Value};
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
    // TODO: add_query does not appear to be necessary (few uses in
    // macros) and should be removed and replaced with `execute`.
    #[allow(clippy::too_many_arguments)]
    fn add_query(
        &self,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        sql: &str,
        auto_begin: bool,
        _bindings: Option<&Value>,
        _abridge_sql_log: bool,
    ) -> AdapterResult<()> {
        let _ = self.execute_inner(
            self.adapter_type().into(),
            self.engine.clone(),
            None,
            conn,
            ctx,
            sql,
            auto_begin,
            false, // default for fetch as in dispatch_adapter_calls()
            None,
            None,
        )?;
        Ok(())
    }

    // reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-postgres/src/dbt/include/postgres/macros/adapters.sql#L63
    fn get_columns_in_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Vec<Column>> {
        let result = execute_macro(
            state,
            &[RelationObject::new(relation).as_value()],
            "get_columns_in_relation",
        )?;
        Ok(Column::vec_from_jinja_value(AdapterType::Postgres, result)?)
    }

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

impl fmt::Display for PostgresAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgresAdapter({})", self.adapter_type())
    }
}
