use crate::base_adapter::{AdapterType, AdapterTyping};
use crate::columns::StdColumn;
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use crate::funcs::execute_macro;
use crate::metadata::*;
use crate::postgres::relation::PostgresRelation;
use crate::relation_object::RelationObject;
use crate::sql_engine::SqlEngine;
use crate::typed_adapter::TypedBaseAdapter;
use arrow::array::{Array, StringArray};
use dbt_schemas::dbt_types::RelationType;
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
    engine: Arc<SqlEngine>,
}

impl PostgresAdapter {
    pub fn new(engine: Arc<SqlEngine>) -> Self {
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

    fn engine(&self) -> &Arc<SqlEngine> {
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

    fn quote(&self, _state: &State, identifier: &str) -> AdapterResult<String> {
        Ok(format!("\"{identifier}\""))
    }

    // reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-postgres/src/dbt/include/postgres/macros/adapters.sql#L63
    fn get_columns_in_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Vec<StdColumn>> {
        let result = execute_macro(
            state,
            &[RelationObject::new(relation).as_value()],
            "get_columns_in_relation",
        )?;
        Ok(StdColumn::vec_from_jinja_value(
            AdapterType::Postgres,
            result,
        )?)
    }

    // reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-postgres/src/dbt/include/postgres/macros/adapters.sql#L85
    fn get_relation(
        &self,
        state: &State,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
        let query_schema = if self.quoting().schema {
            schema.to_string()
        } else {
            schema.to_lowercase()
        };

        let query_identifier = if self.quoting().identifier {
            identifier.to_string()
        } else {
            identifier.to_lowercase()
        };

        let sql = format!(
            r#"
            select 'table' as type
            from pg_tables
            where schemaname = '{query_schema}'
              and tablename = '{query_identifier}'
            union all
            select 'view' as type
            from pg_views
            where schemaname = '{query_schema}'
              and viewname = '{query_identifier}'
            union all
            select 'materialized_view' as type
            from pg_matviews
            where schemaname = '{query_schema}'
              and matviewname = '{query_identifier}'
            "#,
        );

        let batch = self.engine.execute(Some(state), conn, ctx, &sql)?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let column = batch.column_by_name("type").unwrap();
        let string_array = column.as_any().downcast_ref::<StringArray>().unwrap();

        if string_array.len() != 1 {
            return Err(AdapterError::new(
                AdapterErrorKind::UnexpectedResult,
                "Did not find 'type' for a relation",
            ));
        }

        let relation_type = match string_array.value(0) {
            "table" => Some(RelationType::Table),
            "view" => Some(RelationType::View),
            "materialized_view" => Some(RelationType::MaterializedView),
            _ => return invalid_value!("Unsupported relation type {}", string_array.value(0)),
        };

        let relation = PostgresRelation::try_new(
            Some(database.to_string()),
            Some(schema.to_string()),
            Some(identifier.to_string()),
            relation_type,
            self.quoting(),
        )?;
        Ok(Some(Arc::new(relation)))
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
