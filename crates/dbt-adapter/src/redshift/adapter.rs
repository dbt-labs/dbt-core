use crate::adapter_engine::AdapterEngine;
use crate::base_adapter::AdapterTyping;
use crate::column::Column;
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use crate::funcs::execute_macro;
use crate::metadata::*;
use crate::redshift::relation::RedshiftRelation;
use crate::relation_object::RelationObject;
use crate::typed_adapter::TypedBaseAdapter;
use arrow::array::{Array, StringArray};
use dbt_common::adapter::AdapterType;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::{ConstraintSupport, ConstraintType};
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_xdbc::{Connection, QueryCtx};
use minijinja::{State, Value};

use std::borrow::Cow;
use std::fmt;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

/// An adapter for interacting with Redshift.
#[derive(Clone)]
pub struct RedshiftAdapter {
    engine: Arc<AdapterEngine>,
}

impl Debug for RedshiftAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.adapter_type())
    }
}

impl RedshiftAdapter {
    pub fn new(engine: Arc<AdapterEngine>) -> Self {
        Self { engine }
    }
}

impl AdapterTyping for RedshiftAdapter {
    fn as_metadata_adapter(&self) -> Option<&dyn MetadataAdapter> {
        Some(self)
    }

    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        self
    }

    fn engine(&self) -> &Arc<AdapterEngine> {
        &self.engine
    }
}

impl TypedBaseAdapter for RedshiftAdapter {
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
        self.execute_inner(
            self.adapter_type().into(),
            self.engine.clone(),
            None,
            conn,
            ctx,
            sql,
            auto_begin,
            false,
            None,
            None,
        )?;
        Ok(())
    }

    fn quote(&self, _state: &State, identifier: &str) -> AdapterResult<String> {
        Ok(format!("\"{identifier}\""))
    }

    fn verify_database(&self, database: String) -> AdapterResult<Value> {
        let ra3_node = self
            .engine
            .config("ra3_node")
            .unwrap_or(Cow::Borrowed("false"));

        // We have no guarantees that `database` is unquoted, but we do know that `configured_database` will be unquoted.
        // For the Redshift adapter, we can just trim the `"` character per `self.quote`.
        let database = database.trim_matches('\"');
        let configured_database = self.engine.config("database");

        if let Some(configured_database) = configured_database {
            let ra3_node: bool = FromStr::from_str(&ra3_node).map_err(|_| {
                AdapterError::new(
                    AdapterErrorKind::Configuration,
                    r#"Failed to parse ra3_node, expected "true" or "false""#,
                )
            })?;
            if !database.eq_ignore_ascii_case(&configured_database) && !ra3_node {
                return Err(AdapterError::new(
                    AdapterErrorKind::UnexpectedDbReference,
                    format!(
                        "Cross-db references allowed only in RA3.* node ({database} vs {configured_database})"
                    ),
                ));
            }
        }

        Ok(Value::from(()))
    }

    fn get_relation(
        &self,
        state: &State,
        ctx: &QueryCtx,
        conn: &mut dyn Connection,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
        let query_schema = if self.quoting().schema {
            schema.to_string()
        } else {
            schema.to_lowercase()
        };

        // determine table, view, or materialized view
        let sql = format!(
            "WITH materialized_views AS (
    SELECT TRIM(name) AS object_name, 'materialized_view'::text AS object_type
    FROM svv_mv_info
    WHERE TRIM(schema_name) ILIKE '{query_schema}'
        AND TRIM(name) ILIKE '{identifier}'
),
all_objects AS (
    SELECT table_name AS object_name,
        CASE 
            WHEN table_type ILIKE 'BASE TABLE' THEN 'table'::text
            WHEN table_type ILIKE 'VIEW' THEN 'view'::text
            ELSE 'table'
        END AS object_type
    FROM svv_tables
    WHERE table_schema ILIKE '{query_schema}'
        AND table_name ILIKE '{identifier}'
)
SELECT
    COALESCE(mv.object_name, ao.object_name) AS object_name,
    COALESCE(mv.object_type, ao.object_type) AS object_type
FROM all_objects ao
LEFT JOIN materialized_views mv
    ON ao.object_name = mv.object_name"
        );

        let batch = self.engine.execute(Some(state), conn, ctx, &sql)?;

        if batch.num_rows() == 0 {
            // If there are no rows, then we did not find the object
            return Ok(None);
        }

        let column = batch.column_by_name("object_type").unwrap();
        let string_array = column.as_any().downcast_ref::<StringArray>().unwrap();

        if string_array.len() != 1 {
            return Err(AdapterError::new(
                AdapterErrorKind::UnexpectedResult,
                "Did not find 'object_type' for a relation",
            ));
        }

        let relation_type_name = string_array.value(0).to_lowercase();
        let relation_type = match relation_type_name.as_str() {
            "table" => Some(RelationType::Table),
            "view" => Some(RelationType::View),
            "materialized_view" => Some(RelationType::MaterializedView),
            _ => None,
        };

        Ok(Some(Arc::new(RedshiftRelation::new(
            Some(database.to_string()),
            Some(schema.to_string()),
            Some(identifier.to_string()),
            relation_type,
            None,
            self.quoting(),
        ))))
    }

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
        Ok(Column::vec_from_jinja_value(AdapterType::Redshift, result)?)
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-redshift/src/dbt/adapters/redshift/impl.py#L53
    fn get_constraint_support(&self, ct: ConstraintType) -> ConstraintSupport {
        match ct {
            ConstraintType::Check => ConstraintSupport::NotSupported,
            ConstraintType::NotNull => ConstraintSupport::Enforced,
            ConstraintType::Unique | ConstraintType::PrimaryKey | ConstraintType::ForeignKey => {
                ConstraintSupport::NotEnforced
            }
            _ => ConstraintSupport::NotSupported,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::AdapterConfig;
    use crate::query_comment::QueryCommentConfig;
    use crate::sql_types::NaiveTypeOpsImpl;
    use crate::stmt_splitter::NaiveStmtSplitter;
    use dbt_auth::auth_for_backend;
    use dbt_common::adapter::AdapterType;
    use dbt_common::cancellation::never_cancels;
    use dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING;
    use dbt_serde_yaml::Mapping;
    use dbt_xdbc::Backend;

    fn engine() -> Arc<AdapterEngine> {
        let config = Mapping::new();
        let auth = auth_for_backend(Backend::Redshift);
        AdapterEngine::new(
            AdapterType::Redshift,
            auth.into(),
            AdapterConfig::new(config),
            DEFAULT_RESOLVED_QUOTING,
            Arc::new(NaiveStmtSplitter),
            None,
            QueryCommentConfig::from_query_comment(None, AdapterType::Redshift, false),
            Box::new(NaiveTypeOpsImpl::new(AdapterType::Redshift)), // XXX: NaiveTypeOpsImpl
            never_cancels(),
        )
    }

    #[test]
    fn test_quote() {
        let adapter = RedshiftAdapter::new(engine());
        let env = minijinja::Environment::new();
        let state = State::new_for_env(&env);
        assert_eq!(adapter.quote(&state, "abc").unwrap(), "\"abc\"");
    }
}
