use crate::catalog_relation::CatalogRelation;
use crate::columns::StdColumn;
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use crate::funcs::execute_macro;
use crate::load_catalogs;
use crate::record_batch_utils::get_column_values;
use crate::relation_object::RelationObject;
use crate::snowflake::relation::SnowflakeRelation;
use crate::sql_engine::SqlEngine;
use crate::typed_adapter::TypedBaseAdapter;
use crate::{AdapterTyping, metadata::*};

use arrow::array::{Array as _, StringArray};
use dbt_agate::AgateTable;
use dbt_common::FsResult;
use dbt_common::adapter::AdapterType;
use dbt_common::behavior_flags::BehaviorFlag;
use dbt_common::unexpected_fs_err;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::{ConstraintSupport, ConstraintType};
use dbt_schemas::schemas::relations::base::{BaseRelation, TableFormat};
use dbt_xdbc::{Connection, QueryCtx};
use minijinja::{State, Value};

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

/// An adapter for interacting with Snowflake.
#[derive(Clone)]
pub struct SnowflakeAdapter {
    pub engine: Arc<SqlEngine>,
}

impl fmt::Debug for SnowflakeAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.adapter_type())
    }
}

impl SnowflakeAdapter {
    pub fn new(engine: Arc<SqlEngine>) -> Self {
        Self { engine }
    }
}

impl AdapterTyping for SnowflakeAdapter {
    fn as_metadata_adapter(&self) -> Option<&dyn MetadataAdapter> {
        Some(self)
    }
    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        self
    }

    fn engine(&self) -> &Arc<SqlEngine> {
        &self.engine
    }
}

impl TypedBaseAdapter for SnowflakeAdapter {
    fn use_warehouse<'bridge>(
        &self,
        conn: &'_ mut dyn Connection,
        warehouse: String,
        node_id: &str,
    ) -> FsResult<()> {
        let ctx = QueryCtx::default().with_node_id(node_id);
        let sql = format!("use warehouse {warehouse}");
        self.exec_stmt(&ctx, conn, &sql, false)?;
        Ok(())
    }

    fn restore_warehouse(&self, conn: &'_ mut dyn Connection, node_id: &str) -> FsResult<()> {
        let warehouse = self
            .get_db_config("warehouse")
            .ok_or_else(|| unexpected_fs_err!("'warehouse' not found in Snowflake DB config"))?;
        let ctx = QueryCtx::default().with_node_id(node_id);
        let sql = format!("use warehouse {warehouse}");
        self.exec_stmt(&ctx, conn, &sql, false)?;
        Ok(())
    }

    /// [reference](https://github.com/dbt-labs/dbt-adapters/blob/917301379d4ece300d32a3366c71daf0c4ac44aa/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L87)
    fn behavior(&self) -> Vec<BehaviorFlag> {
        let flag = BehaviorFlag::new(
            "enable_iceberg_materializations",
            false,
            Some(
                "Enabling Iceberg materializations introduces latency to metadata queries, specifically within the list_relations_without_caching macro. Since Iceberg benefits only those actively using it, we've made this behavior opt-in to prevent unnecessary latency for other users.",
            ),
            Some(
                r#"Enabling Iceberg materializations introduces latency to metadata queries,
specifically within the list_relations_without_caching macro. Since Iceberg
benefits only those actively using it, we've made this behavior opt-in to
prevent unnecessary latency for other users."#,
            ),
            Some(
                "https://docs.getdbt.com/reference/resource-configs/snowflake-configs#iceberg-table-format",
            ),
        );
        vec![flag]
    }

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
            false, // fetch
            None,
            None,
        )?;

        Ok(())
    }

    fn quote(&self, _state: &State, identifier: &str) -> AdapterResult<String> {
        Ok(format!("\"{identifier}\""))
    }

    // https://github.com/dbt-labs/dbt-adapters/blob/ace1709df001df4232a66f9d5f331a5fda4d3389/dbt-snowflake/src/dbt/include/snowflake/macros/adapters.sql#L138
    fn get_relation(
        &self,
        state: &State,
        ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
        let quoted_database = if self.quoting().database {
            self.quote(state, database)?
        } else {
            database.to_string()
        };
        let quoted_schema = if self.quoting().schema {
            self.quote(state, schema)?
        } else {
            schema.to_string()
        };
        let quoted_identifier = if self.quoting().identifier {
            identifier.to_string()
        } else {
            identifier.to_uppercase()
        };
        // this is a case-insenstive search
        let sql = format!(
            "show objects like '{quoted_identifier}' in schema {quoted_database}.{quoted_schema}"
        );

        let batch = match self.engine.execute(Some(state), conn, ctx, &sql) {
            Ok(b) => b,
            Err(e) => {
                // Previous versions of this code [1] checked the prefix of the error message
                // and looked for "002043 (02000)", but now we can compare the SQLSTATE and
                // vendor code directly.
                //
                // SQLSTATE "02000" means "no data" [1].
                // "002043" is the Snowflake code for "object does not exist or is not found".
                //
                // This error happens specifically when the specified DATABASE.SCHEMA does not exist.
                // If the schema does exist, then the query succeeds and will return zero or more rows.
                //
                // [1] https://github.com/dbt-labs/dbt-adapters/blob/5181389e4d4e2f9649026502bb685741a1c19a8e/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L259
                // [2] https://en.wikipedia.org/wiki/SQLSTATE
                if e.sqlstate() == "02000" && e.vendor_code().is_some_and(|code| code == 2043) {
                    return Ok(None);
                } else {
                    // Other errors should be propagated
                    return Err(e);
                }
            }
        };

        // Handle case where the query succeeds, but no rows are returned.
        // This happens when no objects are LIKE the specified identifier.
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let kind_column = get_column_values::<StringArray>(&batch, "kind")?;

        if kind_column.len() != 1 {
            return Err(AdapterError::new(
                AdapterErrorKind::UnexpectedResult,
                "Did not find expected column 'kind' in 'show objects' query result",
            ));
        }

        // Reference: https://github.com/dbt-labs/dbt-adapters/blob/61221f455f5960daf80024febfae6d6fb4b46251/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L309
        // TODO: We'll have to revisit this when iceberg gets implemented.
        let is_dynamic_column = get_column_values::<StringArray>(&batch, "is_dynamic")?;

        if is_dynamic_column.len() != 1 {
            return Err(AdapterError::new(
                AdapterErrorKind::UnexpectedResult,
                "Did not find expected column 'is_dynamic' in 'show objects' query result",
            ));
        }
        let is_dynamic = is_dynamic_column.value(0);

        let relation_type_name = kind_column.value(0);
        let relation_type = if relation_type_name.eq_ignore_ascii_case("table") {
            Some(snowflake::relation_type_from_table_flags(is_dynamic)?)
        } else if relation_type_name.eq_ignore_ascii_case("view") {
            Some(RelationType::View)
        } else {
            None
        };

        let is_iceberg_column = get_column_values::<StringArray>(&batch, "is_iceberg")?;

        if is_iceberg_column.len() != 1 {
            return Err(AdapterError::new(
                AdapterErrorKind::UnexpectedResult,
                "Did not find expected column 'is_iceberg' in 'show objects' query result",
            ));
        }

        let is_iceberg = is_iceberg_column.value(0);
        let table_format = if try_canonicalize_bool_column_field(is_iceberg)? {
            TableFormat::Iceberg
        } else {
            TableFormat::Default
        };

        Ok(Some(Arc::new(SnowflakeRelation::new(
            Some(database.to_string()),
            Some(schema.to_string()),
            Some(identifier.to_string()),
            relation_type,
            table_format,
            self.quoting(),
        ))))
    }

    fn build_catalog_relation(&self, model: &Value) -> AdapterResult<Value> {
        Ok(Value::from_object(
            CatalogRelation::from_model_config_and_catalogs(
                &self.adapter_type(),
                model,
                load_catalogs::fetch_catalogs(),
            )?,
        ))
    }

    /// reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L329-L330
    fn standardize_grants_dict(
        &self,
        grants_table: Arc<AgateTable>,
    ) -> AdapterResult<BTreeMap<String, Vec<String>>> {
        let record_batch = grants_table.original_record_batch();

        let grantee_cols = get_column_values::<StringArray>(&record_batch, "grantee_name")?;
        let granted_to_cols = get_column_values::<StringArray>(&record_batch, "granted_to")?;
        let privilege_cols = get_column_values::<StringArray>(&record_batch, "privilege")?;

        let mut result = BTreeMap::new();
        for i in 0..record_batch.num_rows() {
            let privilege = privilege_cols.value(i);
            let grantee = grantee_cols.value(i);
            let granted_to = granted_to_cols.value(i);

            if privilege != "OWNERSHIP" && granted_to != "SHARE" && granted_to != "DATABASE_ROLE" {
                let list = result.entry(privilege.to_string()).or_insert_with(Vec::new);
                list.push(grantee.to_string());
            }
        }

        Ok(result)
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L191-L198
    fn get_columns_in_relation(
        &self,
        state: &State,
        relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Vec<StdColumn>> {
        let result = match execute_macro(
            state,
            &[RelationObject::new(relation).as_value()],
            "get_columns_in_relation",
        ) {
            Ok(result) => result,
            Err(err) => {
                // TODO: switch to checking the vendor error code when available.
                // See https://github.com/dbt-labs/fs/pull/4267#discussion_r2182835729
                if err.message().contains("does not exist or not authorized") {
                    return Ok(Vec::new());
                }
                return Err(err);
            }
        };

        Ok(StdColumn::vec_from_jinja_value(
            AdapterType::Snowflake,
            result,
        )?)
    }

    /// Snowflake is special and defaults quoting to false if config is not provided
    fn quote_seed_column(
        &self,
        state: &State,
        column: &str,
        quote_config: Option<bool>,
    ) -> AdapterResult<String> {
        if quote_config.unwrap_or(false) {
            self.quote(state, column)
        } else {
            Ok(column.to_string())
        }
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/aa1de3d16267a456326a36045701fb48a61a6b6c/dbt-snowflake/src/dbt/adapters/snowflake/impl.py#L74
    fn get_constraint_support(&self, ct: ConstraintType) -> ConstraintSupport {
        match ct {
            ConstraintType::Check => ConstraintSupport::NotSupported,
            ConstraintType::NotNull | ConstraintType::ForeignKey => ConstraintSupport::Enforced,
            ConstraintType::Unique | ConstraintType::PrimaryKey => ConstraintSupport::NotEnforced,
            _ => ConstraintSupport::NotSupported,
        }
    }
}

impl fmt::Display for SnowflakeAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SnowflakeAdapter({})", self.adapter_type())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_types::NaiveTypeOpsImpl;
    use crate::stmt_splitter::NaiveStmtSplitter;

    use crate::config::AdapterConfig;
    use crate::query_comment::QueryCommentConfig;
    use dbt_auth::auth_for_backend;
    use dbt_common::cancellation::never_cancels;
    use dbt_schemas::schemas::relations::SNOWFLAKE_RESOLVED_QUOTING;
    use dbt_schemas::schemas::relations::base::ComponentName;
    use dbt_serde_yaml::Mapping;
    use dbt_xdbc::Backend;

    use minijinja::Environment;

    fn engine() -> Arc<SqlEngine> {
        let config = Mapping::from_iter([
            ("user".into(), "U".into()),
            ("password".into(), "P".into()),
            ("account".into(), "A".into()),
            ("database".into(), "D".into()),
            ("schema".into(), "S".into()),
            ("role".into(), "role".into()),
            ("warehouse".into(), "warehouse".into()),
        ]);
        let auth = auth_for_backend(Backend::Snowflake);
        SqlEngine::new(
            AdapterType::Snowflake,
            auth.into(),
            AdapterConfig::new(config),
            SNOWFLAKE_RESOLVED_QUOTING,
            Arc::new(NaiveStmtSplitter),
            None,
            QueryCommentConfig::from_query_comment(None, AdapterType::Snowflake, false),
            Box::new(NaiveTypeOpsImpl::new(AdapterType::Snowflake)), // XXX: NaiveTypeOpsImpl
            never_cancels(),
        )
    }

    #[test]
    fn test_adapter_type() {
        let adapter = SnowflakeAdapter::new(engine());
        assert_eq!(adapter.adapter_type(), AdapterType::Snowflake);
    }

    #[test]
    fn test_quote() {
        let adapter = SnowflakeAdapter::new(engine());
        let env = Environment::new();
        let state = State::new_for_env(&env);
        assert_eq!(adapter.quote(&state, "abc").unwrap(), "\"abc\"");
    }

    #[test]
    fn test_quote_seed_column() -> AdapterResult<()> {
        let adapter = SnowflakeAdapter::new(engine());
        let env = Environment::new();
        let state = State::new_for_env(&env);
        let quoted = adapter
            .quote_seed_column(&state, "my_column", None)
            .unwrap();
        assert_eq!(quoted, "my_column");
        let quoted = adapter
            .quote_seed_column(&state, "my_column", Some(false))
            .unwrap();
        assert_eq!(quoted, "my_column");
        let quoted = adapter
            .quote_seed_column(&state, "my_column", Some(true))
            .unwrap();
        assert_eq!(quoted, "\"my_column\"");
        Ok(())
    }

    #[test]
    fn test_quote_as_configured() -> AdapterResult<()> {
        let config = AdapterConfig::default();
        let auth = auth_for_backend(Backend::Snowflake);
        let engine = SqlEngine::new(
            AdapterType::Snowflake,
            auth.into(),
            config,
            SNOWFLAKE_RESOLVED_QUOTING,
            Arc::new(NaiveStmtSplitter),
            None,
            QueryCommentConfig::from_query_comment(None, AdapterType::Snowflake, false),
            Box::new(NaiveTypeOpsImpl::new(AdapterType::Snowflake)),
            never_cancels(),
        );
        let adapter = SnowflakeAdapter::new(engine);

        let env = Environment::new();
        let state = State::new_for_env(&env);
        let quoted = adapter
            .quote_as_configured(&state, "my_schema", &ComponentName::Schema)
            .unwrap();
        assert_eq!(quoted, "my_schema");

        let quoted = adapter
            .quote_as_configured(&state, "my_database", &ComponentName::Database)
            .unwrap();
        assert_eq!(quoted, "my_database");

        let quoted = adapter
            .quote_as_configured(&state, "my_table", &ComponentName::Identifier)
            .unwrap();
        assert_eq!(quoted, "my_table");
        Ok(())
    }
}
