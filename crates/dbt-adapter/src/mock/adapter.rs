use crate::AdapterEngine;
use crate::adapter_engine::MockEngine;
use crate::base_adapter::{AdapterType, AdapterTyping};
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use crate::funcs::none_value;
use crate::metadata::*;
use crate::relation::snowflake::SnowflakeRelation;
use crate::response::AdapterResponse;
use crate::sql_types::TypeOps;
use crate::typed_adapter::TypedBaseAdapter;
use arrow::array::{ArrayRef, Decimal128Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use dbt_agate::AgateTable;
use dbt_common::behavior_flags::BehaviorFlag;
use dbt_common::cancellation::CancellationToken;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::{BaseRelation, TableFormat};
use dbt_xdbc::{Connection, QueryCtx};
use minijinja::{State, Value};

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

/// An adapter intended to be used in tests only. This adapter
/// hardcodes values used in tests. In the future this adapter could
/// provide a feature to set values per test.
#[derive(Clone)]
pub struct MockAdapter {
    /// Adapter type
    pub adapter_type: AdapterType,
    engine: Arc<AdapterEngine>,
    /// Flags available in dbt_project.yml
    flags: BTreeMap<String, Value>,
    /// Quoting Policy
    quoting: ResolvedQuoting,
    /// Global CLI cancellation token
    cancellation_token: CancellationToken,
}

impl fmt::Debug for MockAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockAdapter")
            .field("adapter_type", &self.adapter_type)
            .field("flags", &self.flags)
            .field("quoting", &self.quoting)
            .finish()
    }
}

impl MockAdapter {
    /// Create a new instance
    pub fn new(
        adapter_type: AdapterType,
        flags: BTreeMap<String, Value>,
        quoting: ResolvedQuoting,
        type_ops: Box<dyn TypeOps>,
        token: CancellationToken,
    ) -> Self {
        Self {
            adapter_type,
            engine: Arc::new(AdapterEngine::Mock(MockEngine::new(adapter_type, type_ops))),
            flags,
            quoting,
            cancellation_token: token,
        }
    }
}

impl AdapterTyping for MockAdapter {
    fn adapter_type(&self) -> AdapterType {
        self.adapter_type
    }

    fn as_metadata_adapter(&self) -> Option<&dyn MetadataAdapter> {
        None // TODO: implement metadata_adapter() for MockAdapter
    }

    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        self
    }

    fn engine(&self) -> &Arc<AdapterEngine> {
        &self.engine
    }

    fn quoting(&self) -> ResolvedQuoting {
        self.quoting
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
}

impl TypedBaseAdapter for MockAdapter {
    fn list_schemas(&self, _result: Arc<RecordBatch>) -> AdapterResult<Vec<String>> {
        Ok(vec![])
    }

    fn execute(
        &self,
        _state: Option<&State>,
        _conn: &'_ mut dyn Connection,
        _ctx: &QueryCtx,
        sql: &str,
        _auto_begin: bool,
        _fetch: bool,
        _limit: Option<i64>,
        _options: Option<HashMap<String, String>>,
    ) -> AdapterResult<(AdapterResponse, AgateTable)> {
        let response = AdapterResponse {
            message: "execute".to_string(),
            code: sql.to_string(),
            rows_affected: 1,
            query_id: None,
        };

        let schema = Arc::new(Schema::new(vec![Field::new(
            "names",
            DataType::Decimal128(38, 10),
            true,
        )]));
        let decimal_array: ArrayRef = Arc::new(Decimal128Array::from(vec![Some(42)]));
        let batch = RecordBatch::try_new(schema, vec![decimal_array]).unwrap();

        let table = AgateTable::from_record_batch(Arc::new(batch));

        Ok((response, table))
    }

    #[allow(clippy::too_many_arguments)]
    fn add_query(
        &self,
        _query_ctx: &QueryCtx,
        _conn: &'_ mut dyn Connection,
        _sql: &str,
        _auto_begin: bool,
        _bindings: Option<&Value>,
        _abridge_sql_log: bool,
    ) -> AdapterResult<()> {
        unimplemented!("query addition to connection in MockAdapter")
    }

    fn quote(&self, _state: &State, identifier: &str) -> AdapterResult<String> {
        Ok(format!("\"{identifier}\""))
    }

    fn get_relation(
        &self,
        _state: &State,
        _query_ctx: &QueryCtx,
        _conn: &'_ mut dyn Connection,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
        Ok(Some(Arc::new(SnowflakeRelation::new(
            Some(database.to_string()),
            Some(schema.to_string()),
            Some(identifier.to_string()),
            None,
            TableFormat::Default,
            self.quoting,
        ))))
    }

    fn drop_relation(
        &self,
        _state: &State,
        _relation: Arc<dyn BaseRelation>,
    ) -> AdapterResult<Value> {
        Ok(none_value())
    }

    /// Lists all relations in the provided [CatalogAndSchema]
    fn list_relations(
        &self,
        _query_ctx: &QueryCtx,
        _conn: &'_ mut dyn Connection,
        _db_schema: &CatalogAndSchema,
    ) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
        Err(AdapterError::new(
            AdapterErrorKind::Internal,
            format!(
                "list_relations_without_caching is not implemented for this adapter: {}",
                self.adapter_type()
            ),
        ))
    }

    fn convert_type(
        &self,
        _state: &State,
        _table: Arc<AgateTable>,
        _col_idx: i64,
    ) -> AdapterResult<String> {
        unimplemented!("type conversion from table column in MockAdapter")
    }

    fn behavior(&self) -> Vec<BehaviorFlag> {
        let is_true = self.flags.get("is_true").is_none_or(|v| v.is_true());
        let is_false = self.flags.get("is_false").is_some_and(|v| v.is_true());
        let is_unknown = self.flags.get("is_unknown").is_none_or(|v| v.is_true());

        // Ane example flag defined by an adapter
        let flags = vec![
            BehaviorFlag::new("is_true", is_true, None, None, None),
            BehaviorFlag::new("is_false", is_false, None, None, None),
            BehaviorFlag::new("is_unknown", is_unknown, None, None, None),
        ];
        flags
    }
}

#[cfg(test)]
mod tests {
    use crate::sql_types::NaiveTypeOpsImpl;
    use dbt_common::cancellation::never_cancels;
    use dbt_schemas::schemas::relations::SNOWFLAKE_RESOLVED_QUOTING;

    use super::*;

    #[test]
    fn test_adapter_type() {
        let adapter = MockAdapter::new(
            AdapterType::Snowflake,
            BTreeMap::new(),
            SNOWFLAKE_RESOLVED_QUOTING,
            Box::new(NaiveTypeOpsImpl::new(AdapterType::Snowflake)),
            never_cancels(),
        );
        assert_eq!(adapter.adapter_type(), AdapterType::Snowflake);
    }

    #[test]
    fn test_quote() {
        let adapter = MockAdapter::new(
            AdapterType::Snowflake,
            BTreeMap::new(),
            SNOWFLAKE_RESOLVED_QUOTING,
            Box::new(NaiveTypeOpsImpl::new(AdapterType::Snowflake)),
            never_cancels(),
        );
        let env = minijinja::Environment::new();
        let state = State::new_for_env(&env);
        assert_eq!(adapter.quote(&state, "abc").unwrap(), "\"abc\"");
    }
}
