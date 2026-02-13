use crate::sql_types::{TypeOps, make_arrow_field_v2};
use crate::typed_adapter::ConcreteAdapter;
use crate::{AdapterEngine, AdapterTyping, TypedBaseAdapter};
use crate::{
    AdapterResult, errors::AsyncAdapterResult, metadata::*, record_batch_utils::get_column_values,
};
use arrow_schema::Schema;

use arrow_array::{RecordBatch, StringArray};

use dbt_common::adapter::ExecutionPhase;
use dbt_common::cancellation::Cancellable;
use dbt_schemas::schemas::{
    legacy_catalog::{CatalogTable, ColumnMetadata},
    relations::base::{BaseRelation, RelationPattern},
};
use dbt_xdbc::{Connection, MapReduce, QueryCtx};
use minijinja::State;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Maximum number of concurrent connections for schema introspection.
const MAX_CONNECTIONS: usize = 4;

pub struct DuckDBMetadataAdapter {
    adapter: ConcreteAdapter,
}

impl DuckDBMetadataAdapter {
    pub fn new(engine: Arc<AdapterEngine>) -> Self {
        let adapter = ConcreteAdapter::new(engine);
        Self { adapter }
    }
}

impl MetadataAdapter for DuckDBMetadataAdapter {
    fn adapter(&self) -> &dyn TypedBaseAdapter {
        &self.adapter
    }

    fn build_schemas_from_stats_sql(
        &self,
        _stats_sql_result: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, CatalogTable>> {
        // DuckDB doesn't use this code path for catalog generation
        Ok(BTreeMap::new())
    }

    fn build_columns_from_get_columns(
        &self,
        _stats_sql_result: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, BTreeMap<String, ColumnMetadata>>> {
        // DuckDB doesn't use this code path for catalog generation
        Ok(BTreeMap::new())
    }

    fn list_relations_schemas_inner(
        &self,
        unique_id: Option<String>,
        phase: Option<ExecutionPhase>,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        type Acc = HashMap<String, AdapterResult<Arc<Schema>>>;

        let table_names = relations
            .iter()
            .map(|relation| relation.semantic_fqn())
            .collect::<Vec<_>>();

        let adapter = self.adapter.clone();
        let new_connection_f = Box::new(move || {
            adapter
                .new_connection(None, None)
                .map_err(Cancellable::Error)
        });

        let adapter = self.adapter.clone();
        let map_f = move |conn: &'_ mut dyn Connection,
                          table_name: &String|
              -> AdapterResult<Arc<Schema>> {
            // Use DESCRIBE to get table schema
            // DuckDB's DESCRIBE returns: column_name, column_type, null, key, default, extra
            let sql = format!("DESCRIBE {};", &table_name);
            let mut ctx = QueryCtx::default().with_desc("Get table schema");
            if let Some(node_id) = unique_id.clone() {
                ctx = ctx.with_node_id(&node_id);
            }
            if let Some(phase) = phase {
                ctx = ctx.with_phase(phase.as_str());
            }
            let (_, table) = adapter.query(&ctx, conn, &sql, None)?;
            let batch = table.original_record_batch();
            let schema = build_schema_from_duckdb_describe(batch, adapter.engine().type_ops())?;
            Ok(schema)
        };

        let reduce_f = |acc: &mut Acc,
                        table_name: String,
                        schema: AdapterResult<Arc<Schema>>|
         -> Result<(), Cancellable<AdapterError>> {
            acc.insert(table_name, schema);
            Ok(())
        };

        let map_reduce = MapReduce::new(
            Box::new(new_connection_f),
            Box::new(map_f),
            Box::new(reduce_f),
            MAX_CONNECTIONS,
        );
        let token = self.adapter.cancellation_token();
        map_reduce.run(Arc::new(table_names), token)
    }

    fn list_relations_schemas_by_patterns_inner(
        &self,
        _patterns: &[RelationPattern],
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        todo!("DuckDBAdapter::list_relations_schemas_by_patterns")
    }

    fn freshness_inner(
        &self,
        _relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>> {
        todo!("DuckDBAdapter::freshness")
    }

    fn create_schemas_if_not_exists(
        &self,
        state: &State<'_, '_>,
        catalog_schemas: Vec<(String, String, String)>,
    ) -> AdapterResult<Vec<(String, String, String, AdapterResult<()>)>> {
        create_schemas_if_not_exists(&self.adapter, self, state, catalog_schemas)
    }

    fn list_relations_in_parallel_inner(
        &self,
        _db_schemas: &[CatalogAndSchema],
    ) -> AsyncAdapterResult<'_, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>> {
        // FIXME: Implement cache hydration
        let future = async move { Ok(BTreeMap::new()) };
        Box::pin(future)
    }
}

/// Build an Arrow Schema from DuckDB's DESCRIBE output.
///
/// DuckDB's DESCRIBE returns columns: column_name, column_type, null, key, default, extra
fn build_schema_from_duckdb_describe(
    describe_result: Arc<RecordBatch>,
    type_ops: &dyn TypeOps,
) -> AdapterResult<Arc<Schema>> {
    let column_names = get_column_values::<StringArray>(&describe_result, "column_name")?;
    let data_types = get_column_values::<StringArray>(&describe_result, "column_type")?;
    let nullability = get_column_values::<StringArray>(&describe_result, "null")?;

    let mut fields = vec![];
    for i in 0..describe_result.num_rows() {
        let name = column_names.value(i);
        // DuckDB returns "YES" or "NO" for nullability
        let nullable = nullability.value(i).to_uppercase() == "YES";
        let text_data_type = data_types.value(i);

        let field = make_arrow_field_v2(
            type_ops,
            name.to_string(),
            text_data_type,
            Some(nullable),
            None, // No comment from DESCRIBE
        )?;
        fields.push(field);
    }

    let schema = Schema::new(fields);
    Ok(Arc::new(schema))
}
