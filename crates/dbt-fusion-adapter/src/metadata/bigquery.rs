use std::sync::Arc;

use arrow_array::StringArray;
use dbt_common::adapter::AdapterType;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_xdbc::{Connection, QueryCtx};

use crate::bigquery::relation::BigqueryRelation;
use crate::metadata::CatalogAndSchema;
use crate::record_batch_utils::get_column_values;
use crate::{AdapterResult, AdapterTyping};

pub fn list_relations(
    adapter: &dyn AdapterTyping,
    query_ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    db_schema: &CatalogAndSchema,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    let sql = format!(
        "SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    {db_schema}.INFORMATION_SCHEMA.TABLES"
    );

    let query_ctx = query_ctx.with_sql(sql);
    let batch = adapter.engine().execute(None, conn, &query_ctx)?;
    let table_names = get_column_values::<StringArray>(&batch, "table_name")?;
    let table_schemas = get_column_values::<StringArray>(&batch, "table_schema")?;
    let table_catalogs = get_column_values::<StringArray>(&batch, "table_catalog")?;
    let table_types = get_column_values::<StringArray>(&batch, "table_type")?;

    let mut result = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let database = table_catalogs.value(i);
        let schema = table_schemas.value(i);
        let identifier = table_names.value(i);
        let relation_type =
            RelationType::from_adapter_type(AdapterType::Bigquery, table_types.value(i));

        result.push(Arc::new(BigqueryRelation::new(
            Some(database.to_string()),
            Some(schema.to_string()),
            Some(identifier.to_string()),
            Some(relation_type),
            None,
            adapter.quoting(),
        )) as Arc<dyn BaseRelation>);
    }
    Ok(result)
}
