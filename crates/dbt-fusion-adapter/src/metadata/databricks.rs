use std::sync::Arc;

use arrow_array::StringArray;
use dbt_common::adapter::AdapterType;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_xdbc::{Connection, QueryCtx};

use crate::databricks::relation::DatabricksRelation;
use crate::metadata::CatalogAndSchema;
use crate::record_batch_utils::get_column_values;
use crate::{AdapterResult, AdapterTyping};

// Reference: https://github.com/databricks/dbt-databricks/blob/92f1442faabe0fce6f0375b95e46ebcbfcea4c67/dbt/include/databricks/macros/adapters/metadata.sql
pub fn list_relations(
    adapter: &dyn AdapterTyping,
    query_ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    db_schema: &CatalogAndSchema,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    let sql = format!("
SELECT
    table_name,
    if(table_type IN ('EXTERNAL', 'MANAGED', 'MANAGED_SHALLOW_CLONE', 'EXTERNAL_SHALLOW_CLONE'), 'table', lower(table_type)) AS table_type,
    lower(data_source_format) AS file_format,
    table_schema,
    table_owner,
    table_catalog,
    if(
    table_type IN (
        'EXTERNAL',
        'MANAGED',
        'MANAGED_SHALLOW_CLONE',
        'EXTERNAL_SHALLOW_CLONE'
    ),
    lower(table_type),
    NULL
    ) AS databricks_table_type
FROM `system`.`information_schema`.`tables`
WHERE table_catalog = '{}'
    AND table_schema = '{}'",
                            &db_schema.resolved_catalog,
                            &db_schema.resolved_schema);

    let query_ctx = query_ctx.with_sql(sql);

    let batch = adapter.engine().execute(None, conn, &query_ctx)?;

    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let mut relations = Vec::new();

    let names = get_column_values::<StringArray>(&batch, "table_name")?;
    let schemas = get_column_values::<StringArray>(&batch, "table_schema")?;
    let catalogs = get_column_values::<StringArray>(&batch, "table_catalog")?;
    let table_types = get_column_values::<StringArray>(&batch, "table_type")?;

    for i in 0..batch.num_rows() {
        let name = names.value(i);
        let schema = schemas.value(i);
        let catalog = catalogs.value(i);
        let table_type = table_types.value(i).to_uppercase();

        let relation = Arc::new(DatabricksRelation::new(
            Some(catalog.to_string()),
            Some(schema.to_string()),
            Some(name.to_string()),
            Some(RelationType::from_adapter_type(
                AdapterType::Databricks,
                table_type.as_str(),
            )),
            None,
            adapter.quoting(),
            None,
            false,
        )) as Arc<dyn BaseRelation>;
        relations.push(relation);
    }

    Ok(relations)
}
