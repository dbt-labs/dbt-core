use std::sync::Arc;

use arrow_array::StringArray;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_xdbc::{Connection, QueryCtx};

use crate::record_batch_utils::get_column_values;
use crate::relation::redshift::RedshiftRelation;
use crate::{AdapterResult, AdapterTyping};

/// Reference: https://github.com/dbt-labs/dbt-adapters/blob/87e81a47baa11c312003377091a9efc0ab72d88e/dbt-redshift/src/dbt/include/redshift/macros/adapters.sql#L226
pub fn list_relations(
    adapter: &dyn AdapterTyping,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    db_schema: &super::CatalogAndSchema,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    let sql = format!(
        "select
    table_catalog as database,
    table_name as name,
    table_schema as schema,
    'table' as type
from information_schema.tables
where table_schema ilike '{}'
and table_type = 'BASE TABLE'
union all
select
    table_catalog as database,
    table_name as name,
    table_schema as schema,
    case
    when view_definition ilike '%create materialized view%'
        then 'materialized_view'
    else 'view'
    end as type
from information_schema.views
where table_schema ilike '{}'",
        &db_schema.resolved_schema, &db_schema.resolved_schema
    );

    let batch = adapter.engine().execute(None, conn, ctx, &sql)?;

    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let mut relations = Vec::new();

    let table_name = get_column_values::<StringArray>(&batch, "name")?;
    let database_name = get_column_values::<StringArray>(&batch, "database")?;
    let schema_name = get_column_values::<StringArray>(&batch, "schema")?;
    let table_type = get_column_values::<StringArray>(&batch, "type")?;

    for i in 0..batch.num_rows() {
        let table_name = table_name.value(i);
        let database_name = database_name.value(i);
        let schema_name = schema_name.value(i);
        let table_type = table_type.value(i);

        let relation = Arc::new(RedshiftRelation::new(
            Some(database_name.to_string()),
            Some(schema_name.to_string()),
            Some(table_name.to_string()),
            Some(RelationType::from(table_type)),
            None,
            adapter.quoting(),
        )) as Arc<dyn BaseRelation>;
        relations.push(relation);
    }

    Ok(relations)
}
