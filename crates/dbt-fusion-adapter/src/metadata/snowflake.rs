use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::{BaseRelation, TableFormat};
use dbt_xdbc::{Connection, QueryCtx};

use crate::errors::{AdapterError, AdapterErrorKind};
use crate::record_batch_utils::get_column_values;
use crate::snowflake::relation::SnowflakeRelation;
use crate::{AdapterResult, AdapterTyping};

pub const ARROW_FIELD_SNOWFLAKE_FIELD_WIDTH_METADATA_KEY: &str = "SNOWFLAKE:field_width";

/// Helper to differentiate between tables and dynamic tables using the is_dynamic flag.
/// TODO: When we implement iceberg tables, we might want to pass in the is_iceberg flag here.
pub fn relation_type_from_table_flags(is_dynamic: &str) -> Result<RelationType, AdapterError> {
    if is_dynamic.eq_ignore_ascii_case("y") {
        Ok(RelationType::DynamicTable)
    } else if is_dynamic.eq_ignore_ascii_case("n") {
        Ok(RelationType::Table)
    } else {
        Err(AdapterError::new(
            AdapterErrorKind::UnexpectedResult,
            format!("Unexpected `is_dynamic` value {is_dynamic}"),
        ))
    }
}

// Helper for serializing query results within `list_relations`
fn build_relations_from_show_objects(
    show_objects_result: &RecordBatch,
    quoting: ResolvedQuoting,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    let mut relations = Vec::new();

    let name = get_column_values::<StringArray>(show_objects_result, "name")?;
    let database_name = get_column_values::<StringArray>(show_objects_result, "database_name")?;
    let schema_name = get_column_values::<StringArray>(show_objects_result, "schema_name")?;
    let table_kind = get_column_values::<StringArray>(show_objects_result, "kind")?;
    let is_dynamic = get_column_values::<StringArray>(show_objects_result, "is_dynamic")?;
    let is_iceberg = get_column_values::<StringArray>(show_objects_result, "is_iceberg")?;

    for i in 0..show_objects_result.num_rows() {
        let name = name.value(i);
        let database_name = database_name.value(i);
        let schema_name = schema_name.value(i);
        let table_kind = table_kind.value(i);
        let is_dynamic = is_dynamic.value(i);
        let is_iceberg = is_iceberg.value(i);

        let relation_type = if table_kind.eq_ignore_ascii_case("table") {
            Some(relation_type_from_table_flags(is_dynamic)?)
        } else if table_kind.eq_ignore_ascii_case("view") {
            Some(RelationType::View)
        } else {
            Some(RelationType::from(table_kind))
        };

        let table_format = if super::try_canonicalize_bool_column_field(is_iceberg)? {
            TableFormat::Iceberg
        } else {
            TableFormat::Default
        };

        let relation = Arc::new(SnowflakeRelation::new(
            Some(database_name.to_string()),
            Some(schema_name.to_string()),
            Some(name.to_string()),
            relation_type,
            table_format,
            quoting,
        )) as Arc<dyn BaseRelation>;
        relations.push(relation);
    }

    Ok(relations)
}

pub fn list_relations(
    adapter: &dyn AdapterTyping,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    db_schema: &crate::metadata::CatalogAndSchema,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    // Paginate through the results
    let limit_size = 10000;
    let mut from_name = None;
    let mut batches = Vec::new();
    loop {
        let sql = format!(
            "SHOW OBJECTS IN SCHEMA {} LIMIT {}{}",
            db_schema,
            limit_size,
            from_name
                .map(|name| format!(" FROM '{name}'"))
                .unwrap_or_default()
        );
        let batch = adapter.engine().execute(None, conn, ctx, &sql)?;

        // From the RecordBatch, get the last row of the vector of name 'name'
        let names = get_column_values::<StringArray>(&batch, "name")?;

        let last_name = match batch.num_rows().checked_sub(1) {
            Some(idx) => names.value(idx).to_string(),
            None => break,
        };

        from_name = Some(last_name);
        batches.push(batch);
        if names.len() < limit_size {
            break;
        }
    }
    // Create Relations from the batches
    let mut relations = Vec::new();
    for batch in batches {
        relations.extend(build_relations_from_show_objects(
            &batch,
            adapter.quoting(),
        )?);
    }
    Ok(relations)
}
