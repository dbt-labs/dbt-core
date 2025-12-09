use std::sync::Arc;

use arrow::array::{Array as _, StringArray};
use dbt_common::adapter::AdapterType;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::relations::base::{BaseRelation, TableFormat};
use dbt_xdbc::{Connection, QueryCtx};
use minijinja::{State, Value};

use crate::TypedBaseAdapter;
use crate::databricks::describe_table::DatabricksTableMetadata;
use crate::metadata::{snowflake, try_canonicalize_bool_column_field};
use crate::record_batch_utils::get_column_values;
use crate::relation::RelationObject;
use crate::relation::bigquery::BigqueryRelation;
use crate::relation::databricks::DatabricksRelation;
use crate::relation::postgres::PostgresRelation;
use crate::relation::redshift::RedshiftRelation;
use crate::relation::salesforce::SalesforceRelation;
use crate::relation::snowflake::SnowflakeRelation;

// TODO: turn this into a struct and collapse all the common code from X_get_relation functions

#[inline(never)]
pub fn get_relation(
    adapter: &dyn TypedBaseAdapter,
    state: &State,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    database: &str,
    schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
    let adapter_type = adapter.adapter_type();
    match adapter_type {
        AdapterType::Snowflake => {
            snowflake_get_relation(adapter, state, ctx, conn, database, schema, identifier)
        }
        AdapterType::Bigquery => {
            bigquery_get_relation(adapter, state, ctx, conn, database, schema, identifier)
        }
        AdapterType::Databricks => {
            databricks_get_relation(adapter, state, ctx, conn, database, schema, identifier)
        }
        AdapterType::Redshift => {
            redshift_get_relation(adapter, state, ctx, conn, database, schema, identifier)
        }
        AdapterType::Postgres => {
            postgres_get_relation(adapter, state, ctx, conn, database, schema, identifier)
        }
        AdapterType::Salesforce => {
            salesforce_get_relation(adapter, state, ctx, conn, database, schema, identifier)
        }
    }
}

// https://github.com/dbt-labs/dbt-adapters/blob/ace1709df001df4232a66f9d5f331a5fda4d3389/dbt-snowflake/src/dbt/include/snowflake/macros/adapters.sql#L138
fn snowflake_get_relation(
    adapter: &dyn TypedBaseAdapter,
    state: &State,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    database: &str,
    schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
    let quoted_database = if adapter.quoting().database {
        adapter.quote(state, database)?
    } else {
        database.to_string()
    };
    let quoted_schema = if adapter.quoting().schema {
        adapter.quote(state, schema)?
    } else {
        schema.to_string()
    };
    let quoted_identifier = if adapter.quoting().identifier {
        identifier.to_string()
    } else {
        identifier.to_uppercase()
    };
    // this is a case-insenstive search
    let sql = format!(
        "show objects like '{quoted_identifier}' in schema {quoted_database}.{quoted_schema}"
    );

    let batch = match adapter.engine().execute(Some(state), conn, ctx, &sql) {
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
        adapter.quoting(),
    ))))
}

fn bigquery_get_relation(
    adapter: &dyn TypedBaseAdapter,
    state: &State,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    database: &str,
    schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
    let query_database = if adapter.quoting().database {
        adapter.quote(state, database)?
    } else {
        database.to_string()
    };
    let query_schema = if adapter.quoting().schema {
        adapter.quote(state, schema)?
    } else {
        schema.to_string()
    };

    let query_identifier = if adapter.quoting().identifier {
        identifier.to_string()
    } else {
        identifier.to_lowercase()
    };

    let sql = format!(
        "SELECT table_catalog,
                    table_schema,
                    table_name,
                    table_type
                FROM {query_database}.{query_schema}.INFORMATION_SCHEMA.TABLES
                WHERE table_name = '{query_identifier}';",
    );

    let result = adapter.engine().execute(Some(state), conn, ctx, &sql);
    let batch = match result {
        Ok(batch) => batch,
        Err(err) => {
            let err_msg = err.to_string();
            if err_msg.contains("Dataset") && err_msg.contains("was not found") {
                return Ok(None);
            } else {
                return Err(err);
            }
        }
    };

    if batch.num_rows() == 0 {
        // If there are no rows, then we did not find the object
        return Ok(None);
    }

    let column = batch.column_by_name("table_type").unwrap();
    let string_array = column.as_any().downcast_ref::<StringArray>().unwrap();

    let relation_type_name = string_array.value(0).to_uppercase();
    let relation_type = RelationType::from_adapter_type(AdapterType::Bigquery, &relation_type_name);

    let mut relation = BigqueryRelation::new(
        Some(database.to_string()),
        Some(schema.to_string()),
        Some(identifier.to_string()),
        Some(relation_type),
        None,
        adapter.quoting(),
    );
    let relation_value = Value::from_object(RelationObject::new(Arc::new(relation.clone())));
    let location = adapter.get_dataset_location(state, conn, relation_value)?;
    relation.location = location;
    Ok(Some(Arc::new(relation)))
}

fn databricks_get_relation(
    adapter: &dyn TypedBaseAdapter,
    state: &State,
    ctx: &QueryCtx,
    conn: &mut dyn Connection,
    database: &str,
    schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
    use crate::metadata::MetadataProcessor as _;
    // though _needs_information is used in dbt to decide if this may be related from relations cache
    // since we don't implement relations cache, it's ignored for now
    // see https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/impl.py#L418

    let query_catalog = if adapter.quoting().database {
        adapter.quote(state, database)?
    } else {
        database.to_string()
    };
    let query_schema = if adapter.quoting().schema {
        adapter.quote(state, schema)?
    } else {
        schema.to_string()
    };
    let query_identifier = if adapter.quoting().identifier {
        adapter.quote(state, identifier)?
    } else {
        identifier.to_string()
    };

    // this deviates from what the existing dbt-adapter impl - here `AS JSON` is used
    // and a critical reason is `DESCRIBE TABLE EXTENDED` returns different results
    // for example, when database is set to the default `hive_metastore` it misses 'Provider' and 'Type' rows
    // and we don't need to parse through rows, see reference https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/impl.py#L433
    //
    // But this will lead to differences the metadata field
    // more details see docs from DatabricksDescribeTableExtended::to_metadata
    // WARNING: system tables from DBX only supports the basic "DESCRIBE TABLE"
    // though I assume it's highly unlikely and unfathomable a user would want to call adapter.get_relation on a system table
    let sql = format!(
        "DESCRIBE TABLE EXTENDED {query_catalog}.{query_schema}.{query_identifier} AS JSON"
    );

    let batch = adapter.engine().execute(Some(state), conn, ctx, &sql);
    if let Err(e) = &batch
        && (e.to_string().contains("cannot be found")
            || e.to_string().contains("TABLE_OR_VIEW_NOT_FOUND"))
    {
        return Ok(None);
    }
    let batch = batch?;
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    debug_assert_eq!(batch.num_rows(), 1);

    let json_metadata = DatabricksTableMetadata::from_record_batch(Arc::new(batch))?;
    let is_delta = json_metadata.provider.as_deref() == Some("delta");
    let relation_type = match json_metadata.type_.as_str() {
        "VIEW" => Some(RelationType::View),
        "MATERIALIZED_VIEW" => Some(RelationType::MaterializedView),
        "STREAMING_TABLE" => Some(RelationType::StreamingTable),
        _ => Some(RelationType::Table),
    };

    Ok(Some(Arc::new(DatabricksRelation::new(
        Some(database.to_string()),
        Some(schema.to_string()),
        Some(identifier.to_string()),
        relation_type,
        None,
        adapter.quoting(),
        Some(json_metadata.into_metadata()),
        is_delta,
    ))))
}

fn redshift_get_relation(
    adapter: &dyn TypedBaseAdapter,
    state: &State,
    ctx: &QueryCtx,
    conn: &mut dyn Connection,
    database: &str,
    schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
    let query_schema = if adapter.quoting().schema {
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

    let batch = adapter.engine().execute(Some(state), conn, ctx, &sql)?;

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
        adapter.quoting(),
    ))))
}

// reference: https://github.com/dbt-labs/dbt-adapters/blob/main/dbt-postgres/src/dbt/include/postgres/macros/adapters.sql#L85
fn postgres_get_relation(
    adapter: &dyn TypedBaseAdapter,
    state: &State,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    database: &str,
    schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
    let query_schema = if adapter.quoting().schema {
        schema.to_string()
    } else {
        schema.to_lowercase()
    };

    let query_identifier = if adapter.quoting().identifier {
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

    let batch = adapter.engine().execute(Some(state), conn, ctx, &sql)?;
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
        adapter.quoting(),
    )?;
    Ok(Some(Arc::new(relation)))
}

fn salesforce_get_relation(
    _adapter: &dyn TypedBaseAdapter,
    _state: &State,
    _query_ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    database: &str,
    _schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Arc<dyn BaseRelation>>> {
    // TODO: resolves relation_table based on the metadata to be returned in schema
    match conn.get_table_schema(Some(database), None, identifier) {
        Ok(_) => Ok(Some(Arc::new(SalesforceRelation::new(
            Some(database.to_string()),
            None,
            Some(identifier.to_string()),
            Some(RelationType::Table),
        )))),
        Err(_) => Ok(None),
    }
}
