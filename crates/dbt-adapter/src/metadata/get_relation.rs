use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array as _, StringArray};
use dbt_common::adapter::AdapterType;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::relations::base::{BaseRelation, TableFormat};
use dbt_xdbc::{Connection, QueryCtx};
use minijinja::State;

use crate::AdapterTyping;
use crate::metadata::databricks::describe_table::DatabricksTableMetadata;
use crate::metadata::{snowflake, try_canonicalize_bool_column_field};
use crate::record_batch_utils::get_column_values;
use crate::relation::bigquery::BigqueryRelation;
use crate::relation::databricks::DatabricksRelation;
use crate::relation::do_create_relation;
use crate::relation::postgres::PostgresRelation;
use crate::relation::redshift::RedshiftRelation;
use crate::relation::salesforce::SalesforceRelation;
use crate::relation::snowflake::SnowflakeRelation;
use crate::typed_adapter::ConcreteAdapter;

// TODO: turn this into a struct and collapse all the common code from X_get_relation functions

#[inline(never)]
pub fn get_relation(
    adapter: &ConcreteAdapter,
    state: &State,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    database: &str,
    schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Box<dyn BaseRelation>>> {
    match adapter.adapter_type() {
        AdapterType::Snowflake => {
            snowflake_get_relation(adapter, state, ctx, conn, database, schema, identifier)
        }
        AdapterType::Bigquery => {
            bigquery_get_relation(adapter, state, ctx, conn, database, schema, identifier)
        }
        AdapterType::Databricks | AdapterType::Spark => {
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
        AdapterType::DuckDB => {
            duckdb_get_relation(adapter, state, ctx, conn, database, schema, identifier)
        }
        AdapterType::Fabric => todo!(),
        AdapterType::Sidecar => {
            // This branch should not be reached - sidecar adapters override get_relation()
            Err(AdapterError::new(
                AdapterErrorKind::Internal,
                "get_relation called on Sidecar adapter type without override",
            ))
        }
    }
}

// https://github.com/dbt-labs/dbt-adapters/blob/ace1709df001df4232a66f9d5f331a5fda4d3389/dbt-snowflake/src/dbt/include/snowflake/macros/adapters.sql#L138
fn snowflake_get_relation(
    adapter: &ConcreteAdapter,
    state: &State,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    database: &str,
    schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Box<dyn BaseRelation>>> {
    let quoted_database = if adapter.quoting().database {
        adapter.quote(database)
    } else {
        database.to_string()
    };
    let quoted_schema = if adapter.quoting().schema {
        adapter.quote(schema)
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

    Ok(Some(Box::new(SnowflakeRelation::new(
        Some(database.to_string()),
        Some(schema.to_string()),
        Some(identifier.to_string()),
        relation_type,
        table_format,
        adapter.quoting(),
    ))))
}

fn bigquery_get_relation(
    adapter: &ConcreteAdapter,
    state: &State,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    database: &str,
    schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Box<dyn BaseRelation>>> {
    let query_database = if adapter.quoting().database {
        adapter.quote(database)
    } else {
        database.to_string()
    };
    let query_schema = if adapter.quoting().schema {
        adapter.quote(schema)
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

    let mut relation = Box::new(BigqueryRelation::new(
        Some(database.to_string()),
        Some(schema.to_string()),
        Some(identifier.to_string()),
        Some(relation_type),
        None,
        adapter.quoting(),
    ));
    let location = adapter.get_dataset_location(state, conn, relation.as_ref())?;
    relation.location = location;
    Ok(Some(relation))
}

fn databricks_get_relation(
    adapter: &ConcreteAdapter,
    state: &State,
    ctx: &QueryCtx,
    conn: &mut dyn Connection,
    database: &str,
    schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Box<dyn BaseRelation>>> {
    use crate::metadata::MetadataProcessor as _;
    use crate::metadata::databricks::DatabricksMetadataAdapter;
    use crate::metadata::databricks::version::DbrVersion;
    use crate::relation::databricks::{INFORMATION_SCHEMA_SCHEMA, SYSTEM_DATABASE};

    // This function is only called when full metadata is needed. See https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/impl.py#L418

    let query_catalog = if adapter.quoting().database {
        adapter.quote(database)
    } else {
        database.to_string()
    };
    let query_schema = if adapter.quoting().schema {
        adapter.quote(schema)
    } else {
        schema.to_string()
    };
    let query_identifier = if adapter.quoting().identifier {
        adapter.quote(identifier)
    } else {
        identifier.to_string()
    };

    // Determine whether `DESCRIBE TABLE EXTENDED ... AS JSON` is supported.
    // This mirrors the safety checks in list_relations_schemas_inner:
    // - External system tables (system.information_schema.*) don't support AS JSON
    // - DBR versions < 16.2 don't support AS JSON
    // - Spark adapter: skip version check (no DBR version concept)
    // See also: https://github.com/databricks/dbt-databricks/blob/822b105b15e644676d9e1f47cbfd765cd4c1541f/dbt/adapters/databricks/impl.py#L423
    let dbr_version = match adapter.adapter_type() {
        AdapterType::Spark => None,
        AdapterType::Databricks => Some(DatabricksMetadataAdapter::get_dbr_version(
            adapter, ctx, conn,
        )?),
        _ => unreachable!(),
    };

    let is_external_system = database.eq_ignore_ascii_case(SYSTEM_DATABASE)
        && schema.eq_ignore_ascii_case(INFORMATION_SCHEMA_SCHEMA);

    let as_json_unsupported = is_external_system
        || dbr_version
            .map(|v| v < DbrVersion::Full(16, 2))
            .unwrap_or(false);

    let fqn = if database.is_empty() {
        format!("{query_schema}.{query_identifier}")
    } else {
        format!("{query_catalog}.{query_schema}.{query_identifier}")
    };

    let sql = if as_json_unsupported {
        format!("DESCRIBE TABLE EXTENDED {fqn}")
    } else {
        format!("DESCRIBE TABLE EXTENDED {fqn} AS JSON")
    };

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

    let (relation_type, is_delta, metadata) = if as_json_unsupported {
        // Parse the non-JSON DESCRIBE TABLE EXTENDED output.
        // The result has col_name/data_type/comment columns. Column definitions come
        // first, followed by an empty separator row, then metadata key-value pairs.
        // Some databases (e.g. hive_metastore) may be missing 'Type' and 'Provider'
        // rows, so we default gracefully.
        let col_names = get_column_values::<StringArray>(&batch, "col_name")?;
        let data_types = get_column_values::<StringArray>(&batch, "data_type")?;

        let mut metadata_map = BTreeMap::new();
        let mut type_str = None;
        let mut provider_str = None;
        let mut in_metadata_section = false;

        for i in 0..batch.num_rows() {
            let key = col_names.value(i).trim();
            let value = data_types.value(i).trim();

            if key.is_empty() {
                in_metadata_section = true;
                continue;
            }
            if !in_metadata_section || key.starts_with('#') {
                continue;
            }

            metadata_map.insert(key.to_string(), value.to_string());
            match key {
                "Type" => type_str = Some(value.to_string()),
                "Provider" => provider_str = Some(value.to_string()),
                _ => {}
            }
        }

        // The non-JSON Type field returns raw Databricks types (MANAGED, EXTERNAL,
        // FOREIGN, VIEW, etc.) — use from_adapter_type for proper mapping.
        let relation_type = Some(match type_str.as_deref() {
            Some(t) => RelationType::from_adapter_type(adapter.adapter_type(), t),
            None => RelationType::Table,
        });
        let is_delta = provider_str.as_deref() == Some("delta");

        (relation_type, is_delta, Some(metadata_map))
    } else {
        debug_assert_eq!(batch.num_rows(), 1);
        let json_metadata = DatabricksTableMetadata::from_record_batch(Arc::new(batch))?;
        let is_delta = json_metadata.provider.as_deref() == Some("delta");
        let relation_type = Some(RelationType::from_adapter_type(
            adapter.adapter_type(),
            &json_metadata.type_,
        ));
        (relation_type, is_delta, Some(json_metadata.into_metadata()))
    };

    let db = if database.is_empty() {
        None
    } else {
        Some(database.to_string())
    };

    Ok(Some(Box::new(DatabricksRelation::new(
        adapter.adapter_type(),
        db,
        Some(schema.to_string()),
        Some(identifier.to_string()),
        relation_type,
        None,
        adapter.quoting(),
        metadata,
        is_delta,
        false,
    ))))
}

fn redshift_get_relation(
    adapter: &ConcreteAdapter,
    state: &State,
    ctx: &QueryCtx,
    conn: &mut dyn Connection,
    database: &str,
    schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Box<dyn BaseRelation>>> {
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

    Ok(Some(Box::new(RedshiftRelation::new(
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
    adapter: &ConcreteAdapter,
    state: &State,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    database: &str,
    schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Box<dyn BaseRelation>>> {
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
    Ok(Some(Box::new(relation)))
}

fn salesforce_get_relation(
    _adapter: &ConcreteAdapter,
    _state: &State,
    _query_ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    database: &str,
    _schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Box<dyn BaseRelation>>> {
    // TODO: resolves relation_table based on the metadata to be returned in schema
    match conn.get_table_schema(Some(database), None, identifier) {
        Ok(_) => Ok(Some(Box::new(SalesforceRelation::new(
            Some(database.to_string()),
            None,
            Some(identifier.to_string()),
            Some(RelationType::Table),
        )))),
        Err(_) => Ok(None),
    }
}

fn duckdb_get_relation(
    adapter: &ConcreteAdapter,
    state: &State,
    ctx: &QueryCtx,
    conn: &'_ mut dyn Connection,
    database: &str,
    schema: &str,
    identifier: &str,
) -> AdapterResult<Option<Box<dyn BaseRelation>>> {
    // DuckDB is case-preserving for quoted identifiers
    // Unquoted identifiers are lowercase by default
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

    // Query INFORMATION_SCHEMA.TABLES for relation metadata
    // DuckDB's table_type values: BASE TABLE, VIEW, LOCAL TEMPORARY
    let sql = format!(
        r#"
            SELECT table_type as type
            FROM information_schema.tables
            WHERE table_schema = '{query_schema}'
              AND table_name = '{query_identifier}'
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

    // Map DuckDB table_type to dbt RelationType
    let relation_type = match string_array.value(0) {
        "BASE TABLE" => Some(RelationType::Table),
        "VIEW" => Some(RelationType::View),
        "LOCAL TEMPORARY" => Some(RelationType::Table), // Treat temp tables as tables
        _ => return invalid_value!("Unsupported relation type {}", string_array.value(0)),
    };

    // Use the logical adapter type to create the appropriate relation
    // This avoids backend-specific limitations (like Postgres 63-char identifier limit)
    // when running in sidecar mode
    let relation = do_create_relation(
        adapter.adapter_type(),
        database.to_string(),
        schema.to_string(),
        Some(identifier.to_string()),
        relation_type,
        adapter.quoting(),
    )?;
    Ok(Some(relation))
}
