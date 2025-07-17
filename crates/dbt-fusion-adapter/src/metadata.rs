use crate::AdapterType;
use crate::errors::AdapterError;
use crate::errors::{AdapterResult, AsyncAdapterResult};
use crate::typed_adapter::TypedBaseAdapter;

use arrow::array::RecordBatch;
use arrow_schema::Schema;
use dbt_schemas::schemas::relations::DEFAULT_DATABRICKS_DATABASE;
use dbt_schemas::schemas::relations::base::{BaseRelation, ComponentName, RelationPattern};
use dbt_xdbc::{Connection, MapReduce, QueryCtx};

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// Maximum number of connections
pub const MAX_CONNECTIONS: usize = 128;

/// The two ways of representing a relation in a pair.
pub type RelationSchemaPair = (Arc<dyn BaseRelation>, Arc<Schema>);

pub struct MetadataFreshness {
    pub last_altered: i128,
    pub is_view: bool,
}

/// Allows serializing record batches into maps and Arrow schemas
pub trait MetadataProcessor {
    // Implementers can choose the map key/value
    type Key: Ord + Clone;
    type Value: Clone;

    fn into_metadata(self) -> BTreeMap<Self::Key, Self::Value>;
    fn from_record_batch(batch: Arc<RecordBatch>) -> AdapterResult<Self>
    where
        Self: Sized;
    fn to_arrow_schema(&self) -> AdapterResult<Arc<Schema>>;
}

/// This represents a UDF downloaded from a remote data warehouse
#[derive(Debug, Clone)]
pub struct UDF {
    pub name: String,
    pub description: String,
    pub signature: String,
    pub adapter_type: AdapterType,
    pub kind: UDFKind,
}

#[derive(Debug, Clone, Copy)]
pub enum UDFKind {
    Scalar,
    Aggregate,
    Table,
}

// XXX: we should unify relation representation as Arrow schemas across the codebase

/// Adapter that supports metadata query
pub trait MetadataAdapter: TypedBaseAdapter + Send + Sync {
    /// List UDFs under a given set of catalog and schemas
    fn list_user_defined_functions(
        &self,
        _catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
    ) -> AsyncAdapterResult<Vec<UDF>> {
        let future = async move { Ok(vec![]) };
        Box::pin(future)
    }

    /// List relations and their schemas
    fn list_relations_schemas(
        &self,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<HashMap<String, AdapterResult<Arc<Schema>>>>;

    /// List relations and their schemas by patterns
    #[allow(clippy::type_complexity)]
    fn list_relations_schemas_by_patterns(
        &self,
        patterns: &[RelationPattern],
    ) -> AsyncAdapterResult<Vec<(String, AdapterResult<RelationSchemaPair>)>>;

    /// Create catalogs if they don't exist
    #[allow(clippy::type_complexity)]
    fn create_catalogs_if_not_exists(
        &self,
        catalogs: &[String],
    ) -> AsyncAdapterResult<Vec<(String, Option<String>, AdapterResult<()>)>>;

    /// Create schemas if they don't exist
    #[allow(clippy::type_complexity)]
    fn create_schemas_if_not_exists(
        &self,
        catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
    ) -> AsyncAdapterResult<Vec<(String, Option<String>, AdapterResult<()>)>>;

    /// Get freshness of relations
    fn freshness(
        &self,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<BTreeMap<String, MetadataFreshness>>;
}

/// Create catalogs or schemas if they don't exist
///
/// catalog here refers to database entity - that'll be project for BigQuery, catalog for Databricks, database for Snowflake etc
///
/// When schema is None, this creates catalogs
/// Otherwise, this create schemas
/// Caveat: you'll want to first use this helper to create catalogs for the schemas you're going to create
/// before using it to create schemas
#[allow(clippy::type_complexity)]
pub fn create_catalogs_schema_if_not_exists(
    adapter: Arc<dyn MetadataAdapter>,
    catalog_schemas: Vec<(String, Option<String>)>,
    adapter_type: AdapterType,
) -> AsyncAdapterResult<'static, Vec<(String, Option<String>, AdapterResult<()>)>> {
    type Acc = Vec<(String, Option<String>, AdapterResult<()>)>;
    let adapter_clone = adapter.clone();
    let new_connection_f = move || adapter_clone.new_connection();
    let map_f = move |conn: &'_ mut dyn Connection,
                      (catalog, schema): &(String, Option<String>)|
          -> AdapterResult<AdapterResult<()>> {
        let (sql, _) = match schema {
            Some(schema) => (create_schema_sql(&adapter, catalog, schema), false),
            None => {
                match adapter_type {
                    // skip creating a database if this is to target default catalog in databricks
                    // otherwise execute below errors 42832: caused by an error of message [MODIFY_BUILTIN_CATALOG] Modifying built-in catalog hive_metastore is not supported
                    AdapterType::Databricks if catalog == DEFAULT_DATABRICKS_DATABASE => {
                        return Ok(Ok(()));
                    }
                    _ => {}
                }
                (create_catalog_sql(&adapter, catalog), true)
            }
        };
        let query_ctx = QueryCtx::new(adapter.adapter_type().to_string())
            .with_sql(sql)
            .with_desc("Ensure catalogs and schemas exist");
        // TODO: see if we can execute this DDL only when we can be certain that the database doesn't exist, only then emit a info log
        // use SHOW DATABASES but this query doesn't return the databases a user doesn't have access to
        // https://github.com/dbt-labs/fs/issues/2789
        let adapter_clone = adapter.clone();
        match adapter_clone.execute(conn, &query_ctx, None, None, None) {
            Ok(_) => Ok(Ok(())),
            Err(e) => {
                if is_tolerable(&e, adapter_type) {
                    Ok(Ok(()))
                } else {
                    Err(e)
                }
            }
        }
    };

    let reduce_f = move |acc: &mut Acc,
                         (catalog, schema): (String, Option<String>),
                         batch_res: AdapterResult<AdapterResult<()>>|
          -> AdapterResult<()> {
        let batch = batch_res?;
        acc.push((catalog, schema, batch));
        Ok(())
    };
    let map_reduce = MapReduce::new(
        Box::new(new_connection_f),
        Box::new(map_f),
        Box::new(reduce_f),
        MAX_CONNECTIONS,
    );
    map_reduce.run(Arc::new(catalog_schemas))
}

/// A helper that transforms the input of [`MetadataAdapter::create_catalogs_if_not_exists`] to what is required by [`create_catalogs_schema_if_not_exists`]
///
/// catalog here refers to database entity - that'll be project for BigQuery, catalog for Databricks, database for Snowflake etc
pub fn transform_catalogs(catalogs: &[String]) -> Vec<(String, Option<String>)> {
    catalogs
        .iter()
        .map(|catalog| {
            // Build the query for all relations in this database
            (catalog.clone(), None)
        })
        .collect::<Vec<_>>()
}

/// A helper that transforms the input of [`MetadataAdapter::create_schemas_if_not_exists`] to what is required by [`create_catalogs_schema_if_not_exists`]`
///
/// catalog here refers to database entity - that'll be project for BigQuery, catalog for Databricks, database for Snowflake etc
pub fn transform_catalog_schemas(
    catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<(String, Option<String>)> {
    catalog_schemas
        .iter()
        .flat_map(|(catalog, schemas)| {
            schemas
                .iter()
                .map(|schema| (catalog.clone(), Some(schema.clone())))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

/// Returns a SQL that creates a catalog
///
/// catalog here refers to database entity - that'll be project for BigQuery, catalog for Databricks, database for Snowflake etc
/// TODO: revisit this to reuse an existing macro
fn create_catalog_sql(adapter: &Arc<dyn MetadataAdapter>, catalog: &str) -> String {
    let catalog = adapter.quote_component(catalog, ComponentName::Database);
    let adapter_type = adapter.adapter_type();
    match adapter_type {
        AdapterType::Snowflake => format!("CREATE DATABASE IF NOT EXISTS {catalog}"),
        AdapterType::Databricks => format!("CREATE CATALOG IF NOT EXISTS {catalog}"),
        _ => unimplemented!("create_catalog_sql for adapter type: {}", adapter_type),
    }
}

/// Returns a SQL that creates a schema
///
/// catalog here refers to database entity - that'll be dataset for BigQuery, schema for Databricks, database for Snowflake etc
/// TODO: revisit this to reuse an existing macro
fn create_schema_sql(adapter: &Arc<dyn MetadataAdapter>, catalog: &str, schema: &str) -> String {
    let catalog = adapter.quote_component(catalog, ComponentName::Database);
    let schema = adapter.quote_component(schema, ComponentName::Schema);
    let adapter_type = adapter.adapter_type();
    match adapter_type {
        AdapterType::Snowflake => format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"),
        AdapterType::Databricks => format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"),
        _ => unimplemented!("create_schema_sql for adapter type: {}", adapter_type),
    }
}

fn is_tolerable(e: &AdapterError, adapter_type: AdapterType) -> bool {
    // this is supposed to be using/extended from ANSI SQL standard but I didn't find any Snowflake documentation
    // the magic strings here are from inspecting the results from fs run on a project with a new database,
    // and a weak role that lack permissions to create a database
    match adapter_type {
        // 42501: insufficient privileges
        // 02000: does not exist or not authorizedntax error
        AdapterType::Snowflake => e.sqlstate() == "42501" || e.sqlstate() == "02000",
        // Databricks doesn't provide an explicit enough SQLSTATE, noticed most of their errors' SQLSTATE is HY000
        // so we have to match on the error message below.
        // By the time of writing down this note, it is a problem from their backend thus not something we can fix on the SDK or driver layer
        // check out data/repros/databricks_create_schema_no_catalog_access on how to repro this error
        AdapterType::Databricks => e.message().contains("PERMISSION_DENIED"),
        _ => {
            #[cfg(debug_assertions)]
            {
                println!("is_error_tolerable: {:?}: {}", e, e.sqlstate());
            }
            false
        }
    }
}
