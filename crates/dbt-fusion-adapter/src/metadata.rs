use crate::AdapterType;
use crate::errors::AdapterResult;
use crate::errors::{AdapterError, AsyncAdapterResult};
use crate::typed_adapter::TypedBaseAdapter;

use arrow::array::RecordBatch;
use arrow_schema::Schema;
use dbt_common::cancellation::{Cancellable, CancellationToken};
use dbt_common::io_args::IoArgs;
use dbt_schemas::schemas::relations::base::{BaseRelation, ComponentName, RelationPattern};
use dbt_xdbc::{Connection, MapReduce, QueryCtx};

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

/// Maximum number of connections
pub const MAX_CONNECTIONS: usize = 128;

/// The two ways of representing a relation in a pair.
pub type RelationSchemaPair = (Arc<dyn BaseRelation>, Arc<Schema>);

/// A collection of relations
pub type RelationVec = Vec<Arc<dyn BaseRelation>>;
/// A struct representing a catalog and a schema
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct CatalogAndSchema {
    pub rendered_catalog: String,
    pub rendered_schema: String,
}

impl fmt::Display for CatalogAndSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.rendered_catalog, self.rendered_schema)
    }
}

impl From<&Arc<dyn BaseRelation>> for CatalogAndSchema {
    fn from(relation: &Arc<dyn BaseRelation>) -> Self {
        let rendered_catalog = relation.quoted(
            &relation
                .database_as_resolved_str()
                .expect("Database is required for relation"),
        );
        let rendered_schema = relation.quoted(
            &relation
                .schema_as_resolved_str()
                .expect("schema is required for relation"),
        );

        Self {
            rendered_catalog,
            rendered_schema,
        }
    }
}

pub struct MetadataFreshness {
    pub last_altered: i128,
    pub is_view: bool,
}

/// Used to represent status of remote download from warehouse
pub enum MetadataDownloadStatus {
    /// To represent no data being found - e.g. empty schema
    NoDataFound,
    /// Successful operation
    Success,
    /// Operation had an error
    Failed,
}

impl fmt::Display for MetadataDownloadStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status_str = match self {
            MetadataDownloadStatus::NoDataFound => "empty",
            MetadataDownloadStatus::Success => "success",
            MetadataDownloadStatus::Failed => "failed",
        };
        write!(f, "{status_str}")
    }
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
        unique_id: Option<String>,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<HashMap<String, AdapterResult<Arc<Schema>>>>;

    /// List relations and their schemas by patterns
    #[allow(clippy::type_complexity)]
    fn list_relations_schemas_by_patterns(
        &self,
        patterns: &[RelationPattern],
    ) -> AsyncAdapterResult<Vec<(String, AdapterResult<RelationSchemaPair>)>>;

    /// Create schemas if they don't exist
    #[allow(clippy::type_complexity)]
    fn create_schemas_if_not_exists(
        &self,
        catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
    ) -> AsyncAdapterResult<Vec<(String, String, AdapterResult<()>)>>;

    /// Get freshness of relations
    fn freshness(
        &self,
        relations: &[Arc<dyn BaseRelation>],
    ) -> AsyncAdapterResult<BTreeMap<String, MetadataFreshness>>;

    /// List relations in the specified database schemas
    ///
    /// # Arguments
    /// * `io` - I/O Arguments to report progress to
    /// * `db_schemas` - List of (catalog, schema) pairs to discover relations in
    ///
    fn list_relations(
        &self,
        _io: &IoArgs,
        _db_schemas: &[CatalogAndSchema],
    ) -> AsyncAdapterResult<BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>>;
}

/// Create schemas if they don't exist
///
/// Caveat: you'll want to first use this helper to create catalogs for the schemas you're going to create
/// before using it to create schemas
#[allow(clippy::type_complexity)]
pub fn create_schemas_if_not_exists(
    adapter: Arc<dyn MetadataAdapter>,
    catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
    adapter_type: AdapterType,
    token: CancellationToken,
) -> AsyncAdapterResult<'static, Vec<(String, String, AdapterResult<()>)>> {
    type Acc = Vec<(String, String, AdapterResult<()>)>;
    let catalog_schemas = flatten_catalog_schemas(catalog_schemas);
    let adapter_clone = adapter.clone();
    let new_connection_f = move || {
        adapter_clone
            .new_connection(None)
            .map_err(Cancellable::Error)
    };
    let map_f = move |conn: &'_ mut dyn Connection,
                      (catalog, schema): &(String, String)|
          -> AdapterResult<AdapterResult<()>> {
        let sql = create_schema_sql(&adapter, catalog, schema);
        let query_ctx = QueryCtx::new(adapter.adapter_type().to_string())
            .with_sql(sql)
            .with_desc("Ensure catalogs and schemas exist");
        // TODO: see if we can execute this DDL only when we can be certain that the database doesn't exist, only then emit a info log
        // use SHOW DATABASES but this query doesn't return the databases a user doesn't have access to
        // https://github.com/dbt-labs/fs/issues/2789
        let adapter_clone = adapter.clone();
        match adapter_clone.exec_stmt(conn, &query_ctx, false) {
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
                         (catalog, schema): (String, String),
                         batch_res: AdapterResult<AdapterResult<()>>|
          -> Result<(), Cancellable<AdapterError>> {
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
    map_reduce.run(Arc::new(catalog_schemas), token)
}

pub fn flatten_catalog_schemas(
    catalog_schemas: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<(String, String)> {
    catalog_schemas
        .iter()
        .flat_map(|(catalog, schemas)| {
            schemas
                .iter()
                .map(|schema| (catalog.clone(), schema.clone()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
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
        AdapterType::Snowflake | AdapterType::Databricks => {
            format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        }
        // Redshift connetions are always to a specific database
        AdapterType::Redshift => format!("CREATE SCHEMA IF NOT EXISTS {schema}"),
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
