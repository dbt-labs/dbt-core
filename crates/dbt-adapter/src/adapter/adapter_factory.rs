use crate::Adapter;
use crate::query_cache::QueryCache;
use crate::stmt_splitter::StmtSplitter;
use dbt_adapter_core::AdapterType;

use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::ReplayMode;
use dbt_schema_store::SchemaStoreTrait;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::ResolvedCloudConfig;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::project::QueryComment;
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_xdbc::Backend;
use minijinja::Value;

use std::collections::BTreeMap;
use std::sync::Arc;

pub fn backend_of(adapter_type: AdapterType) -> Backend {
    match adapter_type {
        AdapterType::Postgres => Backend::Postgres,
        AdapterType::Snowflake => Backend::Snowflake,
        AdapterType::Bigquery => Backend::BigQuery,
        AdapterType::Databricks => Backend::Databricks,
        AdapterType::Redshift => Backend::Redshift,
        AdapterType::Salesforce => Backend::Salesforce,
        AdapterType::Spark => Backend::Spark,
        AdapterType::DuckDB => Backend::DuckDBExtended,
        AdapterType::Fabric => Backend::SQLServer,
        AdapterType::ClickHouse => Backend::ClickHouse,
        AdapterType::Exasol => Backend::Exasol,
        AdapterType::Starburst => todo!("Starburst"),
        AdapterType::Athena => Backend::Athena,
        AdapterType::Trino => todo!("Trino"),
        AdapterType::Dremio => todo!("Dremio"),
        AdapterType::Oracle => todo!("Oracle"),
        AdapterType::Datafusion => todo!("Datafusion"),
    }
}

/// A factory for adapters, relations and columns.
///
/// It can create [Adapter] instances wrapped in `Arc`.
/// Similarly, it can create boxed `dyn BaseRelation`
/// and `Column` objects.
pub trait AdapterFactory: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn create_adapter(
        &self,
        adapter_type: AdapterType,
        config: dbt_yaml::Mapping,
        replay_mode: Option<ReplayMode>,
        flags: BTreeMap<String, Value>,
        schema_cache: Option<Arc<dyn SchemaStoreTrait>>,
        query_cache: Option<Arc<dyn QueryCache>>,
        quoting: ResolvedQuoting,
        query_comment: Option<QueryComment>,
        token: CancellationToken,
        cloud_config: Option<&ResolvedCloudConfig>,
        threads: Option<usize>,
    ) -> FsResult<Arc<Adapter>>;

    /// Return the statement splitter used by this factory.
    fn stmt_splitter(&self) -> Arc<dyn StmtSplitter>;

    /// Create a relation from a InternalDbtNode
    fn create_relation_from_node(
        &self,
        node: &dyn InternalDbtNodeAttributes,
        adapter_type: AdapterType,
    ) -> Result<Box<dyn BaseRelation>, minijinja::Error>;
}
