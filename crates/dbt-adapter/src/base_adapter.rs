use crate::AdapterResult;
use crate::BridgeAdapter;
use crate::column::ColumnStatic;
use crate::engine::AdapterEngine;
use crate::query_cache::QueryCache;
use crate::stmt_splitter::StmtSplitter;
use crate::typed_adapter::*;

use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::ReplayMode;
use dbt_schema_store::SchemaStoreTrait;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::ResolvedCloudConfig;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::project::QueryComment;
use dbt_schemas::schemas::relations::base::{BaseRelation, ComponentName};
use dbt_xdbc::Backend;
use minijinja::{State, Value};

use std::collections::BTreeMap;
use std::sync::Arc;

/// The type of the adapter. Used to identify the specific database adapter being used.
pub type AdapterType = dbt_adapter_core::AdapterType;

pub fn backend_of(adapter_type: AdapterType) -> Backend {
    match adapter_type {
        AdapterType::Postgres => Backend::Postgres,
        AdapterType::Snowflake => Backend::Snowflake,
        AdapterType::Bigquery => Backend::BigQuery,
        AdapterType::Databricks => Backend::Databricks,
        AdapterType::Redshift => Backend::Redshift,
        AdapterType::Salesforce => Backend::Salesforce,
        AdapterType::Spark => Backend::Spark,
        AdapterType::DuckDB => Backend::DuckDB,
        AdapterType::Sidecar => Backend::DuckDB,
        AdapterType::Fabric => Backend::SQLServer,
        AdapterType::ClickHouse => todo!("ClickHouse"),
        AdapterType::Starburst => todo!("Starburst"),
        AdapterType::Athena => todo!("Athena"),
        AdapterType::Trino => todo!("Trino"),
        AdapterType::Dremio => todo!("Dremio"),
        AdapterType::Oracle => todo!("Oracle"),
    }
}

/// Type queries to be implemented for every adapter.
pub trait AdapterTyping {
    /// Returns the [InnerAdapter] discriminator for this adapter.
    ///
    /// This is the primary method that implementors should provide.
    /// Default implementations of [engine()](Self::engine) and
    /// [as_replay()](Self::as_replay) are derived from this.
    fn inner_adapter(&self) -> InnerAdapter<'_>;

    /// Get name/type of this adapter
    fn adapter_type(&self) -> AdapterType {
        match self.inner_adapter() {
            InnerAdapter::Impl(adapter_type, _) | InnerAdapter::Replay(adapter_type, _) => {
                adapter_type
            }
        }
    }

    /// Get a reference to the concrete adapter if supported.
    fn as_concrete_adapter(&self) -> &ConcreteAdapter;

    /// True if called on the [ParseAdapter].
    fn is_parse(&self) -> bool {
        false
    }

    /// This adapter as the replay adapter if it is one, None otherwise.
    fn as_replay(&self) -> Option<&dyn Replayer> {
        match self.inner_adapter() {
            InnerAdapter::Replay(_, replay) => Some(replay),
            InnerAdapter::Impl(..) => None,
        }
    }

    /// Get column type instance
    fn column_type(&self) -> Option<Value> {
        let value = Value::from_object(ColumnStatic::new(self.adapter_type()));
        Some(value)
    }

    /// Get the [AdapterEngine]
    fn engine(&self) -> &Arc<dyn AdapterEngine> {
        match self.inner_adapter() {
            InnerAdapter::Impl(_, engine) => engine,
            InnerAdapter::Replay(_, replay) => replay.engine(),
        }
    }

    /// Get the [ResolvedQuoting]
    fn quoting(&self) -> ResolvedQuoting {
        match self.inner_adapter() {
            InnerAdapter::Impl(_, engine) => engine.quoting(),
            InnerAdapter::Replay(_, replay) => replay.engine().quoting(),
        }
    }

    /// Quote a component of a relation
    fn quote_component(
        &self,
        _state: &State,
        identifier: &str,
        component: ComponentName,
    ) -> AdapterResult<String> {
        let quoted = match component {
            ComponentName::Database => self.quoting().database,
            ComponentName::Schema => self.quoting().schema,
            ComponentName::Identifier => self.quoting().identifier,
        };
        if quoted {
            let adapter = self.as_concrete_adapter();
            Ok(adapter.quote(identifier))
        } else {
            Ok(identifier.to_string())
        }
    }
}

/// A factory for adapters, relations and columns.
///
/// It can create [BridgeAdapter] instances wrapped in `Arc`.
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
    ) -> FsResult<Arc<BridgeAdapter>>;

    /// Return the statement splitter used by this factory.
    fn stmt_splitter(&self) -> Arc<dyn StmtSplitter>;

    /// Create a relation from a InternalDbtNode
    fn create_relation_from_node(
        &self,
        node: &dyn InternalDbtNodeAttributes,
        adapter_type: AdapterType,
    ) -> Result<Box<dyn BaseRelation>, minijinja::Error>;
}
