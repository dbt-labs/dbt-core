use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dbt_adapter::cache::hydrate_relation_cache_if_not_already_cached;
use dbt_adapter::engine::SidecarClient;
use dbt_adapter::{Adapter, AdapterStore};
use dbt_adapter_core::AdapterType;
use dbt_common::FsError;
use dbt_common::FsResult;
use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::EvalArgs;
use dbt_common::io_args::FsCommand;
use dbt_common::io_args::IoArgs;
use dbt_common::io_args::StaticAnalysisKind;
use dbt_dag::schedule::Schedule;
use dbt_defer::DeferState;
use dbt_schema_store::store::SchemaStore;
use dbt_schemas::schemas::profiles::Execute;
use dbt_schemas::schemas::relations::base::BaseRelation;
use dbt_schemas::state::ResolverState;
use dbt_tasks_core::metricflow::MetricflowClient;
use dbt_tasks_core::static_analysis_buckets::StaticAnalysisBuckets;

use crate::config::CompilationConfig;

#[derive(Clone)]
pub struct SchemaHydrationArgs<'a> {
    pub io: &'a IoArgs,
    pub global_static_analysis: Option<StaticAnalysisKind>,
    pub execute_mode: Execute,
    pub scope: SchemaHydrationScope,
    /// `--infer-schemas`: a node whose schema we failed to download should
    /// stay eligible for static analysis instead of having it disabled, so
    /// binding can fall back to inferring the schema from usage.
    pub infer_schemas: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchemaHydrationScope {
    FrontierOnly,
    Full,
}

#[derive(Clone)]
pub struct SchemaHydrationDownloadWarning {
    pub err: Arc<FsError>,
    pub unique_id: String,
}

#[derive(Clone, Default)]
pub struct SchemaHydrationState {
    pub fetched_schema_fqns: HashSet<String>,
    pub download_warnings_by_fqn: HashMap<String, SchemaHydrationDownloadWarning>,
}

#[async_trait::async_trait]
pub trait SchemaHydrator: Send + Sync {
    async fn hydrate_schemas(
        self: Box<Self>,
        arg: &EvalArgs,
        schedule: &Schedule<String>,
        resolved_state: &mut ResolverState,
        schema_hydration_state: &mut SchemaHydrationState,
        defer_state: &mut DeferState,
        token: CancellationToken,
    ) -> FsResult<Box<dyn StaticAnalysisBuckets>>;
}

pub trait SchemaHydratorFactory: Send + Sync {
    fn create(
        &self,
        adapter: Arc<Adapter>,
        adapter_store: Arc<AdapterStore>,
        execute_mode: Execute,
        compilation_config: CompilationConfig,
        schema_store: Arc<SchemaStore>,
        sidecar_client: Option<Arc<dyn SidecarClient>>,
        metricflow_server_client: Option<Arc<dyn MetricflowClient>>,
    ) -> Box<dyn SchemaHydrator>;
}

pub fn should_hydrate_schemas(arg: &EvalArgs) -> bool {
    arg.command != FsCommand::Seed
        && arg.command != FsCommand::Clone
        && arg.command != FsCommand::RunOperation
        && arg.command != FsCommand::Extension("compare")
        && arg.command != FsCommand::Extension("pull")
        && (arg.command != FsCommand::Compile || arg.introspect)
}

pub async fn hydrate_selected_get_relations(
    adapter_store: &AdapterStore,
    schedule: &Schedule<String>,
    resolver_state: &ResolverState,
    state: &SchemaHydrationState,
    scope: SchemaHydrationScope,
    infer_schemas: bool,
) -> FsResult<()> {
    if infer_schemas || scope == SchemaHydrationScope::FrontierOnly {
        return Ok(());
    }
    let selected_get_relations = resolver_state
        .get_relation_calls
        .iter()
        .filter(|(node_id, _)| schedule.selected_nodes.contains(*node_id))
        .flat_map(|(_, relations)| relations)
        .filter(|relation| !state.fetched_schema_fqns.contains(&relation.semantic_fqn()))
        .cloned()
        .collect::<Vec<_>>();
    for (adapter_type, relations) in group_relations_by_adapter(selected_get_relations) {
        let adapter = adapter_store.get(adapter_type)?;
        let _ = hydrate_relation_cache_if_not_already_cached(
            &relations,
            &adapter,
            "downloading referenced relations",
        )
        .await;
    }
    Ok(())
}

fn group_relations_by_adapter(
    relations: Vec<Arc<dyn BaseRelation>>,
) -> Vec<(AdapterType, Vec<Arc<dyn BaseRelation>>)> {
    let mut grouped = HashMap::<AdapterType, Vec<_>>::new();
    for relation in relations {
        grouped
            .entry(relation.adapter_type())
            .or_default()
            .push(relation);
    }
    let mut grouped = grouped.into_iter().collect::<Vec<_>>();
    grouped.sort_by_key(|(adapter_type, _)| adapter_type.to_string());
    grouped
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_adapter::relation::create_relation;
    use dbt_schemas::schemas::common::ResolvedQuoting;

    #[test]
    fn relation_hydration_groups_by_relation_adapter() {
        let relation = |adapter_type| {
            Arc::<dyn BaseRelation>::from(
                create_relation(
                    adapter_type,
                    "db".to_string(),
                    "schema".to_string(),
                    Some("table".to_string()),
                    None,
                    ResolvedQuoting::default(),
                )
                .unwrap(),
            )
        };

        let grouped = group_relations_by_adapter(vec![
            relation(AdapterType::Snowflake),
            relation(AdapterType::DuckDB),
        ]);

        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[0].0, AdapterType::DuckDB);
        assert_eq!(grouped[1].0, AdapterType::Snowflake);
    }
}
