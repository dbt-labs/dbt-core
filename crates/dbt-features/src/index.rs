use std::path::Path;

use async_trait::async_trait;
use dbt_common::FsResult;
use dbt_common::io_args::EvalArgs;
use dbt_schema_store::SchemaStoreTrait;
use dbt_schemas::schemas::manifest::DbtManifest;
use dbt_schemas::state::ResolverState;
use dbt_tasks_core::RunTaskResults;

#[async_trait]
pub trait IndexHooks: Send + Sync {
    /// Write parquet metadata epoch files after tasks complete (compile/run/build path).
    async fn write_artifacts(
        &self,
        _arg: &EvalArgs,
        _manifest: &DbtManifest,
        _resolved_state: &ResolverState,
        _schema_store: Option<&dyn SchemaStoreTrait>,
        _run_task_results: &RunTaskResults,
    ) -> FsResult<()> {
        Ok(())
    }

    fn write_index_direct(&self, _arg: &EvalArgs, _manifest: &DbtManifest) {}

    fn create_docs_providers(
        &self,
        _index_dir: &Path,
        _metadata_dir: Option<&Path>,
    ) -> FsResult<Option<dbt_docs_server::Providers>> {
        Ok(None)
    }

    fn save_artifact_meta(&self, _arg: &EvalArgs) {}
}

pub struct IndexFeature {
    pub hooks: Box<dyn IndexHooks>,
}
