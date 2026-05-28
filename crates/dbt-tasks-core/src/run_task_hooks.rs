use async_trait::async_trait;
use dbt_common::FsResult;

use crate::context::TaskRunnerCtx;
use dbt_schemas::schemas::manifest::DbtSavedQuery;

#[async_trait]
pub trait RunTaskHooks: Send + Sync {
    async fn execute_saved_query(&self, ctx: &TaskRunnerCtx, node: &DbtSavedQuery) -> FsResult<()>;
}
