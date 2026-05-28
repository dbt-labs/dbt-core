use std::{collections::HashSet, sync::Arc};

use crate::context::TaskRunnerCtx;
use async_trait::async_trait;
use dbt_adapter::sql_types::TypeOps;
use dbt_common::FsResult;
use dbt_schemas::schemas::relations::base::BaseRelation;

#[async_trait]
pub trait RenderTaskHooks: Send + Sync {
    async fn will_fetch_schema_for_unit_test_relation(
        &self,
        ctx: &TaskRunnerCtx,
        unit_test_unique_id: &str,
        fetched: &mut HashSet<String>,
        relation: &Arc<dyn BaseRelation>,
        type_ops: &Arc<dyn TypeOps>,
    ) -> FsResult<()>;
}
