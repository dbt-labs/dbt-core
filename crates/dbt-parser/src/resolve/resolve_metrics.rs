use std::{collections::HashMap, sync::Arc};

use dbt_common::FsResult;
use dbt_schemas::schemas::manifest::DbtMetric;

pub async fn resolve_metrics() -> FsResult<(
    HashMap<String, Arc<DbtMetric>>,
    HashMap<String, Arc<DbtMetric>>,
)> {
    let metrics: HashMap<String, Arc<DbtMetric>> = HashMap::new();
    let disabled_metrics: HashMap<String, Arc<DbtMetric>> = HashMap::new();

    Ok((metrics, disabled_metrics))
}
