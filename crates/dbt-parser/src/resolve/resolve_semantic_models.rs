use std::{collections::HashMap, sync::Arc};

use dbt_common::FsResult;
use dbt_schemas::schemas::manifest::DbtSemanticModel;

pub async fn resolve_semantic_models() -> FsResult<(
    HashMap<String, Arc<DbtSemanticModel>>,
    HashMap<String, Arc<DbtSemanticModel>>,
)> {
    let semantic_models: HashMap<String, Arc<DbtSemanticModel>> = HashMap::new();
    let disabled_semantic_models: HashMap<String, Arc<DbtSemanticModel>> = HashMap::new();

    Ok((semantic_models, disabled_semantic_models))
}
