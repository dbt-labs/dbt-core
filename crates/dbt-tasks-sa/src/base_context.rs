use std::collections::BTreeMap;

use dbt_jinja_utils::{jinja_environment::JinjaEnv, phases::build_compile_and_run_base_context};
use dbt_schemas::state::ResolverState;

pub fn build_base_context(
    resolver_state: &ResolverState,
    env: &JinjaEnv,
) -> BTreeMap<String, minijinja::Value> {
    let namespace_keys: Vec<String> = env
        .env
        .get_macro_namespace_registry()
        .map(|r| r.keys().map(|k| k.to_string()).collect())
        .unwrap_or_default();
    build_compile_and_run_base_context(
        resolver_state.node_resolver.clone(),
        &resolver_state.root_project_name,
        &resolver_state.nodes,
        resolver_state.defer_nodes.as_ref(),
        resolver_state.runtime_config.clone(),
        namespace_keys,
    )
}
