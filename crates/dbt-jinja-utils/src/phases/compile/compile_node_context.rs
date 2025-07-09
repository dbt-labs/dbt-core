//! This module contains the scope guard for resolving models.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use dashmap::DashMap;
use dbt_common::serde_utils::convert_json_to_dash_map;
use dbt_fusion_adapter::{load_store::ResultStore, relation_object::create_relation};
use dbt_schemas::schemas::{relations::base::BaseRelation, CommonAttributes, NodeBaseAttributes};
use dbt_schemas::state::{DbtRuntimeConfig, RefsAndSourcesTracker};
use minijinja::{
    constants::{TARGET_PACKAGE_NAME, TARGET_UNIQUE_ID},
    Value as MinijinjaValue,
};
use serde_json::Value;

use crate::phases::MacroLookupContext;

use super::super::compile_and_run_context::RefFunction;
use super::compile_config::CompileConfig;

/// Build a compile model context
/// Returns a context and the current relation
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
pub fn build_compile_node_context(
    model: &MinijinjaValue,
    common_attr: &CommonAttributes,
    base_attr: &NodeBaseAttributes,
    config: &Value,
    adapter_type: &str,
    base_context: &BTreeMap<String, MinijinjaValue>,
    root_project_name: &str,
    packages: BTreeSet<String>,
    refs_and_sources: Arc<dyn RefsAndSourcesTracker>,
    runtime_config: Arc<DbtRuntimeConfig>,
    skip_ref_validation: bool,
) -> (
    BTreeMap<String, MinijinjaValue>,
    Arc<dyn BaseRelation>,
    Arc<DashMap<String, MinijinjaValue>>,
) {
    let mut base_builtins = if let Some(builtins) = base_context.get("builtins") {
        builtins
            .as_object()
            .unwrap()
            .downcast_ref::<BTreeMap<String, MinijinjaValue>>()
            .unwrap()
            .clone()
    } else {
        BTreeMap::new()
    };
    let mut ctx = base_context.clone();

    // Create a relation for 'this' using config values
    let this_relation = create_relation(
        adapter_type.to_string(),
        base_attr.database.clone(),
        base_attr.schema.clone(),
        Some(base_attr.alias.clone()),
        None,
        base_attr.quoting,
    )
    .unwrap();

    ctx.insert("this".to_owned(), this_relation.as_value());
    ctx.insert(
        "database".to_owned(),
        MinijinjaValue::from(base_attr.database.to_string()),
    );
    ctx.insert(
        "schema".to_owned(),
        MinijinjaValue::from(base_attr.schema.to_string()),
    );
    ctx.insert(
        "identifier".to_owned(),
        MinijinjaValue::from(base_attr.alias.clone()),
    );

    let config_map = Arc::new(convert_json_to_dash_map(config.clone()));
    let compile_config = CompileConfig {
        config: config_map.clone(),
    };

    ctx.insert(
        "config".to_owned(),
        MinijinjaValue::from_object(compile_config.clone()),
    );
    base_builtins.insert(
        "config".to_string(),
        MinijinjaValue::from_object(compile_config),
    );

    // Create validated ref function with dependency checking
    let allowed_dependencies: Arc<BTreeSet<String>> =
        Arc::new(base_attr.depends_on.nodes.iter().cloned().collect());

    let ref_function = RefFunction::new_with_validation(
        refs_and_sources.clone(),
        common_attr.package_name.clone(),
        runtime_config,
        allowed_dependencies,
        skip_ref_validation,
    );

    let ref_value = MinijinjaValue::from_object(ref_function);
    ctx.insert("ref".to_string(), ref_value.clone());
    base_builtins.insert("ref".to_string(), ref_value);

    // Register builtins as a global
    ctx.insert(
        "builtins".to_owned(),
        MinijinjaValue::from_object(base_builtins),
    );

    ctx.insert("model".to_owned(), MinijinjaValue::from_serialize(model));

    let result_store = ResultStore::default();
    ctx.insert(
        "store_result".to_owned(),
        MinijinjaValue::from_function(result_store.store_result()),
    );
    ctx.insert(
        "load_result".to_owned(),
        MinijinjaValue::from_function(result_store.load_result()),
    );
    ctx.insert(
        "store_raw_result".to_owned(),
        MinijinjaValue::from_function(result_store.store_raw_result()),
    );
    ctx.insert(
        TARGET_PACKAGE_NAME.to_owned(),
        MinijinjaValue::from(&common_attr.package_name),
    );
    ctx.insert(
        TARGET_UNIQUE_ID.to_owned(),
        MinijinjaValue::from(&common_attr.unique_id),
    );

    let mut packages = packages;
    packages.insert(root_project_name.to_string());

    ctx.insert(
        "context".to_owned(),
        MinijinjaValue::from_object(MacroLookupContext {
            root_project_name: root_project_name.to_string(),
            current_project_name: None,
            packages,
        }),
    );

    (ctx, this_relation, config_map)
}
