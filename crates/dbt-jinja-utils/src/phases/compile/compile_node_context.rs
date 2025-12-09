//! This module contains the scope guard for resolving models.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use dashmap::DashMap;
use dbt_adapter::{AdapterType, load_store::ResultStore};
use dbt_common::io_args::StaticAnalysisKind;
use dbt_common::serde_utils::convert_yml_to_dash_map;
use dbt_schemas::{
    schemas::{InternalDbtNodeAttributes, telemetry::NodeType},
    state::{DbtRuntimeConfig, NodeResolverTracker, ResolverState},
};
use dbt_serde_yaml::Spanned;
use minijinja::{
    Value as MinijinjaValue,
    constants::{
        CURRENT_EXECUTION_PHASE, CURRENT_PATH, CURRENT_SPAN, TARGET_PACKAGE_NAME, TARGET_UNIQUE_ID,
    },
    machinery::Span,
};

use crate::phases::MacroLookupContext;
use crate::phases::compile_and_run_context::FunctionFunction;

use super::super::compile_and_run_context::RefFunction;
use super::compile_config::CompileConfig;

/// The name of the repl model
pub const REPL_MODEL_NAME: &str = "__repl__";

/// Build a compile model context (wrapper for build_compile_node_context_inner)
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
pub fn build_compile_node_context<T>(
    model: &T,
    resolver_state: &ResolverState,
    base_context: &BTreeMap<String, MinijinjaValue>,
    global_static_analysis: Spanned<StaticAnalysisKind>,
    skip_ref_validation: bool,
) -> (
    BTreeMap<String, MinijinjaValue>,
    Arc<DashMap<String, MinijinjaValue>>,
)
where
    T: InternalDbtNodeAttributes + ?Sized,
{
    build_compile_node_context_inner(
        model,
        resolver_state.adapter_type,
        base_context,
        &resolver_state.root_project_name,
        resolver_state.node_resolver.clone(),
        resolver_state.runtime_config.clone(),
        global_static_analysis,
        skip_ref_validation,
    )
}

/// Build a compile model context
/// Returns a context and the current relation
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
pub fn build_compile_node_context_inner<T>(
    model: &T,
    adapter_type: AdapterType,
    base_context: &BTreeMap<String, MinijinjaValue>,
    root_project_name: &str,
    node_resolver: Arc<dyn NodeResolverTracker>,
    runtime_config: Arc<DbtRuntimeConfig>,
    global_static_analysis: Spanned<StaticAnalysisKind>,
    skip_ref_validation: bool,
) -> (
    BTreeMap<String, MinijinjaValue>,
    Arc<DashMap<String, MinijinjaValue>>,
)
where
    T: InternalDbtNodeAttributes + ?Sized,
{
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

    let this_relation = match model.resource_type() {
        NodeType::UnitTest => {
            let ref_name = model
                .base()
                .refs
                .first()
                .cloned()
                .map(|r| r.name)
                .expect("Unit test must have a dependency");
            let (_, this_relation, _, _) = node_resolver
                .lookup_ref(
                    &Some(model.common().package_name.clone()),
                    &ref_name,
                    &None,
                    &None,
                )
                .expect("Ref must exist");
            this_relation
        }
        NodeType::Model => {
            let ref_name = model.common().name.clone();
            // for repl, we use the just create a relation on spot using model passed in.
            if ref_name == REPL_MODEL_NAME {
                dbt_adapter::relation::create_relation(
                    adapter_type,
                    model.base().database.clone(),
                    model.base().schema.clone(),
                    Some(model.base().alias.clone()),
                    None,
                    model.base().quoting,
                )
                .unwrap()
                .as_value()
            } else {
                let (_, this_relation, _, deferred_relation) = node_resolver
                    .lookup_ref(
                        &Some(model.common().package_name.clone()),
                        &ref_name,
                        &model.version().map(|v| v.to_string()),
                        &Some(model.common().package_name.clone()),
                    )
                    .expect("Ref must exist");

                if let Some(deferred_relation_value) = deferred_relation
                    && (matches!(
                        model.base().static_analysis.clone().into_inner(),
                        StaticAnalysisKind::Unsafe
                    ) || global_static_analysis.clone().into_inner()
                        == StaticAnalysisKind::Unsafe
                        || model.introspection().is_unsafe())
                {
                    deferred_relation_value
                } else {
                    this_relation
                }
            }
        }
        _ => dbt_adapter::relation::create_relation(
            adapter_type,
            model.base().database.clone(),
            model.base().schema.clone(),
            Some(model.base().alias.clone()),
            None,
            model.base().quoting,
        )
        .unwrap()
        .as_value(),
    };
    ctx.insert("this".to_owned(), this_relation);
    ctx.insert(
        "database".to_owned(),
        MinijinjaValue::from(model.base().database.to_string()),
    );
    ctx.insert(
        "schema".to_owned(),
        MinijinjaValue::from(model.base().schema.to_string()),
    );
    ctx.insert(
        "identifier".to_owned(),
        MinijinjaValue::from(model.base().alias.clone()),
    );

    let config_map = Arc::new(convert_yml_to_dash_map(model.serialized_config()));
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
        Arc::new(model.base().depends_on.nodes.iter().cloned().collect());

    let ref_function = RefFunction::new_with_validation(
        node_resolver.clone(),
        model.common().package_name.clone(),
        runtime_config.clone(),
        allowed_dependencies.clone(),
        skip_ref_validation,
        // Update to use introspection kind
        (matches!(
            model.base().static_analysis.clone().into_inner(),
            StaticAnalysisKind::Unsafe
        ) || global_static_analysis.into_inner() == StaticAnalysisKind::Unsafe)
            || model.introspection().is_unsafe(),
    );

    let ref_value = MinijinjaValue::from_object(ref_function);
    ctx.insert("ref".to_string(), ref_value.clone());
    base_builtins.insert("ref".to_string(), ref_value);

    // Create validated function function with dependency checking
    let function_function = FunctionFunction::new_with_validation(
        node_resolver.clone(),
        model.common().package_name.clone(),
        runtime_config.clone(),
        allowed_dependencies,
        skip_ref_validation,
    );

    let function_value = MinijinjaValue::from_object(function_function);
    ctx.insert("function".to_string(), function_value.clone());
    base_builtins.insert("function".to_string(), function_value);

    // Register builtins as a global
    ctx.insert(
        "builtins".to_owned(),
        MinijinjaValue::from_object(base_builtins),
    );
    ctx.insert(
        "model".to_owned(),
        MinijinjaValue::from_serialize(model.serialize()),
    );

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
        MinijinjaValue::from(&model.common().package_name),
    );
    ctx.insert(
        TARGET_UNIQUE_ID.to_owned(),
        MinijinjaValue::from(&model.common().unique_id),
    );

    let mut packages = runtime_config
        .dependencies
        .keys()
        .cloned()
        .collect::<BTreeSet<String>>();
    packages.insert(root_project_name.to_string());

    ctx.insert(
        "context".to_owned(),
        MinijinjaValue::from_object(MacroLookupContext {
            root_project_name: root_project_name.to_string(),
            current_project_name: None,
            packages,
        }),
    );

    ctx.insert(
        CURRENT_PATH.to_string(),
        MinijinjaValue::from(model.common().original_file_path.clone().to_string_lossy()),
    );
    ctx.insert(
        CURRENT_SPAN.to_string(),
        MinijinjaValue::from_serialize(Span::default()),
    );

    ctx.insert(
        CURRENT_EXECUTION_PHASE.to_string(),
        MinijinjaValue::from("render"),
    );
    (ctx, config_map)
}
