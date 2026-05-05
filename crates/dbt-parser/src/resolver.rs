//! Module containing the entrypoint for the resolve phase.
use dbt_adapter_core::AdapterType;
#[allow(unused_imports)]
use dbt_common::FsError;
use dbt_common::cancellation::CancellationToken;
use dbt_common::constants::DBT_GENERIC_TESTS_DIR_NAME;
use dbt_common::io_args::FsCommand;
use dbt_common::once_cell_vars::DISPATCH_CONFIG;
use dbt_common::stdfs;
use dbt_common::tracing::emit::{emit_error_log_from_fs_error, emit_warn_log_from_fs_error};
use dbt_common::tracing::event_info::store_event_attributes;
use dbt_common::{ErrorCode, FsResult, err, fs_err};
use dbt_jinja_utils::invocation_args::InvocationArgs;
use dbt_jinja_utils::listener::JinjaTypeCheckingEventListenerFactory;
use dbt_jinja_utils::node_resolver::{
    NodeResolver, check_for_model_deprecations, resolve_dependencies,
};
use dbt_jinja_utils::phases::parse::build_resolve_context;
use dbt_jinja_utils::phases::parse::init::initialize_parse_jinja_environment;
use dbt_jinja_utils::serde::{into_typed_with_error, into_typed_with_jinja};
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_schemas::dbt_utils::resolve_package_quoting;
use dbt_schemas::schemas::common::{Access, DbtIncrementalStrategy};
use dbt_schemas::schemas::macros::{DbtDocsMacro, build_macro_units};
use dbt_schemas::schemas::properties::{MetricsProperties, ModelProperties};
use dbt_schemas::schemas::{InternalDbtNode, Nodes};

use crate::args::ResolveArgs;
use crate::dbt_project_config::{RootProjectConfigs, build_root_project_configs};
use crate::resolve::resolve_groups::resolve_groups;
use crate::resolve::resolve_operations::resolve_operations;
use crate::resolve::resolve_query_comment::resolve_query_comment;
use crate::utils::{self, clear_package_diagnostics};
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schemas::schemas::common::DbtQuoting;
use dbt_schemas::schemas::telemetry::{ExecutionPhase, NodeType, PhaseExecuted};
use dbt_schemas::state::{
    DbtPackage, GenericTestAsset, GetColumnsInRelationCalls, GetRelationCalls, Macros,
    PatternedDanglingSources, RenderResults,
};
use dbt_schemas::state::{DbtRuntimeConfig, Operations};
use dbt_schemas::state::{DbtState, ResolverState};
use minijinja::constants::CURRENT_PATH;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use crate::resolve::resolve_analyses::resolve_analyses;
use crate::resolve::resolve_exposures::resolve_exposures;
use crate::resolve::resolve_functions::resolve_functions;
use crate::resolve::resolve_macros::apply_macro_patches;
use crate::resolve::resolve_macros::resolve_docs_macros;
use crate::resolve::resolve_macros::resolve_macros;
use crate::resolve::resolve_metrics::resolve_metrics;
use crate::resolve::resolve_models::resolve_models;
use crate::resolve::resolve_properties;
use crate::resolve::resolve_properties::resolve_minimal_properties;
use crate::resolve::resolve_saved_queries::resolve_saved_queries;
use crate::resolve::resolve_seeds::resolve_seeds;
use crate::resolve::resolve_semantic_models::resolve_semantic_models;
use crate::resolve::resolve_snapshots::resolve_snapshots;
use crate::resolve::resolve_sources::resolve_sources;
use crate::resolve::resolve_tests::resolve_data_tests::resolve_data_tests;
use crate::resolve::resolve_tests::resolve_unit_tests::resolve_unit_tests;

use crate::resolve::primary_key_inference::infer_and_apply_primary_keys;
use crate::resolve::resolve_selectors::{
    resolve_final_selectors, resolve_manifest_selectors, resolve_selectors_from_yaml,
};
use crate::unused_config_paths::check_unused_resource_config_paths;
use dbt_yaml::Value as YmlValue;

use crate::constants::DEFAULT_OVERVIEW_CONTENTS;

/// Entrypoint for the resolve phase.
///
/// It is responsible for resolving all project source files (i.e. models, seeds, tests,
/// macros etc.) and propagating all configuration properties.
///
/// The final product is the parsed [DbtManifest], along with the collected
/// macros to be used during compilation.
#[tracing::instrument(
    skip_all,
    fields(
        _e = ?store_event_attributes(PhaseExecuted::start_general(ExecutionPhase::Parse)),
    )
)]
#[allow(clippy::too_many_arguments)]
pub async fn resolve(
    arg: &ResolveArgs,
    invocation_args: &InvocationArgs,
    dbt_state: Arc<DbtState>,
    macros: Macros,
    nodes: Nodes,
    get_relation_calls: GetRelationCalls,
    get_columns_in_relation_calls: GetColumnsInRelationCalls,
    patterned_dangling_sources: PatternedDanglingSources,
    token: &CancellationToken,
    jinja_type_checking_event_listener_factory: Arc<dyn JinjaTypeCheckingEventListenerFactory>,
) -> FsResult<(ResolverState, Arc<JinjaEnv>)> {
    // Get the root project name
    let root_project_name = dbt_state.root_project_name();

    let mut macros = macros;
    let mut nodes = nodes;
    let mut get_relation_calls = get_relation_calls;
    let mut get_columns_in_relation_calls = get_columns_in_relation_calls;
    let mut patterned_dangling_sources = patterned_dangling_sources;

    // First, resolve all of the macros from each package
    for package in &dbt_state.packages {
        token.check_cancellation()?;

        let macro_files = package.macro_files.iter().chain(&package.snapshot_files);
        let resolved_macros = resolve_macros(
            &arg.io,
            macro_files.collect::<Vec<_>>().as_slice(),
            package.embedded_file_contents.as_ref(),
        )?;
        macros.macros.extend(resolved_macros);
        let docs_macros = resolve_docs_macros(
            &arg.io,
            &package.docs_files,
            package.embedded_file_contents.as_ref(),
        )?;
        macros.docs_macros.extend(docs_macros);
    }

    // dbt Core always ships a global project with an overview.md that produces
    // doc.dbt.__overview__.  The dbt Docs HTML unconditionally reads this entry
    // (overview controller: `i = n.docs["doc.dbt.__overview__"]`) and crashes
    // with a TypeError if it is absent.  Inject a default entry whenever the
    // user's project (and its dependencies) have not defined their own
    // {% docs __overview__ %} block.
    let overview_uid = "doc.dbt.__overview__".to_string();
    macros
        .docs_macros
        .entry(overview_uid.clone())
        .or_insert_with(|| DbtDocsMacro {
            name: "__overview__".to_string(),
            package_name: "dbt".to_string(),
            path: PathBuf::from("overview.md"),
            original_file_path: PathBuf::from("overview.md"),
            unique_id: overview_uid,
            block_contents: DEFAULT_OVERVIEW_CONTENTS.to_string(),
        });

    let adapter_type = dbt_state.dbt_profile.db_config.adapter_type();

    // Build the root project config
    let root_project_quoting =
        resolve_package_quoting(*dbt_state.root_project().quoting, adapter_type);

    let jinja_env = Arc::new(initialize_parse_jinja_environment(
        root_project_name,
        &dbt_state.dbt_profile.profile,
        &dbt_state.dbt_profile.target,
        adapter_type,
        dbt_state.dbt_profile.db_config.clone(),
        root_project_quoting,
        build_macro_units(&macros.macros),
        dbt_state.vars.clone(),
        dbt_state.cli_vars.clone(),
        dbt_state.root_project_flags(),
        dbt_state.run_started_at,
        invocation_args,
        dbt_state
            .packages
            .iter()
            .map(|p| p.dbt_project.name.clone())
            .collect(),
        arg.io.clone(),
        dbt_state.catalogs.clone(),
    )?);

    // Load and resolve selectors
    let resolved_selectors_map = resolve_selectors_from_yaml(arg, root_project_name, &jinja_env)?;
    let manifest_selectors = resolve_manifest_selectors(resolved_selectors_map.clone())?;
    let resolved_selectors = resolve_final_selectors(resolved_selectors_map, arg)?;

    // Create a map to store full runtime configs for ALL packages
    let mut all_runtime_configs: BTreeMap<String, Arc<DbtRuntimeConfig>> = BTreeMap::new();

    // let mut nodes = Nodes::default();
    let mut disabled_nodes = Nodes::default();
    let root_project_configs = build_root_project_configs(
        arg,
        dbt_state.root_project(),
        root_project_quoting,
        adapter_type,
    )?;
    let root_project_configs = Arc::new(root_project_configs);
    // Process packages in topological order

    let mut node_resolver = NodeResolver::from_dbt_nodes(
        &nodes,
        adapter_type,
        root_project_name.to_string(),
        None,
        arg.sample_config.clone(),
        arg.sample_renaming.clone(),
        arg.command == FsCommand::Compile || arg.command == FsCommand::Test,
    )?;
    let mut collector = RenderResults {
        rendering_results: BTreeMap::new(),
    };

    let package_waves = utils::prepare_package_dependency_levels(dbt_state.clone());

    let mut semantic_layer_spec_is_legacy = false;
    let mut test_name_truncations: HashMap<String, String> = HashMap::new();
    let all_macro_properties: BTreeMap<
        String,
        BTreeMap<String, resolve_properties::MinimalPropertiesEntry>,
    >;

    let (
        resolved_nodes,
        resolved_disabled_nodes,
        resolved_collector,
        resolved_semantic_layer_spec_is_legacy,
        resolved_test_name_truncations,
        resolved_macro_properties,
    ) = resolve_package_waves(
        package_waves,
        arg,
        dbt_state.clone(),
        root_project_name,
        root_project_configs.clone(),
        adapter_type,
        &macros,
        jinja_env.clone(),
        &mut node_resolver,
        &mut all_runtime_configs,
        token,
        jinja_type_checking_event_listener_factory.clone(),
    )
    .await?;
    nodes.extend(resolved_nodes);
    disabled_nodes.extend(resolved_disabled_nodes);
    collector
        .rendering_results
        .extend(resolved_collector.rendering_results);
    semantic_layer_spec_is_legacy |= resolved_semantic_layer_spec_is_legacy;
    test_name_truncations.extend(resolved_test_name_truncations);
    all_macro_properties = resolved_macro_properties;

    // Read the validate_macro_args flag from dbt_project.yml (defaults to true)
    let validate_macro_args = dbt_state
        .root_project_flags()
        .get("validate_macro_args")
        .map(|v| v.is_true())
        .unwrap_or(true);

    // Apply macro patches from YAML schema files
    for (package_name, macro_properties) in all_macro_properties {
        // Get the jinja env and base context for this package
        let package = dbt_state
            .packages
            .iter()
            .find(|p| p.dbt_project.name == package_name);
        if let Some(package) = package {
            let namespace_keys: Vec<String> = jinja_env
                .env
                .get_macro_namespace_registry()
                .map(|r| r.keys().map(|k| k.to_string()).collect())
                .unwrap_or_default();
            let base_ctx = build_resolve_context(
                root_project_name,
                package.dbt_project.name.as_str(),
                &macros.docs_macros,
                DISPATCH_CONFIG.get().unwrap().read().unwrap().clone(),
                namespace_keys,
            );
            apply_macro_patches(
                &arg.io,
                &mut macros.macros,
                &macro_properties,
                &package_name,
                &jinja_env,
                &base_ctx,
                validate_macro_args,
            )?;
        }
    }

    // Ensure that there are no duplicate relations
    check_relation_uniqueness(&nodes)?;

    match nodes.warn_on_microbatch(adapter_type) {
        Ok(_) => {}
        Err(e) => {
            emit_warn_log_from_fs_error(e.as_ref(), arg.io.status_reporter.as_ref());
        }
    }

    let parse_adapter = jinja_env
        .get_adapter()
        .expect("parse adapter must be initialized");
    let parse_adapter_state = parse_adapter
        .parse_adapter_state()
        .expect("adapter must be configured for the parse phase");
    let (
        get_relation_calls_from_parse,
        get_columns_in_relation_calls_from_parse,
        patterned_dangling_sources_from_parse,
    ) = parse_adapter_state.relations_to_fetch();
    get_relation_calls.extend(get_relation_calls_from_parse?);
    get_columns_in_relation_calls.extend(get_columns_in_relation_calls_from_parse?);
    patterned_dangling_sources.extend(patterned_dangling_sources_from_parse);

    let root_runtime_config = all_runtime_configs
        .get(dbt_state.root_project_name())
        .unwrap();

    // Resolve operations (on_run_start and on_run_end) with rendering and dependency extraction
    let mut operations = Operations::default();
    for package in &dbt_state.packages {
        // Get the package-specific runtime config so operations can access package vars
        let package_runtime_config = all_runtime_configs
            .get(&package.dbt_project.name)
            .unwrap_or(root_runtime_config);

        let (on_run_start, on_run_end) = resolve_operations(
            &package.dbt_project,
            &package.package_root_path,
            &arg.io.in_dir,
            &jinja_env,
            &arg.io,
            arg.static_analysis,
            adapter_type,
            &dbt_state.dbt_profile.database,
            &dbt_state.dbt_profile.schema,
            DbtQuoting {
                database: root_project_quoting.database,
                schema: root_project_quoting.schema,
                identifier: root_project_quoting.identifier,
                snowflake_ignore_case: None,
            },
            package_runtime_config.clone(),
        )?;
        operations.on_run_start.extend(on_run_start);
        operations.on_run_end.extend(on_run_end);
    }

    // take refs and sources, resolve them to a unique_id and put in depends_on
    // This returns a set of node IDs that had resolution errors (unresolved refs/sources)
    let nodes_with_resolution_errors = resolve_dependencies(
        &arg.io,
        &mut nodes,
        &mut disabled_nodes,
        &mut operations,
        &node_resolver,
    );
    for warning in microbatch_model_no_event_time_inputs_warnings(&nodes) {
        emit_warn_log_from_fs_error(&warning, arg.io.status_reporter.as_ref());
    }

    // Check for model deprecation warnings
    check_for_model_deprecations(&arg.io, &nodes);

    check_unused_resource_config_paths(
        &arg.io,
        &dbt_state.root_package().package_root_path,
        &nodes,
        &disabled_nodes,
    )?;

    // Check access
    let nodes_with_access_errors = check_access(arg, &nodes, &all_runtime_configs);

    // Set the project name on nodes so that `package:this` selectors can resolve
    nodes.project_name = Some(root_project_name.to_string());

    // Store macros in nodes.macros so that they can be accessed by
    // state:modified for checking macro modifications
    // TODO: Instead of cloning macro_node into an Arc, implement
    //       macro_node as an Arc from the outset. Note that this
    //.      has the potential for a huge blast radius,
    //.      hence why we leave it as a TODO for when we
    //.      have the bandwidth to do it.
    //       See: https://github.com/dbt-labs/fs/pull/8760#discussion_r2965959119
    for (uid, macro_node) in &macros.macros {
        nodes
            .macros
            .insert(uid.clone(), Arc::new(macro_node.clone()));
    }

    Ok((
        ResolverState {
            root_project_name: root_project_name.to_string(),
            adapter_type,
            nodes,
            disabled_nodes,
            macros,
            operations,
            dbt_profile: dbt_state.dbt_profile.clone(),
            render_results: collector,
            run_started_at: dbt_state.run_started_at,
            nodes_with_resolution_errors,
            nodes_with_access_errors,
            node_resolver: Arc::new(node_resolver),
            get_relation_calls,
            get_columns_in_relation_calls,
            patterned_dangling_sources,
            runtime_config: root_runtime_config.clone(),
            manifest_selectors,
            resolved_selectors,
            root_project_quoting: root_project_quoting.try_into()?,
            defer_nodes: None,
            semantic_layer_spec_is_legacy,
            test_name_truncations,
        },
        jinja_env,
    ))
}

// Check that models accessing other models (dependecies) can do so.
// Returns the set of unique_ids that have access violations.
fn check_access(
    arg: &ResolveArgs,
    nodes: &Nodes,
    all_runtime_configs: &BTreeMap<String, Arc<DbtRuntimeConfig>>,
) -> HashSet<String> {
    let mut violations = HashSet::new();

    // Check access for models
    for (unique_id, node) in nodes.models.iter() {
        if check_node_access(
            arg,
            unique_id,
            &node.base().depends_on.nodes_with_ref_location,
            &node.common().package_name,
            nodes,
            all_runtime_configs,
            |target_node, diffent_packages| {
                // Models can access private models if they're in the same group and same package
                node.__model_attr__.group != target_node.__model_attr__.group || diffent_packages
            },
        ) {
            violations.insert(unique_id.clone());
        }
    }

    // Check access for exposures
    for (unique_id, node) in nodes.exposures.iter() {
        if check_node_access(
            arg,
            unique_id,
            &node.base().depends_on.nodes_with_ref_location,
            &node.common().package_name,
            nodes,
            all_runtime_configs,
            |target_node, diffent_packages| {
                // Exposures don't have groups, so they can't access private models
                // unless the private model has no group and they're in the same package
                target_node.__model_attr__.group.is_some() || diffent_packages
            },
        ) {
            violations.insert(unique_id.clone());
        }
    }

    violations
}

fn microbatch_model_no_event_time_inputs_warnings(nodes: &Nodes) -> Vec<FsError> {
    nodes
        .models
        .values()
        .filter(|model| {
            model.__model_attr__.incremental_strategy == Some(DbtIncrementalStrategy::Microbatch)
                && model.__model_attr__.event_time.is_some()
                && !has_event_time_input(nodes, model.as_ref())
        })
        .map(|model| {
            FsError::new(
                ErrorCode::MicrobatchModelNoEventTimeInputs,
                format!(
                    "The microbatch model '{}' has no 'ref' or 'source' input with an 'event_time' configuration. \nThis means no filtering can be applied and can result in unexpected duplicate records in the resulting microbatch model.",
                    model.common().name
                ),
            )
        })
        .collect()
}

fn has_event_time_input(nodes: &Nodes, model: &dyn InternalDbtNode) -> bool {
    model.base().depends_on.nodes.iter().any(|unique_id| {
        nodes
            .models
            .get(unique_id)
            .is_some_and(|node| node.__model_attr__.event_time.is_some())
            || nodes
                .sources
                .get(unique_id)
                .is_some_and(|node| node.deprecated_config.event_time.is_some())
    })
}

/// Helper function to check access for a node referencing other models.
/// Returns true if any access violation was found.
fn check_node_access<F>(
    arg: &ResolveArgs,
    unique_id: &str,
    node_dependencies: &[(String, dbt_common::CodeLocationWithFile)],
    node_package_name: &str,
    nodes: &Nodes,
    all_runtime_configs: &BTreeMap<String, Arc<DbtRuntimeConfig>>,
    should_deny_private_access: F,
) -> bool
where
    F: Fn(&dbt_schemas::schemas::nodes::DbtModel, bool) -> bool,
{
    let mut had_violation = false;
    for (target_unique_id, location) in node_dependencies {
        if let Some(target_node) = nodes.models.get(target_unique_id) {
            let restricted_access = all_runtime_configs
                .get(&target_node.common().package_name)
                .is_some_and(|config| config.inner.restrict_access.unwrap_or(false));

            let diffent_packages =
                target_node.common().package_name != node_package_name && restricted_access;

            if target_node.__model_attr__.access == Access::Private
                && should_deny_private_access(target_node, diffent_packages)
            {
                let err = fs_err!(
                    code => ErrorCode::AccessDenied,
                    loc => location.clone(),
                    "Node '{}' attempted to reference node '{}', which is not allowed because the referenced node is private to the '{}' group",
                    unique_id,
                    target_unique_id,
                    target_node.__model_attr__.group.as_deref().unwrap_or(""),
                );
                emit_error_log_from_fs_error(&err, arg.io.status_reporter.as_ref());
                had_violation = true;
            } else if target_node.__model_attr__.access == Access::Protected && diffent_packages {
                let err = fs_err!(
                    code => ErrorCode::AccessDenied,
                    loc => location.clone(),
                    "Node '{}' attempted to reference node '{}', which is not allowed because the referenced node is protected to the '{}' package",
                    unique_id,
                    target_unique_id,
                    target_node.common().package_name,
                );
                emit_error_log_from_fs_error(&err, arg.io.status_reporter.as_ref());
                had_violation = true;
            }
        }
    }
    had_violation
}

/// Inner resolve function that resolves a single package.
#[allow(clippy::too_many_arguments)]
pub async fn resolve_inner(
    arg: &ResolveArgs,
    package: &DbtPackage,
    dbt_state: Arc<DbtState>,
    root_package_name: &str,
    root_project_configs: &RootProjectConfigs,
    adapter_type: AdapterType,
    macros: &Macros,
    jinja_env: Arc<JinjaEnv>,
    mut node_resolver: NodeResolver,
    runtime_config: Arc<DbtRuntimeConfig>,
    test_name_truncations: &mut HashMap<String, String>,
    token: &CancellationToken,
    jinja_type_checking_event_listener_factory: Arc<dyn JinjaTypeCheckingEventListenerFactory>,
) -> FsResult<(
    Nodes,
    Nodes,
    RenderResults,
    NodeResolver,
    bool,
    BTreeMap<String, resolve_properties::MinimalPropertiesEntry>,
)> {
    let mut nodes = Nodes::default();
    let mut disabled_nodes = Nodes::default();

    let database: &String = &dbt_state.dbt_profile.database;

    let schema = &dbt_state.dbt_profile.schema;

    let package_quoting = resolve_package_quoting(*package.dbt_project.quoting, adapter_type);

    let namespace_keys: Vec<String> = jinja_env
        .env
        .get_macro_namespace_registry()
        .map(|r| r.keys().map(|k| k.to_string()).collect())
        .unwrap_or_default();
    let base_ctx = build_resolve_context(
        root_package_name,
        package.dbt_project.name.as_str(),
        &macros.docs_macros,
        DISPATCH_CONFIG.get().unwrap().read().unwrap().clone(),
        namespace_keys,
    );
    // Resolve the dbt properties (schema.yml) files
    let mut min_properties = resolve_minimal_properties(
        arg,
        package,
        root_package_name,
        root_project_configs,
        &jinja_env,
        &base_ctx,
        token,
    )?;

    let package_name = package.dbt_project.name.as_str();

    // Collect macro properties for patching later (after all packages resolved)
    let macro_properties = std::mem::take(&mut min_properties.macros);

    let mut collected_generic_tests: Vec<GenericTestAsset> = Vec::new();

    let dbt_tests_dir = arg.io.out_dir.join(DBT_GENERIC_TESTS_DIR_NAME);
    stdfs::create_dir_all(&dbt_tests_dir)?;

    let dependency_package_name = dependency_package_name_from_ctx(&jinja_env, &base_ctx);
    let mut typed_models_properties: BTreeMap<String, ModelProperties> = BTreeMap::new();

    let semantic_layer_spec_is_legacy = min_properties.semantic_layer_spec_is_legacy;

    for (model_name, minimal_model_props) in &min_properties.models {
        // Update base context with the relative yaml file path.
        // We do this for accurate error reporting.
        let base_ctx = {
            let mut base_ctx = base_ctx.clone();
            base_ctx.insert(
                CURRENT_PATH.to_string(),
                minijinja::Value::from(minimal_model_props.relative_path.to_string_lossy()),
            );
            base_ctx
        };

        // Extract metrics to be parsed separately, without full Jinja rendering.
        // Metric fields like `filter` and `expr` contain MetricFlow DSL
        // (e.g. `{{ Dimension('...') }}`) that must not be evaluated at parse time.
        // The `description` field is rendered selectively in resolve_metrics.
        let mut maybe_model_metrics_yml: Option<YmlValue> = None;
        let mut model_yml = minimal_model_props.clone().schema_value;
        if let Some(m) = model_yml.as_mapping_mut() {
            maybe_model_metrics_yml = m.remove("metrics");
        }

        let mut typed_model_props: ModelProperties = into_typed_with_jinja(
            &arg.io,
            model_yml,
            false,
            &jinja_env,
            &base_ctx,
            &[],
            dependency_package_name,
            // HACK: to avoid duplicate errors due to multiple parses of model yaml properties (once here and once in resolve_models)
            // do not show_errors_or_warnings because that will be done by resolve_models
            false,
        )?;

        if !semantic_layer_spec_is_legacy {
            // The caveat to parsing model.metrics separately is that any yaml errors will be reported with root path
            // as `models.[$].metrics` meaning if there's an unexpected key such as `models.[$].metrics.[$].non_existent_key`
            // it will report unexpected key at `.[$].non_existent_key`, when it should be `.metrics.[$].non_existent_key`
            //
            // This will be inconsistent with unexpected keys in `models.[$]` where for example an unexpected
            // key in `models.[$].derived_semantics.made_up_key` will report unexpected key at
            // `derived_semantics.[$].non_existent_key`
            if let Some(model_metrics_yml) = maybe_model_metrics_yml {
                let typed_model_metrics_props: Option<Vec<MetricsProperties>> =
                    into_typed_with_error(&arg.io, model_metrics_yml, true, None, None)?;
                typed_model_props.metrics = typed_model_metrics_props;
            }
        }

        typed_models_properties.insert(model_name.clone(), typed_model_props);
    }

    // Resolve sources based on the dbt_state, database, schema, and project name
    let (sources, disabled_sources) = resolve_sources(
        arg,
        package,
        root_package_name,
        dbt_state.root_project(),
        root_project_configs,
        min_properties.source_tables,
        database,
        adapter_type,
        &base_ctx,
        &jinja_env,
        &mut collected_generic_tests,
        test_name_truncations,
        &mut node_resolver,
    )?;
    nodes.sources.extend(sources);
    disabled_nodes.sources.extend(disabled_sources);

    // Resolve seeds based on the dbt_state, database, schema, and project name
    let (seeds, disabled_seeds) = resolve_seeds(
        arg,
        min_properties.seeds,
        package,
        package_quoting,
        dbt_state.root_project(),
        root_project_configs,
        database,
        schema,
        adapter_type,
        package_name,
        &jinja_env,
        &base_ctx,
        &mut collected_generic_tests,
        test_name_truncations,
        &mut node_resolver,
    )?;
    nodes.seeds.extend(seeds);
    disabled_nodes.seeds.extend(disabled_seeds);

    // TODO: resolve_snapshots still creates its own local JinjaTypeCheckingEventListenerFactory
    // instead of receiving the top-level one, so snapshot macro dependencies are populated
    // locally rather than via update_manifest_with_macro_depends_on. Out of scope for now.
    // Resolve snapshots based on the dbt_state, database, schema, and project name
    let (snapshots, disabled_snapshots) = resolve_snapshots(
        arg,
        package,
        package_quoting,
        dbt_state.root_project(),
        root_project_configs,
        min_properties.snapshots,
        &macros.macros,
        database,
        schema,
        adapter_type,
        jinja_env.clone(),
        &base_ctx,
        runtime_config.clone(),
        &mut node_resolver,
        &mut collected_generic_tests,
        test_name_truncations,
        token,
    )
    .await?;
    nodes.snapshots.extend(snapshots);
    disabled_nodes.snapshots.extend(disabled_snapshots);

    let (groups, disabled_groups) = resolve_groups(
        arg,
        &mut min_properties.groups,
        package_name,
        &jinja_env,
        &base_ctx,
    )
    .await?;

    nodes.groups.extend(groups);
    disabled_nodes.groups.extend(disabled_groups);

    // Resolve SQLs and get nodes and rendered SQLs except refs and sources
    let (models, rendering_results, disabled_models) = resolve_models(
        arg,
        package,
        package_quoting,
        dbt_state.root_project(),
        root_project_configs,
        &min_properties.models,
        // TODO: pass in typed_models_properties
        database,
        schema,
        adapter_type,
        package_name,
        jinja_env.clone(),
        &base_ctx,
        runtime_config.clone(),
        &mut collected_generic_tests,
        test_name_truncations,
        &mut node_resolver,
        token,
        jinja_type_checking_event_listener_factory.clone(),
    )
    .await?;
    nodes.models.extend(models);
    disabled_nodes.models.extend(disabled_models);

    // TODO: resolve_analyses still creates its own local JinjaTypeCheckingEventListenerFactory
    // instead of receiving the top-level one — same issue as resolve_snapshots. Out of scope for now.
    let (analyses, analyses_rendering_results) = resolve_analyses(
        arg,
        package,
        package_quoting,
        dbt_state.root_project(),
        root_project_configs,
        &mut min_properties.analyses,
        database,
        schema,
        adapter_type,
        package_name,
        jinja_env.clone(),
        &base_ctx,
        runtime_config.clone(),
        token,
    )
    .await?;
    nodes.analyses.extend(analyses);

    // Resolve functions
    let (functions, functions_rendering_results) = resolve_functions(
        arg,
        package,
        package_quoting,
        dbt_state.root_project(),
        root_project_configs,
        &mut min_properties.functions,
        database,
        schema,
        adapter_type,
        package_name,
        jinja_env.clone(),
        &base_ctx,
        runtime_config.clone(),
        &mut node_resolver,
        token,
    )
    .await?;
    nodes.functions.extend(functions);

    let (exposures, disabled_exposures) = resolve_exposures(
        arg,
        &mut min_properties.exposures,
        package,
        dbt_state.root_project(),
        root_project_configs,
        database,
        schema,
        adapter_type,
        package_name,
        &jinja_env,
        &base_ctx,
    )
    .await?;
    nodes.exposures.extend(exposures);
    disabled_nodes.exposures.extend(disabled_exposures);

    if !semantic_layer_spec_is_legacy {
        let (semantic_models, disabled_semantic_models) = resolve_semantic_models(
            arg,
            package,
            root_project_configs,
            &min_properties.models,
            &typed_models_properties,
            &nodes.models,
            package_name,
            &jinja_env,
            &base_ctx,
        )
        .await?;
        nodes.semantic_models.extend(semantic_models);
        disabled_nodes
            .semantic_models
            .extend(disabled_semantic_models);

        let (metrics, disabled_metrics) = resolve_metrics(
            arg,
            package,
            root_project_configs,
            &min_properties.models,
            &min_properties.metrics,
            &typed_models_properties,
            package_name,
            &jinja_env,
            &base_ctx,
        )
        .await?;
        nodes.metrics.extend(metrics);
        disabled_nodes.metrics.extend(disabled_metrics);

        let (saved_queries, disabled_saved_queries) = resolve_saved_queries(
            arg,
            package,
            root_package_name,
            root_project_configs,
            &mut min_properties.saved_queries,
            database,
            schema,
            package_name,
            jinja_env.clone(),
            &base_ctx,
        )
        .await?;
        nodes.saved_queries.extend(saved_queries);
        disabled_nodes.saved_queries.extend(disabled_saved_queries);
    }

    let (data_tests, disabled_tests) = resolve_data_tests(
        arg,
        package,
        package_quoting,
        dbt_state.root_project(),
        root_project_configs,
        &mut min_properties.tests,
        database,
        schema,
        adapter_type,
        jinja_env.clone(),
        &base_ctx,
        runtime_config.clone(),
        &collected_generic_tests,
        &node_resolver,
        token,
        jinja_type_checking_event_listener_factory.clone(),
        &nodes.models,
    )
    .await?;
    nodes.tests.extend(data_tests);
    disabled_nodes.tests.extend(disabled_tests);

    infer_and_apply_primary_keys(&mut nodes, &disabled_nodes);

    let (unit_tests, disabled_unit_tests) = resolve_unit_tests(
        arg,
        min_properties.unit_tests,
        package,
        package_quoting,
        root_project_configs,
        package_name,
        &jinja_env,
        &base_ctx,
        &min_properties.models,
        &nodes.models,
    )?;

    nodes.unit_tests.extend(unit_tests);
    disabled_nodes.unit_tests.extend(disabled_unit_tests);

    if let Some(query_comment) = package.dbt_project.query_comment.as_ref() {
        resolve_query_comment(query_comment, &jinja_env, &base_ctx)?;
    }

    let collector = RenderResults {
        rendering_results: rendering_results
            .into_iter()
            .chain(analyses_rendering_results)
            .chain(functions_rendering_results)
            .collect(),
    };

    clear_package_diagnostics(&arg.io, package);

    Ok((
        nodes,
        disabled_nodes,
        collector,
        node_resolver,
        semantic_layer_spec_is_legacy,
        macro_properties,
    ))
}

/// Function to check models, seeds, and snapshots for relation uniqueness
pub fn check_relation_uniqueness(nodes: &Nodes) -> FsResult<()> {
    let mut alias_resources: HashMap<String, &dyn InternalDbtNode> = HashMap::new();

    for (_, node) in nodes.iter() {
        // We only check models, seeds and snapshots
        if ![NodeType::Model, NodeType::Seed, NodeType::Snapshot].contains(&node.resource_type()) {
            continue;
        }
        if let Some(node_relation_name) = node.base().relation_name.clone() {
            // Check for alias conflicts
            if let std::collections::hash_map::Entry::Vacant(e) =
                alias_resources.entry(node_relation_name.clone())
            {
                e.insert(node);
            } else {
                // Get node that's already stored
                let existing_node = alias_resources.get(&node_relation_name).unwrap();
                return err!(
                    ErrorCode::InvalidConfig,
                    "dbt found two resources with the database relation {}. Nodes: {}, {}",
                    node_relation_name,
                    node.common().unique_id,
                    existing_node.common().unique_id
                );
            }
        }
    }

    Ok(())
}

/// Resolves a single package asynchronously.
#[allow(clippy::too_many_arguments)]
async fn resolve_package(
    package_name: String,
    arg: &ResolveArgs,
    dbt_state: Arc<DbtState>,
    root_project_name: String,
    root_project_configs: Arc<RootProjectConfigs>,
    adapter_type: AdapterType,
    macros: &Macros,
    jinja_env: Arc<JinjaEnv>,
    node_resolver: NodeResolver,
    all_runtime_configs: &BTreeMap<String, Arc<DbtRuntimeConfig>>,
    token: &CancellationToken,
    jinja_type_checking_event_listener_factory: Arc<dyn JinjaTypeCheckingEventListenerFactory>,
) -> FsResult<(
    String,
    Arc<DbtRuntimeConfig>,
    Nodes,
    Nodes,
    RenderResults,
    NodeResolver,
    bool,
    HashMap<String, String>,
    BTreeMap<String, resolve_properties::MinimalPropertiesEntry>,
)> {
    let package = dbt_state
        .packages
        .iter()
        .find(|p| p.dbt_project.name == package_name)
        .ok_or_else(|| {
            fs_err!(
                ErrorCode::InvalidConfig,
                "Encountered unexpected package not found in project: {}",
                package_name
            )
        })?;
    let vars = dbt_state
        .vars
        .get(&package_name)
        .expect("All packages should have vars initialized");

    let runtime_config = Arc::new(DbtRuntimeConfig::new(
        &arg.io.in_dir,
        package,
        &dbt_state.dbt_profile,
        all_runtime_configs,
        vars,
        &dbt_state.cli_vars.clone(),
    ));

    let mut test_name_truncations: HashMap<String, String> = HashMap::new();
    let (
        new_nodes,
        new_disabled_nodes,
        rendering_results,
        updated_node_resolver,
        semantic_layer_spec_is_legacy,
        macro_properties,
    ) = resolve_inner(
        arg,
        package,
        dbt_state.clone(),
        &root_project_name,
        &root_project_configs,
        adapter_type,
        macros,
        jinja_env.clone(),
        node_resolver,
        runtime_config.clone(),
        &mut test_name_truncations,
        token,
        jinja_type_checking_event_listener_factory.clone(),
    )
    .await?;

    // Return everything needed for merging
    Ok((
        package_name,
        runtime_config,
        new_nodes,
        new_disabled_nodes,
        rendering_results,
        updated_node_resolver,
        semantic_layer_spec_is_legacy,
        test_name_truncations,
        macro_properties,
    ))
}

/// Resolves packages in waves (inter-wave sequential, intra-wave parallel via `dispatch_maybe_parallel`).
#[allow(clippy::too_many_arguments)]
async fn resolve_package_waves(
    package_waves: Vec<Vec<String>>,
    arg: &ResolveArgs,
    dbt_state: Arc<DbtState>,
    root_project_name: &str,
    root_project_configs: Arc<RootProjectConfigs>,
    adapter_type: AdapterType,
    macros: &Macros,
    jinja_env: Arc<JinjaEnv>,
    node_resolver: &mut NodeResolver,
    all_runtime_configs: &mut BTreeMap<String, Arc<DbtRuntimeConfig>>,
    token: &CancellationToken,
    jinja_type_checking_event_listener_factory: Arc<dyn JinjaTypeCheckingEventListenerFactory>,
) -> FsResult<(
    Nodes,
    Nodes,
    RenderResults,
    bool,
    HashMap<String, String>,
    BTreeMap<String, BTreeMap<String, resolve_properties::MinimalPropertiesEntry>>,
)> {
    let max_concurrency =
        crate::parallel::effective_parallelism_with(arg.num_threads, arg.no_parallel);
    let arg = Arc::new(arg.clone());
    let macros = Arc::new(macros.clone());

    let mut nodes = Nodes::default();
    let mut disabled_nodes = Nodes::default();
    let mut collector = RenderResults {
        rendering_results: BTreeMap::new(),
    };
    let mut semantic_layer_spec_is_legacy = false;
    let mut test_name_truncations: HashMap<String, String> = HashMap::new();
    let mut all_macro_properties: BTreeMap<
        String,
        BTreeMap<String, resolve_properties::MinimalPropertiesEntry>,
    > = BTreeMap::new();

    for package_wave in package_waves {
        token.check_cancellation()?;

        // Snapshot per-wave state for parallel tasks
        let runtime_configs_snapshot = Arc::new(all_runtime_configs.clone());
        let node_resolver_snapshot = node_resolver.clone();

        let arg = arg.clone();
        let dbt_state = dbt_state.clone();
        let root_project_name = root_project_name.to_string();
        let root_project_configs = root_project_configs.clone();
        let macros = macros.clone();
        let jinja_env = jinja_env.clone();
        let token = token.clone();
        let jinja_type_checking_event_listener_factory =
            jinja_type_checking_event_listener_factory.clone();

        let results = crate::parallel::dispatch_maybe_parallel(
            package_wave,
            max_concurrency > 1,
            move |package_name: String| {
                let arg = arg.clone();
                let dbt_state = dbt_state.clone();
                let root_project_name = root_project_name.clone();
                let root_project_configs = root_project_configs.clone();
                let macros = macros.clone();
                let jinja_env = jinja_env.clone();
                let node_resolver = node_resolver_snapshot.clone();
                let runtime_configs = runtime_configs_snapshot.clone();
                let token = token.clone();
                let jinja_type_checking_event_listener_factory =
                    jinja_type_checking_event_listener_factory.clone();

                async move {
                    resolve_package(
                        package_name,
                        &arg,
                        dbt_state,
                        root_project_name,
                        root_project_configs,
                        adapter_type,
                        &macros,
                        jinja_env,
                        node_resolver,
                        &runtime_configs,
                        &token,
                        jinja_type_checking_event_listener_factory,
                    )
                    .await
                }
            },
        )
        .await?;

        // Merge wave results back into accumulators
        for result in results {
            let (
                package_name,
                runtime_config,
                new_nodes,
                new_disabled_nodes,
                rendering_results,
                updated_node_resolver,
                resolved_semantic_layer_spec_is_legacy,
                resolved_test_name_truncations,
                macro_properties,
            ) = result;

            semantic_layer_spec_is_legacy |= resolved_semantic_layer_spec_is_legacy;

            dbt_schemas::state::register_global_runtime_config(
                package_name.clone(),
                runtime_config.clone(),
            );
            all_runtime_configs.insert(package_name.clone(), runtime_config);

            if !macro_properties.is_empty() {
                all_macro_properties.insert(package_name.clone(), macro_properties);
            }

            nodes.extend(new_nodes);
            disabled_nodes.extend(new_disabled_nodes);
            collector
                .rendering_results
                .extend(rendering_results.rendering_results);
            test_name_truncations.extend(resolved_test_name_truncations);
            node_resolver.merge(updated_node_resolver);
        }
    }

    Ok((
        nodes,
        disabled_nodes,
        collector,
        semantic_layer_spec_is_legacy,
        test_name_truncations,
        all_macro_properties,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use dbt_schemas::schemas::macros::DbtDocsMacro;

    use crate::constants::DEFAULT_OVERVIEW_CONTENTS;

    /// Helper that applies the same injection logic as the resolver so tests
    /// stay in sync with the production code.
    fn inject_default_overview(docs: &mut BTreeMap<String, DbtDocsMacro>) {
        let overview_uid = "doc.dbt.__overview__".to_string();
        docs.entry(overview_uid.clone())
            .or_insert_with(|| DbtDocsMacro {
                name: "__overview__".to_string(),
                package_name: "dbt".to_string(),
                path: PathBuf::from("overview.md"),
                original_file_path: PathBuf::from("overview.md"),
                unique_id: overview_uid,
                block_contents: DEFAULT_OVERVIEW_CONTENTS.to_string(),
            });
    }

    /// When a project defines no {% docs %} blocks, `doc.dbt.__overview__`
    /// must be present in the manifest so the dbt Docs HTML doesn't crash.
    #[test]
    fn test_default_overview_injected_when_no_docs_defined() {
        let mut docs: BTreeMap<String, DbtDocsMacro> = BTreeMap::new();
        inject_default_overview(&mut docs);

        let entry = docs
            .get("doc.dbt.__overview__")
            .expect("doc.dbt.__overview__ must be injected");

        assert_eq!(entry.name, "__overview__");
        assert_eq!(entry.package_name, "dbt");
        assert_eq!(entry.unique_id, "doc.dbt.__overview__");
        assert!(
            !entry.block_contents.is_empty(),
            "block_contents must not be empty"
        );
    }

    /// A user-defined {% docs __overview__ %} (package_name = project) must
    /// NOT be overwritten by the default injection.
    #[test]
    fn test_user_overview_not_overwritten() {
        let uid = "doc.dbt.__overview__".to_string();
        let user_doc = DbtDocsMacro {
            name: "__overview__".to_string(),
            package_name: "my_project".to_string(),
            path: PathBuf::from("models/overview.md"),
            original_file_path: PathBuf::from("models/overview.md"),
            unique_id: uid.clone(),
            block_contents: "# My custom overview".to_string(),
        };

        let mut docs: BTreeMap<String, DbtDocsMacro> = BTreeMap::new();
        docs.insert(uid, user_doc);
        inject_default_overview(&mut docs);

        let entry = docs
            .get("doc.dbt.__overview__")
            .expect("entry must still exist");
        assert_eq!(
            entry.block_contents, "# My custom overview",
            "user-defined overview must not be replaced by the default"
        );
    }
}
