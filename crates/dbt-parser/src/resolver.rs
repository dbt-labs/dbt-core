//! Module containing the entrypoint for the resolve phase.
#[allow(unused_imports)]
use dbt_common::FsError;
use dbt_common::cancellation::CancellationToken;
use dbt_common::constants::{DBT_GENERIC_TESTS_DIR_NAME, RESOLVING};
use dbt_common::once_cell_vars::DISPATCH_CONFIG;
use dbt_common::tracing::ToTracingValue;
use dbt_common::{ErrorCode, FsResult, err, fs_err, show_error, with_progress};
use dbt_common::{show_warning, stdfs};
use dbt_jinja_utils::invocation_args::InvocationArgs;
use dbt_jinja_utils::phases::parse::build_resolve_context;
use dbt_jinja_utils::phases::parse::init::initialize_parse_jinja_environment;
use dbt_jinja_utils::refs_and_sources::{RefsAndSources, resolve_dependencies};
use dbt_schemas::dbt_utils::resolve_package_quoting;
use dbt_schemas::schemas::common::Access;
use dbt_schemas::schemas::macros::build_macro_units;
use dbt_schemas::schemas::{InternalDbtNode, Nodes};

use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_schemas::state::RenderResults;
use dbt_schemas::state::{DbtPackage, GenericTestAsset, Macros};
use dbt_schemas::state::{DbtRuntimeConfig, Operations};

use crate::args::ResolveArgs;
use crate::dbt_project_config::{RootProjectConfigs, build_root_project_configs};
use crate::resolve::resolve_operations::resolve_operations;
use crate::utils::{self, clear_package_diagnostics};
use dbt_schemas::schemas::telemetry::BuildPhaseInfo;
use dbt_schemas::schemas::telemetry::SharedPhaseInfo;
use dbt_schemas::schemas::telemetry::TelemetryAttributes;
use dbt_schemas::state::DbtState;
use dbt_schemas::state::ResolverState;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::resolve::resolve_analyses::resolve_analyses;
use crate::resolve::resolve_exposures::resolve_exposures;
use crate::resolve::resolve_macros::resolve_docs_macros;
use crate::resolve::resolve_macros::resolve_macros;
use crate::resolve::resolve_metrics::resolve_metrics;
use crate::resolve::resolve_models::resolve_models;
use crate::resolve::resolve_properties::resolve_minimal_properties;
use crate::resolve::resolve_seeds::resolve_seeds;
use crate::resolve::resolve_snapshots::resolve_snapshots;
use crate::resolve::resolve_sources::resolve_sources;
use crate::resolve::resolve_tests::resolve_data_tests::resolve_data_tests;
use crate::resolve::resolve_tests::resolve_unit_tests::resolve_unit_tests;

use crate::resolve::resolve_selectors::resolve_final_selectors;

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
        __event = TelemetryAttributes::Phase(BuildPhaseInfo::Parsing {
            shared: SharedPhaseInfo {
                invocation_id: invocation_args.invocation_id.to_string(),
            }
        }).to_tracing_value(),
    )
)]
pub async fn resolve(
    arg: &ResolveArgs,
    invocation_args: &InvocationArgs,
    dbt_state: Arc<DbtState>,
    macros: Macros,
    nodes: Nodes,
    listener_factory: Option<Arc<dyn dbt_jinja_utils::listener::RenderingEventListenerFactory>>,
    token: &CancellationToken,
) -> FsResult<(ResolverState, Arc<JinjaEnv>)> {
    let _pb = with_progress!(arg.io, spinner => RESOLVING);

    // Get the root project name
    let root_project_name = dbt_state.root_project_name();
    let adapter_type = dbt_state.dbt_profile.db_config.adapter_type();

    // let mut macros = Macros::default();
    let mut macros = macros;
    let mut nodes = nodes;

    // First, resolve all of the macros from each package
    for package in &dbt_state.packages {
        token.check_cancellation()?;

        let macro_files = package.macro_files.iter().chain(&package.snapshot_files);
        let resolved_macros = resolve_macros(&arg.io, macro_files.collect::<Vec<_>>().as_slice())?;
        macros.macros.extend(resolved_macros);
        let docs_macros = resolve_docs_macros(&package.docs_files)?;
        macros.docs_macros.extend(docs_macros);
    }

    let mut operations = Operations::default();
    for package in &dbt_state.packages {
        let (on_run_start, on_run_end) = resolve_operations(
            &package.dbt_project,
            &package.package_root_path,
            &arg.io.in_dir,
        );
        operations.on_run_start.extend(on_run_start);
        operations.on_run_end.extend(on_run_end);
    }

    // Build the root project config
    let root_project_quoting = resolve_package_quoting(
        *dbt_state.root_project().quoting,
        &dbt_state.dbt_profile.db_config.adapter_type(),
    );

    let jinja_env = Arc::new(initialize_parse_jinja_environment(
        root_project_name,
        &dbt_state.dbt_profile.profile,
        &dbt_state.dbt_profile.target,
        &dbt_state.dbt_profile.db_config.adapter_type(),
        &dbt_state.dbt_profile.db_config,
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
        listener_factory,
        token.clone(),
    )?);

    // Compute final selectors
    let resolved_selectors = resolve_final_selectors(root_project_name, &jinja_env, arg)?;

    // Create a map to store full runtime configs for ALL packages
    let mut all_runtime_configs: BTreeMap<String, Arc<DbtRuntimeConfig>> = BTreeMap::new();

    // let mut nodes = Nodes::default();
    let mut disabled_nodes = Nodes::default();
    let root_project_configs =
        build_root_project_configs(&arg.io, dbt_state.root_project(), root_project_quoting)?;
    let root_project_configs = Arc::new(root_project_configs);
    // Process packages in topological order
    let mut refs_and_sources = RefsAndSources::from_dbt_nodes(
        &nodes,
        &adapter_type,
        root_project_name.to_string(),
        None,
        arg.sample_config.clone(),
    )?;
    let mut collector = RenderResults {
        rendering_results: BTreeMap::new(),
    };

    let package_waves = utils::prepare_package_dependency_levels(dbt_state.clone());

    // Use sequential processing if num_threads is 1, otherwise use parallel processing
    if arg.num_threads == Some(1) {
        let (resolved_nodes, resolved_disabled_nodes, resolved_collector) =
            resolve_packages_sequentially(
                package_waves,
                arg,
                dbt_state.clone(),
                root_project_name,
                root_project_configs.clone(),
                &adapter_type,
                &macros,
                jinja_env.clone(),
                &mut refs_and_sources,
                &mut all_runtime_configs,
                token,
            )
            .await?;

        nodes.extend(resolved_nodes);
        disabled_nodes.extend(resolved_disabled_nodes);
        collector
            .rendering_results
            .extend(resolved_collector.rendering_results);
    } else {
        // Parallel processing (original implementation)
        let (resolved_nodes, resolved_disabled_nodes, resolved_collector) =
            resolve_packages_parallel(
                package_waves,
                arg,
                dbt_state.clone(),
                root_project_name,
                root_project_configs.clone(),
                &adapter_type,
                &macros,
                jinja_env.clone(),
                &mut refs_and_sources,
                &mut all_runtime_configs,
                token,
            )
            .await?;

        nodes.extend(resolved_nodes);
        disabled_nodes.extend(resolved_disabled_nodes);
        collector
            .rendering_results
            .extend(resolved_collector.rendering_results);
    }
    // Ensure that there are no duplicate relations
    check_relation_uniqueness(&nodes)?;

    match nodes.warn_on_custom_materializations() {
        Ok(_) => {}
        Err(e) => {
            if arg.command == "parse" {
                show_warning!(arg.io, e);
            } else {
                show_error!(arg.io, e);
            }
        }
    }
    match nodes.warn_on_microbatch() {
        Ok(_) => {}
        Err(e) => {
            show_warning!(arg.io, e);
        }
    }

    let parse_adapter = jinja_env
        .get_parse_adapter()
        .expect("parse adapter must be initialized");
    let (call_get_relation, call_get_columns_in_relation, patterned_dangling_sources) =
        parse_adapter.relations_to_fetch();
    let root_runtime_config = all_runtime_configs
        .get(dbt_state.root_project_name())
        .unwrap();

    // take refs and sources, resolve them to a unique_id and put in depends_on
    // This returns a set of node IDs that had resolution errors (unresolved refs/sources)
    let nodes_with_resolution_errors =
        resolve_dependencies(&arg.io, &mut nodes, &mut disabled_nodes, &refs_and_sources);

    // Check access
    check_access(arg, &nodes, &all_runtime_configs);


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
            refs_and_sources: Arc::new(refs_and_sources),
            get_relation_calls: call_get_relation?,
            get_columns_in_relation_calls: call_get_columns_in_relation?,
            patterned_dangling_sources,
            runtime_config: root_runtime_config.clone(),
            resolved_selectors,
            root_project_quoting: root_project_quoting.try_into()?,
            defer_nodes: None,
        },
        jinja_env,
    ))
}

// Check that models accessing other models (dependecies) can do so.
fn check_access(
    arg: &ResolveArgs,
    nodes: &Nodes,
    all_runtime_configs: &BTreeMap<String, Arc<DbtRuntimeConfig>>,
) {
    // Check access for models
    for (unique_id, node) in nodes.models.iter() {
        check_node_access(
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
        );
    }

    // Check access for exposures
    for (unique_id, node) in nodes.exposures.iter() {
        check_node_access(
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
        );
    }
}

/// Helper function to check access for a node referencing other models
fn check_node_access<F>(
    arg: &ResolveArgs,
    unique_id: &str,
    node_dependencies: &[(String, dbt_common::CodeLocation)],
    node_package_name: &str,
    nodes: &Nodes,
    all_runtime_configs: &BTreeMap<String, Arc<DbtRuntimeConfig>>,
    should_deny_private_access: F,
) where
    F: Fn(&dbt_schemas::schemas::nodes::DbtModel, bool) -> bool,
{
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
                show_error!(arg.io, err);
            } else if target_node.__model_attr__.access == Access::Protected && diffent_packages {
                let err = fs_err!(
                    code => ErrorCode::AccessDenied,
                    loc => location.clone(),
                    "Node '{}' attempted to reference node '{}', which is not allowed because the referenced node is protected to the '{}' package",
                    unique_id,
                    target_unique_id,
                    target_node.common().package_name,
                );
                show_error!(arg.io, err);
            }
        }
    }
}

/// Inner resolve function that resolves a single package.
#[allow(clippy::too_many_arguments)]
pub async fn resolve_inner(
    arg: &ResolveArgs,
    package: &DbtPackage,
    dbt_state: Arc<DbtState>,
    root_package_name: &str,
    root_project_configs: &RootProjectConfigs,
    adapter_type: &str,
    macros: &Macros,
    jinja_env: Arc<JinjaEnv>,
    refs_and_sources: &mut RefsAndSources,
    runtime_config: Arc<DbtRuntimeConfig>,
    token: &CancellationToken,
) -> FsResult<(Nodes, Nodes, RenderResults, RefsAndSources)> {
    let mut nodes = Nodes::default();
    let mut disabled_nodes = Nodes::default();

    let database: &String = &dbt_state.dbt_profile.database;

    let schema = &dbt_state.dbt_profile.schema;

    let package_quoting = resolve_package_quoting(*package.dbt_project.quoting, adapter_type);

    let base_ctx = build_resolve_context(
        root_package_name,
        package.dbt_project.name.as_str(),
        &macros.docs_macros,
        DISPATCH_CONFIG.get().unwrap().read().unwrap().clone(),
    );
    // Resolve the dbt properties (schema.yml) files
    let mut min_properties = resolve_minimal_properties(
        arg,
        package,
        root_package_name,
        &jinja_env,
        &base_ctx,
        token,
    )?;

    let package_name = package.dbt_project.name.as_str();

    let mut collected_generic_tests: Vec<GenericTestAsset> = Vec::new();

    let dbt_tests_dir = arg.io.out_dir.join(DBT_GENERIC_TESTS_DIR_NAME);
    stdfs::create_dir_all(&dbt_tests_dir)?;

    // Resolve sources based on the dbt_state, database, schema, and project name
    let (sources, disabled_sources) = resolve_sources(
        arg,
        package,
        package_quoting,
        root_package_name,
        root_project_configs,
        min_properties.source_tables,
        database,
        adapter_type,
        &base_ctx,
        &jinja_env,
        &mut collected_generic_tests,
        refs_and_sources,
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
        refs_and_sources,
    )?;
    nodes.seeds.extend(seeds);
    disabled_nodes.seeds.extend(disabled_seeds);

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
        refs_and_sources,
        token,
    )
    .await?;
    nodes.snapshots.extend(snapshots);
    disabled_nodes.snapshots.extend(disabled_snapshots);

    // Resolve SQLs and get nodes and rendered SQLs except refs and sources
    let (models, rendering_results, disabled_models) = resolve_models(
        arg,
        package,
        package_quoting,
        dbt_state.root_project(),
        root_project_configs,
        &mut min_properties.models,
        database,
        schema,
        adapter_type,
        package_name,
        jinja_env.clone(),
        &base_ctx,
        runtime_config.clone(),
        &mut collected_generic_tests,
        refs_and_sources,
        token,
    )
    .await?;
    nodes.models.extend(models);
    disabled_nodes.models.extend(disabled_models);

    let (analyses, analyses_rendering_results) = resolve_analyses(
        arg,
        package,
        package_quoting,
        dbt_state.root_project(),
        root_project_configs,
        &mut min_properties.models,
        database,
        schema,
        adapter_type,
        package_name,
        jinja_env.clone(),
        &base_ctx,
        runtime_config.clone(),
        refs_and_sources,
        token,
    )
    .await?;
    nodes.analyses.extend(analyses);

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

    let (metrics, disabled_metrics) = resolve_metrics().await?;
    nodes.metrics.extend(metrics);
    disabled_nodes.metrics.extend(disabled_metrics);

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
        token,
    )
    .await?;
    nodes.tests.extend(data_tests);
    disabled_nodes.tests.extend(disabled_tests);

    let (unit_tests, disabled_unit_tests) = resolve_unit_tests(
        &arg.io,
        min_properties.unit_tests,
        package,
        package_quoting,
        dbt_state.root_project(),
        root_project_configs,
        adapter_type,
        package_name,
        &jinja_env,
        &base_ctx,
        &min_properties.models,
        runtime_config,
        &nodes.models,
    )?;
    nodes.unit_tests.extend(unit_tests);
    disabled_nodes.unit_tests.extend(disabled_unit_tests);

    let collector = RenderResults {
        rendering_results: rendering_results
            .into_iter()
            .chain(analyses_rendering_results)
            .collect(),
    };

    clear_package_diagnostics(&arg.io, package);

    Ok((nodes, disabled_nodes, collector, refs_and_sources.clone()))
}

/// Function to check models, seeds, and snapshots for relation uniqueness
pub fn check_relation_uniqueness(nodes: &Nodes) -> FsResult<()> {
    let mut alias_resources: HashMap<String, &dyn InternalDbtNode> = HashMap::new();

    for (_, node) in nodes.iter() {
        // We only check models, seeds and snapshots
        if !["model", "seed", "snapshot"].contains(&node.resource_type()) {
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
    arg: ResolveArgs,
    dbt_state: Arc<DbtState>,
    root_project_name: String,
    root_project_configs: Arc<RootProjectConfigs>,
    adapter_type: String,
    macros: Macros,
    jinja_env: Arc<JinjaEnv>,
    refs_and_sources: RefsAndSources,
    all_runtime_configs: BTreeMap<String, Arc<DbtRuntimeConfig>>,
    token: &CancellationToken,
) -> FsResult<(
    String,
    Arc<DbtRuntimeConfig>,
    Nodes,
    Nodes,
    RenderResults,
    RefsAndSources,
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
        &all_runtime_configs,
        vars,
        &dbt_state.cli_vars.clone(),
    ));

    let (new_nodes, new_disabled_nodes, rendering_results, updated_refs_and_sources) =
        resolve_inner(
            &arg,
            package,
            dbt_state.clone(),
            &root_project_name,
            &root_project_configs,
            &adapter_type,
            &macros,
            jinja_env.clone(),
            &mut refs_and_sources.clone(),
            runtime_config.clone(),
            token,
        )
        .await?;

    // Return everything needed for merging
    Ok((
        package_name,
        runtime_config,
        new_nodes,
        new_disabled_nodes,
        rendering_results,
        updated_refs_and_sources,
    ))
}

/// Resolves packages sequentially (single-threaded).
#[allow(clippy::too_many_arguments)]
async fn resolve_packages_sequentially(
    package_waves: Vec<Vec<String>>,
    arg: &ResolveArgs,
    dbt_state: Arc<DbtState>,
    root_project_name: &str,
    root_project_configs: Arc<RootProjectConfigs>,
    adapter_type: &str,
    macros: &Macros,
    jinja_env: Arc<JinjaEnv>,
    refs_and_sources: &mut RefsAndSources,
    all_runtime_configs: &mut BTreeMap<String, Arc<DbtRuntimeConfig>>,
    token: &CancellationToken,
) -> FsResult<(Nodes, Nodes, RenderResults)> {
    let mut nodes = Nodes::default();
    let mut disabled_nodes = Nodes::default();
    let mut collector = RenderResults {
        rendering_results: BTreeMap::new(),
    };

    for package_wave in package_waves {
        token.check_cancellation()?;

        for package_name in package_wave {
            let result = resolve_package(
                package_name.clone(),
                arg.clone(),
                dbt_state.clone(),
                root_project_name.to_string(),
                root_project_configs.clone(),
                adapter_type.to_string(),
                macros.clone(),
                jinja_env.clone(),
                refs_and_sources.clone(),
                all_runtime_configs.clone(),
                token,
            )
            .await?;

            let (
                package_name,
                runtime_config,
                new_nodes,
                new_disabled_nodes,
                rendering_results,
                updated_refs_and_sources,
            ) = result;

            // Update runtime configs for next wave
            all_runtime_configs.insert(package_name, runtime_config);
            // Merge results
            nodes.extend(new_nodes);
            disabled_nodes.extend(new_disabled_nodes);
            collector
                .rendering_results
                .extend(rendering_results.rendering_results);
            // Update refs and sources
            refs_and_sources.merge(updated_refs_and_sources);
        }
    }

    Ok((nodes, disabled_nodes, collector))
}

/// Resolves packages in parallel using tokio::spawn.
#[allow(clippy::too_many_arguments)]
async fn resolve_packages_parallel(
    package_waves: Vec<Vec<String>>,
    arg: &ResolveArgs,
    dbt_state: Arc<DbtState>,
    root_project_name: &str,
    root_project_configs: Arc<RootProjectConfigs>,
    adapter_type: &str,
    macros: &Macros,
    jinja_env: Arc<JinjaEnv>,
    refs_and_sources: &mut RefsAndSources,
    all_runtime_configs: &mut BTreeMap<String, Arc<DbtRuntimeConfig>>,
    token: &CancellationToken,
) -> FsResult<(Nodes, Nodes, RenderResults)> {
    let mut nodes = Nodes::default();
    let mut disabled_nodes = Nodes::default();
    let mut collector = RenderResults {
        rendering_results: BTreeMap::new(),
    };

    for package_wave in package_waves {
        token.check_cancellation()?;

        let mut handles = Vec::new();
        for package_name in package_wave {
            let arg = arg.clone();
            let dbt_state = dbt_state.clone();
            let root_project_name = root_project_name.to_string();
            let root_project_configs = root_project_configs.clone();
            let adapter_type = adapter_type.to_string();
            let macros = macros.clone();
            let jinja_env = jinja_env.clone();
            let refs_and_sources = refs_and_sources.clone();
            let all_runtime_configs = all_runtime_configs.clone(); // read-only for this wave
            let dbt_state = dbt_state.clone();
            let token = token.clone();
            handles.push(tokio::spawn(async move {
                resolve_package(
                    package_name,
                    arg,
                    dbt_state,
                    root_project_name,
                    root_project_configs,
                    adapter_type,
                    macros,
                    jinja_env,
                    refs_and_sources,
                    all_runtime_configs,
                    &token,
                )
                .await
                .map_err(|e| *e)
            }));
        }

        // Wait for all packages in this wave to finish, then merge results and update configs
        for handle in handles {
            let result = handle.await;
            let (
                package_name,
                runtime_config,
                new_nodes,
                new_disabled_nodes,
                rendering_results,
                updated_refs_and_sources,
            ) = match result {
                Ok(Ok(val)) => val,
                Ok(Err(e)) => return Err(Box::new(e)),
                Err(e) => return Err(fs_err!(ErrorCode::Unexpected, "Join error: {}", e)),
            };
            // Update runtime configs for next wave
            all_runtime_configs.insert(package_name.clone(), runtime_config);
            // Merge results in main thread
            nodes.extend(new_nodes);
            disabled_nodes.extend(new_disabled_nodes);
            collector
                .rendering_results
                .extend(rendering_results.rendering_results);
            // This could be optimized refs and sources can all be inserted at the end instead of merging
            refs_and_sources.merge(updated_refs_and_sources);
        }
    }

    Ok((nodes, disabled_nodes, collector))
}
