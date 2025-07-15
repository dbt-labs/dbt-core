//! Module containing the entrypoint for the resolve phase.
use dbt_common::constants::{DBT_GENERIC_TESTS_DIR_NAME, RESOLVING};
use dbt_common::once_cell_vars::DISPATCH_CONFIG;
#[allow(unused_imports)]
use dbt_common::FsError;
use dbt_common::{err, fs_err, show_error, with_progress, ErrorCode, FsResult};
use dbt_common::{show_warning, stdfs};
use dbt_jinja_utils::invocation_args::InvocationArgs;
use dbt_jinja_utils::phases::parse::build_resolve_context;
use dbt_jinja_utils::phases::parse::init::initialize_parse_jinja_environment;
use dbt_jinja_utils::refs_and_sources::{resolve_dependencies, RefsAndSources};
use dbt_schemas::dbt_utils::resolve_package_quoting;
use dbt_schemas::schemas::macros::build_macro_units;
use dbt_schemas::schemas::{InternalDbtNode, Nodes};

use dbt_jinja_utils::jinja_environment::JinjaEnvironment;
use dbt_schemas::state::NodesWithChangeset;
use dbt_schemas::state::RenderResults;
use dbt_schemas::state::{DbtPackage, Macros};
use dbt_schemas::state::{DbtRuntimeConfig, Operations};

use dbt_schemas::state::DbtState;
use dbt_schemas::state::ResolverState;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::args::ResolveArgs;
use crate::dbt_project_config::{build_root_project_configs, RootProjectConfigs};
use crate::resolve::resolve_operations::resolve_operations;
use crate::utils::{self};

use crate::resolve::resolve_analyses::resolve_analyses;
use crate::resolve::resolve_macros::resolve_docs_macros;
use crate::resolve::resolve_macros::resolve_macros;
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
pub async fn resolve(
    arg: &ResolveArgs,
    invocation_args: &InvocationArgs,
    dbt_state: Arc<DbtState>,
    _change_set: &Option<NodesWithChangeset>,
) -> FsResult<(ResolverState, JinjaEnvironment<'static>)> {
    let _pb = with_progress!(arg.io, spinner => RESOLVING);

    // Get the root project name
    let root_project_name = dbt_state.root_project_name();
    let adapter_type = dbt_state.dbt_profile.db_config.adapter_type();

    let mut macros = Macros::default();

    // First, resolve all of the macros from each package
    for package in &dbt_state.packages {
        dbt_common::check_cancellation!(arg.io.should_cancel_compilation)?;

        let resolved_macros = resolve_macros(
            &arg.io,
            &package
                .macro_files
                .iter()
                // This is a temporary solution, for a feature that is supposed to be
                // deprecated in the future
                .chain(&package.snapshot_files)
                .collect::<Vec<_>>(),
        )?;
        macros.macros.extend(resolved_macros);
        let docs_macros = resolve_docs_macros(&package.docs_files)?;
        macros.docs_macros.extend(docs_macros);
    }

    let mut operations = Operations::default();
    for package in &dbt_state.packages {
        let (on_run_start, on_run_end) = resolve_operations(&package.dbt_project);
        operations.on_run_start.extend(on_run_start);
        operations.on_run_end.extend(on_run_end);
    }

    // Build the root project config
    let root_project_quoting = resolve_package_quoting(
        *dbt_state.root_project().quoting,
        &dbt_state.dbt_profile.db_config.adapter_type(),
    );

    let jinja_env = initialize_parse_jinja_environment(
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
    )?;

    // Compute final selectors
    let resolved_selectors = resolve_final_selectors(root_project_name, &jinja_env, arg)?;
    // dbg!(&resolved_selectors);

    // Create a map to store full runtime configs for ALL packages
    let mut all_runtime_configs: BTreeMap<String, Arc<DbtRuntimeConfig>> = BTreeMap::new();

    let mut nodes = Nodes::default();
    let mut disabled_nodes = Nodes::default();
    let root_project_configs =
        build_root_project_configs(&arg.io, dbt_state.root_project(), root_project_quoting)?;
    let root_project_configs = Arc::new(root_project_configs);

    // Process packages in topological order
    let mut refs_and_sources =
        RefsAndSources::from_dbt_nodes(&nodes, &adapter_type, root_project_name.to_string(), None)?;
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
                &jinja_env,
                &mut refs_and_sources,
                &mut all_runtime_configs,
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
                &jinja_env,
                &mut refs_and_sources,
                &mut all_runtime_configs,
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
    let (dangling_sources, patterned_dangling_sources) = parse_adapter.dangling_sources();
    let root_runtime_config = all_runtime_configs
        .get(dbt_state.root_project_name())
        .unwrap();

    resolve_dependencies(&arg.io, &mut nodes, &mut disabled_nodes, &refs_and_sources);
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
            refs_and_sources: Arc::new(refs_and_sources),
            dangling_sources: dangling_sources?,
            runtime_config: root_runtime_config.clone(),
            patterned_dangling_sources,
            resolved_selectors,
            root_project_quoting: root_project_quoting.try_into()?,
        },
        jinja_env,
    ))
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
    jinja_env: &JinjaEnvironment<'static>,
    refs_and_sources: &mut RefsAndSources,
    runtime_config: Arc<DbtRuntimeConfig>,
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
    let mut min_properties = resolve_minimal_properties(arg, package, jinja_env, &base_ctx)?;

    let package_name = package.dbt_project.name.as_str();

    let mut collected_tests = Vec::new();

    let dbt_tests_dir = arg.io.out_dir.join(DBT_GENERIC_TESTS_DIR_NAME);
    stdfs::create_dir_all(&dbt_tests_dir)?;

    // Resolve sources based on the dbt_state, database, schema, and project name
    let (sources, disabled_sources) = resolve_sources(
        &arg.io,
        package,
        package_quoting,
        root_project_configs,
        min_properties.source_tables,
        database,
        adapter_type,
        &base_ctx,
        jinja_env,
        &mut collected_tests,
        refs_and_sources,
    )?;
    nodes.sources.extend(sources);
    disabled_nodes.sources.extend(disabled_sources);

    // Resolve seeds based on the dbt_state, database, schema, and project name
    let (seeds, disabled_seeds) = resolve_seeds(
        &arg.io,
        min_properties.seeds,
        package,
        package_quoting,
        dbt_state.root_project(),
        root_project_configs,
        database,
        schema,
        adapter_type,
        package_name,
        jinja_env,
        &base_ctx,
        &mut collected_tests,
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
        jinja_env,
        &base_ctx,
        runtime_config.clone(),
        refs_and_sources,
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
        jinja_env,
        &base_ctx,
        runtime_config.clone(),
        &mut collected_tests,
        refs_and_sources,
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
        jinja_env,
        &base_ctx,
        runtime_config.clone(),
        refs_and_sources,
    )
    .await?;
    nodes.analyses.extend(analyses);

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
        jinja_env,
        &base_ctx,
        runtime_config.clone(),
        &collected_tests,
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
        jinja_env,
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
    jinja_env: JinjaEnvironment<'static>,
    refs_and_sources: RefsAndSources,
    all_runtime_configs: BTreeMap<String, Arc<DbtRuntimeConfig>>,
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
            &jinja_env,
            &mut refs_and_sources.clone(),
            runtime_config.clone(),
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
    jinja_env: &JinjaEnvironment<'static>,
    refs_and_sources: &mut RefsAndSources,
    all_runtime_configs: &mut BTreeMap<String, Arc<DbtRuntimeConfig>>,
) -> FsResult<(Nodes, Nodes, RenderResults)> {
    let mut nodes = Nodes::default();
    let mut disabled_nodes = Nodes::default();
    let mut collector = RenderResults {
        rendering_results: BTreeMap::new(),
    };

    for package_wave in package_waves {
        dbt_common::check_cancellation!(arg.io.should_cancel_compilation)?;

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
    jinja_env: &JinjaEnvironment<'static>,
    refs_and_sources: &mut RefsAndSources,
    all_runtime_configs: &mut BTreeMap<String, Arc<DbtRuntimeConfig>>,
) -> FsResult<(Nodes, Nodes, RenderResults)> {
    let mut nodes = Nodes::default();
    let mut disabled_nodes = Nodes::default();
    let mut collector = RenderResults {
        rendering_results: BTreeMap::new(),
    };

    for package_wave in package_waves {
        dbt_common::check_cancellation!(arg.io.should_cancel_compilation)?;

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
