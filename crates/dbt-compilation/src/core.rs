use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
    time::SystemTime,
};

use dbt_adapter::{AdapterType, adapter::AdapterFactory};
use dbt_common::{
    cancellation::CancellationToken, io_args::FsCommand, path::DbtPath,
    tracing::TracingConfigProvider,
};
use dbt_error::{ErrorCode, FsResult, fs_err};
use dbt_features::compilation::CompilationConfig;
use dbt_jinja_utils::{
    jinja_environment::JinjaEnv, listener::JinjaTypeCheckingEventListenerFactory,
};
use dbt_loader::{
    args::{IoArgs, LoadArgs},
    load,
};
use dbt_metadata::build_cache::{
    PreviousResolvedState, add_all_unchanged_nodes,
    determine_cache_state_from_previous_previous_resolved_nodes, drop_all_unchanged_nodes,
    hydrate_cache,
};
use dbt_parser::args::ResolveArgs;
use dbt_schemas::{
    dbt_utils::resolve_package_quoting,
    schemas::{
        ResolvedCloudConfig, common::DbtQuoting, macros::build_macro_units, project::DbtProject,
    },
    state::{
        CacheState, DbtPackage, DbtState, GetColumnsInRelationCalls, GetRelationCalls,
        PatternedDanglingSources, ResolverState, ResourcePathKind,
    },
};

pub struct DbtLoadedProject {
    config: CompilationConfig,
    adapter_factory: Arc<dyn AdapterFactory>,
    dbt_state: Arc<DbtState>,
}

/// Phase 1: Load and hydrate cache with optional previous state for incremental compilation
async fn load_phase(
    config: CompilationConfig,
    mut load_args: LoadArgs,
    invocation_args: Cow<'_, dbt_jinja_utils::invocation_args::InvocationArgs>,
    adapter_factory: Arc<dyn AdapterFactory>,
    maybe_prev_loaded_project: Option<&DbtLoadedProject>,
    tracing_config: Option<&dyn TracingConfigProvider>,
    token: &CancellationToken,
) -> FsResult<DbtLoadedProject> {
    // Set previous state for incremental compilation if provided
    if let Some(prev_dbt_state) = maybe_prev_loaded_project
        .as_ref()
        .map(|x| x.dbt_state.clone())
    {
        load_args.prev_dbt_state = Some(prev_dbt_state);
        load_args.install_deps = false;
    }

    // Load dbt project
    let dbt_state = load(&load_args, invocation_args, tracing_config, token).await?;

    Ok(DbtLoadedProject {
        config,
        adapter_factory,
        dbt_state: Arc::new(dbt_state),
    })
}

/// Phase 2: Resolve and parse with optional listener factory for LSP
async fn resolve_phase(
    loaded_project: &DbtLoadedProject,
    resolve_args: ResolveArgs,
    invocation_args: &dbt_jinja_utils::invocation_args::InvocationArgs,
    cache_state: Option<&CacheState>,
    token: &CancellationToken,
    jinja_type_checking_event_listener_factory: Arc<dyn JinjaTypeCheckingEventListenerFactory>,
) -> FsResult<(ResolverState, Arc<JinjaEnv>)> {
    use dbt_parser::resolver::resolve;
    use dbt_schemas::schemas::Nodes;
    use dbt_schemas::state::Macros;

    let io = &resolve_args.io;
    let dbt_state = loaded_project.dbt_state.clone();

    // Drop unchanged nodes for build cache and incremental
    let dbt_state = if let Some(cache) = cache_state {
        // only clone if we have a cache state
        let mut dbt_state = dbt_state.as_ref().clone();
        drop_all_unchanged_nodes(io, &mut dbt_state, &cache.file_changes.unimpacted_files);
        Arc::new(dbt_state)
    } else {
        dbt_state
    };

    // Get cached macros and nodes if available
    let macros = if let Some(cache) = cache_state {
        cache.unimpacted_resolved_nodes.macros.clone()
    } else {
        Macros::default()
    };

    let nodes = if let Some(cache) = cache_state {
        cache.unimpacted_resolved_nodes.nodes.clone()
    } else {
        Nodes::default()
    };

    let get_relation_calls = if let Some(cache) = cache_state {
        cache.unimpacted_get_relation_calls.clone()
    } else {
        GetRelationCalls::default()
    };

    let get_columns_in_relation_calls = if let Some(cache) = cache_state {
        cache.unimpacted_get_columns_in_relation_calls.clone()
    } else {
        GetColumnsInRelationCalls::default()
    };

    let patterned_dangling_sources = if let Some(cache) = cache_state {
        cache.unimpacted_patterned_dangling_sources.clone()
    } else {
        PatternedDanglingSources::default()
    };

    // Call the actual resolver
    let (mut resolved_state, jinja_env) = resolve(
        &resolve_args,
        invocation_args,
        dbt_state.clone(),
        macros,
        nodes,
        get_relation_calls,
        get_columns_in_relation_calls,
        patterned_dangling_sources,
        token,
        jinja_type_checking_event_listener_factory,
    )
    .await?;
    // Add unchanged nodes back if we have cache
    if let Some(cache) = cache_state {
        add_all_unchanged_nodes(&mut resolved_state, &cache.unimpacted_resolved_nodes);
    }

    Ok((resolved_state, jinja_env))
}

/// Contains a list of file paths that have been modified.
#[derive(Debug)]
struct FileChangeset {
    /// Files that have been modified.
    pub changed: Vec<String>,
}

fn is_resource_changeable(kind: &ResourcePathKind) -> bool {
    if let ResourcePathKind::ModelPaths
    | ResourcePathKind::SeedPaths
    | ResourcePathKind::AnalysisPaths = kind
    {
        return true;
    }
    false
}

// TODO: Chenyu this will be moved to use CAS as previous state on next PR.
async fn compute_file_changeset(
    prev_dbt_state: &DbtState,
    current_dbt_state: &DbtState,
    token: &CancellationToken,
) -> FsResult<FileChangeset> {
    let prev_packages = &prev_dbt_state.packages;
    let current_packages = &current_dbt_state.packages;

    if prev_packages.len() != current_packages.len() {
        return Err(fs_err!(ErrorCode::CacheError, "Number of packages changed"));
    }

    let mut changed = Vec::new();

    let mut i = 0;
    while i < prev_packages.len() {
        let prev_package = &prev_packages[i];
        let current_package = &current_packages[i];

        if prev_package.package_root_path != current_package.package_root_path {
            return Err(fs_err!(ErrorCode::CacheError, "Packages changed"));
        }

        let prev_fs_timestamps = prev_package.all_paths.clone();
        let mut new_fs_timestamps = current_package.all_paths.clone();

        let prev_fs_keys: HashSet<&ResourcePathKind> =
            HashSet::from_iter(prev_fs_timestamps.keys());
        let new_fs_keys = HashSet::from_iter(new_fs_timestamps.keys());
        if prev_fs_keys != new_fs_keys {
            return Err(fs_err!(ErrorCode::CacheError, "Resource kinds changed"));
        }

        for (key, prev_value) in prev_fs_timestamps {
            token.check_cancellation()?;

            let is_doc = key == ResourcePathKind::DocsPaths;

            let current_value = new_fs_timestamps.get_mut(&key).unwrap();

            // When we check to see if any input files have changed,
            // we take into account case-sensitivity as the casing
            // should be reflected to the user.
            let prev_fs: HashMap<&Path, SystemTime> =
                prev_value.iter().map(|x| (x.0.as_path(), x.1)).collect();
            let current_fs: HashMap<&DbtPath, SystemTime> =
                current_value.iter().map(|x| (&x.0, x.1)).collect();

            if prev_fs.len() != current_fs.len() {
                return Err(fs_err!(ErrorCode::CacheError, "Input files changed"));
            }

            for current in current_fs.iter() {
                token.check_cancellation()?;
                // Using [std::path::Path] for the lookup here means
                // it will always be case-sensitive.
                let Some(prev_timestamp) = prev_fs.get(current.0.as_path()) else {
                    return Err(fs_err!(ErrorCode::CacheError, "Input files changed"));
                };
                if current.1 != prev_timestamp {
                    if is_doc && !current.0.has_extension("md") {
                        // Ignore changes to non-md files if our path-kind is a doc.
                        continue;
                    }
                    if !is_resource_changeable(&key) {
                        return Err(fs_err!(ErrorCode::CacheError, "Input file changed"));
                    }
                    if i > 0 {
                        return Err(fs_err!(
                            ErrorCode::CacheError,
                            "Dependent package contents changed"
                        ));
                    }
                    // The paths in all_paths are already absolute paths
                    // We need to make them relative to the package root path
                    // For the root package, this is the same as io.in_dir
                    let package_root = &current_dbt_state.packages[0].package_root_path;
                    let relative_path = current
                        .0
                        .get_relative_path(&DbtPath::from_path(package_root))
                        .map(|p| p.to_str().unwrap_or_default().to_string())
                        .unwrap_or_else(|| current.0.to_str().unwrap_or_default().to_string());
                    changed.push(relative_path);
                }
            }
        }
        i += 1;
    }

    if changed.is_empty() {
        return Err(fs_err!(
            ErrorCode::NoFilesChangedWarning,
            "No files changed"
        ));
    }

    Ok(FileChangeset { changed })
}

#[allow(clippy::too_many_arguments)]
async fn try_load_cache_state_and_changeset_by_last_write(
    io: &IoArgs,
    prev_dbt_state: &DbtState,
    prev_resolved_state: &ResolverState,
    dbt_state: &DbtState,
    token: &CancellationToken,
) -> FsResult<Option<CacheState>> {
    // Incremental compilation - compute changeset and cache state
    let changeset = compute_file_changeset(prev_dbt_state, dbt_state, token).await?;

    let prev_resolved_state = PreviousResolvedState::from_resolved_state(prev_resolved_state);

    let cache = determine_cache_state_from_previous_previous_resolved_nodes(
        io,
        prev_resolved_state,
        &changeset.changed,
        dbt_state,
    )?;

    if let Some(cache) = cache {
        Ok(Some(cache))
    } else {
        Ok(None)
    }
}

async fn try_load_cache_state(
    command: FsCommand,
    io: &IoArgs,
    config: &CompilationConfig,
    dbt_state: &DbtState,
) -> FsResult<Option<CacheState>> {
    // Hydrate cache if enabled
    if io.should_use_build_cache() && config.cacheable_commands.contains(&command) {
        Ok(hydrate_cache(
            command,
            io,
            &dbt_state.dbt_profile.target,
            dbt_state.root_project_name(),
            dbt_state,
        )?)
    } else {
        Ok(None)
    }
}

/// Loads the cache state.
/// When given a previous resolved project,
/// the cache state is based on that instead of reading from disk.
async fn load_cache(
    loaded_project: &DbtLoadedProject,
    cache_command: FsCommand,
    io: &IoArgs,
    prev_resolved_state: Option<(&DbtLoadedProject, &ResolverState)>,
    token: &CancellationToken,
) -> FsResult<Option<CacheState>> {
    if let Some((prev_loaded_project, prev_resolved_state)) = prev_resolved_state {
        let prev_dbt_state = prev_loaded_project.dbt_state();
        if let Some(cache_state) = try_load_cache_state_and_changeset_by_last_write(
            io,
            &prev_dbt_state,
            prev_resolved_state,
            &loaded_project.dbt_state,
            token,
        )
        .await?
        {
            Ok(Some(cache_state))
        } else {
            Ok(None)
        }
    } else {
        Ok(try_load_cache_state(
            cache_command,
            io,
            &loaded_project.config,
            &loaded_project.dbt_state,
        )
        .await?)
    }
}

impl DbtLoadedProject {
    pub async fn load(
        config: CompilationConfig,
        load_args: LoadArgs,
        invocation_args: Cow<'_, dbt_jinja_utils::invocation_args::InvocationArgs>,
        adapter_factory: Arc<dyn AdapterFactory>,
        prev_loaded_project: Option<&DbtLoadedProject>,
        tracing_config: Option<&dyn TracingConfigProvider>,
        token: &CancellationToken,
    ) -> FsResult<DbtLoadedProject> {
        load_phase(
            config,
            load_args,
            invocation_args,
            adapter_factory,
            prev_loaded_project,
            tracing_config,
            token,
        )
        .await
    }

    pub fn config(&self) -> &CompilationConfig {
        &self.config
    }

    pub fn dbt_state(&self) -> Arc<DbtState> {
        self.dbt_state.clone()
    }

    pub fn dbt_cloud_config(&self) -> Option<&ResolvedCloudConfig> {
        self.dbt_state.cloud_config.as_ref()
    }

    pub fn root_project(&self) -> &DbtProject {
        self.dbt_state.root_project()
    }

    pub fn root_project_name(&self) -> &str {
        self.dbt_state.root_project_name()
    }

    pub fn root_project_id(&self) -> String {
        self.dbt_state.root_project().get_project_id()
    }

    pub fn root_project_quoting(&self) -> DbtQuoting {
        let adapter_type = self.adapter_type();
        resolve_package_quoting(*self.dbt_state.root_project().quoting, adapter_type)
    }

    pub fn adapter_type(&self) -> AdapterType {
        self.dbt_state.dbt_profile.db_config.adapter_type()
    }

    pub fn root_package(&self) -> &DbtPackage {
        self.dbt_state.root_package()
    }

    pub fn adapter_factory(&self) -> Arc<dyn AdapterFactory> {
        self.adapter_factory.clone()
    }

    pub fn create_jinja_env(
        &self,
        resolved_state: &ResolverState,
        io: &IoArgs,
        invocation_args: &dbt_jinja_utils::invocation_args::InvocationArgs,
        _token: &CancellationToken,
    ) -> FsResult<JinjaEnv> {
        let dbt_state = self.dbt_state();
        let root_project_name = self.root_project_name();
        let root_project_quoting = self.root_project_quoting();
        let macros = &resolved_state.macros;
        dbt_jinja_utils::phases::parse::init::initialize_parse_jinja_environment(
            root_project_name,
            &dbt_state.dbt_profile.profile,
            &dbt_state.dbt_profile.target,
            self.adapter_type(),
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
            io.clone(),
            dbt_state.catalogs.clone(),
        )
    }

    pub async fn load_cache(
        &self,
        cache_command: FsCommand,
        io: &IoArgs,
        prev_resolved_state: Option<(&DbtLoadedProject, &ResolverState)>,
        token: &CancellationToken,
    ) -> FsResult<Option<CacheState>> {
        load_cache(self, cache_command, io, prev_resolved_state, token).await
    }

    pub async fn resolve(
        &self,
        resolve_args: ResolveArgs,
        invocation_args: &dbt_jinja_utils::invocation_args::InvocationArgs,
        cache_state: Option<&CacheState>,
        token: &CancellationToken,
        jinja_type_checking_event_listener_factory: Arc<dyn JinjaTypeCheckingEventListenerFactory>,
    ) -> FsResult<(ResolverState, Arc<JinjaEnv>)> {
        resolve_phase(
            self,
            resolve_args,
            invocation_args,
            cache_state,
            token,
            jinja_type_checking_event_listener_factory,
        )
        .await
    }
}
