use dbt_common::constants::DBT_PACKAGES_DIR_NAME;
use dbt_common::io_args::FsCommand;
use dbt_common::path::DbtPath;
use dbt_common::tracing::emit::{emit_info_progress_message, emit_warn_log_message};
use dbt_common::{
    ErrorCode, FsResult,
    constants::{
        DBT_DEPENDENCIES_YML, DBT_PACKAGES_LOCK_FILE, DBT_PACKAGES_YML, DBT_PROFILES_YML,
        DBT_PROJECT_YML, DBT_SELECTORS_YML,
    },
    io_args::IoArgs,
    stdfs,
};
use dbt_schemas::state::{
    GetColumnsInRelationCalls, GetRelationCalls, Macros, Operations, PatternedDanglingSources,
};
use dbt_schemas::{
    schemas::{InternalDbtNodeAttributes, Nodes, telemetry::NodeType},
    state::{
        CacheState, DbtAsset, DbtState, FileChanges, NodeExecutionState, NodeExecutionStatus,
        NodeStatus, ResolvedNodes, ResolverState,
    },
};
use dbt_telemetry::ProgressMessage;
use dbt_yaml::{Value, Verbatim};
use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use crate::{
    file_registry::CompleteStateWithKind,
    hashing::{add_session_input_files, input_file_hashes},
    parquet_cas::read_cas,
    parquet_column::{ParquetColumnRef, read_columns},
    parquet_node::{InputFile, ParquetNodeRef, ResourceType, get_all_input_files, read_nodes},
    parquet_node_serde::deserialize_parquet_nodes_to_resolver_state,
};

const CACHING_ACTION: &str = "Caching";

fn is_sql_file(path: &str) -> bool {
    path.ends_with(".sql")
}

fn is_yml_file(path: &str) -> bool {
    path.ends_with(".yml")
}

fn compute_impacted_models_by_schema_files(
    io: &IoArgs,
    changed_schema_files: &HashSet<String>,
) -> FsResult<HashMap<String, HashMap<ResourceType, HashSet<String>>>> {
    let mut impacted: HashMap<String, HashMap<ResourceType, HashSet<String>>> = HashMap::new();
    for schema_yaml in changed_schema_files {
        let schema_path = schema_yaml.clone();
        let schema_yaml_content = stdfs::read_to_string(io.in_dir.join(schema_yaml))?;
        let doc: Value =
            dbt_yaml::from_str(&schema_yaml_content).unwrap_or(Value::Null(Default::default()));

        let mut resource_map: HashMap<ResourceType, HashSet<String>> = HashMap::new();

        if let Value::Mapping(map, _span) = doc {
            for (key, value) in map {
                if let Value::String(resource_type, _span) = key {
                    let resource_type = {
                        match resource_type.as_str() {
                            "models" => Some(ResourceType::Model),
                            "seeds" => Some(ResourceType::Seed),
                            "snapshots" => Some(ResourceType::Snapshot),
                            "sources" => Some(ResourceType::Source),
                            _ => None,
                        }
                    };

                    let Some(resource_type) = resource_type else {
                        continue;
                    };

                    if let Value::Sequence(items, _span) = value {
                        for item in items {
                            if let Value::Mapping(item_map, _span) = item
                                && let Some(Value::String(name, _span)) = item_map
                                    .get(Value::String("name".to_string(), Default::default()))
                            {
                                resource_map
                                    .entry(resource_type)
                                    .or_default()
                                    .insert(name.clone());
                            }
                        }
                    }
                }
            }
        }
        if !resource_map.is_empty() {
            impacted.insert(schema_path, resource_map);
        }
    }
    Ok(impacted)
}

/// Determines the changeset for the given dbt state and target.
/// It returns a `NodesWithChangeset` struct that contains the changeset and the resolved nodes.
/// This is the core shared logic between [determine_changeset] and [determine_cache_state_from_changes].
#[allow(clippy::cognitive_complexity)]
#[allow(clippy::too_many_arguments)]
fn determine_changeset_core(
    dbt_state: &DbtState,
    parquet_nodes: Vec<ParquetNodeRef>,
    parquet_columns: &HashMap<String, Vec<ParquetColumnRef>>,
    mut unchanged_files: HashSet<String>,
    mut changed_files: HashSet<String>,
    deleted_files: HashSet<String>,
    new_files: HashSet<String>,
    mut changed_schema_files: HashSet<String>,
    _changed_seed_files: HashSet<String>,
    cas: &HashMap<String, String>,
    cmd: FsCommand,
    target: &str,
    project_name: &str,
    io: &IoArgs,
) -> FsResult<Option<CacheState>> {
    let real_changed_files = changed_files.clone();

    // make a p(arquet)node hashmaps
    let pnodes = parquet_nodes
        .into_iter()
        .map(|node| (node.unique_id.to_string(), node))
        .collect::<HashMap<String, ParquetNodeRef>>();
    // get the session node
    let session_unique_id = format!("session.{project_name}.{target}");
    let session_pnode = pnodes.get(&session_unique_id);
    if session_pnode.is_none() {
        emit_warn_log_message(
            ErrorCode::CacheWarning,
            format!(
                "Cache inconsistency: No session node found for unique_id {session_unique_id}, skipping changeset check."
            ),
            io.status_reporter.as_ref(),
        );
        return Ok(None);
    }
    let session_pnode = session_pnode.unwrap();

    // -- compare same session input files in fs and cas --
    let mut dummy_cas = HashMap::new();
    let registry = CompleteStateWithKind::from_dbt_state(io, dbt_state)?;
    let input_files = add_session_input_files(io, &mut dummy_cas, &registry)?;

    // now_compare both input_file list, relative paths have to be the same and hashes have to be the same, if anoyhting differs we have to do a full build
    if session_pnode.input_files.len() != input_files.len() {
        emit_warn_log_message(
            ErrorCode::CacheWarning,
            format!(
                "Cache inconsistency: Input files count mismatch: expected {}, got {}",
                session_pnode.input_files.len(),
                input_files.len()
            ),
            io.status_reporter.as_ref(),
        );
        return Ok(None);
    }
    for (session_input_file, fs_input_file) in
        session_pnode.input_files.iter().zip(input_files.iter())
    {
        if session_input_file.path != fs_input_file.path {
            eprintln!(
                "Session input file path mismatch: expected {}, got {}",
                session_input_file.path, fs_input_file.path
            );
            return Ok(None);
        }
        if session_input_file.file_cas_hash != fs_input_file.file_cas_hash {
            eprintln!(
                "Session input file hash mismatch: expected {}, got {}",
                session_input_file.file_cas_hash, fs_input_file.file_cas_hash
            );
            return Ok(None);
        }
    }

    // -- compare session profile hash --
    let profile_hash = dbt_state.dbt_profile.blake3_hash();
    let session_details = session_pnode
        .session_details
        .as_ref()
        .expect("Session details missing in session node");
    if session_details.profile_hash.as_ref() != Some(&profile_hash) {
        eprintln!(
            "Profile hash mismatch: expected {:?}, got {:?}",
            session_details.profile_hash, profile_hash
        );
        return Ok(None);
    }
    let _packages_install_path = session_details
        .packages_install_path
        .as_deref()
        .unwrap_or(DBT_PACKAGES_DIR_NAME);
    // -- compare more stuff: env_vars, vars, cli arg_hash --

    let mut changed_nodes = HashSet::new();
    for node in pnodes.values() {
        let are_input_files_unchanged = node
            .input_files
            .iter()
            .all(|input_file| unchanged_files.contains(&input_file.path));
        if !are_input_files_unchanged {
            changed_nodes.insert(node.unique_id.to_string());
        }
    }

    // Use the shared helper function to filter nodes
    let (keep, unimpacted_node_statuses) = filter_nodes_by_file_changes(
        io,
        pnodes.clone(),
        &mut unchanged_files,
        &mut changed_files,
        &mut changed_schema_files,
        cmd,
    )?;

    // Now compute resolved_nodes from the filtered nodes
    let unimpacted_resolved_nodes =
        deserialize_parquet_nodes_to_resolver_state(keep, cas, parquet_columns);

    let file_changes = FileChanges {
        changed_files: real_changed_files.into_iter().map(DbtPath::from).collect(),
        unimpacted_files: unchanged_files.into_iter().map(DbtPath::from).collect(),
        impacted_files: changed_files.into_iter().map(DbtPath::from).collect(),
        deleted_files: deleted_files.into_iter().map(DbtPath::from).collect(),
        new_files: new_files.into_iter().map(DbtPath::from).collect(),
    };

    let nodes_with_changeset = CacheState {
        file_changes,
        unimpacted_resolved_nodes,
        unimpacted_node_statuses,
        unimpacted_get_relation_calls: GetRelationCalls::default(),
        unimpacted_get_columns_in_relation_calls: GetColumnsInRelationCalls::default(),
        unimpacted_patterned_dangling_sources: PatternedDanglingSources::default(),
        changed_nodes: Arc::new(changed_nodes),
    };

    /*
      todo: FileChanges and resolved nodes have to be refined, similar to partial parse logic in dbt-core (@gerda is quite familiar with it),
      - a macro, - recompile all nodes that used that macro and downstream of it
      - a doc (.md) file, - recompile nothing
       - a schema.yml, - recompile all nodes that are impacted by config changes


      done:
      - a model file, - recompile all nodes that ref the model -- trick is: use --select path:model.sql+, all will be recompiled
      - a seed file, - recompile all nodes that ref the seed -- trick is: use --select path:seed_name.csv+, all will be recompiled
      - a dependencies.yml or package.yml, - Impacts session input_files, recompile everything
      - a dbt_project.yml, a profile - Impacts session inpout files, recompile everything
    - a schema.yml, - recompile all nodes that are impacted by model/seed/snapshot/source changes
    */

    Ok(Some(nodes_with_changeset))
}

// TODO: move loading of parquet into separate function
/// Determines the changeset for the given dbt state and target.
/// It computes the file hashes from the filesystem and compares them with the hashes stored in the `nodes.parquet` file.
/// It returns a `NodesWithChangeset` struct that contains the changeset and the resolved nodes.
/// - If the input files or profile hash do not match, it returns an empty `NodesWithChangeset`.
/// - If the input files are the same, it returns a `NodesWithChangeset` with the changeset indicating that no changes were found.
/// - If the input files are different, it returns a `NodesWithChangeset` with the changeset indicating that changes were found.
/// - If the input files are missing in the filesystem or in the CAS, it returns a `NodesWithChangeset` with the changeset indicating that files are missing.
#[allow(clippy::cognitive_complexity)]
pub fn determine_changeset(
    dbt_state: &DbtState,
    cmd: FsCommand,
    target: &str,
    project_name: &str,
    io: &IoArgs,
) -> FsResult<Option<CacheState>> {
    // read the cached nodes
    let parquet_nodes = read_nodes(io)?;
    if parquet_nodes.is_empty() {
        return Ok(None);
    }

    let parquet_columns = read_columns(io)?;

    let prev_cas = read_cas(io)?;

    // -- get cas hashes from parquet nodes --
    let input_files = get_all_input_files(&parquet_nodes);

    // -- compute fs hashes --
    let mut fs_hashes = input_file_hashes(io, dbt_state)?;

    // also add input_files to fs (since they are not captured as a dbt_asset)
    for input_file in &input_files {
        fs_hashes.insert(input_file.path.clone(), input_file.file_cas_hash.clone());
    }

    let cas_hashes: HashMap<String, String> = HashMap::from_iter(
        input_files
            .into_iter()
            .map(|f| (f.path.to_string(), f.file_cas_hash)),
    );

    // now partition into 4 parts, files that are the same, files that are different, files that are in fs and missing in cas and files that are i cas missing in fs
    let mut unchanged_files = HashSet::new();
    let mut changed_files = HashSet::new();
    let mut deleted_files = HashSet::new();
    let mut new_files = HashSet::new();
    // These are all .yml files except profiles.yml, dbt_project.yml, dependencies.yml, package.yml, selectors.yml
    let mut changed_schema_files = HashSet::new();
    let mut changed_seed_files = HashSet::new();
    for (path, hash) in &fs_hashes {
        // ignore generated files
        let rel = Path::new(path);
        // todo: handle .md files

        if io.is_generated_file(rel) || rel.extension() == Some(OsStr::new("md")) {
            continue;
        }
        // define is_deps_file to be true if the first componenet of the rel_path is dbt_packages or dbt_internal_packages
        match cas_hashes.get(path) {
            Some(cas_hash) if cas_hash == hash => {
                unchanged_files.insert(path.clone());
            }
            Some(cas_hash) => {
                changed_files.insert(path.clone());
                // todo: only for testing dbg!, eprintln!
                eprintln!("File mismatch: {path:?}, {hash:?}, {cas_hash:?}");

                if is_yml_file(path)
                    && !path.ends_with(DBT_PROFILES_YML)
                    && !path.ends_with(DBT_PROJECT_YML)
                    && !path.ends_with(DBT_DEPENDENCIES_YML)
                    && !path.ends_with(DBT_PACKAGES_YML)
                    && !path.ends_with(DBT_PACKAGES_LOCK_FILE)
                    && !path.ends_with("selectors.yml")
                {
                    changed_schema_files.insert(path.clone());
                } else if path.ends_with(".csv") {
                    changed_seed_files.insert(path.clone());
                }
            }
            None => {
                // This file was in the project but not used as input file for any node
                if !prev_cas.contains_key(hash) {
                    // todo: only for testing dbg!, eprintln!
                    eprintln!("Cas file missing: {path:?}, {hash:?}");
                    new_files.insert(path.clone());
                }
            }
        }
    }
    for (path, hash) in &cas_hashes {
        let rel = Path::new(path);
        // todo: handle .md files, they just become input fields with Doc inputKind
        if io.is_generated_file(rel) || rel.extension() == Some(OsStr::new("md")) {
            continue;
        }
        if !fs_hashes.contains_key(path) {
            // if is_deps_file(rel) {}
            deleted_files.insert(path.clone());
            // todo: only for testing dbg!, eprintln!
            eprintln!("Fs file missing: {path:?}, {hash:?}");
        }
    }

    determine_changeset_core(
        dbt_state,
        parquet_nodes,
        &parquet_columns,
        unchanged_files,
        changed_files,
        deleted_files,
        new_files,
        changed_schema_files,
        changed_seed_files,
        &prev_cas,
        cmd,
        target,
        project_name,
        io,
    )
}

fn is_schema_impacted_model_file(
    schema_impacted_models: &HashMap<String, HashMap<ResourceType, HashSet<String>>>,
    input_file: &InputFile,
    resource_type: ResourceType,
) -> bool {
    if schema_impacted_models.is_empty() {
        return false;
    }
    // Only do this for SQL files.
    if !is_sql_file(&input_file.path) {
        return false;
    }
    // Check the file_stem, example: "A" is the file_stem from "models/A.sql"
    let Some(file_stem) = Path::new(&input_file.path)
        .file_stem()
        .and_then(|x| x.to_str())
    else {
        return false;
    };
    for map in schema_impacted_models.values() {
        let Some(set) = map.get(&resource_type) else {
            continue;
        };

        if set.contains(file_stem) {
            return true;
        }
    }
    false
}

/// Filter nodes based on file changes and command type.
#[allow(clippy::too_many_arguments)]
fn filter_nodes_by_file_changes(
    io: &IoArgs,
    pnodes: HashMap<String, ParquetNodeRef>,
    unchanged_files: &mut HashSet<String>,
    changed_files: &mut HashSet<String>,
    changed_schema_files: &mut HashSet<String>,
    cmd: FsCommand,
) -> FsResult<(HashMap<String, ParquetNodeRef>, HashMap<String, NodeStatus>)> {
    // Find PNodes directly impacted by schema file changes and input file changes
    let mut impacted_nodes_by_unique_id = HashSet::new();

    // This loop will handle catching duplicate model definitions across more than one schema.
    // The majority of the time, this will not loop.
    let mut impacted_schema_files = changed_schema_files.clone();
    loop {
        let schema_impacted_models =
            compute_impacted_models_by_schema_files(io, &impacted_schema_files)?;
        // Clear this so we can potentially pickup additional impacted schema files.
        impacted_schema_files.clear();

        for (unique_id, node) in &pnodes {
            if impacted_nodes_by_unique_id.contains(unique_id) {
                continue;
            }

            for input_file in &node.input_files {
                if changed_files.contains(&input_file.path)
                    || is_schema_impacted_model_file(
                        &schema_impacted_models,
                        input_file,
                        node.resource_type,
                    )
                {
                    impacted_nodes_by_unique_id.insert(unique_id.clone());
                    for input_file in &node.input_files {
                        // Assumes that YML files are schema files.
                        if is_yml_file(&input_file.path)
                            && !changed_schema_files.contains(&input_file.path)
                        {
                            impacted_schema_files.insert(input_file.path.clone());
                        }
                    }
                    break;
                }
            }
        }

        // No additional schema files were impacted, therefore we are done.
        if impacted_schema_files.is_empty() {
            break;
        }

        // Mark the additional schema files as changed.
        for impacted_schema_file in &impacted_schema_files {
            unchanged_files.remove(impacted_schema_file);
            changed_schema_files.insert(impacted_schema_file.clone());
            changed_files.insert(impacted_schema_file.clone());
        }
    }

    // Build reverse dependency map (who depends on whom)
    let mut reverse_deps: HashMap<String, Vec<String>> = HashMap::new();
    for (unique_id, node) in &pnodes {
        for dep in &node.depends_on_nodes {
            reverse_deps
                .entry(dep.clone())
                .or_default()
                .push(unique_id.clone());
        }
    }

    // Find ALL downstream dependencies (transitive closure)
    let mut all_impacted = impacted_nodes_by_unique_id.clone();
    let mut to_process: Vec<String> = impacted_nodes_by_unique_id.into_iter().collect();

    while let Some(current) = to_process.pop() {
        if let Some(dependents) = reverse_deps.get(&current) {
            for dependent in dependents {
                if all_impacted.insert(dependent.clone()) {
                    // New impacted node found, add to processing queue
                    to_process.push(dependent.clone());
                }
            }
        }
    }

    // Partition nodes into keep vs invalidated
    let (keep, invalidated): (
        HashMap<String, ParquetNodeRef>,
        HashMap<String, ParquetNodeRef>,
    ) = pnodes
        .into_iter()
        .partition(|(unique_id, _)| !all_impacted.contains(unique_id));

    // Update file lists for cache reporting
    for node in invalidated.values() {
        if let Some(input) = node.input_files.first()
            && unchanged_files.remove(&input.path)
        {
            changed_files.insert(input.path.clone());
        }
    }

    // Filter nodes based on command and execution state
    let (keep, removed_nodes): (
        HashMap<String, ParquetNodeRef>,
        HashMap<String, ParquetNodeRef>,
    ) = keep.into_iter().partition(|(_, node)| {
        // Always invalidate nodes that had errors
        if node.latest_status == Some(NodeExecutionStatus::Error) {
            return false;
        }

        match cmd {
            FsCommand::Parse => true,
            FsCommand::Run
            | FsCommand::Build
            | FsCommand::Test
            | FsCommand::Snapshot
            | FsCommand::Seed => {
                node.latest_state == Some(NodeExecutionState::Run)
                    || node.resource_type == ResourceType::Macro
                    || node.resource_type == ResourceType::Snapshot
            }
            FsCommand::Compile => {
                node.latest_state == Some(NodeExecutionState::Compiled)
                    || node.latest_state == Some(NodeExecutionState::Run)
                    || node.resource_type == ResourceType::Macro
                    || node.resource_type == ResourceType::Snapshot
            }
            _ => false,
        }
    });

    // Move files from removed nodes from unchanged to changed
    for node in removed_nodes.values() {
        for input_file in &node.input_files {
            if unchanged_files.remove(&input_file.path) {
                changed_files.insert(input_file.path.clone());
            }
        }
    }

    let unchanged_node_statuses = HashMap::from_iter(keep.iter().map(|(unique_id, node)| {
        (
            unique_id.clone(),
            NodeStatus {
                latest_state: node.latest_state,
                latest_status: node.latest_status.clone(),
                latest_time: node.latest_time.clone(),
                latest_message: node.latest_message.clone(),
            },
        )
    }));

    Ok((keep, unchanged_node_statuses))
}

/// Determines cache state from a list of changed files (for LSP incremental compilation).
/// Unlike `determine_changeset`, this doesn't read from disk or compute file hashes.
/// It takes a list of changed files directly from the LSP's file change detection.
#[allow(clippy::too_many_arguments)]
pub fn determine_cache_state_from_changes(
    prev_pnodes: Vec<ParquetNodeRef>,
    changed_files_list: &[String],
    dbt_state: &DbtState,
    cmd: FsCommand,
    target: &str,
    project_name: &str,
    io: &IoArgs,
    cas: &HashMap<String, String>,
) -> FsResult<Option<CacheState>> {
    // Ignore parquet columns
    let prev_pcolumns = HashMap::new();

    // Collect all current files from dbt_state
    let mut all_current_files = HashSet::new();
    for package in &dbt_state.packages {
        let package_root_path = &package.package_root_path;
        for paths in package.all_paths.values() {
            for (path, _) in paths {
                let full_path = if path.has_root() {
                    path.to_str().unwrap_or_default().to_string()
                } else {
                    package_root_path
                        .join(path.as_path())
                        .to_str()
                        .unwrap_or_default()
                        .to_string()
                };

                // Get path relative to io.in_dir
                let relative_path =
                    if let Ok(rel_path) = Path::new(&full_path).strip_prefix(&io.in_dir) {
                        rel_path.to_str().unwrap_or_default().to_string()
                    } else {
                        full_path
                    };
                all_current_files.insert(relative_path);
            }
        }
    }

    // Partition files into changed and unchanged
    let changed_files: HashSet<String> = changed_files_list.iter().cloned().collect();
    let unchanged_files: HashSet<String> = all_current_files
        .difference(&changed_files)
        .cloned()
        .collect();

    // Identify changed schema and seed files
    let changed_schema_files: HashSet<String> = changed_files
        .iter()
        .filter(|path| {
            path.ends_with(".yml")
                && !path.ends_with("profiles.yml")
                && !path.ends_with("dbt_project.yml")
                && !path.ends_with("dependencies.yml")
                && !path.ends_with("package.yml")
                && !path.ends_with("selectors.yml")
        })
        .cloned()
        .collect();

    // Incremental does not support added or removed files (yet).
    let deleted_files = HashSet::new();
    let new_files = HashSet::new();
    let changed_seed_files = HashSet::new();

    determine_changeset_core(
        dbt_state,
        prev_pnodes,
        &prev_pcolumns,
        unchanged_files,
        changed_files,
        deleted_files,
        new_files,
        changed_schema_files,
        changed_seed_files,
        cas,
        cmd,
        target,
        project_name,
        io,
    )
}

/// Hydrates the build cache by determining the changeset for the given dbt state and target.
pub fn hydrate_cache(
    command: FsCommand,
    io: &IoArgs,
    target: &str,
    project_name: &str,
    dbt_state: &DbtState,
) -> FsResult<Option<CacheState>> {
    let start = SystemTime::now();
    match determine_changeset(dbt_state, command, target, project_name, io)? {
        Some(changeset) => {
            if changeset.has_changes() {
                let changes_detected = changeset.file_changes.impacted_files.len()
                    + changeset.file_changes.new_files.len();
                let target = if io.show_timings {
                    format!(
                        "{changes_detected} file changes detected (in {} ms)",
                        start.elapsed().map(|d| d.as_millis()).unwrap_or(0)
                    )
                } else {
                    "file changes detected".to_string()
                };
                emit_info_progress_message(
                    ProgressMessage::new_from_action_and_target(CACHING_ACTION.to_string(), target),
                    io.status_reporter.as_ref(),
                );
            } else {
                // no changes, so we can use the cache
                let target = if io.show_timings {
                    format!(
                        "no file change detected (in {} ms)",
                        start.elapsed().map(|d| d.as_millis()).unwrap_or(0)
                    )
                } else {
                    "no file change detected".to_string()
                };
                emit_info_progress_message(
                    ProgressMessage::new_from_action_and_target(CACHING_ACTION.to_string(), target),
                    io.status_reporter.as_ref(),
                );
            }
            Ok(Some(changeset))
        }
        None => {
            emit_info_progress_message(
                ProgressMessage::new_from_action_and_target(
                    CACHING_ACTION.to_string(),
                    "too many changes detected".to_string(),
                ),
                io.status_reporter.as_ref(),
            );
            Ok(None)
        }
    }
}

fn strip_in_dir(io: &IoArgs, base_path: &Path, path: &Path) -> DbtPath {
    DbtPath::from_path(
        base_path
            .join(path)
            .strip_prefix(&io.in_dir)
            .expect("expected to strip prefix"),
    )
}

fn strip_in_dir_from_asset(io: &IoArgs, asset: &DbtAsset) -> DbtPath {
    strip_in_dir(io, &asset.base_path, &asset.path)
}

fn is_asset_unchanged(io: &IoArgs, asset: &DbtAsset, unchanged_files: &HashSet<DbtPath>) -> bool {
    let rel_path = strip_in_dir_from_asset(io, asset);
    unchanged_files.contains(&rel_path)
}

fn drop_unchanged_nodes_from_assets(
    io: &IoArgs,
    unchanged_files: &HashSet<DbtPath>,
    assets: &mut Vec<DbtAsset>,
) {
    assets.retain(|asset| !is_asset_unchanged(io, asset, unchanged_files));
}

/// Drops all seen assets from the dbt state.
pub fn drop_all_unchanged_nodes(
    io: &IoArgs,
    dbt_state: &mut DbtState,
    unchanged_files: &HashSet<DbtPath>,
) {
    // For each package in dbt_state
    for package in &mut dbt_state.packages {
        drop_unchanged_nodes_from_assets(io, unchanged_files, &mut package.dbt_properties);
        drop_unchanged_nodes_from_assets(io, unchanged_files, &mut package.analysis_files);
        drop_unchanged_nodes_from_assets(io, unchanged_files, &mut package.model_sql_files);
        drop_unchanged_nodes_from_assets(io, unchanged_files, &mut package.macro_files);
        drop_unchanged_nodes_from_assets(io, unchanged_files, &mut package.test_files);
        drop_unchanged_nodes_from_assets(io, unchanged_files, &mut package.seed_files);
        drop_unchanged_nodes_from_assets(io, unchanged_files, &mut package.docs_files);
        drop_unchanged_nodes_from_assets(io, unchanged_files, &mut package.snapshot_files);
        drop_unchanged_nodes_from_assets(io, unchanged_files, &mut package.fixture_files);

        let package_root_path = &package.package_root_path;

        // drop all seen files
        package.all_paths.retain(|_, assets| {
            assets.retain(|(path, _mtime)| {
                let absolute_path = package_root_path.join(path.as_path());
                let rel_path = DbtPath::from_path(
                    absolute_path
                        .strip_prefix(&io.in_dir)
                        .unwrap_or(path.as_path()),
                );
                !unchanged_files.contains(&rel_path)
            });
            !assets.is_empty()
        });

        // Drop all operations.
        package.dbt_project.on_run_start = Verbatim::from(None);
        package.dbt_project.on_run_end = Verbatim::from(None);
    }

    let mut i = 0;
    dbt_state.packages.retain(|x| {
        if i == 0 {
            i += 1;
            true // always retain the root package
        } else {
            let result = !x.all_paths.is_empty();
            i += 1;
            result
        }
    });

    // Fixup package dependencies.
    let mut i = 0;
    while i < dbt_state.packages.len() {
        let cloned_dependencies = dbt_state.packages[i].dependencies.clone();
        for dep in cloned_dependencies {
            if !dbt_state.packages.iter().any(|x| x.dbt_project.name == dep) {
                // Remove dependency from package as it is not included anymore
                dbt_state.packages[i].dependencies.remove(&dep);
            }
        }
        i += 1;
    }
}

/// Adds all nodes from the cached `ResolvedNodes` to the `ResolverState`.
pub fn add_all_unchanged_nodes(resolved_state: &mut ResolverState, cached_nodes: &ResolvedNodes) {
    // todo: maybe pass ownership so that we don't have to clone?
    let ResolvedNodes {
        nodes,
        disabled_nodes,
        macros,
        operations,
    } = cached_nodes;
    // Add all nodes to the resolved state
    resolved_state.nodes.extend(nodes.clone());
    resolved_state.disabled_nodes.extend(disabled_nodes.clone());
    resolved_state.macros.macros.extend(macros.macros.clone());
    resolved_state.operations = operations.clone();
}

/// Drops all seen assets from the dbt state.
pub fn drop_all_seen_assets(
    dbt_state: &mut DbtState,
    seen_files: &HashSet<String>,
) -> FsResult<()> {
    // For each package in dbt_state
    for package in &mut dbt_state.packages {
        // drop all seen files
        package.all_paths.retain(|_, assets| {
            assets.retain(|(path_buf, _mtime)| {
                !seen_files.contains(path_buf.to_str().unwrap_or_default())
            });
            !assets.is_empty()
        });

        // also drop all assets that are in the dbt_properties
        package
            .dbt_properties
            .retain(|asset| !seen_files.contains(asset.path.to_str().unwrap_or_default()));
        package
            .analysis_files
            .retain(|asset| !seen_files.contains(asset.path.to_str().unwrap_or_default()));
        package
            .model_sql_files
            .retain(|asset| !seen_files.contains(asset.path.to_str().unwrap_or_default()));
        package
            .macro_files
            .retain(|asset| !seen_files.contains(asset.path.to_str().unwrap_or_default()));
        package
            .test_files
            .retain(|asset| !seen_files.contains(asset.path.to_str().unwrap_or_default()));
        package
            .seed_files
            .retain(|asset| !seen_files.contains(asset.path.to_str().unwrap_or_default()));
        package
            .docs_files
            .retain(|asset| !seen_files.contains(asset.path.to_str().unwrap_or_default()));
        package
            .snapshot_files
            .retain(|asset| !seen_files.contains(asset.path.to_str().unwrap_or_default()));
    }

    Ok(())
}

/// Adds all nodes from the cached `ResolvedNodes` to the `ResolverState`.
pub fn add_all_seen_nodes(
    resolved_state: &mut ResolverState,
    cached_nodes: &ResolvedNodes,
) -> FsResult<()> {
    // todo: maybe pass ownership so that we don't have to clone?
    let ResolvedNodes {
        nodes,
        disabled_nodes,
        macros,
        operations,
    } = cached_nodes;
    // Add all nodes to the resolved state
    resolved_state.nodes.extend(nodes.clone());
    resolved_state.disabled_nodes.extend(disabled_nodes.clone());
    resolved_state.macros.macros.extend(macros.macros.clone());
    resolved_state
        .operations
        .on_run_start
        .extend(operations.on_run_start.clone());
    resolved_state
        .operations
        .on_run_start
        .extend(operations.on_run_start.clone());
    Ok(())
}

// ----------------------------------------------------------

type DbtNodeRef = Arc<dyn InternalDbtNodeAttributes>;

/// There is a similar function above
/// that could be unified.
fn is_sql_file_path(path: &Path) -> bool {
    path.extension() == Some(OsStr::new("sql"))
}

/// There is a similar function above
/// that could be unified.
fn is_yml_file_path(path: &Path) -> bool {
    path.extension() == Some(OsStr::new("yml"))
}

/// There is a similar function above
/// that could be unified.
fn is_schema_impacted_model_file_path(
    schema_impacted_models: &HashMap<PathBuf, HashMap<NodeType, HashSet<String>>>,
    input_file: &Path,
    resource_type: NodeType,
) -> bool {
    if schema_impacted_models.is_empty() {
        return false;
    }

    // Only do this for SQL files.
    if !is_sql_file_path(input_file) {
        return false;
    }

    // Check the file_stem, example: "A" is the file_stem from "models/A.sql"
    let Some(file_stem) = input_file.file_stem().and_then(|x| x.to_str()) else {
        return false;
    };

    for map in schema_impacted_models.values() {
        let Some(set) = map.get(&resource_type) else {
            continue;
        };

        if set.contains(file_stem) {
            return true;
        }
    }
    false
}

/// There is a similar function above
/// that could be unified.
fn compute_impacted_models_by_schema_file_paths(
    io: &IoArgs,
    changed_schema_files: &HashSet<PathBuf>,
) -> FsResult<HashMap<PathBuf, HashMap<NodeType, HashSet<String>>>> {
    let mut impacted: HashMap<PathBuf, HashMap<NodeType, HashSet<String>>> = HashMap::new();
    for schema_yaml in changed_schema_files {
        let schema_path = schema_yaml.clone();
        let schema_yaml_content = stdfs::read_to_string(io.in_dir.join(schema_yaml))?;
        let doc: Value =
            dbt_yaml::from_str(&schema_yaml_content).unwrap_or(Value::Null(Default::default()));

        let mut resource_map: HashMap<NodeType, HashSet<String>> = HashMap::new();

        if let Value::Mapping(map, _span) = doc {
            for (key, value) in map {
                if let Value::String(resource_type, _span) = key {
                    let resource_type = {
                        match resource_type.as_str() {
                            "models" => Some(NodeType::Model),
                            "seeds" => Some(NodeType::Seed),
                            "snapshots" => Some(NodeType::Snapshot),
                            "sources" => Some(NodeType::Source),
                            _ => None,
                        }
                    };

                    let Some(resource_type) = resource_type else {
                        continue;
                    };

                    if let Value::Sequence(items, _span) = value {
                        for item in items {
                            if let Value::Mapping(item_map, _span) = item
                                && let Some(Value::String(name, _span)) = item_map
                                    .get(Value::String("name".to_string(), Default::default()))
                            {
                                resource_map
                                    .entry(resource_type)
                                    .or_default()
                                    .insert(name.clone());
                            }
                        }
                    }
                }
            }
        }
        if !resource_map.is_empty() {
            impacted.insert(schema_path, resource_map);
        }
    }
    Ok(impacted)
}

/// Filter nodes based on file changes and command type.
/// There is a similar function above
/// that could be unified.
#[allow(clippy::too_many_arguments)]
fn filter_dbt_nodes_by_file_changes(
    io: &IoArgs,
    nodes: HashMap<String, DbtNodeRef>,
    node_statuses: &HashMap<String, NodeStatus>,
    input_file_lookup: HashMap<String, Vec<PathBuf>>,
    out_unimpacted_files: &mut HashSet<PathBuf>,
    out_impacted_files: &mut HashSet<PathBuf>,
    out_impacted_schema_files: &mut HashSet<PathBuf>,
) -> FsResult<(HashMap<String, DbtNodeRef>, HashMap<String, DbtNodeRef>)> {
    // Build reverse dependency map (who depends on whom)
    let mut reverse_deps: HashMap<String, Vec<String>> = HashMap::new();
    for (unique_id, node) in &nodes {
        for dep in &node.base().depends_on.nodes {
            reverse_deps
                .entry(dep.clone())
                .or_default()
                .push(unique_id.clone());
        }
        for dep in &node.base().depends_on.macros {
            reverse_deps
                .entry(dep.clone())
                .or_default()
                .push(unique_id.clone());
        }
    }

    // Find nodes directly impacted by schema file changes and input file changes
    let mut impacted_nodes_by_unique_id = HashSet::new();

    // This loop will handle catching duplicate model definitions across more than one schema.
    // The majority of the time, this will not loop.
    let mut impacted_schema_files = out_impacted_schema_files.clone();
    loop {
        let mut new_impacted_nodes_by_unique_id = HashSet::new();

        let schema_impacted_models =
            compute_impacted_models_by_schema_file_paths(io, &impacted_schema_files)?;
        // Clear this so we can potentially pickup additional impacted schema files.
        impacted_schema_files.clear();

        for (unique_id, node) in &nodes {
            if impacted_nodes_by_unique_id.contains(unique_id) {
                continue;
            }

            let Some(input_files) = input_file_lookup.get(unique_id) else {
                continue;
            };

            let node_status = node_statuses
                .get(unique_id)
                .and_then(|x| x.latest_status.clone());

            let is_impacted_by_status = matches!(node_status, Some(NodeExecutionStatus::Error));

            for input_file in input_files {
                if is_impacted_by_status
                    || out_impacted_files.contains(input_file)
                    || is_schema_impacted_model_file_path(
                        &schema_impacted_models,
                        input_file,
                        node.resource_type(),
                    )
                {
                    new_impacted_nodes_by_unique_id.insert(unique_id.clone());
                    for input_file in input_files {
                        // Assumes that YML files are schema files.
                        if is_yml_file_path(input_file)
                            && !out_impacted_schema_files.contains(input_file)
                        {
                            impacted_schema_files.insert(input_file.clone());
                        }
                    }
                    break;
                }
            }
        }

        // Find ALL downstream dependencies (transitive closure)
        // Only do this for sources.
        let mut to_process: Vec<String> = new_impacted_nodes_by_unique_id.iter().cloned().collect();

        while let Some(current) = to_process.pop() {
            let Some(node) = nodes.get(&current) else {
                continue;
            };
            if node.resource_type() != NodeType::Source {
                continue;
            }
            if let Some(dependents) = reverse_deps.get(&current) {
                for dependent in dependents {
                    if new_impacted_nodes_by_unique_id.insert(dependent.clone()) {
                        // New impacted node found, add to processing queue
                        to_process.push(dependent.clone());
                    }
                }
            }
        }

        impacted_nodes_by_unique_id.extend(new_impacted_nodes_by_unique_id);

        // No additional schema files were impacted, therefore we are done.
        if impacted_schema_files.is_empty() {
            break;
        }

        // Mark the additional schema files as changed.
        for impacted_schema_file in &impacted_schema_files {
            out_unimpacted_files.remove(impacted_schema_file);
            out_impacted_schema_files.insert(impacted_schema_file.clone());
            out_impacted_files.insert(impacted_schema_file.clone());
        }
    }

    // Partition nodes into keep vs invalidated
    let (keep, invalidated): (HashMap<String, DbtNodeRef>, HashMap<String, DbtNodeRef>) = nodes
        .into_iter()
        .partition(|(unique_id, _)| !impacted_nodes_by_unique_id.contains(unique_id));

    // Update file lists for cache reporting
    for node in invalidated.values() {
        let Some(input_files) = input_file_lookup.get(&node.common().unique_id) else {
            continue;
        };
        for input_file in input_files {
            if out_unimpacted_files.remove(input_file) {
                out_impacted_files.insert(input_file.clone());
            }
        }
    }

    Ok((keep, invalidated))
}

pub struct PreviousResolvedState<'a> {
    pub nodes: &'a Nodes,
    pub disabled_nodes: &'a Nodes,
    pub macros: &'a Macros,
    pub operations: &'a Operations,
    pub node_statuses: HashMap<String, NodeStatus>,
    pub get_relation_calls: &'a GetRelationCalls,
    pub get_columns_in_relation_calls: &'a GetColumnsInRelationCalls,
    pub patterned_dangling_sources: &'a PatternedDanglingSources,
}

impl PreviousResolvedState<'_> {
    pub fn from_resolved_state<'a>(resolved_state: &'a ResolverState) -> PreviousResolvedState<'a> {
        // Build node statuses map for nodes with resolution errors so next time they are not reused.
        let mut node_statuses = HashMap::new();
        for unique_id in &resolved_state.nodes_with_resolution_errors {
            let node_status = NodeStatus {
                latest_state: Some(NodeExecutionState::Parsed),
                latest_status: Some(NodeExecutionStatus::Error),
                latest_time: None,
                latest_message: Some("Unresolved reference or source".to_string()),
            };
            node_statuses.insert(unique_id.clone(), node_status);
        }

        PreviousResolvedState {
            nodes: &resolved_state.nodes,
            disabled_nodes: &resolved_state.disabled_nodes,
            macros: &resolved_state.macros,
            operations: &resolved_state.operations,
            node_statuses,
            get_relation_calls: &resolved_state.get_relation_calls,
            get_columns_in_relation_calls: &resolved_state.get_columns_in_relation_calls,
            patterned_dangling_sources: &resolved_state.patterned_dangling_sources,
        }
    }
}

/// There is a similar function above
/// that could be unified.
#[allow(clippy::cognitive_complexity)]
#[allow(clippy::too_many_arguments)]
fn determine_changeset_from_previous_resolved_nodes<'a>(
    io: &IoArgs,
    prev_resolved_state: PreviousResolvedState<'a>,
    dbt_state: &DbtState,
    changed_files: HashSet<String>,
    mut unimpacted_files: HashSet<PathBuf>,
    mut impacted_files: HashSet<PathBuf>,
    mut impacted_schema_files: HashSet<PathBuf>,
) -> FsResult<Option<CacheState>> {
    let prev_nodes = prev_resolved_state.nodes;
    let prev_node_statuses = &prev_resolved_state.node_statuses;

    let root_package_path = &dbt_state.root_package().package_root_path;
    let package_lookup = dbt_state
        .packages
        .iter()
        .map(|x| (x.dbt_project.name.clone(), x))
        .collect::<HashMap<_, _>>();

    let mut input_file_lookup = HashMap::new();
    for (_, node) in prev_nodes.iter() {
        let Some(package) = package_lookup.get(&node.common().package_name) else {
            // REVIEW: Should we only continue on extended models?
            continue;
        };

        let package_path = &package.package_root_path;

        let input_files = if let Some(patch_path) = &node.common().patch_path {
            vec![node.common().path.clone(), patch_path.clone()]
        } else {
            vec![node.common().path.clone()]
        };

        let input_files = input_files
            .into_iter()
            .map(|x| {
                let abs_path = package_path.join(x);
                let Ok(rel_path) = abs_path.strip_prefix(root_package_path) else {
                    return PathBuf::default();
                };
                PathBuf::from(rel_path)
            })
            .collect::<Vec<_>>();

        input_file_lookup.insert(node.common().unique_id.to_string(), input_files);
    }

    let prev_dbt_nodes = prev_nodes
        .into_iter()
        .map(|node| (node.1.common().unique_id.to_string(), node.1.clone()))
        .collect::<HashMap<_, _>>();

    let mut changed_nodes = HashSet::new();
    for node in prev_dbt_nodes.values() {
        let Some(input_files) = input_file_lookup.get(&node.common().unique_id) else {
            continue;
        };

        let are_input_files_unchanged = input_files
            .iter()
            .all(|input_file| unimpacted_files.contains(input_file));
        if !are_input_files_unchanged {
            changed_nodes.insert(node.common().unique_id.to_string());
        }
    }

    // Use the shared helper function to filter nodes
    let (_keep, invalidated) = filter_dbt_nodes_by_file_changes(
        io,
        prev_dbt_nodes,
        prev_node_statuses,
        input_file_lookup,
        &mut unimpacted_files,
        &mut impacted_files,
        &mut impacted_schema_files,
    )?;

    let file_changes = FileChanges {
        changed_files: changed_files.into_iter().map(DbtPath::from).collect(),
        unimpacted_files: unimpacted_files
            .into_iter()
            .map(DbtPath::from_path)
            .collect(),
        impacted_files: impacted_files.into_iter().map(DbtPath::from_path).collect(),
        deleted_files: HashSet::default(), // not supported yet
        new_files: HashSet::default(),     // not supported yet
    };

    let mut resolved_nodes = ResolvedNodes {
        nodes: prev_resolved_state.nodes.clone(),
        disabled_nodes: prev_resolved_state.disabled_nodes.clone(),
        macros: prev_resolved_state.macros.clone(),
        operations: prev_resolved_state.operations.clone(),
    };

    let remove_node = |nodes: &mut Nodes, unique_id: &str| {
        nodes.models.remove(unique_id);
        nodes.seeds.remove(unique_id);
        nodes.snapshots.remove(unique_id);
        nodes.tests.remove(unique_id);
        nodes.unit_tests.remove(unique_id);
        nodes.exposures.remove(unique_id);
        nodes.analyses.remove(unique_id);
        nodes.metrics.remove(unique_id);
        nodes.semantic_models.remove(unique_id);
        nodes.sources.remove(unique_id);
        nodes.groups.remove(unique_id);
        nodes.saved_queries.remove(unique_id);
    };

    let mut get_relation_calls = prev_resolved_state.get_relation_calls.clone();
    let mut get_columns_in_relation_calls =
        prev_resolved_state.get_columns_in_relation_calls.clone();
    let mut patterned_dangling_sources = prev_resolved_state.patterned_dangling_sources.clone();

    for node in invalidated {
        let unique_id = node.0;
        remove_node(&mut resolved_nodes.nodes, &unique_id);
        remove_node(&mut resolved_nodes.disabled_nodes, &unique_id);
        resolved_nodes.macros.macros.remove(&unique_id);
        resolved_nodes.macros.docs_macros.remove(&unique_id);
        get_relation_calls.remove(&unique_id);
        get_columns_in_relation_calls.remove(&unique_id);
        patterned_dangling_sources.remove(&unique_id);
    }

    let nodes_with_changeset = CacheState {
        file_changes,
        unimpacted_resolved_nodes: resolved_nodes,
        unimpacted_node_statuses: HashMap::default(), // Not needed for LSP. For CLI, it is.
        unimpacted_get_relation_calls: get_relation_calls,
        unimpacted_get_columns_in_relation_calls: get_columns_in_relation_calls,
        unimpacted_patterned_dangling_sources: patterned_dangling_sources,
        changed_nodes: Arc::new(changed_nodes),
    };

    // If 500+ files have changed/deleted/added, then we have too many changes.
    if nodes_with_changeset.file_changes.impacted_files.len()
        + nodes_with_changeset.file_changes.new_files.len()
        + nodes_with_changeset.file_changes.deleted_files.len()
        > 500
    {
        return Ok(None);
    }

    Ok(Some(nodes_with_changeset))
}

/// Currently in use for LSP.
/// This could be extended to the CLI cache,
/// but there are a couple of limitations that would have to be addressed:
///     - Does not take into account created or deleted files.
///     - [CacheState::unimpacted_node_statuses] is not populated as LSP does not use it.
#[allow(clippy::too_many_arguments)]
pub fn determine_cache_state_from_previous_previous_resolved_nodes<'a>(
    io: &IoArgs,
    prev_resolved_state: PreviousResolvedState<'a>,
    changed_files_list: &[String],
    dbt_state: &DbtState,
) -> FsResult<Option<CacheState>> {
    // Collect all current files from dbt_state
    let mut all_current_files: HashSet<PathBuf> = HashSet::new();
    for package in &dbt_state.packages {
        let package_root_path = &package.package_root_path;
        for paths in package.all_paths.values() {
            for (path, _) in paths {
                let full_path = if path.has_root() {
                    path.as_path().to_path_buf()
                } else {
                    package_root_path.join(path.as_path())
                };

                // Get path relative to io.in_dir
                let relative_path = if let Ok(rel_path) = full_path.strip_prefix(&io.in_dir) {
                    PathBuf::from(rel_path)
                } else {
                    full_path
                };
                all_current_files.insert(relative_path);
            }
        }
    }

    // Partition files into changed and unchanged
    let changed_files: HashSet<PathBuf> = changed_files_list.iter().map(PathBuf::from).collect();
    let unchanged_files: HashSet<PathBuf> = all_current_files
        .difference(&changed_files)
        .cloned()
        .collect();

    // Identify changed schema and seed files
    let changed_schema_files: HashSet<PathBuf> = changed_files
        .iter()
        .filter(|path| {
            is_yml_file_path(path)
                && !path.ends_with(DBT_PROFILES_YML)
                && !path.ends_with(DBT_PROJECT_YML)
                && !path.ends_with(DBT_DEPENDENCIES_YML)
                && !path.ends_with(DBT_PACKAGES_YML)
                && !path.ends_with(DBT_PACKAGES_LOCK_FILE)
                && !path.ends_with(DBT_SELECTORS_YML)
        })
        .cloned()
        .collect();

    determine_changeset_from_previous_resolved_nodes(
        io,
        prev_resolved_state,
        dbt_state,
        changed_files_list.iter().cloned().collect(),
        unchanged_files,
        changed_files,
        changed_schema_files,
    )
}
