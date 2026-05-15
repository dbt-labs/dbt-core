//! Incremental parse logic: decide whether to incrementally update or fully re-parse
//! the compilation based on file changes since the last cache save.
//!
//! # Cache invalidation decision table
//!
//! `needs_full_parse()` runs on every `--partial-parse` invocation and returns either
//! `None` (Incremental) or `Some(reason)` (FullParse):
//!
//! | Event                           | How detected                   | Outcome     | ~Time (scale_6k) |
//! |---------------------------------|--------------------------------|-------------|------------------|
//! | No files changed                | all file mtimes match          | Incremental | ~500ms           |
//! | model / analysis `.sql` changed | file mtime changed             | Incremental | ~500ms           |
//! | any other file changed          | file mtime + kind/ext check    | FullParse   | ~1.8s            |
//! | any file deleted                | stat fails                     | FullParse   | ~1.8s            |
//! | new file added                  | not in all_paths — not seen    | Incremental | ~500ms (*)       |
//! | `dbt_project.yml` changed       | blake3 content hash            | FullParse   | ~1.8s            |
//! | `profiles.yml` changed          | blake3 content hash            | FullParse   | ~1.8s            |
//! | `--vars` changed                | blake3 hash of serialized vars | FullParse   | ~1.8s            |
//! | env var (used in Jinja) changed | value compared at load time    | FullParse   | ~1.8s            |
//! | binary version changed          | `CARGO_PKG_VERSION` mismatch   | FullParse   | ~1.8s            |
//!
//! (*) New files are not in `all_paths` so `needs_full_parse` cannot see them. They are
//! discovered by the WalkDir inside `initialize()` on the Incremental path — new model files
//! will be compiled, new macro/yml files will trigger a FullParse from `compute_file_changeset`.
//!
//! **Incremental**: passes the previous compilation to `initialize()`, which rescans only
//! changed model/analysis `.sql` files and re-resolves affected nodes (~500ms).
//! **FullParse**: discards the cache and runs a complete fresh parse (~1.8s).
//!
//! Timings measured on scale_6k (11k models). No-partial-parse baseline: ~1.65s.

use dbt_adapter::relation::do_create_relation;
use dbt_common::{
    ErrorCode, FsResult, fs_err,
    io_args::{EvalArgs, FsCommand, IoArgs},
    node_selector::{IndirectSelection, MethodName, SelectExpression},
    path::DbtPath,
};
use dbt_jinja_vars::DbtVars;
use dbt_schemas::{
    schemas::{
        Nodes, common::ResolvedQuoting, macros::DbtDocsMacro, relations::base::RelationPattern,
    },
    state::{
        DbtPackage, DbtProfile, DbtState, GetRelationCalls, Macros, Operations, ResolverState,
        ResourcePathKind,
    },
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const INCREMENTAL_STATE_VERSION: u32 = 2;

pub struct IncrementalState {
    pub version: u32,
    /// Binary version that wrote this state. Mismatches (e.g. after upgrade) invalidate the cache
    /// because any type change can make deserialization produce wrong results.
    pub dbt_version: String,
    pub packages: Vec<PackageSnapshot>,
    pub profile_hash: String,
    /// blake3 hash of raw profiles.yml bytes — catches env_var changes that don't touch the file timestamp.
    pub profile_file_hash: String,
    /// blake3 hash of raw dbt_project.yml bytes — catches env_var changes in project config.
    pub project_file_hash: String,
    /// blake3 hash of serialized CLI --vars — catches variable overrides between invocations.
    pub cli_vars_hash: String,
    pub dbt_profile: DbtProfile,
    pub vars: BTreeMap<String, IndexMap<String, DbtVars>>,
    pub nodes: Nodes,
    pub disabled_nodes: Nodes,
    pub macros: Macros,
    pub operations: Operations,
    pub nodes_with_resolution_errors: HashSet<String>,
    pub nodes_with_access_errors: HashSet<String>,
    /// Env vars observed during Jinja rendering (name → value at parse time).
    /// On reload, any value that differs from the current environment invalidates the cache.
    pub env_vars: HashMap<String, String>,
    /// adapter.get_relation() calls recorded during Jinja rendering: unique_id →
    /// [(database, schema, identifier)]. Reconstructed into Arc<dyn BaseRelation>
    /// on load for classify_introspection_kinds.
    pub get_relation_calls: BTreeMap<String, Vec<(String, String, String)>>,
    /// adapter.get_columns_in_relation() calls recorded during Jinja rendering.
    pub get_columns_in_relation_calls: BTreeMap<String, Vec<(String, String, String)>>,
    /// Patterned dangling sources recorded during Jinja rendering.
    pub patterned_dangling_sources: BTreeMap<String, Vec<RelationPattern>>,
    /// True when any node (or macro) in the project accesses the Jinja `graph` variable.
    /// When true, all node payloads must be loaded before Jinja rendering begins;
    /// lazy payload loading is not safe.
    pub any_uses_graph: bool,
    pub manifest_selectors_json: String,
    pub docs_macros: BTreeMap<String, DbtDocsMacro>,
    pub git_sha: String,
    pub git_branch: String,
    pub git_is_dirty: bool,
}

impl IncrementalState {
    /// Validate that the cached state is still usable given the current inputs.
    /// Returns a human-readable reason for invalidation, or None if valid.
    pub fn validate(
        &self,
        cli_vars: &Option<BTreeMap<String, dbt_yaml::Value>>,
    ) -> Option<&'static str> {
        if self.cli_vars_hash != hash_cli_vars(cli_vars) {
            return Some("--vars changed");
        }
        if self.profile_hash != self.dbt_profile.blake3_hash() {
            return Some("profile config changed");
        }
        if let Some(root_pkg) = self.packages.first() {
            let root_path = Path::new(&root_pkg.package_root_path);
            let profile_path = root_path.join(&self.dbt_profile.relative_profile_path);
            if self.profile_file_hash != hash_file_at_path(&profile_path) {
                return Some("profiles.yml content changed");
            }
            let project_path = root_path.join("dbt_project.yml");
            if self.project_file_hash != hash_file_at_path(&project_path) {
                return Some("dbt_project.yml content changed");
            }
        }
        for (name, old_value) in &self.env_vars {
            match std::env::var(name) {
                Ok(current) if current != *old_value => return Some("env var changed"),
                Err(_) => return Some("env var removed"),
                _ => {}
            }
        }
        None
    }

    /// Stat-based check: determine whether a full re-parse is needed.
    ///
    /// Returns `Some(reason)` if the cache must be discarded (FullParse), or
    /// `None` if the caller should proceed with an incremental parse.
    ///
    /// Only `.sql` files under `ModelPaths` or `AnalysisPaths` are safe for
    /// incremental re-resolve. Every other changed file — `.yml`/`.yaml`,
    /// `.md`, `.csv`, macro/test/snapshot `.sql` — triggers FullParse because
    /// the incremental pipeline either cannot handle it or would silently drop
    /// the change (e.g. `.md` docs are ignored by `compute_file_changeset`).
    pub fn needs_full_parse(&self) -> Option<&'static str> {
        for pkg in &self.packages {
            let root = Path::new(&pkg.package_root_path);
            for (kind, files) in &pkg.all_paths {
                let kind_safe_for_incremental = matches!(
                    kind,
                    ResourcePathKind::ModelPaths | ResourcePathKind::AnalysisPaths
                );
                let is_docs_path = *kind == ResourcePathKind::DocsPaths;
                for (path_str, saved_nanos) in files {
                    // DocsPaths tracks all files in doc directories, but only .md changes
                    // require a full re-parse (SQL/YML in doc dirs are tracked by other kinds).
                    if is_docs_path
                        && !Path::new(path_str)
                            .extension()
                            .and_then(|e| e.to_str())
                            .map(|e| e.eq_ignore_ascii_case("md"))
                            .unwrap_or(false)
                    {
                        continue;
                    }
                    let path = root.join(path_str);
                    let Ok(meta) = std::fs::metadata(&path) else {
                        return Some("file deleted");
                    };
                    let current_nanos = system_time_to_nanos(meta.modified().unwrap_or(UNIX_EPOCH));
                    if current_nanos == *saved_nanos {
                        continue;
                    }
                    // File changed — only model/analysis .sql is safe for incremental.
                    let is_sql = Path::new(path_str)
                        .extension()
                        .and_then(|e| e.to_str())
                        .map(|e| e.eq_ignore_ascii_case("sql"))
                        .unwrap_or(false);
                    if kind_safe_for_incremental && is_sql {
                        continue; // handled by incremental path
                    }
                    return Some("non-incremental file changed");
                }
            }
        }
        None
    }

    /// Returns `true` when every file in `all_paths` has an unchanged mtime.
    /// Used by the `--partial-load` fast-path to avoid a full project reload when
    /// the selected subset is still valid.
    pub fn has_no_file_changes(&self) -> bool {
        for pkg in &self.packages {
            let root = Path::new(&pkg.package_root_path);
            for (kind, files) in &pkg.all_paths {
                let is_docs_path = *kind == ResourcePathKind::DocsPaths;
                for (path_str, saved_nanos) in files {
                    if is_docs_path
                        && !Path::new(path_str)
                            .extension()
                            .and_then(|e| e.to_str())
                            .map(|e| e.eq_ignore_ascii_case("md"))
                            .unwrap_or(false)
                    {
                        continue;
                    }
                    let path = root.join(path_str);
                    let Ok(meta) = std::fs::metadata(&path) else {
                        return false;
                    };
                    let current_nanos = system_time_to_nanos(meta.modified().unwrap_or(UNIX_EPOCH));
                    if current_nanos != *saved_nanos {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Guard against a partial node set being handed to `derive_deps` in the scheduler.
    ///
    /// `derive_deps` errors if any `depends_on.nodes` entry is absent from the loaded
    /// `Nodes` map.  When `payload_kinds_for_command` or `unique_id_filter_for_selector`
    /// narrow the loaded set, some deps may have been intentionally omitted.  Rather than
    /// changing the scheduler, we replicate the same closure check here: if any loaded node
    /// references a dep that wasn't loaded, we fall back to full load before the scheduler
    /// ever sees the incomplete graph.
    ///
    /// This is only reached under `--partial-parse`; for a full parse every node is loaded
    /// so the check always passes.
    pub fn all_deps_present(&self) -> Option<&'static str> {
        for (_unique_id, node) in self.nodes.iter() {
            for dep in node.base().depends_on.nodes.iter() {
                if !self.nodes.contains(dep) {
                    return Some("dependency not in loaded set");
                }
            }
        }
        None
    }
}

fn current_dbt_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

// ── git state ─────────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct GitInfo {
    pub sha: String,
    pub branch: String,
    pub is_dirty: bool,
}

pub fn read_git_info(project_root: &Path) -> Option<GitInfo> {
    let (repo_path, _) = gix_discover::upwards(project_root).ok()?;
    let repo = gix::open(repo_path.as_ref()).ok()?;
    let sha = repo
        .rev_parse_single("HEAD")
        .ok()
        .map(|id| id.to_string())
        .unwrap_or_default();
    // Read HEAD file directly to get branch name for symbolic refs.
    let branch = std::fs::read_to_string(repo.path().join("HEAD"))
        .ok()
        .and_then(|s| {
            s.trim()
                .strip_prefix("ref: refs/heads/")
                .map(|b| b.to_string())
        })
        .unwrap_or_default();
    Some(GitInfo {
        sha,
        branch,
        is_dirty: false,
    })
}

fn blake3_hex(data: &[u8]) -> String {
    let hash = blake3::hash(data);
    hex::encode(&hash.as_bytes()[..16])
}

pub fn hash_cli_vars(vars: &Option<BTreeMap<String, dbt_yaml::Value>>) -> String {
    match vars.as_ref().filter(|m| !m.is_empty()) {
        None => blake3_hex(b"{}"),
        Some(v) => {
            let json = serde_json::to_vec(v).unwrap_or_default();
            blake3_hex(&json)
        }
    }
}

pub fn hash_file_at_path(path: &Path) -> String {
    match std::fs::read(path) {
        Ok(bytes) => blake3_hex(&bytes),
        Err(_) => blake3_hex(b"__missing__"),
    }
}

#[derive(Serialize, Deserialize)]
pub struct PackageSnapshot {
    pub package_root_path: String,
    pub package_name: String,
    pub all_paths: HashMap<ResourcePathKind, Vec<(String, u64)>>,
    pub dependencies: BTreeSet<String>,
}

fn system_time_to_nanos(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos() as u64
}

fn nanos_to_system_time(nanos: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(nanos)
}

fn snapshot_packages(dbt_state: &DbtState) -> Vec<PackageSnapshot> {
    crate::parse_state::snapshot_packages(&dbt_state.packages)
}

fn snapshot_relation_calls(
    calls: &GetRelationCalls,
) -> BTreeMap<String, Vec<(String, String, String)>> {
    calls
        .iter()
        .map(|(uid, relations)| {
            let coords = relations
                .iter()
                .map(|r| {
                    (
                        r.database().unwrap_or_default().to_string(),
                        r.schema().unwrap_or_default().to_string(),
                        r.identifier().unwrap_or_default().to_string(),
                    )
                })
                .collect();
            (uid.clone(), coords)
        })
        .collect()
}

/// Reconstruct GetRelationCalls / GetColumnsInRelationCalls from serialized coordinates.
pub fn reconstruct_relation_calls(
    serialized: &BTreeMap<String, Vec<(String, String, String)>>,
    adapter_type: dbt_adapter_core::AdapterType,
    quoting: ResolvedQuoting,
) -> GetRelationCalls {
    use dbt_schemas::schemas::relations::base::BaseRelation;
    use std::sync::Arc;
    serialized
        .iter()
        .filter_map(|(uid, coords)| {
            let relations: Vec<_> = coords
                .iter()
                .filter_map(|(db, schema, id)| {
                    do_create_relation(
                        adapter_type,
                        db.clone(),
                        schema.clone(),
                        Some(id.clone()),
                        None,
                        quoting,
                    )
                    .ok()
                    .map(|r| Arc::from(r) as Arc<dyn BaseRelation>)
                })
                .collect();
            if relations.is_empty() {
                None
            } else {
                Some((uid.clone(), relations))
            }
        })
        .collect()
}

/// Save incremental state to `target/parse_state/`.
///
/// `changed_nodes` controls which node rows are written:
/// - `None`       — cold start: write every node.
/// - `Some(set)`  — incremental: only nodes in `set`; deleted nodes are removed.
pub fn save_parse_state(
    io: &IoArgs,
    dbt_state: &DbtState,
    resolved_state: &ResolverState,
    cli_vars: &Option<BTreeMap<String, dbt_yaml::Value>>,
    env_vars: HashMap<String, String>,
    changed_nodes: Option<&HashSet<String>>,
) -> FsResult<()> {
    let root_pkg = dbt_state.packages.first();
    let project_file_path = root_pkg
        .map(|p| p.package_root_path.join("dbt_project.yml"))
        .unwrap_or_default();
    let profile_file_path = root_pkg
        .map(|p| {
            p.package_root_path
                .join(&dbt_state.dbt_profile.relative_profile_path)
        })
        .unwrap_or_default();

    // Serialise the fields that go into `meta` as JSON strings.
    let dbt_profile_json = serde_json::to_string(&dbt_state.dbt_profile)
        .map_err(|e| fs_err!(ErrorCode::Generic, "Failed to serialise dbt_profile: {e}"))?;
    let vars_json = serde_json::to_string(&dbt_state.vars)
        .map_err(|e| fs_err!(ErrorCode::Generic, "Failed to serialise vars: {e}"))?;
    let env_vars_json = serde_json::to_string(&env_vars)
        .map_err(|e| fs_err!(ErrorCode::Generic, "Failed to serialise env_vars: {e}"))?;
    let get_relation_calls_json =
        serde_json::to_string(&snapshot_relation_calls(&resolved_state.get_relation_calls))
            .map_err(|e| {
                fs_err!(
                    ErrorCode::Generic,
                    "Failed to serialise get_relation_calls: {e}"
                )
            })?;
    let get_columns_in_relation_calls_json = serde_json::to_string(&snapshot_relation_calls(
        &resolved_state.get_columns_in_relation_calls,
    ))
    .map_err(|e| {
        fs_err!(
            ErrorCode::Generic,
            "Failed to serialise get_columns_in_relation_calls: {e}"
        )
    })?;
    let patterned_dangling_sources_json =
        serde_json::to_string(&resolved_state.patterned_dangling_sources).map_err(|e| {
            fs_err!(
                ErrorCode::Generic,
                "Failed to serialise patterned_dangling_sources: {e}"
            )
        })?;
    let nodes_with_resolution_errors_json =
        serde_json::to_string(&resolved_state.nodes_with_resolution_errors).map_err(|e| {
            fs_err!(
                ErrorCode::Generic,
                "Failed to serialise nodes_with_resolution_errors: {e}"
            )
        })?;
    let nodes_with_access_errors_json =
        serde_json::to_string(&resolved_state.nodes_with_access_errors).map_err(|e| {
            fs_err!(
                ErrorCode::Generic,
                "Failed to serialise nodes_with_access_errors: {e}"
            )
        })?;
    let operations_json = serde_json::to_string(&resolved_state.operations)
        .map_err(|e| fs_err!(ErrorCode::Generic, "Failed to serialise operations: {e}"))?;

    let packages = snapshot_packages(dbt_state);
    let git_info =
        read_git_info(project_file_path.parent().unwrap_or(&project_file_path)).unwrap_or_default();

    crate::parse_state::save(&crate::parse_state::SaveArgs {
        out_dir: &io.out_dir,
        version: INCREMENTAL_STATE_VERSION,
        dbt_version: &current_dbt_version(),
        profile_hash: &dbt_state.dbt_profile.blake3_hash(),
        profile_file_hash: &hash_file_at_path(&profile_file_path),
        project_file_hash: &hash_file_at_path(&project_file_path),
        cli_vars_hash: &hash_cli_vars(cli_vars),
        dbt_profile_json: &dbt_profile_json,
        vars_json: &vars_json,
        env_vars_json: &env_vars_json,
        get_relation_calls_json: &get_relation_calls_json,
        get_columns_in_relation_calls_json: &get_columns_in_relation_calls_json,
        patterned_dangling_sources_json: &patterned_dangling_sources_json,
        nodes_with_resolution_errors_json: &nodes_with_resolution_errors_json,
        nodes_with_access_errors_json: &nodes_with_access_errors_json,
        operations_json: &operations_json,
        packages: &packages,
        nodes: &resolved_state.nodes,
        disabled_nodes: &resolved_state.disabled_nodes,
        macros: &resolved_state.macros,
        changed_nodes,
        ingested_at: dbt_state.run_started_at.timestamp_millis(),
        selectors_json: &serde_json::to_string(&resolved_state.manifest_selectors)
            .unwrap_or_default(),
        git_sha: &git_info.sha,
        git_branch: &git_info.branch,
        git_is_dirty: git_info.is_dirty,
    })
    .map_err(|e| fs_err!(ErrorCode::Generic, "Failed to save incremental state: {e}"))?;

    Ok(())
}

/// Read only the `any_uses_graph` meta flag from the SQLite cache without loading nodes.
pub fn peek_any_uses_graph(io: &IoArgs) -> bool {
    crate::parse_state::peek_any_uses_graph(&io.out_dir)
}

// ── selector payload guard ────────────────────────────────────────────────────

/// Returns true when the selector expression contains any method that requires
/// full node payloads to evaluate correctly.
///
/// Safe for lazy load (index fields only): `fqn`, `path`, `file`, `tag`, `package`,
/// `resource_type`, `source`.
/// Require payload: everything else, including `config:`, `access:`, `state:`,
/// `result:`, `source_status:`, `group:`, `version:`, etc.
// NOTE: graph-walk modifiers (+x, x+, @x) are intentionally NOT checked here.
// The BFS in the scheduler (multi_source_bfs / ancestor-descendant walks) walks
// only the nodes that were actually loaded — it silently stops at the boundary of
// unloaded kinds.  For the currently partial-loaded commands (seed, snapshot) this is
// correct by construction: seeds/snapshots are leaf nodes with no model parents.
// If partial loading is ever extended to run/test/compile, parents_depth / children_depth
// must be checked here (or the allowed_kinds set must include all transitively
// reachable kinds) to avoid silently wrong +x results.
fn expr_needs_full_payload(expr: &SelectExpression) -> bool {
    match expr {
        SelectExpression::Atom(criteria) => {
            let needs = !matches!(
                criteria.method,
                MethodName::Fqn
                    | MethodName::Path
                    | MethodName::File
                    | MethodName::Tag
                    | MethodName::Package
                    | MethodName::ResourceType
                    | MethodName::Source
            );
            needs
                || criteria
                    .exclude
                    .as_ref()
                    .is_some_and(|e| expr_needs_full_payload(e))
        }
        SelectExpression::And(exprs) | SelectExpression::Or(exprs) => {
            exprs.iter().any(expr_needs_full_payload)
        }
        SelectExpression::Exclude(expr) => expr_needs_full_payload(expr),
    }
}

/// Compute the set of node kinds whose payloads need to be loaded for a given command.
///
/// Returns `None` when a full load is required for correctness.
/// Returns `Some(kinds)` when only those kinds' payloads are needed.
///
/// Guards that force a full load:
/// 1. `write_json = true`     — manifest export needs all nodes
/// 2. `FsCommand::Docs`       — docs generation needs all nodes
/// 3. `any_uses_graph = true` — Jinja `graph` variable requires all nodes at render time
/// 4. `defer = true`          — defer_sa_upstreams may expand frontier past selected nodes
/// 5. Selector uses payload-only methods (config, state, result, source_status, …)
pub fn payload_kinds_for_command(eval: &EvalArgs, io: &IoArgs) -> Option<HashSet<&'static str>> {
    if eval.write_json {
        return None;
    }
    if eval.command == FsCommand::Docs {
        return None;
    }
    if peek_any_uses_graph(io) {
        return None;
    }
    if eval.defer {
        return None;
    }
    let select_needs_payload = eval.select.as_ref().is_some_and(expr_needs_full_payload)
        || eval.exclude.as_ref().is_some_and(expr_needs_full_payload);
    if select_needs_payload {
        return None;
    }

    // All guards passed — return the minimal kinds set for this command.
    // Macros, sources, exposures, metrics, semantic_models, saved_queries, functions, and groups
    // are always included because they can appear as dependencies of the primary resource type.
    let shared: &[&'static str] = &[
        "macro",
        "source",
        "exposure",
        "metric",
        "semantic_model",
        "saved_query",
        "function",
        "group",
    ];
    match eval.command {
        FsCommand::Seed => Some(shared.iter().copied().chain(["seed"]).collect()),
        FsCommand::Snapshot => Some(shared.iter().copied().chain(["snapshot"]).collect()),
        // show compiles a single model (or inline SQL) and fetches rows. It never
        // schedules seeds, snapshots, or unit_tests, so those payloads can be skipped.
        // unique_id_filter_for_selector further narrows to just the selected node +
        // its transitive deps when the selector is a simple atom.
        FsCommand::Show => Some(
            shared
                .iter()
                .copied()
                .chain(["model", "test", "analysis"])
                .collect(),
        ),
        // run/test/compile could also narrow kinds (e.g. skip seeds for run), but any
        // omitted kind that appears in depends_on.nodes of a loaded node would cause
        // all_deps_present() to fall back to full load anyway, so the gain is small.
        _ => None,
    }
}

/// Try to resolve a unique_id filter from a selector expression using only `node_index`
/// (no payload deserialization).
///
/// Returns `Some(ids)` — the set of unique_ids to load (matched nodes + their direct
/// `depends_on` entries + all macros) — when the selector is a single atom with:
/// - a safe index-lookup method (Fqn, Tag, Package, ResourceType)
/// - no graph-walk modifiers (`parents_depth`, `children_depth`, or the `@` operator)
/// - no glob wildcards in the value
///
/// Returns `None` (fall back to full load) for anything else: graph walks (`+x`, `x+`,
/// `@x`), wildcard values, compound expressions (And/Or), unsafe methods, or any error.
pub fn unique_id_filter_for_selector(eval: &EvalArgs, io: &IoArgs) -> Option<HashSet<String>> {
    // --exclude at the top level: fall back — we'd need to subtract from the full set.
    if eval.exclude.is_some() {
        return None;
    }
    let expr = eval.select.as_ref()?;
    // The default indirect selection mode for this invocation.
    // apply_default_indirect_selection stamps the mode onto each atom; here we
    // supply it as the fallback for atoms that haven't been stamped yet.
    let default_indirect = eval.indirect_selection.unwrap_or_default();
    resolve_expr(expr, &io.out_dir, default_indirect)
}

/// Returns `true` for selector methods that can only be evaluated at schedule time
/// (after payloads are loaded and external artifacts are available). These arms are
/// silently skipped during phase-1 load-time resolution — the scheduler applies the
/// full expression after load.
fn is_phase2_only(criteria: &dbt_common::node_selector::SelectionCriteria) -> bool {
    match criteria.method {
        // state:dirty is phase-1 (mtime-based). All other state: values need
        // deserialized payloads + a --state manifest, so they are phase-2-only.
        MethodName::State => criteria.value != "dirty",
        MethodName::Result | MethodName::SourceStatus => true,
        _ => false,
    }
}

/// Recursively resolve a `SelectExpression` to a `unique_id` set using only `node_index`.
/// Returns `None` if any sub-expression requires a full payload load.
///
/// `default_indirect` is the fallback `IndirectSelection` mode when an atom's own
/// `criteria.indirect` field is `None`.
fn resolve_expr(
    expr: &SelectExpression,
    out_dir: &Path,
    default_indirect: IndirectSelection,
) -> Option<HashSet<String>> {
    use crate::index_resolution::{
        IndexLookupMethod, resolve_dirty_unique_ids_from_index, resolve_unique_ids_from_index,
    };
    match expr {
        SelectExpression::Atom(criteria) => {
            // @x — fall back.
            if criteria.childrens_parents {
                return None;
            }
            // Nested --exclude inside an atom — fall back.
            if criteria.exclude.is_some() {
                return None;
            }

            // state:dirty — phase-1 resolvable from mtime index. Children walk
            // allowed: resolve_dirty_unique_ids_from_index loads the full ancestor
            // closure so all_deps_present() always passes.
            if matches!(criteria.method, MethodName::State) && criteria.value == "dirty" {
                let indirect = criteria.indirect.unwrap_or(default_indirect);
                let include_indirect = !matches!(
                    indirect,
                    IndirectSelection::Empty | IndirectSelection::Cautious
                );
                return resolve_dirty_unique_ids_from_index(
                    out_dir,
                    criteria.parents_depth,
                    criteria.children_depth,
                    include_indirect,
                );
            }

            let method = match criteria.method {
                MethodName::Fqn => IndexLookupMethod::Fqn,
                MethodName::Tag => IndexLookupMethod::Tag,
                MethodName::Package => IndexLookupMethod::Package,
                MethodName::ResourceType => IndexLookupMethod::ResourceType,
                _ => return None,
            };
            let indirect = criteria.indirect.unwrap_or(default_indirect);
            // Eager and Buildable expand to include tests/unit_tests that depend on
            // the selected nodes (indirect selection).  Empty/Cautious do not.
            let include_indirect = !matches!(
                indirect,
                IndirectSelection::Empty | IndirectSelection::Cautious
            );
            resolve_unique_ids_from_index(
                out_dir,
                method,
                &criteria.value,
                criteria.parents_depth,
                criteria.children_depth,
                include_indirect,
            )
        }
        SelectExpression::And(exprs) => {
            // Phase-2-only arms (state:modified, result:, source_status:) cannot be
            // resolved at load time — they need payloads or external artifacts. Skip
            // them here; the scheduler applies the full AND expression after load, so
            // they still filter the result correctly.
            //
            // INVARIANT: the phase-1 arms must form a superset of what the phase-2
            // arms would select. For `state:dirty+ state:modified` this holds:
            // state:dirty+ loads all descendants of touched nodes (+ their ancestor
            // closure), and state:modified only selects nodes reachable from local
            // changes — which are a subset of the dirty closure within the incremental
            // path (yml/macro changes trigger FullParse before reaching here).
            let mut result: Option<HashSet<String>> = None;
            let mut has_phase1 = false;
            for e in exprs {
                if let SelectExpression::Atom(c) = e {
                    if is_phase2_only(c) {
                        continue;
                    }
                }
                let arm = resolve_expr(e, out_dir, default_indirect)?;
                has_phase1 = true;
                result = Some(match result {
                    None => arm,
                    Some(acc) => acc.into_iter().filter(|id| arm.contains(id)).collect(),
                });
            }
            if has_phase1 { result } else { None }
        }
        SelectExpression::Or(exprs) => {
            // Union: if any arm falls back, we fall back.
            // OR with a phase-2-only selector is architecturally unsound — the
            // phase-2 arm could select nodes not yet loaded, which the scheduler
            // would silently miss. Fall back to full load in that case.
            let mut result: HashSet<String> = HashSet::new();
            for e in exprs {
                let arm = resolve_expr(e, out_dir, default_indirect)?;
                result.extend(arm);
            }
            Some(result)
        }
        SelectExpression::Exclude(_) => None, // nested exclude — fall back
    }
}

/// Remove `state:dirty` atoms from a selector expression tree before handing to the
/// scheduler. The scheduler has no `state:dirty` implementation — it was fully resolved
/// at load time by `resolve_dirty_unique_ids_from_index`. Leaving it in would cause
/// `match_state("dirty")` to hit `_ => Ok(false)` and silently select nothing.
///
/// - `And([state:dirty+, state:modified])` → `state:modified`
/// - `Atom(state:dirty+)` alone → `None` (select all loaded nodes)
/// - `Or([state:dirty+, tag:foo])` → unchanged (Or is not touched — falling back to full
///   load is correct for Or with phase-2 selectors)
pub fn strip_dirty_selector(expr: SelectExpression) -> Option<SelectExpression> {
    match expr {
        SelectExpression::Atom(ref c)
            if matches!(c.method, MethodName::State) && c.value == "dirty" =>
        {
            None
        }
        SelectExpression::And(exprs) => {
            let filtered: Vec<SelectExpression> =
                exprs.into_iter().filter_map(strip_dirty_selector).collect();
            match filtered.len() {
                0 => None,
                1 => filtered.into_iter().next(),
                _ => Some(SelectExpression::And(filtered)),
            }
        }
        other => Some(other),
    }
}

/// Returns `true` if the selector expression tree contains a `state:dirty` atom.
pub fn selector_contains_dirty(expr: &SelectExpression) -> bool {
    match expr {
        SelectExpression::Atom(c) => matches!(c.method, MethodName::State) && c.value == "dirty",
        SelectExpression::And(exprs) | SelectExpression::Or(exprs) => {
            exprs.iter().any(selector_contains_dirty)
        }
        SelectExpression::Exclude(inner) => selector_contains_dirty(inner),
    }
}

/// Load incremental state from `target/parse_state/`.
///
/// Returns `None` if:
/// - the file does not exist (cold start)
/// - version or dbt_version mismatches (binary upgrade — fast path, no node I/O)
/// - the file is corrupt
pub fn load_parse_state(io: &IoArgs) -> Option<IncrementalState> {
    load_parse_state_filtered(io, None)
}

/// Like [`load_parse_state`] but only deserialises node payloads for the given
/// `allowed_kinds` set and/or `allowed_unique_ids` set.
///
/// Pass `None` for either filter to skip it.  Both filters are ANDed when both are `Some`.
/// Callers are responsible for ensuring the narrowed set is correct for the command being run.
pub fn load_parse_state_filtered(
    io: &IoArgs,
    allowed_kinds: Option<&HashSet<&'static str>>,
) -> Option<IncrementalState> {
    load_parse_state_filtered_with_unique_ids(io, allowed_kinds, None)
}

/// Like [`load_parse_state_filtered`] but also restricts to specific `unique_id`s.
/// Use [`unique_id_filter_for_selector`] to compute the set from a selector expression.
pub fn load_parse_state_filtered_with_unique_ids(
    io: &IoArgs,
    allowed_kinds: Option<&HashSet<&'static str>>,
    allowed_unique_ids: Option<&HashSet<String>>,
) -> Option<IncrementalState> {
    let loaded = crate::parse_state::load_filtered_with_unique_ids(
        &io.out_dir,
        allowed_kinds,
        allowed_unique_ids,
    )?;

    // Fast staleness check — no node deserialisation yet.
    if loaded.version != INCREMENTAL_STATE_VERSION {
        return None;
    }
    if loaded.dbt_version != current_dbt_version() {
        return None;
    }

    // Deserialise the JSON meta fields back into their typed forms.
    let dbt_profile: DbtProfile = serde_json::from_str(&loaded.dbt_profile_json).ok()?;
    let vars: BTreeMap<String, IndexMap<String, DbtVars>> =
        serde_json::from_str(&loaded.vars_json).unwrap_or_default();
    let env_vars: HashMap<String, String> =
        serde_json::from_str(&loaded.env_vars_json).unwrap_or_default();
    let get_relation_calls: BTreeMap<String, Vec<(String, String, String)>> =
        serde_json::from_str(&loaded.get_relation_calls_json).unwrap_or_default();
    let get_columns_in_relation_calls: BTreeMap<String, Vec<(String, String, String)>> =
        serde_json::from_str(&loaded.get_columns_in_relation_calls_json).unwrap_or_default();
    let patterned_dangling_sources: BTreeMap<String, Vec<RelationPattern>> =
        serde_json::from_str(&loaded.patterned_dangling_sources_json).unwrap_or_default();
    let nodes_with_resolution_errors: HashSet<String> =
        serde_json::from_str(&loaded.nodes_with_resolution_errors_json).unwrap_or_default();
    let nodes_with_access_errors: HashSet<String> =
        serde_json::from_str(&loaded.nodes_with_access_errors_json).unwrap_or_default();
    let operations: Operations = serde_json::from_str(&loaded.operations_json).unwrap_or_default();

    Some(IncrementalState {
        version: loaded.version,
        dbt_version: loaded.dbt_version,
        packages: loaded.packages,
        profile_hash: loaded.profile_hash,
        profile_file_hash: loaded.profile_file_hash,
        project_file_hash: loaded.project_file_hash,
        cli_vars_hash: loaded.cli_vars_hash,
        dbt_profile,
        vars,
        nodes: loaded.nodes,
        disabled_nodes: loaded.disabled_nodes,
        macros: Macros {
            docs_macros: loaded.docs_macros.clone(),
            ..Macros::default()
        },
        operations,
        nodes_with_resolution_errors,
        nodes_with_access_errors,
        env_vars,
        get_relation_calls,
        get_columns_in_relation_calls,
        patterned_dangling_sources,
        any_uses_graph: loaded.any_uses_graph,
        manifest_selectors_json: loaded.selectors_json,
        docs_macros: loaded.docs_macros,
        git_sha: loaded.git_sha,
        git_branch: loaded.git_branch,
        git_is_dirty: loaded.git_is_dirty,
    })
}

/// Returns `true` when every tracked file in the DbtPackage list has an unchanged mtime.
/// Used by the partial-load fast path to skip the expensive WalkDir scan when nothing changed.
pub fn dbt_packages_have_no_file_changes(packages: &[DbtPackage]) -> bool {
    for pkg in packages {
        for (kind, files) in &pkg.all_paths {
            let is_docs_path = *kind == ResourcePathKind::DocsPaths;
            for (dbt_path, saved_time) in files {
                if is_docs_path
                    && !dbt_path
                        .extension()
                        .and_then(|e| e.to_str())
                        .map(|e| e.eq_ignore_ascii_case("md"))
                        .unwrap_or(false)
                {
                    continue;
                }
                let abs = pkg.package_root_path.join(dbt_path.as_path());
                let Ok(meta) = std::fs::metadata(&abs) else {
                    return false;
                };
                let current = meta.modified().unwrap_or(UNIX_EPOCH);
                if current != *saved_time {
                    return false;
                }
            }
        }
    }
    true
}

/// Reconstruct DbtPackage file metadata from a PackageSnapshot.
/// Only populates fields needed by compute_file_changeset: package_root_path,
/// all_paths, and dependencies. Other fields are left at defaults/empty.
pub fn reconstruct_package_metadata(snapshot: &PackageSnapshot) -> FsResult<DbtPackage> {
    let package_root_path = PathBuf::from(&snapshot.package_root_path);

    let all_paths: HashMap<ResourcePathKind, Vec<(DbtPath, SystemTime)>> = snapshot
        .all_paths
        .iter()
        .map(|(kind, paths)| {
            let entries: Vec<(DbtPath, SystemTime)> = paths
                .iter()
                .map(|(path_str, nanos)| {
                    (
                        DbtPath::from_path(Path::new(path_str)),
                        nanos_to_system_time(*nanos),
                    )
                })
                .collect();
            (kind.clone(), entries)
        })
        .collect();

    let minimal_project_json = serde_json::json!({ "name": &snapshot.package_name });
    let dbt_project = serde_json::from_value(minimal_project_json).map_err(|e| {
        fs_err!(
            ErrorCode::Generic,
            "Failed to construct DbtProject stub: {e}"
        )
    })?;

    Ok(DbtPackage {
        dbt_project,
        package_root_path,
        dbt_properties: vec![],
        analysis_files: vec![],
        model_sql_files: vec![],
        function_sql_files: vec![],
        macro_files: vec![],
        test_files: vec![],
        fixture_files: vec![],
        seed_files: vec![],
        docs_files: vec![],
        snapshot_files: vec![],
        inline_file: None,
        dependencies: snapshot.dependencies.clone(),
        all_paths,
        embedded_file_contents: None,
    })
}

#[cfg(test)]
mod tests {
    //! # Partial-parse / partial-load decision table
    //!
    //! Every row below is covered by at least one test in this module.
    //! Columns:
    //!   pp  = --partial-parse enabled
    //!   ll  = --partial-load enabled
    //!   cache  = parquet state on disk (exists / none)
    //!   Δfiles = files changed since last parse
    //!     0  = nothing changed
    //!     M  = model .sql only (incremental-safe)
    //!     Y  = yaml / macro / other (non-incremental)
    //!     D  = file deleted
    //!   selector = --select expression
    //!     none       = no selector
    //!     fqn-1      = single exact FQN match
    //!     fqn-N      = multiple FQN matches
    //!     fqn-match  = FQN that covers the changed file(s)
    //!     no-overlap = FQN that does NOT cover the changed file(s)
    //!     walk       = +model or model+ (graph-walk modifier)
    //!
    //! Outcome column:
    //!   fast-path     = partial-load skips WalkDir entirely (dbt_packages_have_no_file_changes)
    //!   Incremental   = prev compilation reused; only changed nodes re-resolved
    //!   FullParse     = cache discarded; full fresh parse
    //!   NoFilesChanged = no WalkDir result change; prev compilation returned as-is
    //!
    //! Written column (what save_parse_state writes):
    //!   none          = no write (fast-path or NoFilesChanged path; maybe_write_json_and_exit not reached)
    //!   all           = cold write (all nodes, epoch-0 or full-rewrite)
    //!   only-changed  = delta epoch (changed_nodes = Some(set), ≤100 nodes → new epoch file)
    //!
    //! ┌────┬────┬────────┬────────┬────────────┬─────────────────────────┬──────────────┐
    //! │ pp │ ll │ cache  │ Δfiles │ selector   │ outcome                 │ written      │
    //! ├────┼────┼────────┼────────┼────────────┼─────────────────────────┼──────────────┤
    //! │ N  │ -  │ -      │ -      │ -          │ FullParse (no cache)    │ none         │
    //! │ Y  │ N  │ none   │ -      │ -          │ FullParse (cold start)  │ all          │
    //! │ Y  │ N  │ exists │ 0      │ -          │ NoFilesChanged → reuse  │ none         │ ← R5
    //! │ Y  │ N  │ exists │ M      │ -          │ Incremental             │ only-changed │ ← R6
    //! │ Y  │ N  │ exists │ Y      │ -          │ FullParse               │ all          │ ← R7
    //! │ Y  │ N  │ exists │ D      │ -          │ FullParse               │ all          │
    //! │ Y  │ Y  │ none   │ -      │ -          │ FullParse (cold start)  │ all          │
    //! │ Y  │ Y  │ exists │ 0      │ fqn-1      │ fast-path               │ none         │ ← R1
    //! │ Y  │ Y  │ exists │ 0      │ fqn-N      │ fast-path               │ none         │ ← R1
    //! │ Y  │ Y  │ exists │ 0      │ none       │ fast-path               │ none         │ ← R1
    //! │ Y  │ Y  │ exists │ 0      │ walk       │ fast-path               │ none         │ ← R1/R9
    //! │ Y  │ Y  │ exists │ 0      │ fqn-1      │ fast-path               │ none         │ ← R8 (+strict)
    //! │ Y  │ Y  │ exists │ M      │ fqn-match  │ Incremental             │ only-changed │ ← R2
    //! │ Y  │ Y  │ exists │ M      │ no-overlap │ Incremental (M skipped) │ only-changed │ ← R3
    //! │ Y  │ Y  │ exists │ Y      │ -          │ FullParse               │ all          │ ← R4
    //! │ Y  │ Y  │ exists │ D      │ -          │ FullParse               │ all          │
    //! └────┴────┴────────┴────────┴────────────┴─────────────────────────┴──────────────┘
    //!
    //! Key invariants:
    //!   I1  ll=N never takes the fast-path.
    //!   I2  ll=Y, Δfiles=0 → always fast-path (no WalkDir).
    //!   I3  ll=Y, Δfiles=M, selector∩M≠∅ → delta write with changed nodes only.
    //!   I4  ll=Y, Δfiles=M, selector∩M=∅ → delta silently skips M; corrected on next full run.
    //!   I5  Δfiles=Y/D → always FullParse regardless of ll.
    //!   I6  written=all only on cold start or FullParse.
    //!   I7  unique_id_filter includes ALL transitive deps so all_deps_present() always passes.
    //!   I8  --static-analysis strict does not force a full load by itself.

    use super::*;
    use dbt_schemas::schemas::profiles::{DatafusionDbConfig, DbConfig};

    fn test_profile() -> DbtProfile {
        DbtProfile {
            profile: "test".into(),
            target: "dev".into(),
            db_config: DbConfig::Datafusion(Box::new(DatafusionDbConfig {
                database: Some("testdb".into()),
                schema: Some("public".into()),
                execute: None,
            })),
            schema: "public".into(),
            database: "testdb".into(),
            relative_profile_path: PathBuf::from("profiles.yml"),
            threads: None,
        }
    }

    fn test_state() -> IncrementalState {
        let profile = test_profile();
        IncrementalState {
            version: INCREMENTAL_STATE_VERSION,
            dbt_version: current_dbt_version(),
            packages: vec![PackageSnapshot {
                package_root_path: "/tmp/project".into(),
                package_name: "my_project".into(),
                all_paths: HashMap::from([(
                    ResourcePathKind::ModelPaths,
                    vec![
                        ("/tmp/project/models/a.sql".into(), 1_000_000_000u64),
                        ("/tmp/project/models/b.sql".into(), 2_000_000_000u64),
                    ],
                )]),
                dependencies: BTreeSet::from(["dep_a".into()]),
            }],
            profile_hash: profile.blake3_hash(),
            profile_file_hash: hash_file_at_path(Path::new("/nonexistent")),
            project_file_hash: hash_file_at_path(Path::new("/nonexistent")),
            cli_vars_hash: hash_cli_vars(&None),
            dbt_profile: profile,
            vars: Default::default(),
            nodes: Nodes::default(),
            disabled_nodes: Nodes::default(),
            macros: Macros::default(),
            operations: Operations::default(),
            nodes_with_resolution_errors: HashSet::from(["model.bad".into()]),
            nodes_with_access_errors: HashSet::new(),
            env_vars: HashMap::from([
                ("DBT_TARGET".into(), "dev".into()),
                ("DBT_WAREHOUSE".into(), "wh1".into()),
            ]),
            get_relation_calls: BTreeMap::from([(
                "model.my_project.incremental_model".into(),
                vec![("testdb".into(), "pub".into(), "ext_table".into())],
            )]),
            get_columns_in_relation_calls: Default::default(),
            patterned_dangling_sources: Default::default(),
            any_uses_graph: false,
            manifest_selectors_json: String::new(),
            docs_macros: BTreeMap::new(),
            git_sha: String::new(),
            git_branch: String::new(),
            git_is_dirty: false,
        }
    }

    #[test]
    fn save_and_load_round_trip() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };

        let profile = test_profile();
        let packages = vec![DbtPackage {
            dbt_project: serde_json::from_value(serde_json::json!({"name": "my_project"})).unwrap(),
            package_root_path: PathBuf::from("/tmp/project"),
            dbt_properties: vec![],
            analysis_files: vec![],
            model_sql_files: vec![],
            function_sql_files: vec![],
            macro_files: vec![],
            test_files: vec![],
            fixture_files: vec![],
            seed_files: vec![],
            docs_files: vec![],
            snapshot_files: vec![],
            inline_file: None,
            dependencies: BTreeSet::new(),
            all_paths: HashMap::from([(
                ResourcePathKind::ModelPaths,
                vec![(
                    DbtPath::from_path(Path::new("/tmp/project/models/x.sql")),
                    nanos_to_system_time(5_000_000_000),
                )],
            )]),
            embedded_file_contents: None,
        }];

        let dbt_state = DbtState {
            dbt_profile: profile,
            run_started_at: chrono::Utc::now().with_timezone(&chrono_tz::UTC),
            packages,
            vars: Default::default(),
            cli_vars: Default::default(),
            catalogs: None,
            cloud_config: None,
            warn_error: false,
            warn_error_options: Default::default(),
        };

        let resolved_state = ResolverState {
            nodes: Nodes::default(),
            disabled_nodes: Nodes::default(),
            macros: Macros::default(),
            operations: Operations::default(),
            nodes_with_resolution_errors: HashSet::from(["err_node".into()]),
            nodes_with_access_errors: HashSet::new(),
            ..make_minimal_resolver_state(&dbt_state)
        };

        save_parse_state(
            &io,
            &dbt_state,
            &resolved_state,
            &None,
            HashMap::new(),
            None,
        )
        .unwrap();

        let loaded = load_parse_state(&io).expect("should load");
        assert_eq!(loaded.version, INCREMENTAL_STATE_VERSION);
        assert_eq!(loaded.packages.len(), 1);
        assert_eq!(loaded.packages[0].package_name, "my_project");
        assert!(loaded.nodes_with_resolution_errors.contains("err_node"));

        // Reconstruct package metadata and verify paths survive
        let pkg = reconstruct_package_metadata(&loaded.packages[0]).unwrap();
        assert_eq!(pkg.package_root_path, PathBuf::from("/tmp/project"));
        assert_eq!(pkg.all_paths[&ResourcePathKind::ModelPaths].len(), 1);
    }

    #[test]
    fn load_missing_file_returns_none() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        assert!(load_parse_state(&io).is_none());
    }

    #[test]
    fn load_corrupted_file_returns_none() {
        let tmp = tempfile::tempdir().expect("tempdir");
        // Write garbage bytes into project.parquet; the reader will fail gracefully.
        let cache_dir = tmp.path().join(crate::parse_state::CACHE_DIR_NAME);
        std::fs::create_dir_all(&cache_dir).unwrap();
        std::fs::write(
            cache_dir.join("project.parquet"),
            b"not a parquet file at all",
        )
        .unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        assert!(load_parse_state(&io).is_none());
    }

    #[test]
    fn load_version_mismatch_returns_none() {
        use crate::parse_state::{SaveArgs, save};
        let tmp = tempfile::tempdir().expect("tempdir");
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        // Write a cache with a bogus version; load should discard it.
        save(&SaveArgs {
            out_dir: &io.out_dir,
            version: 9999,
            dbt_version: &current_dbt_version(),
            profile_hash: "",
            profile_file_hash: "",
            project_file_hash: "",
            cli_vars_hash: "",
            dbt_profile_json: "{}",
            vars_json: "{}",
            env_vars_json: "{}",
            get_relation_calls_json: "{}",
            get_columns_in_relation_calls_json: "{}",
            patterned_dangling_sources_json: "{}",
            nodes_with_resolution_errors_json: "[]",
            nodes_with_access_errors_json: "[]",
            operations_json: "{\"on_run_start\":[],\"on_run_end\":[]}",
            packages: &[],
            nodes: &Nodes::default(),
            disabled_nodes: &Nodes::default(),
            changed_nodes: None,
            ingested_at: 0,
            macros: &Macros::default(),
            selectors_json: "",
            git_sha: "",
            git_branch: "",
            git_is_dirty: false,
        })
        .expect("save");
        assert!(
            load_parse_state(&io).is_none(),
            "version mismatch should return None"
        );
    }

    #[test]
    fn load_dbt_version_mismatch_returns_none() {
        use crate::parse_state::{SaveArgs, save};
        let tmp = tempfile::tempdir().expect("tempdir");
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        save(&SaveArgs {
            out_dir: &io.out_dir,
            version: INCREMENTAL_STATE_VERSION,
            dbt_version: "old-version-0.0.0",
            profile_hash: "",
            profile_file_hash: "",
            project_file_hash: "",
            cli_vars_hash: "",
            dbt_profile_json: "{}",
            vars_json: "{}",
            env_vars_json: "{}",
            get_relation_calls_json: "{}",
            get_columns_in_relation_calls_json: "{}",
            patterned_dangling_sources_json: "{}",
            nodes_with_resolution_errors_json: "[]",
            nodes_with_access_errors_json: "[]",
            operations_json: "{\"on_run_start\":[],\"on_run_end\":[]}",
            packages: &[],
            nodes: &Nodes::default(),
            disabled_nodes: &Nodes::default(),
            changed_nodes: None,
            ingested_at: 0,
            macros: &Macros::default(),
            selectors_json: "",
            git_sha: "",
            git_branch: "",
            git_is_dirty: false,
        })
        .expect("save");
        assert!(
            load_parse_state(&io).is_none(),
            "build hash mismatch should return None"
        );
    }

    #[test]
    fn timestamp_round_trip() {
        let original = SystemTime::now();
        let nanos = system_time_to_nanos(original);
        let restored = nanos_to_system_time(nanos);
        let diff = original
            .duration_since(restored)
            .or_else(|e| Ok::<_, std::convert::Infallible>(e.duration()))
            .unwrap();
        assert!(diff.as_micros() < 1, "timestamp drift exceeds 1µs");
    }

    #[test]
    fn cli_vars_hash_differs_when_vars_change() {
        let none_hash = hash_cli_vars(&None);
        let empty_hash = hash_cli_vars(&Some(BTreeMap::new()));
        assert_eq!(
            none_hash, empty_hash,
            "None and empty map should produce same hash"
        );

        let mut vars = BTreeMap::new();
        vars.insert(
            "key".into(),
            serde_json::from_str::<dbt_yaml::Value>("\"val1\"").unwrap(),
        );
        let hash_a = hash_cli_vars(&Some(vars.clone()));

        vars.insert(
            "key".into(),
            serde_json::from_str::<dbt_yaml::Value>("\"val2\"").unwrap(),
        );
        let hash_b = hash_cli_vars(&Some(vars));

        assert_ne!(
            hash_a, hash_b,
            "different var values should produce different hashes"
        );
    }

    #[test]
    #[allow(clippy::disallowed_methods)]
    fn env_var_changed_invalidates() {
        let mut state = test_state();
        // Set an env var that matches the saved value
        unsafe { std::env::set_var("__DBT_TEST_ENV_VAR_A", "dev") };
        state.env_vars = HashMap::from([("__DBT_TEST_ENV_VAR_A".into(), "dev".into())]);
        assert!(
            state.validate(&None).is_none(),
            "matching env var should be valid"
        );

        // Change the env var
        unsafe { std::env::set_var("__DBT_TEST_ENV_VAR_A", "prod") };
        assert_eq!(state.validate(&None), Some("env var changed"));

        // Remove the env var
        unsafe { std::env::remove_var("__DBT_TEST_ENV_VAR_A") };
        assert_eq!(state.validate(&None), Some("env var removed"));
    }

    #[test]
    fn file_hash_changes_with_content() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let file = tmp.path().join("test.yml");
        std::fs::write(&file, b"version: 1").unwrap();
        let hash_a = hash_file_at_path(&file);

        std::fs::write(&file, b"version: 2").unwrap();
        let hash_b = hash_file_at_path(&file);

        assert_ne!(
            hash_a, hash_b,
            "different file content should produce different hashes"
        );
    }

    #[test]
    fn file_hash_missing_file_is_deterministic() {
        let h1 = hash_file_at_path(Path::new("/does/not/exist/a"));
        let h2 = hash_file_at_path(Path::new("/does/not/exist/b"));
        assert_eq!(h1, h2, "missing files should hash the same sentinel");
    }

    // ── validate() invalidation tests ────────────────────────────────────────────

    /// Build a minimal `IncrementalState` whose `validate()` will pass given the
    /// real `profiles.yml` and `dbt_project.yml` paths in `project_root`.
    fn state_for_validation(project_root: &Path) -> IncrementalState {
        let profile = test_profile();
        // The profile path embedded in DbtProfile is relative; validate() resolves it
        // against the package root. We keep the fixture simple: relative_profile_path
        // stays "profiles.yml" (the default), so we just write that file.
        let mut state = test_state();
        state.packages[0].package_root_path = project_root.to_str().unwrap().into();
        state.profile_file_hash = hash_file_at_path(&project_root.join("profiles.yml"));
        state.project_file_hash = hash_file_at_path(&project_root.join("dbt_project.yml"));
        state.dbt_profile = profile;
        state.env_vars.clear(); // avoid env var noise in these tests
        state
    }

    #[test]
    fn validate_passes_when_files_match() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("profiles.yml"), b"profile: v1").unwrap();
        std::fs::write(tmp.path().join("dbt_project.yml"), b"project: v1").unwrap();
        let state = state_for_validation(tmp.path());
        assert!(
            state.validate(&None).is_none(),
            "should be valid when hashes match"
        );
    }

    #[test]
    fn validate_invalidates_on_profiles_yml_change() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("profiles.yml"), b"profile: v1").unwrap();
        std::fs::write(tmp.path().join("dbt_project.yml"), b"project: v1").unwrap();
        let state = state_for_validation(tmp.path());
        // Now overwrite profiles.yml with different content
        std::fs::write(tmp.path().join("profiles.yml"), b"profile: v2").unwrap();
        assert_eq!(
            state.validate(&None),
            Some("profiles.yml content changed"),
            "profiles.yml change must invalidate"
        );
    }

    #[test]
    fn validate_invalidates_on_dbt_project_yml_change() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("profiles.yml"), b"profile: v1").unwrap();
        std::fs::write(tmp.path().join("dbt_project.yml"), b"project: v1").unwrap();
        let state = state_for_validation(tmp.path());
        std::fs::write(tmp.path().join("dbt_project.yml"), b"project: v2").unwrap();
        assert_eq!(
            state.validate(&None),
            Some("dbt_project.yml content changed"),
            "dbt_project.yml change must invalidate"
        );
    }

    #[test]
    fn validate_invalidates_on_cli_vars_change() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("profiles.yml"), b"p: v1").unwrap();
        std::fs::write(tmp.path().join("dbt_project.yml"), b"p: v1").unwrap();
        // State was saved with no cli vars
        let state = state_for_validation(tmp.path());
        assert!(state.validate(&None).is_none(), "no vars — should be valid");
        // Now run with a different var
        let mut vars = BTreeMap::new();
        vars.insert(
            "my_var".into(),
            serde_json::from_str::<dbt_yaml::Value>("\"new_value\"").unwrap(),
        );
        assert_eq!(
            state.validate(&Some(vars)),
            Some("--vars changed"),
            "--vars change must invalidate"
        );
    }

    #[test]
    fn needs_full_parse_detects_deletion() {
        let tmp = tempfile::tempdir().unwrap();
        let model_path = tmp.path().join("models").join("a.sql");
        std::fs::create_dir_all(model_path.parent().unwrap()).unwrap();
        std::fs::write(&model_path, b"select 1").unwrap();
        let saved_nanos =
            system_time_to_nanos(std::fs::metadata(&model_path).unwrap().modified().unwrap());

        let mut state = test_state();
        state.packages[0].package_root_path = tmp.path().to_str().unwrap().into();
        state.packages[0].all_paths = HashMap::from([(
            ResourcePathKind::ModelPaths,
            vec![("models/a.sql".into(), saved_nanos)],
        )]);
        assert_eq!(
            state.needs_full_parse(),
            None,
            "file present — incremental ok"
        );

        std::fs::remove_file(&model_path).unwrap();
        assert_eq!(
            state.needs_full_parse(),
            Some("file deleted"),
            "deleted file → FullParse"
        );
    }

    #[test]
    fn needs_full_parse_model_sql_is_incremental() {
        let tmp = tempfile::tempdir().unwrap();
        let model_path = tmp.path().join("models").join("b.sql");
        std::fs::create_dir_all(model_path.parent().unwrap()).unwrap();
        std::fs::write(&model_path, b"select 1").unwrap();
        let old_nanos =
            system_time_to_nanos(std::fs::metadata(&model_path).unwrap().modified().unwrap());

        let mut state = test_state();
        state.packages[0].package_root_path = tmp.path().to_str().unwrap().into();
        state.packages[0].all_paths = HashMap::from([(
            ResourcePathKind::ModelPaths,
            vec![("models/b.sql".into(), old_nanos)],
        )]);

        std::thread::sleep(Duration::from_millis(10));
        std::fs::write(&model_path, b"select 2").unwrap();
        assert_eq!(
            state.needs_full_parse(),
            None,
            "model .sql modified → incremental (None)"
        );
    }

    #[test]
    fn needs_full_parse_new_file_not_detected() {
        // New files are not in all_paths, so needs_full_parse can't see them.
        // They are discovered by WalkDir inside initialize() on the incremental path.
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("models")).unwrap();

        let mut state = test_state();
        state.packages[0].package_root_path = tmp.path().to_str().unwrap().into();
        state.packages[0].all_paths = HashMap::from([(ResourcePathKind::ModelPaths, vec![])]);
        std::fs::write(tmp.path().join("models").join("new.sql"), b"select 1").unwrap();
        assert_eq!(
            state.needs_full_parse(),
            None,
            "new file not in all_paths — not detected, incremental path handles it"
        );
    }

    // ── payload_kinds_for_command / expr_needs_full_payload tests ────────────────

    fn make_eval(command: FsCommand, select: Option<SelectExpression>) -> EvalArgs {
        EvalArgs {
            command,
            select,
            ..EvalArgs::default()
        }
    }

    fn atom(method: MethodName) -> SelectExpression {
        atom_with_value(method, "anything")
    }

    fn atom_with_value(method: MethodName, value: &str) -> SelectExpression {
        use dbt_common::node_selector::SelectionCriteria;
        SelectExpression::Atom(SelectionCriteria::new(
            method,
            vec![],
            value.into(),
            false,
            None,
            None,
            None,
            None,
        ))
    }

    /// All safe methods should NOT force a full payload load.
    #[test]
    fn safe_selector_methods_do_not_need_full_payload() {
        use MethodName::*;
        for method in [Fqn, Path, File, Tag, Package, ResourceType, Source] {
            assert!(
                !expr_needs_full_payload(&atom(method)),
                "{method:?} should be safe for lazy load"
            );
        }
    }

    /// Unsafe methods must force full payload load.
    #[test]
    fn unsafe_selector_methods_need_full_payload() {
        use MethodName::*;
        for method in [
            Config,
            State,
            Result,
            SourceStatus,
            Access,
            Group,
            Exposure,
            Metric,
            SavedQuery,
            SemanticModel,
            TestName,
            TestType,
            UnitTest,
            Version,
        ] {
            assert!(
                expr_needs_full_payload(&atom(method)),
                "{method:?} should require full payload load"
            );
        }
    }

    /// `payload_kinds_for_command` returns `None` (full load) when `write_json` is set.
    #[test]
    fn payload_kinds_write_json_forces_full_load() {
        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = EvalArgs {
            command: FsCommand::Seed,
            write_json: true,
            ..EvalArgs::default()
        };
        assert!(payload_kinds_for_command(&eval, &io).is_none());
    }

    /// `payload_kinds_for_command` returns `None` for `FsCommand::Docs`.
    #[test]
    fn payload_kinds_docs_forces_full_load() {
        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = make_eval(FsCommand::Docs, None);
        assert!(payload_kinds_for_command(&eval, &io).is_none());
    }

    // ── unique_id_filter_for_selector tests ──────────────────────────────────

    /// @x (childrens_parents) → None (conservative fallback).
    #[test]
    fn unique_id_filter_childrens_parents_falls_back() {
        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = EvalArgs {
            command: FsCommand::Run,
            select: Some(SelectExpression::Atom(
                dbt_common::node_selector::SelectionCriteria::new(
                    MethodName::Fqn,
                    vec![],
                    "my_model".into(),
                    true, // childrens_parents = @
                    None,
                    None,
                    None,
                    None,
                ),
            )),
            ..EvalArgs::default()
        };
        assert!(
            unique_id_filter_for_selector(&eval, &io).is_none(),
            "@x should fall back to full load"
        );
    }

    // ── graph-walk BFS tests ─────────────────────────────────────────────────
    //
    // Graph: a → b → c  (a depends on b, b depends on c)
    //   model.p.a  depends_on: [model.p.b]
    //   model.p.b  depends_on: [model.p.c]
    //   model.p.c  depends_on: []
    //
    // +a  (parents of a)   = {a, b, c}
    // a+  (children of a)  = {a}         (nothing depends on a)
    // c+  (children of c)  = {c, b, a}
    // 1+a (1 hop parents)  = {a, b}

    fn write_graph_cache(out_dir: &Path) {
        use crate::parse_state::{SaveArgs, save};
        use dbt_schemas::schemas::DbtModel;
        use dbt_schemas::schemas::common::NodeDependsOn;
        use dbt_schemas::schemas::nodes::{CommonAttributes, NodeBaseAttributes};

        let profile = test_profile();
        let dbt_profile_json = serde_json::to_string(&profile).unwrap();

        let make_model = |uid: &str, name: &str, deps: Vec<String>| DbtModel {
            __common_attr__: CommonAttributes {
                unique_id: uid.into(),
                name: name.into(),
                package_name: "p".into(),
                fqn: vec!["p".into(), name.into()],
                ..CommonAttributes::default()
            },
            __base_attr__: NodeBaseAttributes {
                depends_on: NodeDependsOn {
                    nodes: deps,
                    ..NodeDependsOn::default()
                },
                ..NodeBaseAttributes::default()
            },
            ..DbtModel::default()
        };

        let mut nodes = Nodes::default();
        nodes.models.insert(
            "model.p.a".into(),
            make_model("model.p.a", "a", vec!["model.p.b".into()]).into(),
        );
        nodes.models.insert(
            "model.p.b".into(),
            make_model("model.p.b", "b", vec!["model.p.c".into()]).into(),
        );
        nodes.models.insert(
            "model.p.c".into(),
            make_model("model.p.c", "c", vec![]).into(),
        );

        save(&SaveArgs {
            out_dir,
            version: INCREMENTAL_STATE_VERSION,
            dbt_version: &current_dbt_version(),
            profile_hash: "",
            profile_file_hash: "",
            project_file_hash: "",
            cli_vars_hash: "",
            dbt_profile_json: &dbt_profile_json,
            vars_json: "{}",
            env_vars_json: "{}",
            get_relation_calls_json: "{}",
            get_columns_in_relation_calls_json: "{}",
            patterned_dangling_sources_json: "{}",
            nodes_with_resolution_errors_json: "[]",
            nodes_with_access_errors_json: "[]",
            operations_json: "{\"on_run_start\":[],\"on_run_end\":[]}",
            packages: &[],
            nodes: &nodes,
            disabled_nodes: &Nodes::default(),
            changed_nodes: None,
            ingested_at: 0,
            macros: &Macros::default(),
            selectors_json: "",
            git_sha: "",
            git_branch: "",
            git_is_dirty: false,
        })
        .unwrap();
    }

    fn graph_eval(name: &str, parents_depth: Option<u32>, children_depth: Option<u32>) -> EvalArgs {
        EvalArgs {
            command: FsCommand::Compile,
            select: Some(SelectExpression::Atom(
                dbt_common::node_selector::SelectionCriteria::new(
                    MethodName::Fqn,
                    vec![],
                    name.into(),
                    false,
                    parents_depth,
                    children_depth,
                    None,
                    None,
                ),
            )),
            ..EvalArgs::default()
        }
    }

    #[test]
    fn unique_id_filter_parents_walk() {
        let tmp = tempfile::tempdir().unwrap();
        write_graph_cache(tmp.path());
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };

        // +a → a, b, c (full ancestry)
        let ids = unique_id_filter_for_selector(&graph_eval("a", Some(u32::MAX), None), &io)
            .expect("should return Some");
        assert!(ids.contains("model.p.a"), "a must be included");
        assert!(
            ids.contains("model.p.b"),
            "b (parent of a) must be included"
        );
        assert!(
            ids.contains("model.p.c"),
            "c (grandparent of a) must be included"
        );
    }

    #[test]
    fn unique_id_filter_parents_walk_depth_limited() {
        let tmp = tempfile::tempdir().unwrap();
        write_graph_cache(tmp.path());
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };

        // 1+a → walk gives {a, b}; c added as frontier dep of b (scheduler needs it)
        let ids = unique_id_filter_for_selector(&graph_eval("a", Some(1), None), &io)
            .expect("should return Some");
        assert!(ids.contains("model.p.a"));
        assert!(ids.contains("model.p.b"));
        assert!(
            ids.contains("model.p.c"),
            "c is the frontier dep of b and must be loaded for the scheduler"
        );
    }

    /// Children walk (x+) now works for index-resolvable methods — the old guard was
    /// performance pessimism; all_deps_present() and the ancestor closure make it safe.
    #[test]
    fn unique_id_filter_children_walk_now_works() {
        let tmp = tempfile::tempdir().unwrap();
        write_graph_cache(tmp.path());
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };

        // c+ (children of c) = {c, b, a} — c has no deps, b depends on c, a depends on b.
        let ids = unique_id_filter_for_selector(&graph_eval("c", None, Some(u32::MAX)), &io)
            .expect("c+ should now return Some (children walk enabled)");
        assert!(ids.contains("model.p.c"), "c is the seed");
        assert!(ids.contains("model.p.b"), "b depends on c");
        assert!(ids.contains("model.p.a"), "a depends on b");
    }

    /// Or (space-separated): union of both arms.
    #[test]
    fn unique_id_filter_or_union() {
        let tmp = tempfile::tempdir().unwrap();
        write_graph_cache(tmp.path());
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };

        // "a b" → {a} ∪ {b} (plus their frontier deps)
        let eval = EvalArgs {
            command: FsCommand::Compile,
            select: Some(SelectExpression::Or(vec![
                atom_with_value(MethodName::Fqn, "a"),
                atom_with_value(MethodName::Fqn, "b"),
            ])),
            ..EvalArgs::default()
        };
        let ids = unique_id_filter_for_selector(&eval, &io).expect("Or should return Some");
        assert!(ids.contains("model.p.a"));
        assert!(ids.contains("model.p.b"));
    }

    /// And (comma-separated): intersection of both arms.
    #[test]
    fn unique_id_filter_and_intersection() {
        let tmp = tempfile::tempdir().unwrap();
        write_graph_cache(tmp.path());
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };

        // "tag:nightly,fqn:a" — tag:nightly matches nothing in our cache, so intersection = empty
        // Use resource_type:model,fqn:a → {a,b,c} ∩ {a} = {a} (plus frontier)
        let eval = EvalArgs {
            command: FsCommand::Compile,
            select: Some(SelectExpression::And(vec![
                atom_with_value(MethodName::ResourceType, "model"),
                atom_with_value(MethodName::Fqn, "a"),
            ])),
            ..EvalArgs::default()
        };
        let ids = unique_id_filter_for_selector(&eval, &io).expect("And should return Some");
        assert!(ids.contains("model.p.a"), "a matches both arms");
        // c is a transitive dep of a (a→b→c) and must be loaded so all_deps_present() passes.
        assert!(
            ids.contains("model.p.c"),
            "c is a transitive dep of a and must be included"
        );
    }

    /// And with an unsafe method → falls back.
    #[test]
    fn unique_id_filter_and_with_unsafe_arm_falls_back() {
        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = EvalArgs {
            command: FsCommand::Compile,
            select: Some(SelectExpression::And(vec![
                atom_with_value(MethodName::Fqn, "a"),
                atom(MethodName::Config),
            ])),
            ..EvalArgs::default()
        };
        assert!(
            unique_id_filter_for_selector(&eval, &io).is_none(),
            "And with unsafe arm must fall back"
        );
    }

    /// And with no cache file → None (cache miss).
    #[test]
    fn unique_id_filter_and_no_cache_falls_back() {
        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = EvalArgs {
            command: FsCommand::Run,
            select: Some(SelectExpression::And(vec![
                atom(MethodName::Fqn),
                atom(MethodName::Tag),
            ])),
            ..EvalArgs::default()
        };
        assert!(
            unique_id_filter_for_selector(&eval, &io).is_none(),
            "And selector should fall back to full load"
        );
    }

    // ── indirect selection tests ──────────────────────────────────────────────
    //
    // Graph with a test node attached to model b:
    //   model.p.a  depends_on: [model.p.b]
    //   model.p.b  depends_on: [model.p.c]
    //   model.p.c  depends_on: []
    //   test.p.not_null_b  depends_on: [model.p.b]
    //   test.p.unique_b    depends_on: [model.p.b]

    fn write_graph_cache_with_tests(out_dir: &Path) {
        use crate::parse_state::{SaveArgs, save};
        use dbt_schemas::schemas::common::NodeDependsOn;
        use dbt_schemas::schemas::nodes::{CommonAttributes, NodeBaseAttributes};
        use dbt_schemas::schemas::{DbtModel, DbtTest};

        let profile = test_profile();
        let dbt_profile_json = serde_json::to_string(&profile).unwrap();

        let make_model = |uid: &str, name: &str, deps: Vec<String>| DbtModel {
            __common_attr__: CommonAttributes {
                unique_id: uid.into(),
                name: name.into(),
                package_name: "p".into(),
                fqn: vec!["p".into(), name.into()],
                ..CommonAttributes::default()
            },
            __base_attr__: NodeBaseAttributes {
                depends_on: NodeDependsOn {
                    nodes: deps,
                    ..NodeDependsOn::default()
                },
                ..NodeBaseAttributes::default()
            },
            ..DbtModel::default()
        };

        let make_test = |uid: &str, name: &str, dep: &str| DbtTest {
            __common_attr__: CommonAttributes {
                unique_id: uid.into(),
                name: name.into(),
                package_name: "p".into(),
                fqn: vec!["p".into(), name.into()],
                ..CommonAttributes::default()
            },
            __base_attr__: NodeBaseAttributes {
                depends_on: NodeDependsOn {
                    nodes: vec![dep.into()],
                    ..NodeDependsOn::default()
                },
                ..NodeBaseAttributes::default()
            },
            ..DbtTest::default()
        };

        let mut nodes = Nodes::default();
        nodes.models.insert(
            "model.p.a".into(),
            make_model("model.p.a", "a", vec!["model.p.b".into()]).into(),
        );
        nodes.models.insert(
            "model.p.b".into(),
            make_model("model.p.b", "b", vec!["model.p.c".into()]).into(),
        );
        nodes.models.insert(
            "model.p.c".into(),
            make_model("model.p.c", "c", vec![]).into(),
        );
        nodes.tests.insert(
            "test.p.not_null_b".into(),
            make_test("test.p.not_null_b", "not_null_b", "model.p.b").into(),
        );
        nodes.tests.insert(
            "test.p.unique_b".into(),
            make_test("test.p.unique_b", "unique_b", "model.p.b").into(),
        );

        save(&SaveArgs {
            out_dir,
            version: INCREMENTAL_STATE_VERSION,
            dbt_version: &current_dbt_version(),
            profile_hash: "",
            profile_file_hash: "",
            project_file_hash: "",
            cli_vars_hash: "",
            dbt_profile_json: &dbt_profile_json,
            vars_json: "{}",
            env_vars_json: "{}",
            get_relation_calls_json: "{}",
            get_columns_in_relation_calls_json: "{}",
            patterned_dangling_sources_json: "{}",
            nodes_with_resolution_errors_json: "[]",
            nodes_with_access_errors_json: "[]",
            operations_json: "{\"on_run_start\":[],\"on_run_end\":[]}",
            packages: &[],
            nodes: &nodes,
            disabled_nodes: &Nodes::default(),
            changed_nodes: None,
            ingested_at: 0,
            macros: &Macros::default(),
            selectors_json: "",
            git_sha: "",
            git_branch: "",
            git_is_dirty: false,
        })
        .unwrap();
    }

    /// Eager indirect selection (default): selecting model b includes its attached tests.
    #[test]
    fn unique_id_filter_eager_includes_tests() {
        let tmp = tempfile::tempdir().unwrap();
        write_graph_cache_with_tests(tmp.path());
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };

        // Default indirect_selection = Eager → tests must be included.
        let eval = EvalArgs {
            command: FsCommand::Run,
            select: Some(atom_with_value(MethodName::Fqn, "b")),
            indirect_selection: None, // None → default = Eager
            ..EvalArgs::default()
        };
        let ids = unique_id_filter_for_selector(&eval, &io).expect("should return Some");
        assert!(ids.contains("model.p.b"), "b must be included");
        assert!(
            ids.contains("test.p.not_null_b"),
            "not_null_b test must be included via Eager indirect selection"
        );
        assert!(
            ids.contains("test.p.unique_b"),
            "unique_b test must be included via Eager indirect selection"
        );
    }

    /// Empty indirect selection: selecting model b does NOT include its tests.
    #[test]
    fn unique_id_filter_empty_indirect_excludes_tests() {
        let tmp = tempfile::tempdir().unwrap();
        write_graph_cache_with_tests(tmp.path());
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };

        let eval = EvalArgs {
            command: FsCommand::Run,
            select: Some(atom_with_value(MethodName::Fqn, "b")),
            indirect_selection: Some(IndirectSelection::Empty),
            ..EvalArgs::default()
        };
        let ids = unique_id_filter_for_selector(&eval, &io).expect("should return Some");
        assert!(ids.contains("model.p.b"), "b must be included");
        assert!(
            !ids.contains("test.p.not_null_b"),
            "test must NOT be included with Empty indirect selection"
        );
        assert!(
            !ids.contains("test.p.unique_b"),
            "test must NOT be included with Empty indirect selection"
        );
    }

    /// Wildcard value → None.
    #[test]
    fn unique_id_filter_wildcard_falls_back() {
        use dbt_common::node_selector::SelectionCriteria;
        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = EvalArgs {
            command: FsCommand::Run,
            select: Some(SelectExpression::Atom(SelectionCriteria::new(
                MethodName::Fqn,
                vec![],
                "my_*".into(),
                false,
                None,
                None,
                None,
                None,
            ))),
            ..EvalArgs::default()
        };
        assert!(
            unique_id_filter_for_selector(&eval, &io).is_none(),
            "wildcard value should fall back to full load"
        );
    }

    /// No selector → None (nothing to filter, full load is correct).
    #[test]
    fn unique_id_filter_no_selector_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = EvalArgs {
            command: FsCommand::Run,
            ..EvalArgs::default()
        };
        assert!(unique_id_filter_for_selector(&eval, &io).is_none());
    }

    /// Unsafe selector method → None.
    #[test]
    fn unique_id_filter_unsafe_method_falls_back() {
        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = make_eval(FsCommand::Run, Some(atom(MethodName::Config)));
        assert!(
            unique_id_filter_for_selector(&eval, &io).is_none(),
            "config: selector should fall back to full load"
        );
    }

    /// `payload_kinds_for_command` returns `None` when `defer` is set.
    #[test]
    fn payload_kinds_defer_forces_full_load() {
        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = EvalArgs {
            command: FsCommand::Seed,
            defer: true,
            ..EvalArgs::default()
        };
        assert!(payload_kinds_for_command(&eval, &io).is_none());
    }

    /// Safe selector on `dbt seed` → narrow kinds (lazy load).
    #[test]
    fn payload_kinds_seed_safe_selector_is_lazy() {
        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = make_eval(FsCommand::Seed, Some(atom(MethodName::Tag)));
        let kinds = payload_kinds_for_command(&eval, &io);
        let kinds = kinds.expect("seed with tag: selector should be lazy");
        assert!(kinds.contains("seed"), "seed kind must be included");
        assert!(
            kinds.contains("macro"),
            "macro kind must always be included"
        );
        assert!(
            !kinds.contains("model"),
            "model kind must not be included for seed"
        );
    }

    /// Safe selector on `dbt snapshot` → narrow kinds (lazy load).
    #[test]
    fn payload_kinds_snapshot_safe_selector_is_lazy() {
        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = make_eval(FsCommand::Snapshot, Some(atom(MethodName::Fqn)));
        let kinds = payload_kinds_for_command(&eval, &io);
        let kinds = kinds.expect("snapshot with fqn: selector should be lazy");
        assert!(kinds.contains("snapshot"), "snapshot kind must be included");
        assert!(
            !kinds.contains("seed"),
            "seed kind must not be included for snapshot"
        );
    }

    /// Safe selector on `dbt show` → narrow kinds (model/test/analysis only, no seed/snapshot).
    #[test]
    fn payload_kinds_show_safe_selector_is_lazy() {
        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = make_eval(FsCommand::Show, Some(atom(MethodName::Fqn)));
        let kinds = payload_kinds_for_command(&eval, &io);
        let kinds = kinds.expect("show with fqn: selector should be lazy");
        assert!(
            kinds.contains("model"),
            "model kind must be included for show"
        );
        assert!(
            kinds.contains("macro"),
            "macro kind must always be included"
        );
        assert!(
            !kinds.contains("seed"),
            "seed kind must not be included for show"
        );
        assert!(
            !kinds.contains("snapshot"),
            "snapshot kind must not be included for show"
        );
    }

    /// Unsafe selector on `dbt seed` forces full load.
    #[test]
    fn payload_kinds_seed_unsafe_selector_forces_full_load() {
        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = make_eval(FsCommand::Seed, Some(atom(MethodName::Config)));
        assert!(
            payload_kinds_for_command(&eval, &io).is_none(),
            "config: selector must force full load"
        );
    }

    /// Nested And with a safe + unsafe method → full load.
    #[test]
    fn payload_kinds_and_with_unsafe_forces_full_load() {
        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let expr = SelectExpression::And(vec![atom(MethodName::Tag), atom(MethodName::State)]);
        let eval = make_eval(FsCommand::Seed, Some(expr));
        assert!(
            payload_kinds_for_command(&eval, &io).is_none(),
            "And with state: must force full load"
        );
    }

    /// Differential: same seed tag selector selects same node set whether lazy or full load.
    ///
    /// This test verifies that `load_parse_state_filtered` with a narrow kinds set
    /// returns the exact same seed nodes as a full load would, i.e. no false negatives.
    #[test]
    fn differential_seed_lazy_vs_full_same_nodes() {
        use crate::parse_state::{SaveArgs, save};
        use dbt_schemas::schemas::{
            DbtSeed,
            nodes::{CommonAttributes, NodeBaseAttributes},
        };

        let tmp = tempfile::tempdir().unwrap();
        let out_dir = tmp.path().to_path_buf();

        let profile = test_profile();
        let dbt_profile_json = serde_json::to_string(&profile).unwrap();

        // Build a Nodes set with one seed.
        let mut nodes = Nodes::default();
        let seed = DbtSeed {
            __common_attr__: CommonAttributes {
                unique_id: "seed.proj.my_seed".into(),
                name: "my_seed".into(),
                package_name: "proj".into(),
                ..CommonAttributes::default()
            },
            __base_attr__: NodeBaseAttributes::default(),
            ..DbtSeed::default()
        };
        nodes.seeds.insert("seed.proj.my_seed".into(), seed.into());

        save(&SaveArgs {
            out_dir: &out_dir,
            version: INCREMENTAL_STATE_VERSION,
            dbt_version: &current_dbt_version(),
            profile_hash: "",
            profile_file_hash: "",
            project_file_hash: "",
            cli_vars_hash: "",
            dbt_profile_json: &dbt_profile_json,
            vars_json: "{}",
            env_vars_json: "{}",
            get_relation_calls_json: "{}",
            get_columns_in_relation_calls_json: "{}",
            patterned_dangling_sources_json: "{}",
            nodes_with_resolution_errors_json: "[]",
            nodes_with_access_errors_json: "[]",
            operations_json: "{\"on_run_start\":[],\"on_run_end\":[]}",
            packages: &[],
            nodes: &nodes,
            disabled_nodes: &Nodes::default(),
            changed_nodes: None,
            ingested_at: 0,
            macros: &Macros::default(),
            selectors_json: "",
            git_sha: "",
            git_branch: "",
            git_is_dirty: false,
        })
        .unwrap();

        let io = IoArgs {
            out_dir,
            ..IoArgs::default()
        };

        // Full load
        let full = load_parse_state_filtered(&io, None).expect("full load");
        // Lazy load — only seed kind
        let seed_kinds: HashSet<&'static str> =
            ["seed", "macro", "source"].iter().copied().collect();
        let lazy = load_parse_state_filtered(&io, Some(&seed_kinds)).expect("lazy load");

        // Seeds present in both
        let full_seeds: BTreeSet<String> = full.nodes.seeds.keys().cloned().collect();
        let lazy_seeds: BTreeSet<String> = lazy.nodes.seeds.keys().cloned().collect();
        assert_eq!(
            full_seeds, lazy_seeds,
            "lazy load must return the same seeds as full load"
        );
    }

    // ── CRUD × extension × hash-vs-mtime matrix ──────────────────────────────
    //
    // Two tracking mechanisms:
    //   (A) Content hash  — profiles.yml, dbt_project.yml, cli_vars
    //       blake3(file bytes) stored in `meta` table.  Deterministic: same
    //       bytes → same hash regardless of mtime or inode.
    //   (B) mtime (nanoseconds) — every file in `all_paths`
    //       Stored in `filestamps` table.  Deterministic within a run; a
    //       write always advances the clock on sane filesystems.
    //
    // File kinds tracked in all_paths and their extensions:
    //   ModelPaths    : .sql  .py  .yml/.yaml
    //   MacroPaths    : .sql       .yml/.yaml
    //   TestPaths     : .sql       .yml/.yaml
    //   SeedPaths     : .csv  .parquet  .json
    //   SnapshotPaths : .sql       .yml/.yaml
    //   AnalysisPaths : .sql       .yml/.yaml
    //   DocsPaths     : .md   (non-.md changes in DocsPaths are silently ignored)
    //   FixturePaths  : .csv  .sql
    //   FunctionPaths : .sql  .py  .yml/.yaml
    //
    // CRUD outcomes for needs_full_parse():
    //   Create  → None — new file is NOT in all_paths; WalkDir in initialize() discovers it
    //   Read    — no-op
    //   Update  → None for model/analysis .sql (incremental)
    //           → Some("non-incremental file changed") for everything else
    //   Delete  → Some("file deleted")
    //
    // env_vars: stored as JSON string in meta; validated key-by-key at load
    //           time (not hashed); any changed or removed key invalidates.

    // ── helpers for the matrix tests ─────────────────────────────────────────

    fn write_file(path: &Path, content: &[u8]) -> SystemTime {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, content).unwrap();
        std::fs::metadata(path).unwrap().modified().unwrap()
    }

    // Build a PackageSnapshot where `all_paths` contains exactly one file at
    // `rel_path` (relative to `root`) with the given saved mtime.
    // dir_mtimes is left empty — suitable for tests that don't need new-file detection.
    fn pkg_with_file(
        root: &Path,
        kind: ResourcePathKind,
        rel_path: &str,
        saved_mtime: SystemTime,
    ) -> PackageSnapshot {
        PackageSnapshot {
            package_root_path: root.to_str().unwrap().into(),
            package_name: "proj".into(),
            all_paths: HashMap::from([(
                kind,
                vec![(rel_path.into(), system_time_to_nanos(saved_mtime))],
            )]),
            dependencies: BTreeSet::new(),
        }
    }

    fn base_state_with_pkg(pkg: PackageSnapshot) -> IncrementalState {
        let mut s = test_state();
        s.packages = vec![pkg];
        s.env_vars.clear();
        s
    }

    // Sleep just long enough that a subsequent write changes the mtime.
    // 10 ms is enough on Linux (mtime resolution is 1 ns on ext4/tmpfs).
    fn sleep_for_mtime_change() {
        std::thread::sleep(Duration::from_millis(10));
    }

    // ── (B) mtime: CRUD × extension ──────────────────────────────────────────

    // Helper: run needs_full_parse after doing `op` to the file.
    // Returns the result from needs_full_parse.
    fn mtime_check_after<F>(kind: ResourcePathKind, filename: &str, op: F) -> Option<&'static str>
    where
        F: FnOnce(&Path),
    {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join(filename);
        let mtime = write_file(&file, b"v1");
        let pkg = pkg_with_file(tmp.path(), kind, filename, mtime);
        let state = base_state_with_pkg(pkg);
        sleep_for_mtime_change();
        op(&file);
        state.needs_full_parse()
    }

    // --- ModelPaths ---

    #[test]
    fn mtime_model_sql_unchanged() {
        let r = mtime_check_after(ResourcePathKind::ModelPaths, "m.sql", |_| {});
        assert_eq!(r, None, "no change → incremental");
    }

    #[test]
    fn mtime_model_sql_updated() {
        let r = mtime_check_after(ResourcePathKind::ModelPaths, "m.sql", |f| {
            std::fs::write(f, b"v2").unwrap();
        });
        assert_eq!(r, None, "model .sql → incremental");
    }

    #[test]
    fn mtime_model_yml_updated() {
        let r = mtime_check_after(ResourcePathKind::ModelPaths, "schema.yml", |f| {
            std::fs::write(f, b"v2").unwrap();
        });
        assert_eq!(
            r,
            Some("non-incremental file changed"),
            "model .yml → FullParse"
        );
    }

    #[test]
    fn mtime_model_sql_deleted() {
        let r = mtime_check_after(ResourcePathKind::ModelPaths, "m.sql", |f| {
            std::fs::remove_file(f).unwrap();
        });
        assert_eq!(r, Some("file deleted"), "model .sql delete → FullParse");
    }

    #[test]
    fn mtime_model_yml_deleted() {
        let r = mtime_check_after(ResourcePathKind::ModelPaths, "schema.yml", |f| {
            std::fs::remove_file(f).unwrap();
        });
        assert_eq!(r, Some("file deleted"), "model .yml delete → FullParse");
    }

    // --- MacroPaths ---

    #[test]
    fn mtime_macro_sql_updated() {
        let r = mtime_check_after(ResourcePathKind::MacroPaths, "my_macro.sql", |f| {
            std::fs::write(f, b"v2").unwrap();
        });
        assert_eq!(
            r,
            Some("non-incremental file changed"),
            "macro .sql → FullParse"
        );
    }

    #[test]
    fn mtime_macro_yml_updated() {
        let r = mtime_check_after(ResourcePathKind::MacroPaths, "macros.yml", |f| {
            std::fs::write(f, b"v2").unwrap();
        });
        assert_eq!(
            r,
            Some("non-incremental file changed"),
            "macro .yml → FullParse"
        );
    }

    #[test]
    fn mtime_macro_sql_deleted() {
        let r = mtime_check_after(ResourcePathKind::MacroPaths, "my_macro.sql", |f| {
            std::fs::remove_file(f).unwrap();
        });
        assert_eq!(r, Some("file deleted"));
    }

    // --- TestPaths ---

    #[test]
    fn mtime_test_sql_updated() {
        let r = mtime_check_after(ResourcePathKind::TestPaths, "test.sql", |f| {
            std::fs::write(f, b"v2").unwrap();
        });
        assert_eq!(
            r,
            Some("non-incremental file changed"),
            "test .sql → FullParse"
        );
    }

    #[test]
    fn mtime_test_yml_updated() {
        let r = mtime_check_after(ResourcePathKind::TestPaths, "schema.yml", |f| {
            std::fs::write(f, b"v2").unwrap();
        });
        assert_eq!(
            r,
            Some("non-incremental file changed"),
            "test .yml → FullParse"
        );
    }

    // --- SeedPaths ---

    #[test]
    fn mtime_seed_csv_updated() {
        let r = mtime_check_after(ResourcePathKind::SeedPaths, "data.csv", |f| {
            std::fs::write(f, b"id\n2").unwrap();
        });
        assert_eq!(
            r,
            Some("non-incremental file changed"),
            "seed .csv → FullParse"
        );
    }

    #[test]
    fn mtime_seed_csv_deleted() {
        let r = mtime_check_after(ResourcePathKind::SeedPaths, "data.csv", |f| {
            std::fs::remove_file(f).unwrap();
        });
        assert_eq!(r, Some("file deleted"));
    }

    // --- SnapshotPaths ---

    #[test]
    fn mtime_snapshot_sql_updated() {
        let r = mtime_check_after(ResourcePathKind::SnapshotPaths, "snap.sql", |f| {
            std::fs::write(f, b"v2").unwrap();
        });
        assert_eq!(
            r,
            Some("non-incremental file changed"),
            "snapshot .sql → FullParse"
        );
    }

    #[test]
    fn mtime_snapshot_yml_updated() {
        let r = mtime_check_after(ResourcePathKind::SnapshotPaths, "snap.yml", |f| {
            std::fs::write(f, b"v2").unwrap();
        });
        assert_eq!(
            r,
            Some("non-incremental file changed"),
            "snapshot .yml → FullParse"
        );
    }

    // --- DocsPaths: .md changes are also FullParse (compute_file_changeset silently
    // drops .md changes so they'd never take effect under incremental) ---

    #[test]
    fn mtime_docs_md_updated() {
        let r = mtime_check_after(ResourcePathKind::DocsPaths, "docs.md", |f| {
            std::fs::write(f, b"# v2").unwrap();
        });
        assert_eq!(
            r,
            Some("non-incremental file changed"),
            "docs .md → FullParse"
        );
    }

    #[test]
    fn mtime_docs_non_md_updated() {
        let r = mtime_check_after(ResourcePathKind::DocsPaths, "readme.txt", |f| {
            std::fs::write(f, b"v2").unwrap();
        });
        // Non-.md files under DocsPaths are ignored — matches compute_file_changeset behavior.
        // Only .md changes in doc directories require a full re-parse.
        assert_eq!(r, None, "docs non-.md → skipped (not a parse trigger)");
    }

    // --- AnalysisPaths ---

    #[test]
    fn mtime_analysis_sql_updated() {
        let r = mtime_check_after(ResourcePathKind::AnalysisPaths, "analysis.sql", |f| {
            std::fs::write(f, b"v2").unwrap();
        });
        assert_eq!(r, None, "analysis .sql → incremental");
    }

    #[test]
    fn mtime_analysis_yml_updated_forces_full_parse() {
        let r = mtime_check_after(ResourcePathKind::AnalysisPaths, "schema.yml", |f| {
            std::fs::write(f, b"v2").unwrap();
        });
        assert_eq!(
            r,
            Some("non-incremental file changed"),
            "analysis .yml → FullParse"
        );
    }

    // --- FunctionPaths ---

    #[test]
    fn mtime_function_sql_updated() {
        let r = mtime_check_after(ResourcePathKind::FunctionPaths, "fn.sql", |f| {
            std::fs::write(f, b"v2").unwrap();
        });
        assert_eq!(
            r,
            Some("non-incremental file changed"),
            "function .sql → FullParse"
        );
    }

    #[test]
    fn mtime_function_yml_updated() {
        let r = mtime_check_after(ResourcePathKind::FunctionPaths, "fn.yml", |f| {
            std::fs::write(f, b"v2").unwrap();
        });
        assert_eq!(
            r,
            Some("non-incremental file changed"),
            "function .yml → FullParse"
        );
    }

    // ── (A) content hash: profiles.yml, dbt_project.yml ──────────────────────

    #[test]
    fn hash_profiles_yml_crud() {
        let tmp = tempfile::tempdir().unwrap();
        let profiles = tmp.path().join("profiles.yml");
        let project = tmp.path().join("dbt_project.yml");
        std::fs::write(&profiles, b"profile: v1").unwrap();
        std::fs::write(&project, b"name: proj").unwrap();
        let state = state_for_validation(tmp.path());

        // Read — still valid
        assert!(state.validate(&None).is_none(), "read: still valid");

        // Update profiles.yml
        std::fs::write(&profiles, b"profile: v2").unwrap();
        assert_eq!(
            state.validate(&None),
            Some("profiles.yml content changed"),
            "update"
        );

        // Delete profiles.yml — hash of missing file ≠ hash of old content
        std::fs::remove_file(&profiles).unwrap();
        assert_eq!(
            state.validate(&None),
            Some("profiles.yml content changed"),
            "delete: missing file hash differs from saved hash"
        );

        // Recreate with original content — should be valid again
        std::fs::write(&profiles, b"profile: v1").unwrap();
        assert!(
            state.validate(&None).is_none(),
            "recreate with same content: valid"
        );
    }

    #[test]
    fn hash_dbt_project_yml_crud() {
        let tmp = tempfile::tempdir().unwrap();
        let profiles = tmp.path().join("profiles.yml");
        let project = tmp.path().join("dbt_project.yml");
        std::fs::write(&profiles, b"profile: v1").unwrap();
        std::fs::write(&project, b"name: proj_v1").unwrap();
        let state = state_for_validation(tmp.path());

        assert!(state.validate(&None).is_none(), "baseline: valid");

        std::fs::write(&project, b"name: proj_v2").unwrap();
        assert_eq!(
            state.validate(&None),
            Some("dbt_project.yml content changed"),
            "update"
        );

        std::fs::remove_file(&project).unwrap();
        assert_eq!(
            state.validate(&None),
            Some("dbt_project.yml content changed"),
            "delete: missing file hash differs"
        );
    }

    // Hash determinism: same bytes → same hash, regardless of when/where written.
    #[test]
    fn hash_is_deterministic_same_content() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("a.yml");
        let b = tmp.path().join("b.yml");
        let content = b"profile: test\ntarget: dev";
        std::fs::write(&a, content).unwrap();
        sleep_for_mtime_change();
        std::fs::write(&b, content).unwrap(); // different mtime, same bytes
        assert_eq!(
            hash_file_at_path(&a),
            hash_file_at_path(&b),
            "same content → same hash regardless of mtime"
        );
    }

    #[test]
    fn hash_differs_for_different_content() {
        let tmp = tempfile::tempdir().unwrap();
        let f = tmp.path().join("f.yml");
        std::fs::write(&f, b"v1").unwrap();
        let h1 = hash_file_at_path(&f);
        std::fs::write(&f, b"v2").unwrap();
        let h2 = hash_file_at_path(&f);
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_missing_file_is_stable_sentinel() {
        // Missing file always hashes to the same sentinel — two missing paths
        // hash equal, and the sentinel differs from any real file content.
        let h_missing = hash_file_at_path(Path::new("/no/such/file/ever"));
        let h_missing2 = hash_file_at_path(Path::new("/also/does/not/exist"));
        assert_eq!(
            h_missing, h_missing2,
            "all missing files hash to the same sentinel"
        );

        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real.yml");
        std::fs::write(&real, b"content").unwrap();
        let h_real = hash_file_at_path(&real);
        assert_ne!(
            h_missing, h_real,
            "sentinel differs from any real-content hash"
        );
    }

    // ── cli_vars hash ─────────────────────────────────────────────────────────

    #[test]
    fn cli_vars_hash_is_deterministic() {
        // BTreeMap iteration order is sorted → serde_json output is deterministic.
        let mut vars = BTreeMap::new();
        vars.insert(
            "b".into(),
            serde_json::from_str::<dbt_yaml::Value>("\"two\"").unwrap(),
        );
        vars.insert(
            "a".into(),
            serde_json::from_str::<dbt_yaml::Value>("\"one\"").unwrap(),
        );
        // Build a second BTreeMap with the same entries (insertion order differs).
        let mut vars2 = BTreeMap::new();
        vars2.insert(
            "a".into(),
            serde_json::from_str::<dbt_yaml::Value>("\"one\"").unwrap(),
        );
        vars2.insert(
            "b".into(),
            serde_json::from_str::<dbt_yaml::Value>("\"two\"").unwrap(),
        );
        assert_eq!(
            hash_cli_vars(&Some(vars)),
            hash_cli_vars(&Some(vars2)),
            "BTreeMap serialises in sorted order → hash is insertion-order-independent"
        );
    }

    #[test]
    fn cli_vars_none_and_empty_are_same_hash() {
        assert_eq!(hash_cli_vars(&None), hash_cli_vars(&Some(BTreeMap::new())));
    }

    #[test]
    fn cli_vars_changed_invalidates() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("profiles.yml"), b"p").unwrap();
        std::fs::write(tmp.path().join("dbt_project.yml"), b"p").unwrap();
        let state = state_for_validation(tmp.path());

        let mut vars = BTreeMap::new();
        vars.insert(
            "k".into(),
            serde_json::from_str::<dbt_yaml::Value>("\"v2\"").unwrap(),
        );
        assert_eq!(state.validate(&Some(vars)), Some("--vars changed"));
    }

    // ── env_vars (stored as value snapshot, not hashed) ───────────────────────

    #[test]
    #[allow(clippy::disallowed_methods)]
    fn env_vars_crud() {
        // Create: env var now present that wasn't observed at parse time → no
        // invalidation (we only check vars that were OBSERVED during parse).
        let mut state = test_state();
        state.env_vars = HashMap::from([("__DBT_MATRIX_VAR__".into(), "original".into())]);
        unsafe { std::env::set_var("__DBT_MATRIX_VAR__", "original") };

        // Read — no change
        assert!(state.validate(&None).is_none(), "env var unchanged → valid");

        // Update — changed value
        unsafe { std::env::set_var("__DBT_MATRIX_VAR__", "changed") };
        assert_eq!(
            state.validate(&None),
            Some("env var changed"),
            "update → invalid"
        );

        // Delete — var removed
        unsafe { std::env::remove_var("__DBT_MATRIX_VAR__") };
        assert_eq!(
            state.validate(&None),
            Some("env var removed"),
            "delete → invalid"
        );

        // A NEW env var (not in state.env_vars) is not checked — we only
        // validate vars that were observed during the original parse.
        let mut state_no_vars = test_state();
        state_no_vars.env_vars.clear();
        unsafe { std::env::set_var("__DBT_MATRIX_NEW_VAR__", "whatever") };
        assert!(
            state_no_vars.validate(&None).is_none(),
            "new env var not observed at parse time → not checked"
        );
        unsafe { std::env::remove_var("__DBT_MATRIX_NEW_VAR__") };
    }

    #[test]
    fn all_deps_present_catches_missing_dep() {
        use dbt_schemas::schemas::DbtModel;
        use dbt_schemas::schemas::common::NodeDependsOn;
        use dbt_schemas::schemas::nodes::{CommonAttributes, NodeBaseAttributes};

        let make_model = |uid: &str, deps: Vec<String>| DbtModel {
            __common_attr__: CommonAttributes {
                unique_id: uid.into(),
                name: uid.into(),
                package_name: "p".into(),
                ..CommonAttributes::default()
            },
            __base_attr__: NodeBaseAttributes {
                depends_on: NodeDependsOn {
                    nodes: deps,
                    ..NodeDependsOn::default()
                },
                ..NodeBaseAttributes::default()
            },
            ..DbtModel::default()
        };

        // Complete set: a→b, b→c, c has no deps — all present → None.
        let mut state = test_state();
        state.nodes.models.insert(
            "model.p.a".into(),
            make_model("model.p.a", vec!["model.p.b".into()]).into(),
        );
        state.nodes.models.insert(
            "model.p.b".into(),
            make_model("model.p.b", vec!["model.p.c".into()]).into(),
        );
        state
            .nodes
            .models
            .insert("model.p.c".into(), make_model("model.p.c", vec![]).into());
        assert_eq!(state.all_deps_present(), None, "complete set passes");

        // Partial set: a→b loaded, but b is missing → Some(...).
        let mut state2 = test_state();
        state2.nodes.models.insert(
            "model.p.a".into(),
            make_model("model.p.a", vec!["model.p.b".into()]).into(),
        );
        assert_eq!(
            state2.all_deps_present(),
            Some("dependency not in loaded set"),
            "missing dep detected"
        );
    }

    // ── Decision-table coverage ───────────────────────────────────────────────
    //
    // The table below maps each row of the partial-parse / partial-load decision
    // table to the test(s) that assert it.  Columns: pp=--partial-parse,
    // pl=--partial-load, cache, Δfiles, selector → outcome, written.
    //
    // Row  pp  pl  cache   Δfiles  selector    outcome / written
    // ──────────────────────────────────────────────────────────
    // R1   Y   Y   exists  0       fqn-1       fast-path / none
    // R2   Y   Y   exists  M       fqn-matches Incremental / only-changed
    // R3   Y   Y   exists  M       no-overlap  Incremental, changed node absent → delta skips it
    // R4   Y   Y   exists  Y       -           FullParse / all
    // R5   Y   N   exists  0       -           NoFilesChanged reuse / none
    // R6   Y   N   exists  M       -           Incremental / only-changed
    // R7   Y   N   exists  Y       -           FullParse / all
    // R8   Y   Y   exists  0       static-analysis-strict  fast-path / none
    // R9   Y   Y   exists  0       +model (walk)  full-load from parquet (all_deps walk)

    // R1 — partial-load 0-file fast path: dbt_packages_have_no_file_changes returns true
    // when every tracked file has an unchanged mtime.
    #[test]
    fn ll_fastpath_no_file_changes_true_when_all_match() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("models").join("m.sql");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"select 1").unwrap();
        let saved_mtime = std::fs::metadata(&file).unwrap().modified().unwrap();

        let pkg = DbtPackage {
            dbt_project: serde_json::from_value(serde_json::json!({"name": "p"})).unwrap(),
            package_root_path: tmp.path().to_path_buf(),
            all_paths: HashMap::from([(
                ResourcePathKind::ModelPaths,
                vec![(DbtPath::from_path(Path::new("models/m.sql")), saved_mtime)],
            )]),
            dbt_properties: vec![],
            analysis_files: vec![],
            model_sql_files: vec![],
            function_sql_files: vec![],
            macro_files: vec![],
            test_files: vec![],
            fixture_files: vec![],
            seed_files: vec![],
            docs_files: vec![],
            snapshot_files: vec![],
            inline_file: None,
            dependencies: Default::default(),
            embedded_file_contents: None,
        };

        assert!(
            dbt_packages_have_no_file_changes(&[pkg]),
            "unchanged file → fast-path should trigger"
        );
    }

    // R1 — fast-path returns false when a model file has been touched.
    #[test]
    fn ll_fastpath_no_file_changes_false_when_file_touched() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("models").join("m.sql");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"select 1").unwrap();
        let old_mtime = std::fs::metadata(&file).unwrap().modified().unwrap();

        let pkg = DbtPackage {
            dbt_project: serde_json::from_value(serde_json::json!({"name": "p"})).unwrap(),
            package_root_path: tmp.path().to_path_buf(),
            all_paths: HashMap::from([(
                ResourcePathKind::ModelPaths,
                vec![(DbtPath::from_path(Path::new("models/m.sql")), old_mtime)],
            )]),
            dbt_properties: vec![],
            analysis_files: vec![],
            model_sql_files: vec![],
            function_sql_files: vec![],
            macro_files: vec![],
            test_files: vec![],
            fixture_files: vec![],
            seed_files: vec![],
            docs_files: vec![],
            snapshot_files: vec![],
            inline_file: None,
            dependencies: Default::default(),
            embedded_file_contents: None,
        };

        sleep_for_mtime_change();
        std::fs::write(&file, b"select 2").unwrap();

        assert!(
            !dbt_packages_have_no_file_changes(&[pkg]),
            "touched file → fast-path must NOT trigger"
        );
    }

    // R1 — has_no_file_changes on IncrementalState (ParseState path).
    #[test]
    fn incremental_state_has_no_file_changes_true_when_all_match() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("m.sql");
        let mtime = write_file(&file, b"v1");
        let pkg = pkg_with_file(tmp.path(), ResourcePathKind::ModelPaths, "m.sql", mtime);
        let state = base_state_with_pkg(pkg);
        assert!(state.has_no_file_changes(), "unchanged → true");
    }

    #[test]
    fn incremental_state_has_no_file_changes_false_when_file_touched() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("m.sql");
        let mtime = write_file(&file, b"v1");
        let pkg = pkg_with_file(tmp.path(), ResourcePathKind::ModelPaths, "m.sql", mtime);
        let state = base_state_with_pkg(pkg);
        sleep_for_mtime_change();
        std::fs::write(&file, b"v2").unwrap();
        assert!(!state.has_no_file_changes(), "touched file → false");
    }

    // R2/R3 — transitive dep walk: selecting `fqn:a` in graph a→b→c must load all three.
    // This ensures all_deps_present() passes and the partial-load path is taken rather
    // than falling back to FullParse.
    #[test]
    fn unique_id_filter_fqn_includes_transitive_deps() {
        let tmp = tempfile::tempdir().unwrap();
        write_graph_cache(tmp.path());
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };

        // Bare `fqn:a` — no graph walk.  Old code yielded {a,b}; new code must yield {a,b,c}.
        let eval = EvalArgs {
            command: FsCommand::Compile,
            select: Some(atom_with_value(MethodName::Fqn, "a")),
            ..EvalArgs::default()
        };
        let ids =
            unique_id_filter_for_selector(&eval, &io).expect("bare fqn selector should resolve");
        assert!(ids.contains("model.p.a"), "a must be in set");
        assert!(
            ids.contains("model.p.b"),
            "b (direct dep of a) must be in set"
        );
        assert!(
            ids.contains("model.p.c"),
            "c (transitive dep a→b→c) must be in set so all_deps_present() passes"
        );
    }

    // R3 — delta write skips nodes not in resolved_state (changed-but-unselected).
    // Verifies that collect_delta_rows silently skips IDs absent from nodes,
    // leaving the old epoch-0 row in place (stale but corrected on next full run).
    #[test]
    fn delta_write_skips_node_not_in_resolved_set() {
        use crate::parse_state::{SaveArgs, load_filtered_with_unique_ids, save};

        let tmp = tempfile::tempdir().unwrap();
        let profile = test_profile();
        let dbt_profile_json = serde_json::to_string(&profile).unwrap();

        // Cold write: a and b both present.
        let mut nodes = Nodes::default();
        let make_model = |uid: &str| {
            use dbt_schemas::schemas::DbtModel;
            use dbt_schemas::schemas::nodes::CommonAttributes;
            std::sync::Arc::new(DbtModel {
                __common_attr__: CommonAttributes {
                    unique_id: uid.into(),
                    name: uid.split('.').next_back().unwrap_or(uid).into(),
                    package_name: "p".into(),
                    ..CommonAttributes::default()
                },
                ..DbtModel::default()
            })
        };
        nodes
            .models
            .insert("model.p.a".into(), make_model("model.p.a"));
        nodes
            .models
            .insert("model.p.b".into(), make_model("model.p.b"));

        save(&SaveArgs {
            out_dir: tmp.path(),
            version: INCREMENTAL_STATE_VERSION,
            dbt_version: &current_dbt_version(),
            profile_hash: "",
            profile_file_hash: "",
            project_file_hash: "",
            cli_vars_hash: "",
            dbt_profile_json: &dbt_profile_json,
            vars_json: "{}",
            env_vars_json: "{}",
            get_relation_calls_json: "{}",
            get_columns_in_relation_calls_json: "{}",
            patterned_dangling_sources_json: "{}",
            nodes_with_resolution_errors_json: "[]",
            nodes_with_access_errors_json: "[]",
            operations_json: "{\"on_run_start\":[],\"on_run_end\":[]}",
            packages: &[],
            nodes: &nodes,
            disabled_nodes: &Nodes::default(),
            changed_nodes: None, // cold write
            ingested_at: 0,
            macros: &Macros::default(),
            selectors_json: "",
            git_sha: "",
            git_branch: "",
            git_is_dirty: false,
        })
        .unwrap();

        // Delta write: only `a` in resolved set, but `b` is listed as changed.
        // b should be silently skipped — its epoch-0 row persists.
        let only_a_nodes = {
            let mut n = Nodes::default();
            n.models.insert("model.p.a".into(), make_model("model.p.a"));
            n
        };
        let changed: HashSet<String> = ["model.p.a".into(), "model.p.b".into()].into();
        save(&SaveArgs {
            out_dir: tmp.path(),
            version: INCREMENTAL_STATE_VERSION,
            dbt_version: &current_dbt_version(),
            profile_hash: "",
            profile_file_hash: "",
            project_file_hash: "",
            cli_vars_hash: "",
            dbt_profile_json: &dbt_profile_json,
            vars_json: "{}",
            env_vars_json: "{}",
            get_relation_calls_json: "{}",
            get_columns_in_relation_calls_json: "{}",
            patterned_dangling_sources_json: "{}",
            nodes_with_resolution_errors_json: "[]",
            nodes_with_access_errors_json: "[]",
            operations_json: "{\"on_run_start\":[],\"on_run_end\":[]}",
            packages: &[],
            nodes: &only_a_nodes,
            disabled_nodes: &Nodes::default(),
            changed_nodes: Some(&changed),
            ingested_at: 0,
            macros: &Macros::default(),
            selectors_json: "",
            git_sha: "",
            git_branch: "",
            git_is_dirty: false,
        })
        .unwrap();

        // Both a and b must still be readable (b from epoch-0, a from delta epoch).
        let loaded = load_filtered_with_unique_ids(tmp.path(), None, None)
            .expect("must load after delta write");
        assert!(
            loaded.nodes.models.contains_key("model.p.a"),
            "a must be present (from delta epoch)"
        );
        assert!(
            loaded.nodes.models.contains_key("model.p.b"),
            "b must be present (from epoch-0 — delta skipped it)"
        );
    }

    // R8 — static-analysis=strict with a safe fqn selector does NOT force a full load.
    // payload_kinds_for_command does not inspect static_analysis; the partial-load path
    // must be taken even with --static-analysis strict.
    #[test]
    fn payload_kinds_static_analysis_strict_does_not_force_full_load() {
        use dbt_common::io_args::StaticAnalysisKind;

        let tmp = tempfile::tempdir().unwrap();
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = EvalArgs {
            command: FsCommand::Compile,
            static_analysis: Some(StaticAnalysisKind::Strict),
            select: Some(atom_with_value(MethodName::Fqn, "my_model")),
            ..EvalArgs::default()
        };
        // payload_kinds returns None for compile (no kind restriction) — full parquet load.
        // The key invariant: static_analysis alone must NOT force None from payload_kinds.
        // (For compile, payload_kinds returns None regardless; the important thing is that
        // no guard inside payload_kinds_for_command inspects static_analysis.)
        let _kinds = payload_kinds_for_command(&eval, &io);
        // unique_id_filter must still resolve — static_analysis is irrelevant to it.
        write_graph_cache(tmp.path());
        let eval_with_cache = EvalArgs {
            command: FsCommand::Compile,
            static_analysis: Some(StaticAnalysisKind::Strict),
            select: Some(atom_with_value(MethodName::Fqn, "a")),
            ..EvalArgs::default()
        };
        let ids = unique_id_filter_for_selector(&eval_with_cache, &io)
            .expect("static-analysis strict must not prevent index lookup");
        assert!(ids.contains("model.p.a"), "a must be resolved with strict");
        assert!(
            ids.contains("model.p.c"),
            "transitive deps must be included even with strict"
        );
    }

    // R9 — parent walk (+model) includes full ancestor chain via parents_depth walk,
    // not the frontier mechanism — all three nodes present, all_deps_present passes.
    #[test]
    fn unique_id_filter_parent_walk_all_deps_present() {
        let tmp = tempfile::tempdir().unwrap();
        write_graph_cache(tmp.path());
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };

        // +a with parents_depth=u32::MAX — walks entire ancestry.
        let ids = unique_id_filter_for_selector(&graph_eval("a", Some(u32::MAX), None), &io)
            .expect("parent walk must resolve");

        // Build a mock IncrementalState with only the returned ids loaded, then
        // verify all_deps_present() passes.
        let make_node = |uid: &str, deps: Vec<String>| {
            use dbt_schemas::schemas::DbtModel;
            use dbt_schemas::schemas::common::NodeDependsOn;
            use dbt_schemas::schemas::nodes::{CommonAttributes, NodeBaseAttributes};
            std::sync::Arc::new(DbtModel {
                __common_attr__: CommonAttributes {
                    unique_id: uid.into(),
                    name: uid.split('.').next_back().unwrap_or(uid).into(),
                    package_name: "p".into(),
                    ..CommonAttributes::default()
                },
                __base_attr__: NodeBaseAttributes {
                    depends_on: NodeDependsOn {
                        nodes: deps,
                        ..NodeDependsOn::default()
                    },
                    ..NodeBaseAttributes::default()
                },
                ..DbtModel::default()
            })
        };

        let mut state = test_state();
        state.nodes = Nodes::default();
        // Insert only the nodes that the filter returned.
        let graph = [
            ("model.p.a", vec!["model.p.b".to_string()]),
            ("model.p.b", vec!["model.p.c".to_string()]),
            ("model.p.c", vec![]),
        ];
        for (uid, deps) in &graph {
            if ids.contains(*uid) {
                state
                    .nodes
                    .models
                    .insert((*uid).to_string(), make_node(uid, deps.clone()));
            }
        }

        assert_eq!(
            state.all_deps_present(),
            None,
            "+a walk must produce a self-contained set where all_deps_present() passes"
        );
    }

    fn make_minimal_resolver_state(dbt_state: &DbtState) -> ResolverState {
        use dbt_schemas::state::{DbtRuntimeConfig, DummyNodeResolverTracker};
        use std::sync::Arc;

        ResolverState {
            root_project_name: dbt_state
                .packages
                .first()
                .map(|p| p.dbt_project.name.clone())
                .unwrap_or_default(),
            adapter_type: dbt_state.dbt_profile.db_config.adapter_type(),
            nodes: Nodes::default(),
            disabled_nodes: Nodes::default(),
            macros: Macros::default(),
            operations: Operations::default(),
            dbt_profile: dbt_state.dbt_profile.clone(),
            render_results: Default::default(),
            node_resolver: Arc::new(DummyNodeResolverTracker),
            get_relation_calls: Default::default(),
            get_columns_in_relation_calls: Default::default(),
            patterned_dangling_sources: Default::default(),
            run_started_at: dbt_state.run_started_at,
            runtime_config: Arc::new(DbtRuntimeConfig::default()),
            manifest_selectors: Default::default(),
            resolved_selectors: Default::default(),
            root_project_quoting: Default::default(),
            defer_nodes: None,
            nodes_with_resolution_errors: Default::default(),
            nodes_with_access_errors: Default::default(),
            semantic_layer_spec_is_legacy: false,
            test_name_truncations: Default::default(),
        }
    }

    // ── state:dirty selector tests ────────────────────────────────────────────

    fn dirty_atom(parents_depth: Option<u32>, children_depth: Option<u32>) -> SelectExpression {
        use dbt_common::node_selector::SelectionCriteria;
        SelectExpression::Atom(SelectionCriteria::new(
            MethodName::State,
            vec![],
            "dirty".into(),
            false,
            parents_depth,
            children_depth,
            None,
            None,
        ))
    }

    /// state:dirty with no files changed → Some(empty set) not None.
    #[test]
    fn state_dirty_nothing_changed_returns_some_empty() {
        let tmp = tempfile::tempdir().unwrap();
        write_graph_cache(tmp.path());
        // graph_cache uses no packages/filestamps, so resolve_dirty returns Some({}).
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = EvalArgs {
            command: FsCommand::Compile,
            select: Some(dirty_atom(None, None)),
            ..EvalArgs::default()
        };
        // Cache exists but no filestamps → treated as nothing dirty.
        let result = unique_id_filter_for_selector(&eval, &io);
        // Should return Some (cache exists) even if empty.
        assert!(
            result.is_some(),
            "state:dirty with existing cache should return Some, not fall back"
        );
    }

    /// state:dirty+ AND state:modified — the And arm skips the phase-2 state:modified
    /// arm and returns Some from the state:dirty+ arm.
    #[test]
    fn state_dirty_and_state_modified_returns_some() {
        let tmp = tempfile::tempdir().unwrap();
        write_graph_cache(tmp.path());
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        // AND expression: state:dirty+ space state:modified
        let eval = EvalArgs {
            command: FsCommand::Compile,
            select: Some(SelectExpression::And(vec![
                dirty_atom(None, Some(u32::MAX)),
                atom_with_value(MethodName::State, "modified"),
            ])),
            ..EvalArgs::default()
        };
        // state:modified is phase-2-only → skipped; state:dirty+ resolves → Some.
        let result = unique_id_filter_for_selector(&eval, &io);
        assert!(
            result.is_some(),
            "state:dirty+ AND state:modified should return Some (not fall back)"
        );
    }

    /// state:dirty OR state:modified — Or arm: state:modified returns None →
    /// whole Or falls back to None (OR with phase-2-only is architecturally unsound).
    #[test]
    fn state_dirty_or_state_modified_falls_back() {
        let tmp = tempfile::tempdir().unwrap();
        write_graph_cache(tmp.path());
        let io = IoArgs {
            out_dir: tmp.path().to_path_buf(),
            ..IoArgs::default()
        };
        let eval = EvalArgs {
            command: FsCommand::Compile,
            select: Some(SelectExpression::Or(vec![
                dirty_atom(None, Some(u32::MAX)),
                atom_with_value(MethodName::State, "modified"),
            ])),
            ..EvalArgs::default()
        };
        assert!(
            unique_id_filter_for_selector(&eval, &io).is_none(),
            "state:dirty OR state:modified should fall back (Or with phase-2 arm)"
        );
    }

    // ── strip_dirty_selector tests ────────────────────────────────────────────

    #[test]
    fn test_strip_dirty_atom_alone() {
        // Atom(state:dirty+) alone → None (select all loaded nodes).
        let expr = dirty_atom(None, Some(u32::MAX));
        assert!(
            strip_dirty_selector(expr).is_none(),
            "bare state:dirty should strip to None"
        );
    }

    #[test]
    fn test_strip_dirty_and_with_modified() {
        // And([state:dirty+, state:modified]) → state:modified.
        let expr = SelectExpression::And(vec![
            dirty_atom(None, Some(u32::MAX)),
            atom_with_value(MethodName::State, "modified"),
        ]);
        let stripped = strip_dirty_selector(expr).expect("should strip to state:modified");
        match &stripped {
            SelectExpression::Atom(c) => {
                assert_eq!(c.method, MethodName::State);
                assert_eq!(c.value, "modified");
            }
            other => panic!("expected Atom(state:modified), got {other:?}"),
        }
    }

    #[test]
    fn test_strip_dirty_or_unchanged() {
        // Or([state:dirty+, tag:foo]) → unchanged (strip only removes from And/Atom).
        let original = SelectExpression::Or(vec![
            dirty_atom(None, Some(u32::MAX)),
            atom_with_value(MethodName::Tag, "foo"),
        ]);
        let stripped = strip_dirty_selector(original);
        // Or is passed through as-is (both arms kept).
        match stripped {
            Some(SelectExpression::Or(arms)) => assert_eq!(arms.len(), 2),
            other => panic!("Or should be preserved, got {other:?}"),
        }
    }

    #[test]
    fn test_selector_contains_dirty() {
        assert!(selector_contains_dirty(&dirty_atom(None, None)));
        assert!(selector_contains_dirty(&SelectExpression::And(vec![
            dirty_atom(None, None),
            atom_with_value(MethodName::Fqn, "x"),
        ])));
        assert!(!selector_contains_dirty(&atom_with_value(
            MethodName::Fqn,
            "dirty"
        )));
        assert!(!selector_contains_dirty(&atom_with_value(
            MethodName::State,
            "modified"
        )));
    }
}
