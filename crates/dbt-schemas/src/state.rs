use chrono_tz::Tz;
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use crate::schemas::{
    DbtSource, InternalDbtNodeAttributes, Nodes,
    common::{DbtQuoting, ResolvedQuoting},
    macros::{DbtDocsMacro, DbtMacro},
    manifest::DbtOperation,
    profiles::DbConfig,
    project::{
        DbtProject, ProjectDataTestConfig, ProjectModelConfig, ProjectSeedConfig,
        ProjectSnapshotConfig, ProjectSourceConfig, QueryComment,
    },
    relations::base::{BaseRelation, RelationPattern},
    selectors::ResolvedSelector,
    serde::{FloatOrString, StringOrArrayOfStrings},
};
use blake3::Hasher;
use chrono::{DateTime, Local, Utc};
use dbt_common::{ErrorCode, FsResult, fs_err, serde_utils::convert_json_to_map};
use minijinja::compiler::parser::materialization_macro_name;
use minijinja::{MacroSpans, Value as MinijinjaValue, value::Object};
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

#[derive(Debug, Hash, Eq, PartialEq, Clone, Ord, PartialOrd)]
pub enum ResourcePathKind {
    ProfilePaths,
    ProjectPaths,
    ModelPaths,
    AnalysisPaths,
    AssetPaths,
    DocsPaths,
    MacroPaths,
    SeedPaths,
    SnapshotPaths,
    TestPaths,
    FixturePaths,
}

impl fmt::Display for ResourcePathKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind_str = match self {
            ResourcePathKind::ModelPaths => "model paths",
            ResourcePathKind::AnalysisPaths => "analysis paths",
            ResourcePathKind::AssetPaths => "asset paths",
            ResourcePathKind::DocsPaths => "docs paths",
            ResourcePathKind::MacroPaths => "macro paths",
            ResourcePathKind::SeedPaths => "seed paths",
            ResourcePathKind::SnapshotPaths => "snapshot paths",
            ResourcePathKind::TestPaths => "test paths",
            ResourcePathKind::ProjectPaths => "project paths",
            ResourcePathKind::ProfilePaths => "profile paths",
            ResourcePathKind::FixturePaths => "fixture paths",
        };
        write!(f, "{kind_str}")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DbtAsset {
    // in_dir (or project_dir), if the asset is input,
    // out_dir (or target_dir), if asset is an output
    pub base_path: PathBuf,
    // relative path to project root
    pub path: PathBuf,
    // package name
    pub package_name: String,
}

impl DbtAsset {
    /// Assumes all paths used are canonicalized
    pub fn to_display_path(&self, project_root: &Path) -> PathBuf {
        let absolute_path = self.base_path.join(&self.path);
        if project_root == self.base_path {
            self.path.clone()
        } else {
            absolute_path
                .strip_prefix(project_root)
                .unwrap_or(&absolute_path)
                .to_owned()
        }
    }
}

impl fmt::Display for DbtAsset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DbtAsset {{ base_path: {}, path: {}, package_name: {} }}",
            self.base_path.display(),
            self.path.display(),
            self.package_name
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtProfile {
    pub profile: String,
    pub target: String,
    pub db_config: DbConfig,
    pub schema: String,
    pub database: String,
    pub relative_profile_path: PathBuf,
    #[serde(skip)]
    pub threads: Option<usize>, // from flags in dbt
}

impl DbtProfile {
    pub fn blake3_hash(&self) -> String {
        let mut hasher = Hasher::new();
        // Serialize self, skipping threads due to #[serde(skip)]
        let bytes = serde_json::to_vec(self).expect("Serialization failed");
        hasher.update(&bytes);
        let hash = hasher.finalize();
        // Truncate to 16 bytes and encode as hex
        hex::encode(&hash.as_bytes()[..16])
    }
}
impl fmt::Display for DbtProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DbtProfile {{ profile: {}, target: {}, db_config: {:?}, schema: {}, database: {} , path: {}, threads: {:?}}}",
            self.profile,
            self.target,
            self.db_config,
            self.schema,
            self.database,
            self.relative_profile_path.display(),
            self.threads,
        )
    }
}

#[derive(Debug)]
pub struct DbtPackage {
    pub dbt_project: DbtProject,
    pub dbt_properties: Vec<DbtAsset>,
    pub analysis_files: Vec<DbtAsset>,
    pub model_sql_files: Vec<DbtAsset>,
    pub macro_files: Vec<DbtAsset>,
    pub test_files: Vec<DbtAsset>,
    pub fixture_files: Vec<DbtAsset>,
    pub seed_files: Vec<DbtAsset>,
    pub docs_files: Vec<DbtAsset>,
    pub snapshot_files: Vec<DbtAsset>,
    pub dependencies: BTreeSet<String>,
    pub all_paths: HashMap<ResourcePathKind, Vec<(PathBuf, SystemTime)>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum DbtVars {
    Vars(BTreeMap<String, DbtVars>),
    Value(dbt_serde_yaml::Value),
}

#[derive(Debug)]
pub struct DbtState {
    pub dbt_profile: DbtProfile,
    pub run_started_at: DateTime<Tz>,
    pub packages: Vec<DbtPackage>,
    /// Key is the package name, value are all package scoped vars
    pub vars: BTreeMap<String, BTreeMap<String, DbtVars>>,
    pub cli_vars: BTreeMap<String, dbt_serde_yaml::Value>,
}

impl DbtState {
    /// Assumes the root project is at the first entry
    /// see `fn all_package_paths` impl and its caller
    pub fn root_project_name(&self) -> &str {
        self.root_project().name.as_str()
    }

    pub fn root_project(&self) -> &DbtProject {
        &self.packages[0].dbt_project
    }

    pub fn root_project_flags(&self) -> BTreeMap<String, minijinja::Value> {
        let flags = self.root_project().flags.clone();
        if let Some(flags) = flags {
            convert_json_to_map(flags)
        } else {
            BTreeMap::new()
        }
    }
}

impl fmt::Display for DbtState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for package in self.packages.iter() {
            writeln!(f, "Package: {}", package.dbt_project.name)?;
            let mut sorted_paths: Vec<_> = package.all_paths.iter().collect();
            sorted_paths.sort_by(|a, b| a.0.cmp(b.0));

            for (path_kind, paths) in sorted_paths {
                if !paths.is_empty() {
                    writeln!(f, "  {path_kind}:")?;
                    for (path, system_time) in paths {
                        let datetime: DateTime<Local> = DateTime::from(*system_time);
                        writeln!(
                            f,
                            "    {}, {}",
                            path.display(),
                            datetime.format("%Y-%m-%d %H:%M:%S")
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
}

pub trait RefsAndSourcesTracker: fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn insert_ref(
        &mut self,
        node: &dyn InternalDbtNodeAttributes,
        adapter_type: &str,
        model_status: ModelStatus,
        overwrite: bool,
    ) -> FsResult<()>;
    fn insert_source(
        &mut self,
        package_name: &str,
        source: &DbtSource,
        adapter_type: &str,
        model_status: ModelStatus,
    ) -> FsResult<()>;
    fn lookup_ref(
        &self,
        package_name: &Option<String>,
        name: &str,
        version: &Option<String>,
        node_package_name: &Option<String>,
    ) -> FsResult<(String, MinijinjaValue, ModelStatus)>;
    fn lookup_source(
        &self,
        package_name: &str,
        source_name: &str,
        table_name: &str,
    ) -> FsResult<(String, MinijinjaValue, ModelStatus)>;
}

// test only
#[derive(Debug)]
pub struct DummyRefsAndSourcesTracker;

impl RefsAndSourcesTracker for DummyRefsAndSourcesTracker {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn insert_ref(
        &mut self,
        _node: &dyn InternalDbtNodeAttributes,
        _adapter_type: &str,
        _model_status: ModelStatus,
        _overwrite: bool,
    ) -> FsResult<()> {
        // No-op for dummy
        Ok(())
    }

    fn insert_source(
        &mut self,
        _package_name: &str,
        _source: &DbtSource,
        _adapter_type: &str,
        _model_status: ModelStatus,
    ) -> FsResult<()> {
        // No-op for dummy
        Ok(())
    }

    fn lookup_ref(
        &self,
        _package_name: &Option<String>,
        name: &str,
        _version: &Option<String>,
        _node_package_name: &Option<String>,
    ) -> FsResult<(String, MinijinjaValue, ModelStatus)> {
        Err(fs_err!(
            ErrorCode::Generic,
            "DummyRefsAndSourcesTracker: lookup_ref not implemented for '{}'",
            name
        ))
    }

    fn lookup_source(
        &self,
        _package_name: &str,
        source_name: &str,
        table_name: &str,
    ) -> FsResult<(String, MinijinjaValue, ModelStatus)> {
        Err(fs_err!(
            ErrorCode::Generic,
            "DummyRefsAndSourcesTracker: lookup_source not implemented for '{}.{}'",
            source_name,
            table_name
        ))
    }
}

impl Default for DummyRefsAndSourcesTracker {
    fn default() -> Self {
        DummyRefsAndSourcesTracker
    }
}
#[derive(Debug, Clone, Default)]
pub struct Macros {
    pub macros: BTreeMap<String, DbtMacro>,
    pub docs_macros: BTreeMap<String, DbtDocsMacro>,
}

#[derive(Debug, Default, Clone)]
pub struct Operations {
    pub on_run_start: Vec<DbtOperation>,
    pub on_run_end: Vec<DbtOperation>,
}

#[derive(Debug, Clone)]
pub struct ResolverState {
    pub root_project_name: String,
    pub adapter_type: String,
    pub nodes: Nodes,
    pub disabled_nodes: Nodes,
    pub macros: Macros,
    pub operations: Operations,
    pub dbt_profile: DbtProfile,
    pub render_results: RenderResults,
    pub refs_and_sources: Arc<dyn RefsAndSourcesTracker>,
    pub get_relation_calls: BTreeMap<String, Vec<Arc<dyn BaseRelation>>>,
    pub get_columns_in_relation_calls: BTreeMap<String, Vec<Arc<dyn BaseRelation>>>,
    pub patterned_dangling_sources: BTreeMap<String, Vec<RelationPattern>>,
    pub run_started_at: DateTime<Tz>,
    pub runtime_config: Arc<DbtRuntimeConfig>,
    pub resolved_selectors: ResolvedSelector,
    pub root_project_quoting: ResolvedQuoting,
    pub defer_nodes: Option<Nodes>,
}

impl ResolverState {
    // TODO: support finding custom materialization https://github.com/dbt-labs/fs/issues/2736
    // a few details is here https://github.com/dbt-labs/fs/pull/3967#discussion_r2153355927
    pub fn find_materialization_macro_name(
        &self,
        materialization: impl fmt::Display,
        adapter: &str,
    ) -> FsResult<String> {
        let adapter_package = format!("dbt_{adapter}");
        for package in [&adapter_package, "dbt"] {
            for adapter in [adapter, "default"] {
                if let Some(macro_) = self.macros.macros.values().find(|m| {
                    m.name == materialization_macro_name(&materialization, adapter)
                        && m.package_name == package
                }) {
                    return Ok(format!("{}.{}", package, macro_.name));
                }
            }
        }

        Err(fs_err!(
            ErrorCode::Unexpected,
            "Materialization macro not found for materialization: {}, adapter: {}",
            materialization,
            adapter
        ))
    }
}

impl fmt::Display for ResolverState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ResolverState {{ nodes: {:?}, dbt_profile: {}, macro_collector: {:?} }}",
            self.nodes, self.dbt_profile, self.render_results
        )
    }
}

// A subset of resolver state
#[derive(Debug, Clone, Default)]
pub struct ResolvedNodes {
    pub nodes: Nodes,
    pub disabled_nodes: Nodes,
    pub macros: Macros,
    pub operations: Operations,
}
// A changeset describes the difference between two sets of files
// - one on the file system
// - one in content addressable store CAS) in a dbt project.
// The changeset contains:
// - files that are the same in both sets
// - files that are different in both sets
// - files that are missing in the filesystem
// - files that are missing in the CAS
// - whether the deps are the same e.g. (i.e dependencies.yml, package.lock and all dbt_packages)
// files are represented by their relative path to the project root
#[derive(Debug, Clone, Default)]
pub struct FileChanges {
    pub unchanged_files: HashSet<String>,
    // updated files
    pub changed_files: HashSet<String>,
    // deleted files
    pub deleted_files: HashSet<String>,
    // new files
    pub new_files: HashSet<String>,
}
impl FileChanges {
    pub fn no_change(&self) -> bool {
        self.changed_files.is_empty()
            && self.deleted_files.is_empty()
            && self.new_files.is_empty()
            && !self.unchanged_files.is_empty()
    }
    pub fn has_changes(&self) -> bool {
        !self.changed_files.is_empty() || !self.new_files.is_empty()
    }
}
/// Represents the execution state of a node in the dbt project.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum NodeExecutionState {
    #[default]
    NotProcessed,
    Parsed,
    Compiled,
    Run,
}
impl NodeExecutionState {
    /// Converts a command string to a NodeExecutionState
    pub fn from_cmd(cmd: &str) -> Self {
        match cmd {
            "parse" => NodeExecutionState::Parsed,
            "compile" => NodeExecutionState::Compiled,
            "run" | "build" | "test" | "snapshot" | "seed" => NodeExecutionState::Run,
            _ => NodeExecutionState::NotProcessed,
        }
    }
}
impl fmt::Display for NodeExecutionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
/// Represents the status of a phase in the execution of a node.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum NodeExecutionStatus {
    #[default]
    Success,
    Error,
    Skipped,
    Aborted, // e.g. interrupted by user.
    Reused,
    Passed, // For test nodes.
    Failed, // For test nodes.
}
impl fmt::Display for NodeExecutionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct NodeStatus {
    pub latest_state: Option<NodeExecutionState>,
    pub latest_status: Option<NodeExecutionStatus>,
    pub latest_time: Option<String>,
    pub latest_message: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct CacheState {
    pub file_changes: FileChanges,
    // only the resolved nodes which input files are unchanged
    pub resolved_nodes: ResolvedNodes,
    // updated nodes which input files are changed
    pub unchanged_node_statuses: HashMap<String, NodeStatus>,
}
impl CacheState {
    pub fn has_changes(&self) -> bool {
        self.file_changes.has_changes()
    }
}
#[derive(Debug, Clone, Default)]
pub struct RenderResults {
    pub rendering_results: BTreeMap<String, (String, MacroSpans)>,
}

#[derive(Debug, Clone, Default)]
pub struct DbtRuntimeConfig {
    pub runtime_config: BTreeMap<String, minijinja::Value>,
    pub dependencies: BTreeMap<String, Arc<DbtRuntimeConfig>>,
    pub inner: DbtRuntimeConfigInner,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct InvocationArgs {
    pub require_explicit_package_overrides_for_builtin_materializations: bool,
    pub require_resource_names_without_spaces: bool,
    pub source_freshness_run_project_hooks: bool,
    pub skip_nodes_if_on_run_start_fails: bool,
    pub state_modified_compare_more_unrendered_values: bool,
    pub require_yaml_configuration_for_mf_time_spines: bool,
    pub require_batched_execution_for_custom_microbatch_strategy: bool,
}

impl Default for InvocationArgs {
    fn default() -> Self {
        Self {
            require_explicit_package_overrides_for_builtin_materializations: true,
            require_resource_names_without_spaces: true,
            source_freshness_run_project_hooks: true,
            skip_nodes_if_on_run_start_fails: true,
            state_modified_compare_more_unrendered_values: true,
            require_yaml_configuration_for_mf_time_spines: true,
            require_batched_execution_for_custom_microbatch_strategy: true,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DbtRuntimeConfigInner {
    // Profile configuration
    pub profile_name: String,
    pub target_name: String,
    pub threads: Option<usize>,
    pub credentials: Option<DbConfig>,
    pub profile_env_vars: HashMap<String, String>,
    pub args: InvocationArgs,

    // Project configuration
    pub project_name: String,
    pub version: Option<String>,
    pub project_root: PathBuf,

    // Path configurations
    pub model_paths: Vec<String>,
    pub macro_paths: Vec<String>,
    pub seed_paths: Vec<String>,
    pub test_paths: Vec<String>,
    pub analysis_paths: Vec<String>,
    pub docs_paths: Vec<String>,
    pub asset_paths: Vec<String>,
    pub target_path: String,
    pub snapshot_paths: Vec<String>,
    pub clean_targets: Vec<String>,
    pub log_path: String,

    // Package configurations
    pub packages_install_path: String,

    // Project configurations
    pub quoting: Option<DbtQuoting>,
    pub models: Option<ProjectModelConfig>,
    pub seeds: Option<ProjectSeedConfig>,
    pub snapshots: Option<ProjectSnapshotConfig>,
    pub sources: Option<ProjectSourceConfig>,
    pub tests: Option<ProjectDataTestConfig>,
    pub query_comment: Option<QueryComment>,

    // Variables and hooks
    pub vars: BTreeMap<String, DbtVars>,
    pub cli_vars: BTreeMap<String, dbt_serde_yaml::Value>,
    pub on_run_start: Vec<String>,
    pub on_run_end: Vec<String>,

    // Version info
    pub config_version: Option<i32>,
    pub require_dbt_version: Option<StringOrArrayOfStrings>,
    pub restrict_access: Option<bool>,

    // Runtime info
    pub invoked_at: DateTime<Utc>,
}

impl DbtRuntimeConfig {
    /// Adds a self reference to the dependencies
    pub fn add_self_to_dependencies(&mut self, package_name: &str) {
        // TODO: This is a hack to get around the fact that DbtRuntimeConfig is a circular reference
        // it fixes this issue one level deep, but not more (hence, config.dependencies[self].dependencies will
        // not contain itself).
        let self_clone = self.clone();
        self.dependencies
            .insert(package_name.to_string(), Arc::new(self_clone));
    }

    pub fn new(
        in_dir: &Path,
        package: &DbtPackage,
        profile: &DbtProfile,
        dependency_lookup: &BTreeMap<String, Arc<DbtRuntimeConfig>>,
        vars: &BTreeMap<String, DbtVars>,
        cli_vars: &BTreeMap<String, dbt_serde_yaml::Value>,
    ) -> Self {
        let runtime_config_inner = DbtRuntimeConfigInner {
            profile_name: profile.profile.clone(),
            target_name: profile.target.clone(),
            threads: profile.threads,
            credentials: Some(profile.db_config.clone()),
            profile_env_vars: HashMap::new(),

            project_name: package.dbt_project.name.clone(),
            version: package.dbt_project.version.clone().map(|v| match v {
                FloatOrString::Number(n) => n.to_string(),
                FloatOrString::String(s) => s,
            }),
            project_root: in_dir.to_path_buf(),
            model_paths: package.dbt_project.model_paths.clone().unwrap_or_default(),
            macro_paths: package.dbt_project.macro_paths.clone().unwrap_or_default(),
            seed_paths: package.dbt_project.seed_paths.clone().unwrap_or_default(),
            test_paths: package.dbt_project.test_paths.clone().unwrap_or_default(),
            analysis_paths: package
                .dbt_project
                .analysis_paths
                .clone()
                .unwrap_or_default(),
            docs_paths: package.dbt_project.docs_paths.clone().unwrap_or_default(),
            asset_paths: package.dbt_project.asset_paths.clone().unwrap_or_default(),
            target_path: package
                .dbt_project
                .target_path
                .clone()
                .unwrap_or_default()
                .to_string(),
            snapshot_paths: package
                .dbt_project
                .snapshot_paths
                .clone()
                .unwrap_or_default(),
            clean_targets: package
                .dbt_project
                .clean_targets
                .clone()
                .unwrap_or_default(),
            log_path: package
                .dbt_project
                .log_path
                .clone()
                .unwrap_or_default()
                .to_string(),
            packages_install_path: package
                .dbt_project
                .packages_install_path
                .clone()
                .unwrap_or_default(),
            quoting: package.dbt_project.quoting.clone().into_inner(),
            models: package.dbt_project.models.clone(),
            seeds: package.dbt_project.seeds.clone(),
            snapshots: package.dbt_project.snapshots.clone(),
            sources: package.dbt_project.sources.clone(),
            tests: package.dbt_project.tests.clone(),
            query_comment: (*package.dbt_project.query_comment).clone(),
            vars: vars.clone(),
            cli_vars: cli_vars.clone(),
            on_run_start: match &*package.dbt_project.on_run_start {
                Some(StringOrArrayOfStrings::String(s)) => vec![s.clone()],
                Some(StringOrArrayOfStrings::ArrayOfStrings(v)) => v.clone(),
                _ => vec![],
            },
            on_run_end: match &*package.dbt_project.on_run_end {
                Some(StringOrArrayOfStrings::String(s)) => vec![s.clone()],
                Some(StringOrArrayOfStrings::ArrayOfStrings(v)) => v.clone(),
                _ => vec![],
            },
            config_version: package.dbt_project.config_version,
            require_dbt_version: package.dbt_project.require_dbt_version.clone(),
            restrict_access: package.dbt_project.restrict_access,
            invoked_at: Utc::now(),
            args: InvocationArgs::default(),
        };

        let mut runtime_config = Self {
            runtime_config: convert_json_to_map(
                serde_json::to_value(&runtime_config_inner).unwrap(),
            ),
            dependencies: BTreeMap::new(),
            inner: runtime_config_inner,
        };

        runtime_config.add_self_to_dependencies(&package.dbt_project.name);
        for package_name in package.dependencies.iter() {
            runtime_config.dependencies.insert(
                package_name.clone(),
                dependency_lookup
                    .get(package_name)
                    .expect("Dependency not resolved in correct order")
                    .clone(),
            );
        }
        runtime_config
    }

    /// Converts this runtime config to a pure map structure for MiniJinja
    pub fn to_minijinja_map(&self) -> BTreeMap<String, minijinja::Value> {
        let mut result = self.runtime_config.clone();

        // Convert dependencies to maps recursively
        let mut deps_map = BTreeMap::new();
        for (key, dep_config) in &self.dependencies {
            deps_map.insert(
                key.clone(),
                minijinja::Value::from_object(dep_config.to_minijinja_map()),
            );
        }

        // Add dependencies to the result
        result.insert(
            "dependencies".to_string(),
            minijinja::Value::from_object(deps_map),
        );

        result
    }
}

impl Object for DbtRuntimeConfig {
    fn get_value(self: &Arc<Self>, key: &minijinja::Value) -> Option<minijinja::value::Value> {
        match key.as_str()? {
            // This is a special case for the dependencies
            // We use the to_minijinja_map helper to convert dependencies recursively
            "dependencies" => {
                let mut deps = BTreeMap::new();
                for (key, value) in self.dependencies.iter() {
                    deps.insert(
                        key.clone(),
                        minijinja::Value::from_object(value.to_minijinja_map()),
                    );
                }
                Some(minijinja::Value::from_object(deps))
            }
            // Otherwise, we just return the value from the runtime config
            other => self.runtime_config.get(other).cloned(),
        }
    }
}

/// Represents the status of a model
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum ModelStatus {
    /// Model is enabled and successfully parsed
    Enabled,
    /// Model is disabled by configuration
    Disabled,
    /// Model failed to parse
    ParsingFailed,
}
