//! Distribution, install channel, and self-update/uninstall detection for dbt.

pub mod command;
pub mod dist;
mod proc;
pub mod python;
use std::{
    collections::HashSet,
    env,
    io::Read,
    path::{Path, PathBuf},
    sync::mpsc,
    time::Duration,
};

use dbt_common::{ErrorCode, FsResult, err, error::WrappedError, fs_err};
pub use dist::{Channel, DistInfo, Distribution, Generation};

use crate::proc::{GRACE_WAIT, NORMAL_WAIT, ProcessOutput, real_run};
pub use crate::python::PythonPackageManager;

/// Entry point for discovering [`DistInfo`] about one or more `dbt`
/// installations.
pub enum DistInfoDiscovery<'a> {
    /// The currently running binary.
    Current,
    /// A specific `dbt` executable found elsewhere on disk.
    AtLocation(&'a Path),
    /// Every `dbt`/`dbt.exe` found on `PATH`.
    AllInPath,
}

impl<'a> DistInfoDiscovery<'a> {
    /// `command_name` is the CLI-brand name of the currently running binary
    /// (e.g. `"dbt-core"` for OSS, or the proprietary Fusion build's name) —
    /// used only to resolve the *current* process's own distribution, since a
    /// process can't `--version`-probe itself the way it does other `dbt`s
    /// found on `PATH`.
    pub fn discover(self, command_name: &str) -> FsResult<Vec<DistInfo>> {
        match self {
            Self::Current => get_current(command_name).map(|d| vec![d]),
            Self::AtLocation(file_path) => get_at_path(file_path, command_name).map(|d| vec![d]),
            Self::AllInPath => get_all_in_path(command_name),
        }
    }
}

/// Environment and subprocess access, threaded through as closures (rather
/// than called directly) so the channel-detection rules that depend on them
/// can be exercised with canned inputs instead of real env mutation or real
/// tools on `PATH`.
struct DiscoveryContext<'a> {
    env: &'a dyn Fn(&str) -> Option<String>,
    run: &'a dyn Fn(&str, &[&str]) -> Option<ProcessOutput>,
}

fn real_env(name: &str) -> Option<String> {
    env::var(name).ok()
}

impl DiscoveryContext<'static> {
    fn real() -> Self {
        DiscoveryContext {
            env: &real_env,
            run: &real_run,
        }
    }
}

/// Whether a resolved file is a native executable, a script (shebang or
/// Windows launcher shim), or undeterminable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileKind {
    NativeBinary,
    Script,
    Unknown,
}

/// Reads the first few bytes of `path` and classifies it as a native
/// executable (ELF/Mach-O/PE magic bytes) or a script (leading `#!`).
/// This is the sole discriminator needed for the one genuinely ambiguous
/// case in channel detection: a real file at `~/.local/bin/dbt` could be
/// either the standalone native binary or a `pip --user` console script.
fn sniff_file_kind(path: &Path) -> FileKind {
    const MACHO_MAGICS: [[u8; 4]; 5] = [
        [0xFE, 0xED, 0xFA, 0xCE], // Mach-O 32-bit BE
        [0xCE, 0xFA, 0xED, 0xFE], // Mach-O 32-bit LE
        [0xFE, 0xED, 0xFA, 0xCF], // Mach-O 64-bit BE
        [0xCF, 0xFA, 0xED, 0xFE], // Mach-O 64-bit LE
        [0xCA, 0xFE, 0xBA, 0xBE], // universal/fat binary
    ];

    let Ok(mut file) = std::fs::File::open(path) else {
        return FileKind::Unknown;
    };
    let mut buf = [0u8; 4];
    let Ok(n) = file.read(&mut buf) else {
        return FileKind::Unknown;
    };

    if n >= 4 && buf == *b"\x7fELF" {
        return FileKind::NativeBinary;
    }
    if n >= 4 && MACHO_MAGICS.contains(&buf) {
        return FileKind::NativeBinary;
    }
    if n >= 2 && &buf[0..2] == b"MZ" {
        return FileKind::NativeBinary;
    }
    if n >= 2 && &buf[0..2] == b"#!" {
        return FileKind::Script;
    }
    FileKind::Unknown
}

/// A file exists and has its executable bit set (unix) or simply exists
/// (other platforms, where "executable" isn't a permission-bit concept for
/// our purposes). Follows symlinks.
fn is_executable(path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// A path's symlinks/shims resolved to their real target and classified:
/// `given` is the original path, `resolved` is its real target, and `kind`
/// is what that target actually is.
struct ClassifiedPath {
    given: PathBuf,
    resolved: PathBuf,
    kind: FileKind,
}

/// Resolves symlinks/shims to their real target, then classifies the target.
fn classify_path(path: &Path) -> ClassifiedPath {
    let resolved = dbt_common::stdfs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let kind = sniff_file_kind(&resolved);
    ClassifiedPath {
        given: path.to_path_buf(),
        resolved,
        kind,
    }
}

fn home_dir(ctx: &DiscoveryContext) -> Option<PathBuf> {
    (ctx.env)("HOME")
        .or_else(|| (ctx.env)("USERPROFILE"))
        .map(PathBuf::from)
}

/// Result of applying the path-based release-channel rules to a resolved
/// `dbt` location: the detected channel (if any), a distribution the path
/// alone was enough to prove (e.g. a Homebrew-tap conflict identifying the
/// dbt Cloud CLI), and a package-manager hint for `pypi` installs.
#[derive(Debug, Clone, Default)]
struct PathDiscovery {
    channel: Option<Channel>,
    distribution_override: Option<Distribution>,
    py_package_manager_hint: Option<PythonPackageManager>,
}

impl PathDiscovery {
    /// Derives the upgrade/uninstall commands to surface to the user.
    ///
    /// Checked ahead of `channel`: a Homebrew-tap conflict identifies the dbt
    /// Cloud CLI (see `detect_homebrew`) without resolving a release channel
    /// at all, but we still owe the user an uninstall command for the
    /// conflicting install.
    fn command_strings(
        &self,
        manager: Option<PythonPackageManager>,
    ) -> (Option<String>, Option<String>) {
        if self.distribution_override == Some(Distribution::CloudCLI) {
            return (
                None,
                Some("brew uninstall dbt-labs/dbt-cli/dbt".to_string()),
            );
        }
        match self.channel {
            Some(Channel::Standalone) => (
                Some("dbt system update".to_string()),
                Some("dbt system uninstall".to_string()),
            ),
            Some(Channel::Brew) => (
                Some("brew upgrade dbt".to_string()),
                Some("brew uninstall dbt".to_string()),
            ),
            Some(Channel::Winget) => (
                Some("winget upgrade --id dbtLabs.dbt --exact".to_string()),
                Some("winget uninstall --id dbtLabs.dbt --exact".to_string()),
            ),
            Some(Channel::Pypi) => {
                let Some(manager) = manager else {
                    return (None, None);
                };
                let commands = match manager {
                    PythonPackageManager::Pip
                    | PythonPackageManager::Asdf
                    | PythonPackageManager::Mise
                    | PythonPackageManager::Pyenv => {
                        ("pip install --upgrade dbt", "pip uninstall dbt")
                    }
                    PythonPackageManager::Pipx => ("pipx upgrade dbt", "pipx uninstall dbt"),
                    PythonPackageManager::Uv => ("uv tool upgrade dbt", "uv tool uninstall dbt"),
                    PythonPackageManager::Poetry => ("poetry update dbt", "poetry remove dbt"),
                    PythonPackageManager::Pdm => ("pdm update dbt", "pdm remove dbt"),
                    PythonPackageManager::Pipenv => ("pipenv update dbt", "pipenv uninstall dbt"),
                    PythonPackageManager::Hatch => (
                        "hatch run pip install --upgrade dbt",
                        "hatch run pip uninstall dbt",
                    ),
                    PythonPackageManager::Conda => ("conda update dbt", "conda remove dbt"),
                    PythonPackageManager::Rye => ("rye install dbt", "rye uninstall dbt"),
                };
                (Some(commands.0.to_string()), Some(commands.1.to_string()))
            }
            None => (None, None),
        }
    }
}

fn is_home_local_bin_dbt(resolved: &Path, home: &Path) -> bool {
    resolved == home.join(".local/bin/dbt") || resolved == home.join(".local/bin/dbt.exe")
}

/// Determines whether `resolved` sits under the Homebrew prefix and, if so,
/// which tap installed it. Returns `None` when not under Homebrew at all.
fn detect_homebrew(
    ctx: &DiscoveryContext,
    resolved: &Path,
) -> Option<(Option<Channel>, Option<Distribution>)> {
    let prefix_output = (ctx.run)("brew", &["--prefix"])?;
    if !prefix_output.success {
        return None;
    }
    let prefix = prefix_output.stdout.trim();
    if prefix.is_empty() || !resolved.starts_with(prefix) {
        return None;
    }

    const UNTRUSTED_TAP_ERROR: &str =
        "Refusing to load formula dbt-labs/dbt-cli/dbt from untrusted tap dbt-labs/dbt-cli";

    match (ctx.run)("brew", &["info", "dbt", "--json=v2"]) {
        Some(info) if info.success => {
            let tap = serde_json::from_str::<serde_json::Value>(&info.stdout)
                .ok()
                .and_then(|v| {
                    v.get("formulae")?
                        .as_array()?
                        .first()?
                        .get("tap")?
                        .as_str()
                        .map(str::to_string)
                });
            match tap.as_deref() {
                Some("dbt-labs/dbt-cli") => Some((None, Some(Distribution::CloudCLI))),
                _ => Some((Some(Channel::Brew), None)),
            }
        }
        Some(info) if info.stderr.contains(UNTRUSTED_TAP_ERROR) => {
            Some((None, Some(Distribution::CloudCLI)))
        }
        _ => Some((Some(Channel::Brew), None)),
    }
}

fn detect_winget(path: &Path) -> bool {
    let normalized = path.to_string_lossy().replace('/', "\\").to_lowercase();
    normalized.contains("\\winget\\links\\") || normalized.contains("\\winget\\packages\\")
}

/// Checks whether `given` or `resolved` falls under a tool-isolated /
/// version-manager directory, honoring an env-var override when the tool
/// supports relocating it (e.g. `PIPX_HOME`), falling back to the default
/// `$HOME`-relative location.
fn matches_tool_dir(
    given: &Path,
    resolved: &Path,
    home: Option<&Path>,
    ctx: &DiscoveryContext,
    env_override: Option<&str>,
    default_rel: &[&str],
) -> bool {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(env_var) = env_override {
        if let Some(custom) = (ctx.env)(env_var) {
            if !custom.is_empty() {
                candidates.push(PathBuf::from(custom));
            }
        }
    }
    if let Some(home) = home {
        let mut dir = home.to_path_buf();
        for part in default_rel {
            dir.push(part);
        }
        candidates.push(dir);
    }
    candidates
        .iter()
        .any(|dir| given.starts_with(dir) || resolved.starts_with(dir))
}

/// Applies the path-based release-channel rules in the order documented in
/// "Resolving the release channel". Native-binary-only channels
/// (standalone/brew/winget) are gated on `kind == NativeBinary`; every other
/// rule matches on location alone, since a Windows pip/pipx launcher shim is
/// itself a PE binary and would otherwise defeat a kind-based check there.
fn discover_channel(
    ctx: &DiscoveryContext,
    given: &Path,
    resolved: &Path,
    kind: FileKind,
) -> PathDiscovery {
    let home = home_dir(ctx);

    if kind == FileKind::NativeBinary {
        if let Some(home) = &home {
            if is_home_local_bin_dbt(resolved, home) {
                return PathDiscovery {
                    channel: Some(Channel::Standalone),
                    ..Default::default()
                };
            }
        }
        if let Some((channel, distribution_override)) = detect_homebrew(ctx, resolved) {
            return PathDiscovery {
                channel,
                distribution_override,
                ..Default::default()
            };
        }
        if detect_winget(given) || detect_winget(resolved) {
            return PathDiscovery {
                channel: Some(Channel::Winget),
                ..Default::default()
            };
        }
    }

    let home_ref = home.as_deref();

    if matches_tool_dir(
        given,
        resolved,
        home_ref,
        ctx,
        Some("PIPX_HOME"),
        &[".local", "pipx"],
    ) {
        return PathDiscovery {
            channel: Some(Channel::Pypi),
            py_package_manager_hint: Some(PythonPackageManager::Pipx),
            ..Default::default()
        };
    }
    if matches_tool_dir(
        given,
        resolved,
        home_ref,
        ctx,
        Some("UV_TOOL_DIR"),
        &[".local", "share", "uv", "tools"],
    ) {
        return PathDiscovery {
            channel: Some(Channel::Pypi),
            py_package_manager_hint: Some(PythonPackageManager::Uv),
            ..Default::default()
        };
    }
    if matches_tool_dir(given, resolved, home_ref, ctx, None, &[".rye", "tools"]) {
        return PathDiscovery {
            channel: Some(Channel::Pypi),
            py_package_manager_hint: Some(PythonPackageManager::Rye),
            ..Default::default()
        };
    }
    if matches_tool_dir(given, resolved, home_ref, ctx, None, &[".asdf"]) {
        return PathDiscovery {
            channel: Some(Channel::Pypi),
            py_package_manager_hint: Some(PythonPackageManager::Asdf),
            ..Default::default()
        };
    }
    if matches_tool_dir(given, resolved, home_ref, ctx, None, &[".pyenv", "shims"])
        || matches_tool_dir(
            given,
            resolved,
            home_ref,
            ctx,
            None,
            &[".pyenv", "versions"],
        )
    {
        return PathDiscovery {
            channel: Some(Channel::Pypi),
            py_package_manager_hint: Some(PythonPackageManager::Pyenv),
            ..Default::default()
        };
    }
    if matches_tool_dir(
        given,
        resolved,
        home_ref,
        ctx,
        None,
        &[".local", "share", "mise", "shims"],
    ) {
        return PathDiscovery {
            channel: Some(Channel::Pypi),
            py_package_manager_hint: Some(PythonPackageManager::Mise),
            ..Default::default()
        };
    }

    if let Some(virtual_env) = (ctx.env)("VIRTUAL_ENV") {
        let venv_bin =
            PathBuf::from(&virtual_env).join(if cfg!(windows) { "Scripts" } else { "bin" });
        if resolved.starts_with(&venv_bin) {
            return PathDiscovery {
                channel: Some(Channel::Pypi),
                ..Default::default()
            };
        }
    }

    if kind == FileKind::Script {
        return PathDiscovery {
            channel: Some(Channel::Pypi),
            ..Default::default()
        };
    }

    PathDiscovery::default()
}

/// Location of the virtual/Conda environment `resolved` was launched from,
/// independent of which channel rule (if any) matched.
fn venv_root(ctx: &DiscoveryContext, resolved: &Path) -> Option<String> {
    if let Some(virtual_env) = (ctx.env)("VIRTUAL_ENV") {
        let venv_bin =
            PathBuf::from(&virtual_env).join(if cfg!(windows) { "Scripts" } else { "bin" });
        if resolved.starts_with(&venv_bin) {
            return Some(virtual_env);
        }
    }
    if let Some(conda_prefix) = (ctx.env)("CONDA_PREFIX") {
        let conda_bin =
            PathBuf::from(&conda_prefix).join(if cfg!(windows) { "Scripts" } else { "bin" });
        if resolved.starts_with(&conda_bin) {
            return Some(conda_prefix);
        }
    }
    None
}

fn manager_from_manifest_signals(cwd: &Path) -> Option<PythonPackageManager> {
    const SIGNALS: [(&str, PythonPackageManager); 7] = [
        ("uv.lock", PythonPackageManager::Uv),
        ("poetry.lock", PythonPackageManager::Poetry),
        ("pdm.lock", PythonPackageManager::Pdm),
        ("Pipfile.lock", PythonPackageManager::Pipenv),
        ("environment.yml", PythonPackageManager::Conda),
        ("requirements.txt", PythonPackageManager::Pip),
        ("setup.cfg", PythonPackageManager::Pip),
    ];
    for dir in cwd.ancestors() {
        for (file_name, manager) in SIGNALS {
            if dir.join(file_name).is_file() {
                return Some(manager);
            }
        }
    }
    None
}

fn probe_package_manager(ctx: &DiscoveryContext) -> Option<PythonPackageManager> {
    const CANDIDATES: [(&str, PythonPackageManager); 9] = [
        ("uv", PythonPackageManager::Uv),
        ("pipx", PythonPackageManager::Pipx),
        ("poetry", PythonPackageManager::Poetry),
        ("pdm", PythonPackageManager::Pdm),
        ("pipenv", PythonPackageManager::Pipenv),
        ("conda", PythonPackageManager::Conda),
        ("hatch", PythonPackageManager::Hatch),
        ("rye", PythonPackageManager::Rye),
        ("pip", PythonPackageManager::Pip),
    ];
    for (command, manager) in CANDIDATES {
        if let Some(output) = (ctx.run)(command, &["--version"]) {
            if output.success {
                return Some(manager);
            }
        }
    }
    None
}

/// Resolves the Python package manager for a `pypi`-channel install, in
/// order: a hint already implied by the install location (e.g. a `uv tool`
/// dir), then managed-project manifest/lockfile signals, then a presence
/// probe of installed package managers.
fn resolve_package_manager(
    ctx: &DiscoveryContext,
    cwd: &Path,
    hint: Option<PythonPackageManager>,
) -> Option<PythonPackageManager> {
    hint.or_else(|| manager_from_manifest_signals(cwd))
        .or_else(|| probe_package_manager(ctx))
}

/// Fields discoverable from a `dbt` executable's path alone, independent of
/// whether the caller can also introspect its distribution/generation (the
/// current process) or must fall back to a `--version` probe (some other
/// `dbt` found on `PATH`).
struct DiscoveredDistFields {
    channel: Option<Channel>,
    distribution_override: Option<Distribution>,
    py_package_manager: Option<PythonPackageManager>,
    py_venv_root: Option<String>,
    upgrade_cmd: Option<String>,
    uninstall_cmd: Option<String>,
}

/// The shared core behind both `get_current` and the legacy-dbt fallback, so
/// the two can never drift: only the distribution/generation source differs
/// between "this binary" and "some other dbt we can't introspect".
fn discover_from_path(
    ctx: &DiscoveryContext,
    cwd: &Path,
    given: &Path,
    resolved: &Path,
    kind: FileKind,
) -> DiscoveredDistFields {
    let path_discovery = discover_channel(ctx, given, resolved, kind);
    let py_venv_root = venv_root(ctx, resolved);
    let py_package_manager = if path_discovery.channel == Some(Channel::Pypi) {
        resolve_package_manager(ctx, cwd, path_discovery.py_package_manager_hint)
    } else {
        None
    };
    let (upgrade_cmd, uninstall_cmd) = path_discovery.command_strings(py_package_manager);
    DiscoveredDistFields {
        channel: path_discovery.channel,
        distribution_override: path_discovery.distribution_override,
        py_package_manager,
        py_venv_root,
        upgrade_cmd,
        uninstall_cmd,
    }
}

fn current_cwd() -> PathBuf {
    env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn get_current(command_name: &str) -> FsResult<DistInfo> {
    let exe_path = env::current_exe().map_err(|e| {
        fs_err!(ErrorCode::IoError, "Failed to get current executable")
            .with_cause(WrappedError::Io(e))
    })?;

    let ctx = DiscoveryContext::real();
    let cwd = current_cwd();
    let ClassifiedPath {
        given,
        resolved,
        kind,
    } = classify_path(&exe_path);
    let fields = discover_from_path(&ctx, &cwd, &given, &resolved, kind);

    Ok(DistInfo {
        path: resolved.to_string_lossy().into_owned(),
        channel: fields.channel,
        distribution: fields
            .distribution_override
            .or_else(|| Some(distribution_from_name(command_name))),
        generation: Generation::V2,
        py_package_manager: fields.py_package_manager,
        py_venv_root: fields.py_venv_root,
        upgrade_cmd: fields.upgrade_cmd,
        uninstall_cmd: fields.uninstall_cmd,
    })
}

/// Upper bound on how long a single on-PATH `dbt` is given to answer
/// discovery probes before its result is given up on. `get_at_path` can make
/// up to two sequential subprocess calls per install (the native
/// `get-distribution-info` introspection, then a `--version` fallback), each
/// individually bounded by `NORMAL_WAIT + GRACE_WAIT` in `real_run` — this
/// needs enough headroom to cover both worst cases plus a little slack for
/// filesystem probing, rather than cutting a well-behaved second probe off
/// mid-flight because the first one had to be killed.
const DISCOVERY_TIMEOUT: Duration =
    Duration::from_secs(2 * (NORMAL_WAIT.as_secs() + GRACE_WAIT.as_secs()) + 5);

fn get_all_in_path(command_name: &str) -> FsResult<Vec<DistInfo>> {
    discover_all_in_path_value(env::var_os("PATH").as_deref(), command_name)
}

fn discover_all_in_path_value(
    path_var: Option<&std::ffi::OsStr>,
    command_name: &str,
) -> FsResult<Vec<DistInfo>> {
    let Some(path_var) = path_var else {
        return Ok(vec![]);
    };

    let exe_names: &[&str] = if cfg!(windows) {
        &["dbt.exe", "dbt"]
    } else {
        &["dbt"]
    };

    let mut seen = HashSet::new();
    let mut resolved_paths: Vec<PathBuf> = Vec::new();
    for dir in env::split_paths(path_var) {
        for name in exe_names {
            let candidate = dir.join(name);
            if is_executable(&candidate) {
                let key = dbt_common::stdfs::canonicalize(&candidate)
                    .unwrap_or_else(|_| candidate.clone());
                if seen.insert(key) {
                    resolved_paths.push(candidate);
                }
                break;
            }
        }
    }

    // Each resolved path is discovered independently (its own subprocess
    // spawns, its own filesystem probes), so run them concurrently rather
    // than paying for the sum of every install's discovery time instead of
    // just the slowest one.
    //
    // These are detached threads rather than scoped ones: a `dbt` found on
    // PATH is an arbitrary, possibly badly-behaved program, and a scoped
    // thread must be joined before the scope can exit, so a single hung
    // subprocess would block discovery forever. Detached threads let us give
    // up on a slow probe after DISCOVERY_TIMEOUT and move on; the abandoned
    // thread (and its subprocess, if still running) is left to finish on its
    // own rather than taking the whole process down with it.
    let receivers: Vec<mpsc::Receiver<FsResult<DistInfo>>> = resolved_paths
        .into_iter()
        .map(|path| {
            let command_name = command_name.to_string();
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                let _ = tx.send(get_at_path(&path, &command_name));
            });
            rx
        })
        .collect();

    receivers
        .into_iter()
        .filter_map(|rx| rx.recv_timeout(DISCOVERY_TIMEOUT).ok())
        .collect()
}

/// Whether `file_path` resolves to the currently running executable.
///
/// A `get-distribution-info` invocation whose target is itself must be
/// short-circuited to [`get_current`] rather than spawned as a subprocess:
/// spawning would run the exact same command again, which would in turn
/// detect that its own target is itself and spawn again, forever.
fn is_current_executable(file_path: &Path) -> bool {
    let Ok(current) = env::current_exe() else {
        return false;
    };
    let current = dbt_common::stdfs::canonicalize(&current).unwrap_or(current);
    let target =
        dbt_common::stdfs::canonicalize(file_path).unwrap_or_else(|_| file_path.to_path_buf());
    current == target
}

fn get_at_path(file_path: &Path, command_name: &str) -> FsResult<DistInfo> {
    if !is_executable(file_path) {
        return err!(
            ErrorCode::FileNotFound,
            "File not found or is not executable: {}",
            file_path.to_str().unwrap_or("<invalid path>")
        );
    }

    if is_current_executable(file_path) {
        return get_current(command_name);
    }

    let ctx = DiscoveryContext::real();
    let classified = classify_path(file_path);

    // Only a native v2 binary can possibly understand `internal
    // get-distribution-info`; a script (pip/pipx/asdf/pyenv/... install) is
    // guaranteed to reject it, so skip straight to the legacy `--version`
    // fallback rather than paying for a doomed subprocess spawn — for a
    // Python-based `dbt-core` this roughly halves the interpreter-startup
    // cost paid per PATH entry.
    if classified.kind == FileKind::NativeBinary {
        if let Some(dist_info) = introspect_for_dist_info(&ctx, file_path) {
            return Ok(dist_info);
        }
    }

    discover_dist_info_from_legacy_dbt(&ctx, classified)
}

// Given a path to a dbt executable, try running `dbt internal
// get-distribution-info`, and if it succeeds, parse the result. Any
// failures (not found, non-zero exit, malformed JSON) are treated as None.
fn introspect_for_dist_info(ctx: &DiscoveryContext, file_path: &Path) -> Option<DistInfo> {
    let stdout = run_dbt_internal(ctx, file_path)?;
    parse_dist_info_json(&stdout)
}

fn run_dbt_internal(ctx: &DiscoveryContext, file_path: &Path) -> Option<String> {
    let path_str = file_path.to_str()?;
    // Passing the target path explicitly (rather than relying on the
    // child's own arg0) is what lets a freshly downloaded `dbt` classify a
    // different on-path install.
    let output = (ctx.run)(path_str, &["internal", "get-distribution-info", path_str])?;
    output.success.then_some(output.stdout)
}

fn parse_dist_info_json(stdout: &str) -> Option<DistInfo> {
    serde_json::from_str(stdout).ok()
}

fn discover_dist_info_from_legacy_dbt(
    ctx: &DiscoveryContext,
    classified: ClassifiedPath,
) -> FsResult<DistInfo> {
    let cwd = current_cwd();
    let ClassifiedPath {
        given,
        resolved,
        kind,
    } = classified;
    let fields = discover_from_path(ctx, &cwd, &given, &resolved, kind);

    // A Homebrew-tap conflict (detected via the path alone, above) already
    // tells us the distribution — e.g. a non-brew-installed dbt Cloud CLI
    // could otherwise print a `--version` line that spuriously parses as a
    // v2 banner. Skip the probe entirely once the path resolver has an
    // answer, rather than letting it clobber `generation` with a bogus
    // reading for a program that was never a v1/v2 `dbt` in the first place.
    let version_probe = fields
        .distribution_override
        .is_none()
        .then(|| probe_generation_and_distribution(ctx, &given))
        .flatten();
    let generation = version_probe.map_or(Generation::NotApplicable, |(generation, _)| generation);
    let distribution = fields
        .distribution_override
        .or_else(|| version_probe.map(|(_, distribution)| distribution));

    Ok(DistInfo {
        path: resolved.to_string_lossy().into_owned(),
        channel: fields.channel,
        distribution,
        generation,
        py_package_manager: fields.py_package_manager,
        py_venv_root: fields.py_venv_root,
        upgrade_cmd: fields.upgrade_cmd,
        uninstall_cmd: fields.uninstall_cmd,
    })
}

/// When a `dbt` doesn't support `dbt internal get-distribution-info` (a
/// legacy v1 Python `dbt-core`, or a v2 binary that predates the plumbing
/// command), fall back to `<dbt> --version` to recover a generation and
/// distribution signal.
///
/// v1 prints a multi-line `Core:\n  - installed: ...` block and is always
/// the OSS distribution. v2 prints a single `<name> <version>` banner line
/// (clap's default `--version` format): the OSS v2 build (`dbt-sa-cli`)
/// brands itself `dbt-core`, while the proprietary Fusion build brands
/// itself something else — currently `dbt-fusion`, though that string is
/// cosmetic and may change (e.g. dropping "fusion") — so `dbt-core` is the
/// one name checked for and everything else is treated as Fusion.
///
/// Callers should only reach for this once the path-based channel resolver
/// couldn't already name a distribution outright (e.g. a Homebrew-tap
/// conflict identifying the dbt Cloud CLI) — that binary isn't a v1/v2 `dbt`
/// at all, and its own `--version` output isn't something this function
/// knows how to interpret.
fn probe_generation_and_distribution(
    ctx: &DiscoveryContext,
    file_path: &Path,
) -> Option<(Generation, Distribution)> {
    let path_str = file_path.to_str()?;
    let output = (ctx.run)(path_str, &["--version"])?;
    if !output.success {
        return None;
    }
    classify_version_output(&output.stdout)
}

fn classify_version_output(stdout: &str) -> Option<(Generation, Distribution)> {
    if stdout.contains("Core:") {
        return Some((Generation::V1, Distribution::OSS));
    }
    if stdout.starts_with("dbt Cloud CLI") {
        return Some((Generation::NotApplicable, Distribution::CloudCLI));
    }
    // Validation check: OSS, Fusion, and Cloud CLI contain "dbt"
    // in the output.
    if !stdout.contains("dbt") {
        return None;
    }
    let mut parts = stdout.split_whitespace();
    let name = parts.next()?;
    let version = parts.next()?;
    if !version.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    Some((Generation::V2, distribution_from_name(name)))
}

/// Classifies a CLI-brand name (the same string printed as the leading token
/// of a v2 binary's `--version` banner, and injected into the running
/// process as its own `command_name`) into a [Distribution]. The OSS v2
/// build (`dbt-sa-cli`) brands itself `dbt-core`; every other name is
/// treated as Fusion.
fn distribution_from_name(name: &str) -> Distribution {
    if name == "dbt-core" {
        Distribution::OSS
    } else {
        Distribution::Fusion
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn no_run(_: &str, _: &[&str]) -> Option<ProcessOutput> {
        None
    }

    fn env_from(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // ---- sniff_file_kind ----

    #[test]
    fn sniffs_elf_as_native_binary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dbt");
        std::fs::write(&path, [0x7f, b'E', b'L', b'F', 0, 0, 0, 0]).unwrap();
        assert_eq!(sniff_file_kind(&path), FileKind::NativeBinary);
    }

    #[test]
    fn sniffs_macho_as_native_binary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dbt");
        std::fs::write(&path, [0xCF, 0xFA, 0xED, 0xFE, 0, 0, 0, 0]).unwrap();
        assert_eq!(sniff_file_kind(&path), FileKind::NativeBinary);
    }

    #[test]
    fn sniffs_pe_as_native_binary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dbt.exe");
        std::fs::write(&path, [b'M', b'Z', 0, 0]).unwrap();
        assert_eq!(sniff_file_kind(&path), FileKind::NativeBinary);
    }

    #[test]
    fn sniffs_shebang_as_script() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dbt");
        std::fs::write(&path, b"#!/usr/bin/env python\nprint('hi')\n").unwrap();
        assert_eq!(sniff_file_kind(&path), FileKind::Script);
    }

    #[test]
    fn sniffs_empty_file_as_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dbt");
        std::fs::write(&path, []).unwrap();
        assert_eq!(sniff_file_kind(&path), FileKind::Unknown);
    }

    #[test]
    fn sniffs_missing_file_as_unknown() {
        let path = PathBuf::from("/nonexistent/path/dbt");
        assert_eq!(sniff_file_kind(&path), FileKind::Unknown);
    }

    // ---- is_executable ----

    #[test]
    #[cfg(unix)]
    fn is_executable_true_when_exec_bit_set() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dbt");
        std::fs::write(&path, b"#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(is_executable(&path));
    }

    #[test]
    #[cfg(unix)]
    fn is_executable_false_when_exec_bit_unset() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dbt");
        std::fs::write(&path, b"#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(!is_executable(&path));
    }

    #[test]
    fn is_executable_false_for_missing_file() {
        assert!(!is_executable(Path::new("/nonexistent/path/dbt")));
    }

    // ---- classify_path ----

    #[test]
    #[cfg(unix)]
    fn classify_path_resolves_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("real-dbt");
        std::fs::write(&target, [0x7f, b'E', b'L', b'F']).unwrap();
        let link = dir.path().join("dbt");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let classified = classify_path(&link);
        assert_eq!(classified.given, link);
        assert_eq!(
            classified.resolved,
            dbt_common::stdfs::canonicalize(&target).unwrap()
        );
        assert_eq!(classified.kind, FileKind::NativeBinary);
    }

    #[test]
    fn classify_path_falls_back_to_given_for_dangling_path() {
        let path = PathBuf::from("/nonexistent/path/dbt");
        let classified = classify_path(&path);
        assert_eq!(classified.resolved, path);
        assert_eq!(classified.kind, FileKind::Unknown);
    }

    // ---- discover_channel ----

    #[test]
    fn standalone_native_binary_at_local_bin() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let path = PathBuf::from("/home/user/.local/bin/dbt");
        let result = discover_channel(&ctx, &path, &path, FileKind::NativeBinary);
        assert_eq!(result.channel, Some(Channel::Standalone));
    }

    #[test]
    fn pip_user_script_at_local_bin_is_not_standalone() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let path = PathBuf::from("/home/user/.local/bin/dbt");
        let result = discover_channel(&ctx, &path, &path, FileKind::Script);
        assert_eq!(result.channel, Some(Channel::Pypi));
    }

    #[test]
    fn brew_dbt_labs_dbt_tap_resolves_brew_channel() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let run = |cmd: &str, args: &[&str]| -> Option<ProcessOutput> {
            if cmd != "brew" {
                return None;
            }
            if matches!(args, ["--prefix"]) {
                return Some(ProcessOutput {
                    success: true,
                    stdout: "/opt/homebrew\n".to_string(),
                    stderr: String::new(),
                });
            }
            if matches!(args, ["info", "dbt", "--json=v2"]) {
                return Some(ProcessOutput {
                    success: true,
                    stdout: r#"{"formulae":[{"tap":"dbt-labs/dbt"}]}"#.to_string(),
                    stderr: String::new(),
                });
            }
            None
        };
        let ctx = DiscoveryContext {
            env: &env,
            run: &run,
        };
        let path = PathBuf::from("/opt/homebrew/bin/dbt");
        let result = discover_channel(&ctx, &path, &path, FileKind::NativeBinary);
        assert_eq!(result.channel, Some(Channel::Brew));
        assert_eq!(result.distribution_override, None);
    }

    #[test]
    fn brew_dbt_cli_tap_resolves_cloud_cli_override() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let run = |cmd: &str, args: &[&str]| -> Option<ProcessOutput> {
            if cmd != "brew" {
                return None;
            }
            if matches!(args, ["--prefix"]) {
                return Some(ProcessOutput {
                    success: true,
                    stdout: "/opt/homebrew\n".to_string(),
                    stderr: String::new(),
                });
            }
            if matches!(args, ["info", "dbt", "--json=v2"]) {
                return Some(ProcessOutput {
                    success: true,
                    stdout: r#"{"formulae":[{"tap":"dbt-labs/dbt-cli"}]}"#.to_string(),
                    stderr: String::new(),
                });
            }
            None
        };
        let ctx = DiscoveryContext {
            env: &env,
            run: &run,
        };
        let path = PathBuf::from("/opt/homebrew/bin/dbt");
        let result = discover_channel(&ctx, &path, &path, FileKind::NativeBinary);
        assert_eq!(result.channel, None);
        assert_eq!(result.distribution_override, Some(Distribution::CloudCLI));
    }

    #[test]
    fn brew_untrusted_tap_error_resolves_cloud_cli_override() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let run = |cmd: &str, args: &[&str]| -> Option<ProcessOutput> {
            if cmd != "brew" {
                return None;
            }
            if matches!(args, ["--prefix"]) {
                return Some(ProcessOutput {
                    success: true,
                    stdout: "/opt/homebrew\n".to_string(),
                    stderr: String::new(),
                });
            }
            if matches!(args, ["info", "dbt", "--json=v2"]) {
                return Some(ProcessOutput {
                    success: false,
                    stdout: String::new(),
                    stderr: "Error: Refusing to load formula dbt-labs/dbt-cli/dbt from untrusted tap dbt-labs/dbt-cli".to_string(),
                });
            }
            None
        };
        let ctx = DiscoveryContext {
            env: &env,
            run: &run,
        };
        let path = PathBuf::from("/opt/homebrew/bin/dbt");
        let result = discover_channel(&ctx, &path, &path, FileKind::NativeBinary);
        assert_eq!(result.channel, None);
        assert_eq!(result.distribution_override, Some(Distribution::CloudCLI));
    }

    #[test]
    fn brew_prefix_with_unrecognized_tap_falls_back_to_brew() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let run = |cmd: &str, args: &[&str]| -> Option<ProcessOutput> {
            if cmd != "brew" {
                return None;
            }
            if matches!(args, ["--prefix"]) {
                return Some(ProcessOutput {
                    success: true,
                    stdout: "/opt/homebrew\n".to_string(),
                    stderr: String::new(),
                });
            }
            if matches!(args, ["info", "dbt", "--json=v2"]) {
                return Some(ProcessOutput {
                    success: true,
                    stdout: r#"{"formulae":[{"tap":"some-other/tap"}]}"#.to_string(),
                    stderr: String::new(),
                });
            }
            None
        };
        let ctx = DiscoveryContext {
            env: &env,
            run: &run,
        };
        let path = PathBuf::from("/opt/homebrew/bin/dbt");
        let result = discover_channel(&ctx, &path, &path, FileKind::NativeBinary);
        assert_eq!(result.channel, Some(Channel::Brew));
    }

    #[test]
    fn not_under_homebrew_prefix_does_not_resolve_brew() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let run = |cmd: &str, args: &[&str]| -> Option<ProcessOutput> {
            if cmd == "brew" && matches!(args, ["--prefix"]) {
                return Some(ProcessOutput {
                    success: true,
                    stdout: "/opt/homebrew\n".to_string(),
                    stderr: String::new(),
                });
            }
            None
        };
        let ctx = DiscoveryContext {
            env: &env,
            run: &run,
        };
        let path = PathBuf::from("/usr/local/other/dbt");
        let result = discover_channel(&ctx, &path, &path, FileKind::NativeBinary);
        assert_eq!(result.channel, None);
    }

    #[test]
    fn winget_links_dir_resolves_winget_channel() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let path = PathBuf::from(r"C:\Users\user\AppData\Local\Microsoft\WinGet\Links\dbt.exe");
        let result = discover_channel(&ctx, &path, &path, FileKind::NativeBinary);
        assert_eq!(result.channel, Some(Channel::Winget));
    }

    #[test]
    fn winget_packages_dir_resolves_winget_channel() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let given = PathBuf::from(r"C:\Users\user\AppData\Local\Microsoft\WinGet\Links\dbt.exe");
        let resolved = PathBuf::from(
            r"C:\Users\user\AppData\Local\Microsoft\WinGet\Packages\dbtLabs.dbt_abc123\dbt.exe",
        );
        let result = discover_channel(&ctx, &given, &resolved, FileKind::NativeBinary);
        assert_eq!(result.channel, Some(Channel::Winget));
    }

    #[test]
    fn pipx_dir_resolves_pypi_with_pipx_manager() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let given = PathBuf::from("/home/user/.local/bin/dbt");
        let resolved = PathBuf::from("/home/user/.local/pipx/venvs/dbt/bin/dbt");
        let result = discover_channel(&ctx, &given, &resolved, FileKind::Script);
        assert_eq!(result.channel, Some(Channel::Pypi));
        assert_eq!(
            result.py_package_manager_hint,
            Some(PythonPackageManager::Pipx)
        );
    }

    #[test]
    fn uv_tool_dir_resolves_pypi_with_uv_manager() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let path = PathBuf::from("/home/user/.local/share/uv/tools/dbt/bin/dbt");
        let result = discover_channel(&ctx, &path, &path, FileKind::Script);
        assert_eq!(result.channel, Some(Channel::Pypi));
        assert_eq!(
            result.py_package_manager_hint,
            Some(PythonPackageManager::Uv)
        );
    }

    #[test]
    fn rye_tools_dir_resolves_pypi_with_rye_manager() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let path = PathBuf::from("/home/user/.rye/tools/dbt/bin/dbt");
        let result = discover_channel(&ctx, &path, &path, FileKind::Script);
        assert_eq!(result.channel, Some(Channel::Pypi));
        assert_eq!(
            result.py_package_manager_hint,
            Some(PythonPackageManager::Rye)
        );
    }

    #[test]
    fn asdf_shim_resolves_pypi_with_asdf_manager() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let path = PathBuf::from("/home/user/.asdf/shims/dbt");
        let result = discover_channel(&ctx, &path, &path, FileKind::Script);
        assert_eq!(result.channel, Some(Channel::Pypi));
        assert_eq!(
            result.py_package_manager_hint,
            Some(PythonPackageManager::Asdf)
        );
    }

    #[test]
    fn pyenv_shim_resolves_pypi_with_pyenv_manager() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let given = PathBuf::from("/home/user/.pyenv/shims/dbt");
        let resolved = PathBuf::from("/home/user/.pyenv/versions/3.12.0/bin/dbt");
        let result = discover_channel(&ctx, &given, &resolved, FileKind::Script);
        assert_eq!(result.channel, Some(Channel::Pypi));
        assert_eq!(
            result.py_package_manager_hint,
            Some(PythonPackageManager::Pyenv)
        );
    }

    #[test]
    fn mise_shim_resolves_pypi_with_mise_manager() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let path = PathBuf::from("/home/user/.local/share/mise/shims/dbt");
        let result = discover_channel(&ctx, &path, &path, FileKind::Script);
        assert_eq!(result.channel, Some(Channel::Pypi));
        assert_eq!(
            result.py_package_manager_hint,
            Some(PythonPackageManager::Mise)
        );
    }

    #[test]
    fn venv_script_resolves_pypi_without_manager_hint() {
        let map = env_from(&[
            ("HOME", "/home/user"),
            ("VIRTUAL_ENV", "/home/user/project/.venv"),
        ]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let path = PathBuf::from("/home/user/project/.venv/bin/dbt");
        let result = discover_channel(&ctx, &path, &path, FileKind::Script);
        assert_eq!(result.channel, Some(Channel::Pypi));
        assert_eq!(result.py_package_manager_hint, None);
    }

    #[test]
    fn generic_script_falls_back_to_pypi() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let path = PathBuf::from("/usr/local/bin/dbt");
        let result = discover_channel(&ctx, &path, &path, FileKind::Script);
        assert_eq!(result.channel, Some(Channel::Pypi));
    }

    #[test]
    fn unmatched_native_binary_resolves_no_channel() {
        let map = env_from(&[("HOME", "/home/user")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let path = PathBuf::from("/opt/custom/dbt");
        let result = discover_channel(&ctx, &path, &path, FileKind::NativeBinary);
        assert_eq!(result.channel, None);
    }

    // ---- venv_root ----

    #[test]
    fn venv_root_set_when_inside_virtual_env() {
        let map = env_from(&[("VIRTUAL_ENV", "/home/user/project/.venv")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let bin_dir = if cfg!(windows) { "Scripts" } else { "bin" };
        let resolved = PathBuf::from(format!("/home/user/project/.venv/{bin_dir}/dbt"));
        assert_eq!(
            venv_root(&ctx, &resolved),
            Some("/home/user/project/.venv".to_string())
        );
    }

    #[test]
    fn venv_root_none_when_outside_virtual_env() {
        let map = env_from(&[("VIRTUAL_ENV", "/home/user/project/.venv")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let resolved = PathBuf::from("/usr/local/bin/dbt");
        assert_eq!(venv_root(&ctx, &resolved), None);
    }

    #[test]
    fn venv_root_set_for_conda_prefix() {
        let map = env_from(&[("CONDA_PREFIX", "/home/user/miniconda3/envs/proj")]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let bin_dir = if cfg!(windows) { "Scripts" } else { "bin" };
        let resolved = PathBuf::from(format!("/home/user/miniconda3/envs/proj/{bin_dir}/dbt"));
        assert_eq!(
            venv_root(&ctx, &resolved),
            Some("/home/user/miniconda3/envs/proj".to_string())
        );
    }

    // ---- resolve_package_manager ----

    #[test]
    fn hint_short_circuits_manager_resolution() {
        let map = env_from(&[]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("uv.lock"), "").unwrap();
        let manager = resolve_package_manager(&ctx, dir.path(), Some(PythonPackageManager::Pipx));
        assert_eq!(manager, Some(PythonPackageManager::Pipx));
    }

    #[test]
    fn manifest_signal_detected_from_ancestor_dir() {
        let map = env_from(&[]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("poetry.lock"), "").unwrap();
        let nested = dir.path().join("sub").join("dir");
        std::fs::create_dir_all(&nested).unwrap();
        let manager = resolve_package_manager(&ctx, &nested, None);
        assert_eq!(manager, Some(PythonPackageManager::Poetry));
    }

    #[test]
    fn presence_probe_used_when_no_hint_or_manifest() {
        let map = env_from(&[]);
        let env = |n: &str| map.get(n).cloned();
        let run = |cmd: &str, _: &[&str]| -> Option<ProcessOutput> {
            if cmd == "pdm" {
                Some(ProcessOutput {
                    success: true,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            } else {
                None
            }
        };
        let ctx = DiscoveryContext {
            env: &env,
            run: &run,
        };
        let dir = tempfile::tempdir().unwrap();
        let manager = resolve_package_manager(&ctx, dir.path(), None);
        assert_eq!(manager, Some(PythonPackageManager::Pdm));
    }

    #[test]
    fn presence_probe_returns_none_when_nothing_found() {
        let map = env_from(&[]);
        let env = |n: &str| map.get(n).cloned();
        let ctx = DiscoveryContext {
            env: &env,
            run: &no_run,
        };
        let dir = tempfile::tempdir().unwrap();
        let manager = resolve_package_manager(&ctx, dir.path(), None);
        assert_eq!(manager, None);
    }

    // ---- PathDiscovery::command_strings ----

    #[test]
    fn command_strings_for_channel_and_manager() {
        for (channel, distribution_override, manager, expected) in [
            (
                Some(Channel::Standalone),
                None,
                None,
                (Some("dbt system update"), Some("dbt system uninstall")),
            ),
            (
                Some(Channel::Brew),
                None,
                None,
                (Some("brew upgrade dbt"), Some("brew uninstall dbt")),
            ),
            (
                Some(Channel::Winget),
                None,
                None,
                (
                    Some("winget upgrade --id dbtLabs.dbt --exact"),
                    Some("winget uninstall --id dbtLabs.dbt --exact"),
                ),
            ),
            (
                Some(Channel::Pypi),
                None,
                Some(PythonPackageManager::Pip),
                (Some("pip install --upgrade dbt"), Some("pip uninstall dbt")),
            ),
            (
                Some(Channel::Pypi),
                None,
                Some(PythonPackageManager::Uv),
                (Some("uv tool upgrade dbt"), Some("uv tool uninstall dbt")),
            ),
            (None, None, None, (None, None)),
            (Some(Channel::Pypi), None, None, (None, None)),
            (
                None,
                Some(Distribution::CloudCLI),
                None,
                (None, Some("brew uninstall dbt-labs/dbt-cli/dbt")),
            ),
        ] {
            let path_discovery = PathDiscovery {
                channel,
                distribution_override,
                ..Default::default()
            };
            let (upgrade, uninstall) = path_discovery.command_strings(manager);
            assert_eq!(
                (upgrade.as_deref(), uninstall.as_deref()),
                expected,
                "channel={channel:?}, distribution_override={distribution_override:?}, manager={manager:?}"
            );
        }
    }

    // ---- run_dbt_internal / parse_dist_info_json ----

    #[test]
    fn run_dbt_internal_returns_stdout_on_success() {
        let run = |_: &str, _: &[&str]| -> Option<ProcessOutput> {
            Some(ProcessOutput {
                success: true,
                stdout: "{}".to_string(),
                stderr: String::new(),
            })
        };
        let ctx = DiscoveryContext {
            env: &real_env,
            run: &run,
        };
        let result = run_dbt_internal(&ctx, Path::new("/usr/local/bin/dbt"));
        assert_eq!(result.as_deref(), Some("{}"));
    }

    #[test]
    fn run_dbt_internal_none_on_nonzero_exit() {
        let run = |_: &str, _: &[&str]| -> Option<ProcessOutput> {
            Some(ProcessOutput {
                success: false,
                stdout: String::new(),
                stderr: "boom".to_string(),
            })
        };
        let ctx = DiscoveryContext {
            env: &real_env,
            run: &run,
        };
        let result = run_dbt_internal(&ctx, Path::new("/usr/local/bin/dbt"));
        assert_eq!(result, None);
    }

    #[test]
    fn run_dbt_internal_none_when_spawn_fails() {
        let ctx = DiscoveryContext {
            env: &real_env,
            run: &no_run,
        };
        let result = run_dbt_internal(&ctx, Path::new("/usr/local/bin/dbt"));
        assert_eq!(result, None);
    }

    #[test]
    fn parse_dist_info_json_success() {
        let json = r#"{
            "path": "/home/user/.local/share/uv/tools/dbt/bin/dbt",
            "channel": "pypi",
            "distribution": "dbt",
            "generation": "v2",
            "py_package_manager": "uv",
            "py_venv_root": null,
            "upgrade_cmd": "uv tool upgrade dbt",
            "uninstall_cmd": "uv tool uninstall dbt"
        }"#;
        let info = parse_dist_info_json(json).unwrap();
        assert_eq!(info.channel, Some(Channel::Pypi));
        assert_eq!(info.distribution, Some(Distribution::Fusion));
    }

    #[test]
    fn parse_dist_info_json_malformed_returns_none() {
        assert!(parse_dist_info_json("not json").is_none());
    }

    #[test]
    fn parse_dist_info_json_empty_returns_none() {
        assert!(parse_dist_info_json("").is_none());
    }

    // ---- classify_version_output / probe_generation_and_distribution ----

    const V1_VERSION_OUTPUT: &str = "\
Core:
  - installed: 1.12.0
  - latest:    1.12.0 - Up to date!

Plugins:
";

    #[test]
    fn classify_version_output_v1_core_block_is_oss() {
        assert_eq!(
            classify_version_output(V1_VERSION_OUTPUT),
            Some((Generation::V1, Distribution::OSS))
        );
    }

    #[test]
    fn classify_version_output_v2_banner_is_fusion() {
        assert_eq!(
            classify_version_output("dbt-fusion 2.0.0-preview.196\n"),
            Some((Generation::V2, Distribution::Fusion))
        );
    }

    #[test]
    fn classify_version_output_v2_banner_without_fusion_branding_is_still_fusion() {
        // The banner's display name is cosmetic and may change (e.g. drop
        // "fusion"); anything other than the OSS build's `dbt-core` name is
        // treated as Fusion.
        assert_eq!(
            classify_version_output("dbt 2.0.0-preview.196\n"),
            Some((Generation::V2, Distribution::Fusion))
        );
    }

    #[test]
    fn classify_version_output_v2_dbt_core_banner_is_oss() {
        // `dbt-sa-cli` (the OSS-only v2 build) brands its `--version` banner
        // as `dbt-core`, so v2 alone doesn't imply Fusion.
        assert_eq!(
            classify_version_output("dbt-core 2.0.0-preview.200\n"),
            Some((Generation::V2, Distribution::OSS))
        );
    }

    #[test]
    fn classify_version_output_dbt_cloud_cli() {
        assert_eq!(
            classify_version_output(
                "dbt Cloud CLI - 0.40.18 (aa58f643af1725e279e559883b75cf9e26596d51 2026-06-18T20:34:06Z)\n"
            ),
            Some((Generation::NotApplicable, Distribution::CloudCLI))
        );
    }

    #[test]
    fn classify_version_output_none_for_unrecognized_output() {
        assert_eq!(classify_version_output("not a dbt binary\n"), None);
        assert_eq!(classify_version_output(""), None);
    }

    #[test]
    fn probe_generation_and_distribution_none_on_nonzero_exit() {
        let run = |_: &str, _: &[&str]| -> Option<ProcessOutput> {
            Some(ProcessOutput {
                success: false,
                stdout: V1_VERSION_OUTPUT.to_string(),
                stderr: String::new(),
            })
        };
        let ctx = DiscoveryContext {
            env: &real_env,
            run: &run,
        };
        let result = probe_generation_and_distribution(&ctx, Path::new("/usr/local/bin/dbt"));
        assert_eq!(result, None);
    }

    #[test]
    fn probe_generation_and_distribution_none_when_spawn_fails() {
        let ctx = DiscoveryContext {
            env: &real_env,
            run: &no_run,
        };
        let result = probe_generation_and_distribution(&ctx, Path::new("/usr/local/bin/dbt"));
        assert_eq!(result, None);
    }

    // ---- get_all_in_path / discover_all_in_path_value ----

    #[test]
    fn discover_all_in_path_value_none_returns_empty() {
        let result = discover_all_in_path_value(None, "dbt-core").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    #[cfg(unix)]
    fn discover_all_in_path_value_finds_one_executable() {
        use std::os::unix::fs::PermissionsExt;

        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();

        // dir1 has a non-executable dbt; dir2 has the real (executable) one.
        std::fs::write(dir1.path().join("dbt"), b"#!/bin/sh\n").unwrap();

        let exe = dir2.path().join("dbt");
        std::fs::write(&exe, b"#!/bin/sh\nexit 1\n").unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();

        let path_var = env::join_paths([dir1.path(), dir2.path()]).unwrap();
        let result = discover_all_in_path_value(Some(path_var.as_os_str()), "dbt-core").unwrap();
        assert_eq!(result.len(), 1);
    }

    // ---- get_at_path ----

    #[test]
    fn get_at_path_errors_for_missing_file() {
        let result = get_at_path(Path::new("/nonexistent/path/dbt"), "dbt-core");
        assert!(result.is_err());
    }

    #[test]
    #[cfg(unix)]
    fn get_at_path_falls_back_to_legacy_detection() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("dbt");
        // A script that isn't a real dbt: `internal get-distribution-info`
        // will "succeed" (exit 0) but print nothing parseable as DistInfo,
        // so introspection fails and legacy detection takes over.
        std::fs::write(&exe, b"#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();

        let result = get_at_path(&exe, "dbt-core").unwrap();
        assert_eq!(result.generation, Generation::NotApplicable);
        assert_eq!(
            result.path,
            dbt_common::stdfs::canonicalize(&exe)
                .unwrap()
                .to_string_lossy()
                .into_owned()
        );
    }

    #[test]
    #[cfg(unix)]
    fn get_at_path_falls_back_to_version_probe_for_v1_dbt_core() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("dbt");
        // `internal get-distribution-info` isn't a recognized subcommand for
        // this legacy dbt-core stand-in, so it fails; `--version` prints the
        // classic dbt-core block.
        std::fs::write(
            &exe,
            format!(
                "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then cat <<'EOF'\n{V1_VERSION_OUTPUT}EOF\nelse exit 1\nfi\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();

        let result = get_at_path(&exe, "dbt-core").unwrap();
        assert_eq!(result.generation, Generation::V1);
        assert_eq!(result.distribution, Some(Distribution::OSS));
    }

    #[test]
    #[cfg(unix)]
    fn get_at_path_falls_back_to_version_probe_for_v2_binary() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("dbt");
        // A pre-`dbt internal` v2 binary: `--version` prints the plain
        // clap-style banner.
        std::fs::write(
            &exe,
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'dbt-fusion 2.0.0-preview.196'; else exit 1; fi\n",
        )
        .unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();

        let result = get_at_path(&exe, "dbt-core").unwrap();
        assert_eq!(result.generation, Generation::V2);
        assert_eq!(result.distribution, Some(Distribution::Fusion));
    }

    #[test]
    #[cfg(unix)]
    fn get_at_path_falls_back_to_version_probe_for_v2_oss_binary() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("dbt");
        // The OSS-only v2 build (`dbt-sa-cli`) brands its `--version` banner
        // as `dbt-core`, even though it's a v2 binary, not the legacy v1
        // Python `dbt-core`.
        std::fs::write(
            &exe,
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'dbt-core 2.0.0-preview.200'; else exit 1; fi\n",
        )
        .unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();

        let result = get_at_path(&exe, "dbt-core").unwrap();
        assert_eq!(result.generation, Generation::V2);
        assert_eq!(result.distribution, Some(Distribution::OSS));
    }

    #[test]
    fn get_at_path_targeting_the_running_binary_short_circuits_to_current() {
        // Regression test: this must not spawn a subprocess. If it did, that
        // subprocess would hit this exact same "target is myself" case and
        // spawn again, forever.
        let current = env::current_exe().unwrap();
        let result = get_at_path(&current, "dbt-core").unwrap();
        assert_eq!(result.generation, Generation::V2);
        assert_eq!(result.distribution, Some(Distribution::OSS));
    }

    #[test]
    fn is_current_executable_true_for_current_exe() {
        let current = env::current_exe().unwrap();
        assert!(is_current_executable(&current));
    }

    #[test]
    fn is_current_executable_false_for_other_path() {
        assert!(!is_current_executable(Path::new("/nonexistent/path/dbt")));
    }

    // ---- get_current (smoke test against the real test binary) ----

    #[test]
    fn get_current_succeeds_against_real_binary() {
        let info = get_current("dbt-core").unwrap();
        assert_eq!(info.generation, Generation::V2);
        assert_eq!(info.distribution, Some(Distribution::OSS));
        assert!(!info.path.is_empty());
        assert_eq!(
            Path::new(&info.path),
            dbt_common::stdfs::canonicalize(env::current_exe().unwrap()).unwrap()
        );
    }
}
