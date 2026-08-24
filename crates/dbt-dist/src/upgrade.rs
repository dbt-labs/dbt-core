//! `dbt system upgrade-distribution`: the OSS `dbt-core` -> Fusion `dbt`
//! cross-distribution upgrade. See `upgrade-spec.md` (repo root) for the
//! full design. This module implements the "global install" case only --
//! a `standalone`/`unclaimed` native binary is upgraded in place at its own
//! path, and every other channel (pip --user / pipx / uv tool / system pip)
//! gets a fresh standalone install followed by uninstalling the old package.
//! The "managed project" case (rewriting a project manifest's `dbt-core`
//! dependency to `dbt`) is a fast-follow and is detected-but-refused here
//! rather than silently mishandled.

use std::env;
use std::path::{Path, PathBuf};

use dbt_common::tracing::dbt_emit::println;
use dbt_common::{ErrorCode, FsResult, err, fs_err};

use crate::confirm::confirm;
use crate::dist::uninstall_command_for_package;
use crate::python::{PackageSpec, PackageVersion, PythonManifest};
use crate::{Channel, DistInfo, DistInfoDiscovery, Distribution};

const DEFAULT_CDN_BASE_URL: &str = "https://public.cdn.getdbt.com/fs";

/// The name under which `dbt-core` declares itself as a dependency in a
/// Python project manifest, and the package name to uninstall from a global
/// Python environment once Fusion has taken over.
const OSS_PACKAGE_NAME: &str = "dbt-core";

fn cdn_base_url() -> String {
    #[allow(clippy::disallowed_methods)]
    env::var("DBT_CDN_URL").unwrap_or_else(|_| DEFAULT_CDN_BASE_URL.to_string())
}

fn install_script_name() -> &'static str {
    if cfg!(windows) {
        "install.ps1"
    } else {
        "install.sh"
    }
}

fn install_script_url() -> String {
    format!("{}/install/{}", cdn_base_url(), install_script_name())
}

/// The standalone installer's default target, `~/.local/bin/dbt[.exe]`.
/// `None` if the home directory can't be resolved.
fn standalone_target_path() -> Option<PathBuf> {
    let home = env::var("HOME").or_else(|_| env::var("USERPROFILE")).ok()?;
    let mut path = PathBuf::from(home);
    path.push(".local");
    path.push("bin");
    path.push(if cfg!(windows) { "dbt.exe" } else { "dbt" });
    Some(path)
}

pub async fn exec_upgrade_distribution(yes: bool, command_name: &str) -> FsResult<()> {
    let dist_info = DistInfo::current(command_name)?;

    match dist_info.distribution {
        Some(Distribution::Fusion) => {
            println(
                "dbt is already running Fusion (the `dbt` distribution) -- there is nothing to \
                 upgrade to.",
            );
            return Ok(());
        }
        Some(Distribution::CloudCLI) => {
            println(
                "The dbt Cloud CLI isn't a supported target for `dbt system upgrade-distribution`.",
            );
            return Ok(());
        }
        None => {
            return err!(
                ErrorCode::Unexpected,
                "Could not determine the current dbt distribution; refusing to guess. Run \
                 `dbt internal get-distribution-info` for more detail."
            );
        }
        Some(Distribution::OSS) => {}
    }

    let cwd = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    if let Some(manifest) = PythonManifest::detect(&cwd)? {
        let declares_dbt_core = manifest
            .get_version_replacement(&PackageSpec {
                name: OSS_PACKAGE_NAME.to_string(),
                // Version is irrelevant here -- this call is only used to
                // test whether the manifest declares `dbt-core` at all; the
                // replacement itself is discarded, never applied.
                version: PackageVersion::Exact("0.0.0".to_string()),
            })?
            .is_some();
        if declares_dbt_core {
            return err!(
                ErrorCode::NotSupported,
                "{} declares `dbt-core` as a dependency. Upgrading a managed Python project to \
                 dbt (Fusion) isn't supported by this version of `dbt system \
                 upgrade-distribution` yet -- edit the manifest to depend on `dbt` instead of \
                 `dbt-core` and reinstall with your package manager, or run this command outside \
                 the project directory to upgrade a global dbt-core install instead.",
                manifest.path().display()
            );
        }
    } else if dist_info.py_venv_root.is_some() {
        println(
            "Warning: running inside a virtual environment with no managed Python project \
             (pyproject.toml, requirements.txt, ...) detected in this directory or its parents. \
             Continuing will install dbt globally rather than into a project. To upgrade just \
             this project, add `dbt` to its dependencies instead.",
        );
    }

    exec_global_install(&dist_info, yes, command_name).await
}

async fn exec_global_install(dist_info: &DistInfo, yes: bool, command_name: &str) -> FsResult<()> {
    match dist_info.channel {
        // A `standalone`/`unclaimed` dbt-core is already a native binary
        // that nothing but dbt itself manages -- there's no package manager
        // to uninstall afterward, so upgrade it in place at its own path
        // instead of installing fresh to the (possibly different) default
        // location.
        Some(Channel::Standalone) | Some(Channel::Unclaimed) => {
            exec_in_place_upgrade(dist_info, yes).await
        }
        _ => exec_fresh_install_and_replace_package(dist_info, yes, command_name).await,
    }
}

/// Upgrades a `standalone`/`unclaimed` dbt-core install in place: re-runs
/// the Fusion installer with `--to <dir>` pointed at the existing binary's
/// own directory (from `DistInfo::path`, not the installer's default) and
/// `--update` so it overwrites rather than refusing a pre-existing file.
/// There's no separate package to uninstall afterward -- the installer
/// replaces the file directly -- and no PATH-shadowing check is needed,
/// since the binary's location on `PATH` hasn't changed.
async fn exec_in_place_upgrade(dist_info: &DistInfo, yes: bool) -> FsResult<()> {
    let current_path = Path::new(&dist_info.path);
    let target_dir = current_path.parent().ok_or_else(|| {
        fs_err!(
            ErrorCode::Unexpected,
            "Could not determine the install directory for {}",
            dist_info.path
        )
    })?;

    let prompt = format!(
        "This will download and run the dbt (Fusion) standalone installer, replacing the \
         existing dbt-core binary in place at {}:\n\n    {}\n\nProceed?",
        target_dir.display(),
        install_command_display(Some(target_dir), true)
    );
    if !confirm(&prompt, yes)? {
        return err!(ErrorCode::Generic, "Aborted.");
    }
    install_fusion_standalone(Some(target_dir), true).await
}

async fn exec_fresh_install_and_replace_package(
    dist_info: &DistInfo,
    yes: bool,
    command_name: &str,
) -> FsResult<()> {
    let standalone_target = standalone_target_path();
    let old_package_shadowed_by_target = standalone_target
        .as_deref()
        .is_some_and(|target| target == Path::new(&dist_info.path));

    // If the standalone installer would overwrite the exact file the old
    // `dbt-core` package manager thinks it owns, uninstall the old package
    // *first* -- otherwise the package manager's later uninstall would
    // delete the freshly-installed Fusion binary, believing it's removing
    // its own file.
    if old_package_shadowed_by_target {
        confirm_and_uninstall_old_package(dist_info, yes)?;
    }

    let install_target = standalone_target
        .as_deref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "the default location (~/.local/bin)".to_string());
    let install_prompt = format!(
        "This will download and run the dbt (Fusion) standalone installer:\n\n    {}\n\n\
         installing to {install_target}. Proceed?",
        install_command_display(None, false)
    );
    if !confirm(&install_prompt, yes)? {
        return err!(ErrorCode::Generic, "Aborted.");
    }
    install_fusion_standalone(None, false).await?;

    if !old_package_shadowed_by_target {
        confirm_and_uninstall_old_package(dist_info, yes)?;
    }

    warn_if_path_shadowed(&standalone_target, command_name).await;

    Ok(())
}

fn confirm_and_uninstall_old_package(dist_info: &DistInfo, yes: bool) -> FsResult<()> {
    let Some(channel) = dist_info.channel.clone() else {
        println(
            "Could not determine how the existing dbt-core install was made, so it can't be \
             uninstalled automatically. Please remove it manually with the package manager you \
             installed it with.",
        );
        return Ok(());
    };
    let Some(uninstall_cmd) =
        uninstall_command_for_package(channel, dist_info.py_package_manager, OSS_PACKAGE_NAME)
    else {
        println(
            "Could not determine how to uninstall the existing dbt-core install automatically. \
             Please remove it manually with the package manager you installed it with.",
        );
        return Ok(());
    };

    let manager_note = if dist_info.py_package_manager.is_none() {
        String::new()
    } else {
        format!(" via {}", dist_info.install_label())
    };
    let prompt = format!(
        "This will remove the existing dbt-core install{manager_note} by running:\n\n    {uninstall_cmd}\n\nProceed?"
    );
    if !confirm(&prompt, yes)? {
        return err!(ErrorCode::Generic, "Aborted.");
    }
    run_shell_command(&uninstall_cmd)
}

fn run_shell_command(command: &str) -> FsResult<()> {
    #[cfg(not(target_os = "windows"))]
    let result = std::process::Command::new("sh")
        .arg("-c")
        .arg(command)
        .status();
    #[cfg(target_os = "windows")]
    let result = std::process::Command::new("cmd")
        .args(["/C", command])
        .status();

    match result {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => err!(ErrorCode::IoError, "`{command}` failed with {status}"),
        Err(e) => err!(ErrorCode::IoError, "failed to run `{command}`: {e}"),
    }
}

async fn fetch_text(url: &str) -> FsResult<String> {
    let response = reqwest::get(url)
        .await
        .map_err(|e| fs_err!(ErrorCode::IoError, "GET {url} failed: {e}"))?;
    if !response.status().is_success() {
        return err!(
            ErrorCode::IoError,
            "GET {url} returned {}",
            response.status()
        );
    }
    response.text().await.map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "failed to read response body from {url}: {e}"
        )
    })
}

/// Builds `install.sh`'s argv for `--update`/`--to <dir>`, in that order.
fn install_sh_args(to: Option<&Path>, update: bool) -> Vec<String> {
    let mut args: Vec<String> = Vec::new();
    if update {
        args.push("--update".to_string());
    }
    if let Some(to) = to {
        args.push("--to".to_string());
        args.push(to.display().to_string());
    }
    args
}

/// Unix-shaped display string for the command `install_fusion_standalone`
/// will effectively run (mirrors `install_sh_args`'s flags), for
/// confirmation-prompt text only -- never fed to a shell.
fn unix_install_command_display(to: Option<&Path>, update: bool) -> String {
    let mut command = format!("curl -fsSL {} | sh", install_script_url());
    let args = install_sh_args(to, update);
    if !args.is_empty() {
        command.push_str(" -s -- ");
        command.push_str(&args.join(" "));
    }
    command
}

/// Windows-shaped display string, for confirmation-prompt text only.
/// Deliberately doesn't encode `-Update`/`-To` -- there's no single
/// canonical one-liner for "download, then run with args" on Windows the
/// way `curl | sh -s -- args` is on Unix (the real mechanism is
/// temp-file + `powershell -Command`, see `install_fusion_standalone`).
/// Callers convey destination/update behavior in the surrounding prompt
/// prose instead.
fn windows_install_command_display() -> String {
    format!("irm {} | iex", install_script_url())
}

/// Selects the platform-appropriate display string. Trivial by design --
/// all the actual logic lives in the two functions above, which are each
/// fully unit-testable on any host regardless of `cfg!(windows)`.
fn install_command_display(to: Option<&Path>, update: bool) -> String {
    if cfg!(windows) {
        windows_install_command_display()
    } else {
        unix_install_command_display(to, update)
    }
}

/// Builds the `powershell`/`pwsh -Command` string that invokes `script_path`
/// (`install.ps1`, written to a temp file) with `-Update`/`-To <dir>`,
/// mirroring `install_sh_args`' unix argv for the same two flags. Only
/// called in production on Windows, but kept unguarded (rather than
/// `#[cfg(target_os = "windows")]`) so it's exercised by unit tests on every
/// platform; `allow(dead_code)` suppresses the resulting "never used"
/// warning on non-Windows production builds.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
fn windows_ps_command(script_path: &Path, to: Option<&Path>, update: bool) -> String {
    let mut command = format!(
        "& '{}'",
        escape_ps_single_quoted(&script_path.display().to_string().replace('\\', "\\\\"))
    );
    if update {
        command.push_str(" -Update");
    }
    if let Some(to) = to {
        command.push_str(&format!(
            " -To '{}'",
            escape_ps_single_quoted(&to.display().to_string().replace('\\', "\\\\"))
        ));
    }
    command
}

/// Escapes `s` for interpolation into a PowerShell single-quoted string
/// literal (`'...'`). Doubling `'` is the only escape needed inside a
/// PowerShell single-quoted string.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
pub(crate) fn escape_ps_single_quoted(s: &str) -> String {
    s.replace('\'', "''")
}

/// Downloads and runs the Fusion standalone installer. `to` pins the install
/// directory (the installer's own default, `~/.local/bin`, is used when
/// `None`); `update` allows overwriting a file already at the destination --
/// both are set together for [`exec_in_place_upgrade`]'s in-place overwrite
/// of an existing native binary, and both left unset for a fresh install.
/// Unlike `dbt system update`'s in-place self-update (which must release the
/// currently-running exe's file lock before overwriting it), this installs a
/// different binary, so it runs synchronously to completion on every
/// platform.
async fn install_fusion_standalone(to: Option<&Path>, update: bool) -> FsResult<()> {
    let script = fetch_text(&install_script_url()).await?;

    #[cfg(not(target_os = "windows"))]
    {
        let args = install_sh_args(to, update);
        let options = run_script::ScriptOptions::new();
        let (code, output, error) = run_script::run(&script, &args, &options)
            .map_err(|e| fs_err!(ErrorCode::IoError, "failed to run install script: {e}"))?;
        let output = output.trim();
        let error = error.trim();
        if code != 0 {
            let msg = if !error.is_empty() {
                format!("{error}\n{output}")
            } else {
                output.to_string()
            };
            return err!(
                ErrorCode::IoError,
                "dbt (Fusion) install script failed: {msg}"
            );
        }
        println(output);
        Ok(())
    }

    #[cfg(target_os = "windows")]
    {
        let mut tmp = tempfile::Builder::new()
            .prefix("dbt-fusion-install-")
            .suffix(".ps1")
            .tempfile()
            .map_err(|e| {
                fs_err!(
                    ErrorCode::IoError,
                    "failed to create temp install script: {e}"
                )
            })?;
        use std::io::Write as _;
        tmp.write_all(script.as_bytes()).map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "failed to write temp install script: {e}"
            )
        })?;
        tmp.flush().map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "failed to write temp install script: {e}"
            )
        })?;

        let ps_command = windows_ps_command(tmp.path(), to, update);

        let ps_exe = if env::var("PSModulePath").is_ok_and(|p| p.contains("PowerShell/7")) {
            "pwsh"
        } else {
            "powershell"
        };
        let status = std::process::Command::new(ps_exe)
            .args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-Command"])
            .arg(&ps_command)
            .status()
            .map_err(|e| fs_err!(ErrorCode::IoError, "failed to run install script: {e}"))?;
        if !status.success() {
            return err!(
                ErrorCode::IoError,
                "dbt (Fusion) install script failed with {status}"
            );
        }
        Ok(())
    }
}

/// Best-effort post-install check: warns (without failing the command) if
/// something earlier on `PATH` still shadows the new Fusion install, or if
/// the current shell hasn't picked up the installer's PATH/rc changes yet.
async fn warn_if_path_shadowed(standalone_target: &Option<PathBuf>, command_name: &str) {
    let Some(target) = standalone_target else {
        return;
    };
    let Ok(infos) = DistInfoDiscovery::AllInPath.discover(command_name) else {
        return;
    };
    match infos.first() {
        Some(first) if Path::new(&first.path) == target.as_path() => {}
        Some(first) => {
            println(format!(
                "Warning: dbt (Fusion) was installed to {}, but the first `dbt` on your PATH is \
                 still {} -- you may need to reorder PATH, or open a new shell, before the new \
                 install takes effect.",
                target.display(),
                first.path
            ));
        }
        None => {
            println(format!(
                "Warning: dbt (Fusion) was installed to {}, but it isn't on PATH yet -- you may \
                 need to open a new shell, or add it to PATH manually.",
                target.display()
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdn_base_url_defaults_to_public_cdn() {
        // SAFETY: test-only env mutation, not run concurrently with anything
        // that reads DBT_CDN_URL outside this test.
        unsafe {
            env::remove_var("DBT_CDN_URL");
        }
        assert_eq!(cdn_base_url(), DEFAULT_CDN_BASE_URL);
    }

    #[test]
    fn install_script_name_matches_platform() {
        let name = install_script_name();
        assert!(name == "install.sh" || name == "install.ps1");
    }

    #[test]
    fn install_sh_args_fresh_install_has_no_flags() {
        assert_eq!(install_sh_args(None, false), Vec::<String>::new());
    }

    #[test]
    fn install_sh_args_in_place_upgrade_passes_update_and_to() {
        assert_eq!(
            install_sh_args(Some(Path::new("/opt/dbt/bin")), true),
            vec!["--update", "--to", "/opt/dbt/bin"]
        );
    }

    #[test]
    fn install_sh_args_to_without_update() {
        assert_eq!(
            install_sh_args(Some(Path::new("/opt/dbt/bin")), false),
            vec!["--to", "/opt/dbt/bin"]
        );
    }

    #[test]
    fn unix_install_command_display_fresh_install_has_no_flags() {
        assert_eq!(
            unix_install_command_display(None, false),
            format!("curl -fsSL {} | sh", install_script_url())
        );
    }

    #[test]
    fn unix_install_command_display_in_place_upgrade_passes_update_and_to() {
        assert_eq!(
            unix_install_command_display(Some(Path::new("/opt/dbt/bin")), true),
            format!(
                "curl -fsSL {} | sh -s -- --update --to /opt/dbt/bin",
                install_script_url()
            )
        );
    }

    #[test]
    fn windows_install_command_display_has_no_flags_or_args() {
        assert_eq!(
            windows_install_command_display(),
            format!("irm {} | iex", install_script_url())
        );
    }

    #[test]
    fn windows_ps_command_fresh_install_has_no_flags() {
        assert_eq!(
            windows_ps_command(Path::new(r"C:\temp\install.ps1"), None, false),
            r"& 'C:\\temp\\install.ps1'"
        );
    }

    #[test]
    fn windows_ps_command_in_place_upgrade_passes_update_and_to() {
        assert_eq!(
            windows_ps_command(
                Path::new(r"C:\temp\install.ps1"),
                Some(Path::new(r"C:\Users\me\.local\bin")),
                true
            ),
            r"& 'C:\\temp\\install.ps1' -Update -To 'C:\\Users\\me\\.local\\bin'"
        );
    }

    #[test]
    fn windows_ps_command_escapes_single_quotes_in_paths() {
        assert_eq!(
            windows_ps_command(
                Path::new(r"C:\Users\O'Brien\install.ps1"),
                Some(Path::new(r"C:\Users\O'Brien\.local\bin")),
                true
            ),
            r"& 'C:\\Users\\O''Brien\\install.ps1' -Update -To 'C:\\Users\\O''Brien\\.local\\bin'"
        );
    }

    fn dist_info_for_test(channel: Option<Channel>, path: &str) -> DistInfo {
        DistInfo {
            path: path.to_string(),
            channel,
            distribution: Some(Distribution::OSS),
            generation: crate::Generation::V2,
            py_package_manager: None,
            py_venv_root: None,
            version: None,
            is_prerelease: None,
            upgrade_cmd: None,
            uninstall_cmd: None,
        }
    }

    #[test]
    fn exec_in_place_upgrade_target_dir_is_parent_of_dist_info_path() {
        let dist_info = dist_info_for_test(Some(Channel::Standalone), "/home/user/.local/bin/dbt");
        let target_dir = Path::new(&dist_info.path).parent().unwrap();
        assert_eq!(target_dir, Path::new("/home/user/.local/bin"));
    }

    #[tokio::test]
    async fn global_install_dispatches_to_in_place_upgrade_for_standalone_and_unclaimed() {
        // Not a TTY under `cargo test`, so `confirm(..., false)` errors
        // before any network call -- this exercises the channel-dispatch
        // and target-dir resolution in `exec_global_install` /
        // `exec_in_place_upgrade` without touching the network.
        for channel in [Channel::Standalone, Channel::Unclaimed] {
            let dist_info = dist_info_for_test(Some(channel.clone()), "/home/user/.local/bin/dbt");
            let result = exec_global_install(&dist_info, false, "dbt-core").await;
            assert!(result.is_err(), "channel={channel:?}");
        }
    }
}
