use std::env;

#[cfg(not(target_os = "windows"))]
use run_script::ScriptOptions;

#[cfg(target_os = "windows")]
use std::{
    fs::File,
    io::Write,
    process::{Command, Stdio},
};

#[cfg(target_os = "windows")]
use uuid::Uuid;

use dbt_common::{ErrorCode, err};
use dbt_common::{FsResult, constants::DBT_CDN_URL};

/// Enforces that dbt may remove its own binary, raising a `NotSupported` error
/// otherwise. Only the standalone installer's binary is self-managed; removing a
/// binary owned by Homebrew, pip, or winget would leave that manager pointing at
/// a file that no longer exists, so we refuse and surface that manager's
/// uninstall command instead.
fn ensure_is_not_managed_installation(
    install_method: crate::install_method::InstallMethod,
) -> FsResult<()> {
    if install_method.is_self_updatable() {
        return Ok(());
    }
    match install_method.uninstall_command() {
        Some(command) => err!(
            ErrorCode::NotSupported,
            "dbt was installed via {}. To uninstall, run:\n\n    {}\n\n\
             (Removing the binary here would leave {} thinking dbt is still installed.)",
            install_method.label(),
            command,
            install_method.label(),
        ),
        None => err!(
            ErrorCode::NotSupported,
            "dbt was installed by another package manager, so it can't uninstall itself. \
             Please uninstall dbt using the package manager you installed it with."
        ),
    }
}

#[cfg_attr(target_os = "windows", allow(unreachable_code))]
pub async fn exec_uninstall() -> FsResult<()> {
    ensure_is_not_managed_installation(crate::install_method::InstallMethod::detect())?;

    println!("Removing dbt from your system");

    let mut curr_path = String::new();
    match env::current_exe() {
        Ok(exe_path) => {
            let _ = &exe_path.to_str().unwrap().clone_into(&mut curr_path);
        }

        Err(_e) => {
            return err!(ErrorCode::IoError, "Failed to get current exe path.");
        }
    };

    let mut pre_string: String = "Current exe at ".to_owned();
    pre_string.push_str(&curr_path);
    //console.println(Prty::progress(ANALYZING, &pre_string, ""));

    // Download appropriate script based on platform
    #[cfg(not(target_os = "windows"))]
    let script_name = "uninstall.sh";

    #[cfg(target_os = "windows")]
    let script_name = "uninstall.ps1";

    let script_url = format!("{DBT_CDN_URL}/install/{script_name}");
    let response = reqwest::get(&script_url)
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    let script = response
        .text()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    #[cfg(not(target_os = "windows"))]
    {
        let args = vec![curr_path.to_string()];
        let options = ScriptOptions::new();
        let (code, _, _) = run_script::run(&script, &args, &options).unwrap();
        if code != 0 {
            return err!(ErrorCode::IoError, "Error: Failed to uninstall dbt.");
        }
    }

    #[cfg(target_os = "windows")]
    {
        // Create a temporary directory for the script with a unique filename
        let temp_dir = env::temp_dir();
        let unique_id = Uuid::new_v4().to_string();
        let script_path = temp_dir.join(format!("uninstall_{unique_id}.ps1"));

        // Write the PowerShell script to a temporary file
        let mut file = match File::create(&script_path) {
            Ok(file) => file,
            Err(e) => {
                return err!(
                    ErrorCode::IoError,
                    "Failed to create temporary script file: {}",
                    e
                );
            }
        };

        if let Err(e) = file.write_all(script.as_bytes()) {
            return err!(
                ErrorCode::IoError,
                "Failed to write to temporary script file: {}",
                e
            );
        }

        // Important: Close the file handle before executing
        drop(file);

        let path_str = script_path
            .to_string_lossy()
            .to_string()
            .replace("\\", "\\\\");

        // Determine which PowerShell to use (pwsh vs powershell)
        let ps_exe = if env::var("PSModulePath").is_ok_and(|path| path.contains("PowerShell/7")) {
            "pwsh"
        } else {
            "powershell"
        };

        // Launch PowerShell and exit dbt to release the file lock
        match Command::new(ps_exe)
            .args([
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                &format!("& '{}'", path_str),
            ])
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
        {
            Ok(_child) => {
                // Wait briefly to ensure PowerShell starts
                std::thread::sleep(std::time::Duration::from_millis(100));
                // Exit dbt to release the file lock
                std::process::exit(0);
            }
            Err(e) => {
                return err!(
                    ErrorCode::IoError,
                    "Failed to start uninstall process: {}",
                    e
                );
            }
        }
    }

    println!("Successfully removed dbt.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::install_method::InstallMethod;

    #[test]
    fn direct_install_may_self_uninstall() {
        assert!(ensure_is_not_managed_installation(InstallMethod::Direct).is_ok());
    }

    #[test]
    fn package_managed_installs_surface_their_uninstall_command() {
        for method in [
            InstallMethod::Homebrew,
            InstallMethod::Pip,
            InstallMethod::Winget,
        ] {
            let err = ensure_is_not_managed_installation(method)
                .expect_err(&format!("{method:?} should be blocked"));
            let command = method.uninstall_command().expect("has uninstall command");
            assert!(
                err.context.contains(&command),
                "{method:?} message should mention `{command}`, got: {}",
                err.context
            );
        }
    }

    #[test]
    fn other_install_gets_generic_fallback() {
        let err = ensure_is_not_managed_installation(InstallMethod::Other)
            .expect_err("Other should be blocked");
        assert!(
            err.context
                .contains("package manager you installed it with")
        );
    }
}
