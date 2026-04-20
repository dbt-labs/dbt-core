use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::Path;

use tempfile::TempDir;

#[cfg(windows)]
const HOME_ENV_VAR: &str = "USERPROFILE";

#[cfg(not(windows))]
const HOME_ENV_VAR: &str = "HOME";

/// Guard that isolates tests from ambient dbt Cloud config.
///
/// `new()` creates a per-test temporary home directory, copies through
/// `~/.dbt/profiles.yml` when present, and intentionally omits `dbt_cloud.yml`.
/// This avoids cross-process races when tests run in parallel under `nextest`.
///
/// On Unix the guard also symlinks the real home's cache/extension directories
/// (`.cache`, `Library/Caches` on macOS, `.duckdb`, `.snowsql`) into the temp
/// home so subprocesses that rely on `dirs::cache_dir()` or DuckDB extensions
/// keep hitting the populated cache instead of triggering a CDN download on
/// every test (which times out the sidecar's 20s session-ready window).
///
/// The guard restores the original home env var when dropped. This remains
/// process-global state, so overlapping tests in the same process can still
/// interfere with each other.
///
/// # Example
/// ```
/// use dbt_test_utils::DbtCloudConfigGuard;
///
/// fn test_without_cloud_config() {
///     let _guard = DbtCloudConfigGuard::new();
///     // Test code here runs without dbt_cloud.yml being accessible
/// }
/// ```
pub struct DbtCloudConfigGuard {
    /// Per-test temporary home directory.
    temp_home_dir: Option<TempDir>,
    /// Original home env var value restored when the guard is dropped.
    original_home_env: Option<OsString>,
}

impl DbtCloudConfigGuard {
    fn noop() -> Self {
        Self {
            temp_home_dir: None,
            original_home_env: None,
        }
    }

    fn home_env() -> Option<OsString> {
        env::var_os(HOME_ENV_VAR)
    }

    fn set_home_env(path: &Path) {
        unsafe {
            #[allow(clippy::disallowed_methods)]
            env::set_var(HOME_ENV_VAR, path);
        }
    }

    fn restore_home_env(original_home_env: &Option<OsString>) {
        match original_home_env {
            Some(home) => unsafe {
                #[allow(clippy::disallowed_methods)]
                env::set_var(HOME_ENV_VAR, home)
            },
            None => unsafe {
                #[allow(clippy::disallowed_methods)]
                env::remove_var(HOME_ENV_VAR)
            },
        }
    }

    /// Create a new guard that isolates tests from `dbt_cloud.yml`.
    pub fn new() -> Self {
        let temp_home_dir = match tempfile::tempdir() {
            Ok(dir) => dir,
            Err(_) => return Self::noop(),
        };
        let temp_home = temp_home_dir.path().to_path_buf();
        let temp_dbt_dir = temp_home.join(".dbt");

        if fs::create_dir_all(&temp_dbt_dir).is_err() {
            return Self::noop();
        }

        if let Some(source_home) = dirs::home_dir() {
            let source_profiles = source_home.join(".dbt").join("profiles.yml");
            let temp_profiles = temp_dbt_dir.join("profiles.yml");
            if source_profiles.exists() {
                let _ = fs::copy(&source_profiles, &temp_profiles);
            }
            link_preserved_caches(&temp_home, &source_home);
        }

        let original_home_env = Self::home_env();
        Self::set_home_env(&temp_home);

        Self {
            temp_home_dir: Some(temp_home_dir),
            original_home_env,
        }
    }
}

/// Symlink cache/extension directories from the real home into the temp home
/// so subprocesses that look them up via HOME-based paths still find
/// pre-populated content (ADBC driver cache via `dirs::cache_dir()`, DuckDB
/// extensions under `~/.duckdb`, etc.).
#[cfg(unix)]
fn link_preserved_caches(temp_home: &Path, source_home: &Path) {
    use std::os::unix::fs::symlink;

    let mut entries: Vec<&'static str> = vec![".duckdb", ".snowsql"];
    #[cfg(not(target_os = "macos"))]
    entries.push(".cache");

    for entry in entries {
        let src = source_home.join(entry);
        if src.exists() {
            let _ = symlink(&src, temp_home.join(entry));
        }
    }

    #[cfg(target_os = "macos")]
    {
        let src = source_home.join("Library").join("Caches");
        if src.exists() {
            let library = temp_home.join("Library");
            if fs::create_dir_all(&library).is_ok() {
                let _ = symlink(&src, library.join("Caches"));
            }
        }
    }
}

#[cfg(not(unix))]
fn link_preserved_caches(_temp_home: &Path, _source_home: &Path) {}

impl Drop for DbtCloudConfigGuard {
    fn drop(&mut self) {
        if self.temp_home_dir.is_none() {
            return;
        }

        Self::restore_home_env(&self.original_home_env);
    }
}

impl Default for DbtCloudConfigGuard {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    struct HomeEnvGuard {
        original_home: Option<OsString>,
    }

    impl HomeEnvGuard {
        fn set(path: &Path) -> Self {
            let original_home = DbtCloudConfigGuard::home_env();
            DbtCloudConfigGuard::set_home_env(path);
            Self { original_home }
        }
    }

    impl Drop for HomeEnvGuard {
        fn drop(&mut self) {
            DbtCloudConfigGuard::restore_home_env(&self.original_home);
        }
    }

    #[test]
    fn test_new_uses_isolated_home_with_profiles_only() {
        let _lock = crate::env_test_lock::ENV_LOCK.lock().unwrap();

        let source_home = TempDir::new().unwrap();
        let _home_guard = HomeEnvGuard::set(source_home.path());
        let source_dbt_dir = source_home.path().join(".dbt");
        fs::create_dir_all(&source_dbt_dir).unwrap();

        let profiles_path = source_dbt_dir.join("profiles.yml");
        fs::write(
            &profiles_path,
            "fusion_tests:\n  target: duckdb\n  outputs:\n    duckdb:\n      type: duckdb\n      path: '/tmp/test.db'\n      schema: test_schema\n",
        )
        .unwrap();

        fs::write(
            source_dbt_dir.join("dbt_cloud.yml"),
            "version: '1'\ncontext:\n  active_project: '1'\n  active_host: 'cloud.getdbt.com'\nprojects: []\n",
        )
        .unwrap();

        {
            let _guard = DbtCloudConfigGuard::new();
            let isolated_home = PathBuf::from(DbtCloudConfigGuard::home_env().unwrap());
            assert_ne!(isolated_home, source_home.path());
            assert_eq!(dirs::home_dir().unwrap(), isolated_home);

            let isolated_profiles = isolated_home.join(".dbt").join("profiles.yml");
            assert!(isolated_profiles.exists());
            assert_eq!(
                fs::read_to_string(&isolated_profiles).unwrap(),
                fs::read_to_string(&profiles_path).unwrap()
            );
            assert!(!isolated_home.join(".dbt").join("dbt_cloud.yml").exists());
        }

        assert_eq!(
            PathBuf::from(DbtCloudConfigGuard::home_env().unwrap()),
            source_home.path()
        );
        assert_eq!(dirs::home_dir().unwrap(), source_home.path());
    }

    #[cfg(unix)]
    #[test]
    fn test_new_symlinks_cache_directories() {
        let _lock = crate::env_test_lock::ENV_LOCK.lock().unwrap();

        let source_home = TempDir::new().unwrap();
        let _home_guard = HomeEnvGuard::set(source_home.path());

        // Populate a file inside each directory the guard should preserve
        // so we can assert the symlinked view still resolves to it.
        #[cfg(target_os = "macos")]
        let source_cache = source_home.path().join("Library").join("Caches");
        #[cfg(not(target_os = "macos"))]
        let source_cache = source_home.path().join(".cache");
        fs::create_dir_all(&source_cache).unwrap();
        fs::write(source_cache.join("marker"), "cache-marker").unwrap();

        let source_duckdb = source_home.path().join(".duckdb");
        fs::create_dir_all(&source_duckdb).unwrap();
        fs::write(source_duckdb.join("marker"), "duckdb-marker").unwrap();

        let _guard = DbtCloudConfigGuard::new();
        let isolated_home = PathBuf::from(DbtCloudConfigGuard::home_env().unwrap());

        #[cfg(target_os = "macos")]
        let temp_cache = isolated_home.join("Library").join("Caches");
        #[cfg(not(target_os = "macos"))]
        let temp_cache = isolated_home.join(".cache");

        assert_eq!(
            fs::read_to_string(temp_cache.join("marker")).unwrap(),
            "cache-marker"
        );
        assert_eq!(
            fs::read_to_string(isolated_home.join(".duckdb").join("marker")).unwrap(),
            "duckdb-marker"
        );
    }

    #[test]
    fn test_new_handles_missing_profiles() {
        let _lock = crate::env_test_lock::ENV_LOCK.lock().unwrap();

        let source_home = TempDir::new().unwrap();
        let _home_guard = HomeEnvGuard::set(source_home.path());
        fs::create_dir_all(source_home.path().join(".dbt")).unwrap();

        let isolated_home = {
            let _guard = DbtCloudConfigGuard::new();
            let isolated_home = PathBuf::from(DbtCloudConfigGuard::home_env().unwrap());
            assert_ne!(isolated_home, source_home.path());
            assert!(!isolated_home.join(".dbt").join("profiles.yml").exists());
            isolated_home
        };

        assert!(!isolated_home.exists());
        assert_eq!(dirs::home_dir().unwrap(), source_home.path());
    }
}
