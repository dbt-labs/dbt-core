use std::fs;
use std::path::{Path, PathBuf};

/// Guard that temporarily moves dbt_cloud.yml to prevent it from being picked up during tests.
///
/// This guard moves the ~/.dbt/dbt_cloud.yml file to a temporary location when created,
/// and restores it when dropped. This ensures tests run in isolation from any local
/// dbt Cloud configuration.
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
    /// The original path of the dbt_cloud.yml file
    original_path: PathBuf,
    /// The temporary path where the file was moved
    temp_path: Option<PathBuf>,
    /// Whether the file existed and was moved
    was_moved: bool,
}

impl DbtCloudConfigGuard {
    /// Create a new guard that temporarily moves dbt_cloud.yml
    pub fn new() -> Self {
        let home_dir = match dirs::home_dir() {
            Some(dir) => dir,
            None => {
                // No home directory, nothing to guard
                return Self {
                    original_path: PathBuf::new(),
                    temp_path: None,
                    was_moved: false,
                };
            }
        };

        let original_path = home_dir.join(".dbt").join("dbt_cloud.yml");

        // Check if the file exists
        if !original_path.exists() {
            return Self {
                original_path,
                temp_path: None,
                was_moved: false,
            };
        }

        // Create a temporary path by adding a suffix
        let temp_path = original_path.with_extension("yml.test_backup");

        // Move the file
        match fs::rename(&original_path, &temp_path) {
            Ok(_) => Self {
                original_path,
                temp_path: Some(temp_path),
                was_moved: true,
            },
            Err(_) => {
                // Failed to move, proceed without guarding
                Self {
                    original_path,
                    temp_path: None,
                    was_moved: false,
                }
            }
        }
    }

    /// Create a guard with a custom dbt config directory path
    pub fn with_path<P: AsRef<Path>>(dbt_config_dir: P) -> Self {
        let original_path = dbt_config_dir.as_ref().join("dbt_cloud.yml");

        if !original_path.exists() {
            return Self {
                original_path,
                temp_path: None,
                was_moved: false,
            };
        }

        let temp_path = original_path.with_extension("yml.test_backup");

        match fs::rename(&original_path, &temp_path) {
            Ok(_) => Self {
                original_path,
                temp_path: Some(temp_path),
                was_moved: true,
            },
            Err(_) => Self {
                original_path,
                temp_path: None,
                was_moved: false,
            },
        }
    }
}

impl Drop for DbtCloudConfigGuard {
    fn drop(&mut self) {
        if self.was_moved
            && let Some(temp_path) = &self.temp_path
        {
            // Restore the file
            let _ = fs::rename(temp_path, &self.original_path);
        }
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
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_guard_moves_and_restores_file() {
        // Create a temporary directory to act as the .dbt config dir
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("dbt_cloud.yml");

        // Create a test file
        {
            let mut file = fs::File::create(&config_path).unwrap();
            writeln!(file, "test_content").unwrap();
        }

        // Verify file exists
        assert!(config_path.exists());

        {
            // Create guard
            let _guard = DbtCloudConfigGuard::with_path(temp_dir.path());

            // File should not exist at original location
            assert!(!config_path.exists());

            // Backup should exist
            let backup_path = config_path.with_extension("yml.test_backup");
            assert!(backup_path.exists());
        }

        // After guard is dropped, file should be restored
        assert!(config_path.exists());

        // Verify content is preserved
        let content = fs::read_to_string(&config_path).unwrap();
        assert_eq!(content.trim(), "test_content");
    }

    #[test]
    fn test_guard_handles_missing_file() {
        let temp_dir = TempDir::new().unwrap();

        // Create guard for non-existent file
        let guard = DbtCloudConfigGuard::with_path(temp_dir.path());

        // Should not panic and was_moved should be false
        assert!(!guard.was_moved);
    }
}
