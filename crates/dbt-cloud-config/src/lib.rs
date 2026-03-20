use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
};

pub use dbt_schemas::schemas::{DbtCloudConfig, DbtCloudContext, DbtCloudProject};

/// Returns the expected path to `~/.dbt/dbt_cloud.yml`.
pub fn get_cloud_project_path() -> Result<PathBuf, String> {
    dirs::home_dir()
        .map(|home| home.join(".dbt").join("dbt_cloud.yml"))
        .ok_or_else(|| "Could not determine home directory".to_string())
}

/// Reads and parses `dbt_cloud.yml` at `path`, returning the active [`DbtCloudProject`] if
/// one is configured, or `None` if the file does not exist or no matching project is found.
pub fn parse_active_cloud_project(path: &Path) -> Result<Option<DbtCloudProject>, String> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(format!("Failed to read {}: {}", path.display(), e));
        }
    };

    let config: DbtCloudConfig = dbt_yaml::from_str(&content)
        .map_err(|e| format!("Failed to parse dbt_cloud.yml: {}", e))?;

    let active_project_id = config.context.active_project.clone();
    Ok(config
        .projects
        .into_iter()
        .find(|p| p.project_id == active_project_id))
}
