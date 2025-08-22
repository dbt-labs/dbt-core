use crate::adapter_config::{
    ConfigMap, DefaultProfileParser, ProfileDefaults, ProfileParser, setup_bigquery_profile,
    setup_databricks_profile, setup_postgres_profile, setup_redshift_profile,
    setup_snowflake_profile,
};
use crate::dbt_cloud_client::{CloudProject, DbtCloudClient, DbtCloudYml};
use crate::pretty_string::GREEN;
use crate::yaml_utils::{has_top_level_key_parsed_file, remove_top_level_key_from_str};
use crate::{ErrorCode, FsResult, fs_err};
use dialoguer::{Confirm, Select};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileOutput {
    #[serde(rename = "type")]
    pub adapter_type: String,
    #[serde(flatten)]
    pub config: ConfigMap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    pub target: String,
    pub outputs: HashMap<String, ProfileOutput>,
}

#[derive(Debug, Clone)]
pub struct ProjectStore {
    config: DbtCloudYml,
}

impl ProjectStore {
    pub fn from_dbt_cloud_yml() -> FsResult<Option<Self>> {
        let home_dir = match dirs::home_dir() {
            Some(dir) => dir,
            None => return Ok(None),
        };

        let dbt_cloud_config_path = home_dir.join(".dbt").join("dbt_cloud.yml");
        if !dbt_cloud_config_path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&dbt_cloud_config_path)?;
        let config: DbtCloudYml = dbt_serde_yaml::from_str(&content)
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to parse dbt_cloud.yml: {}", e))?;

        Ok(Some(Self { config }))
    }

    pub fn get_active_project(&self) -> Option<&CloudProject> {
        let active_project_id = &self.config.context.active_project;
        self.config
            .projects
            .iter()
            .find(|project| project.project_id == *active_project_id)
    }

    pub fn get_all_projects(&self) -> &Vec<CloudProject> {
        &self.config.projects
    }

    pub fn get_active_project_id(&self) -> &str {
        &self.config.context.active_project
    }

    pub fn get_base_url(&self, project_id: Option<&str>) -> String {
        if let Some(project_id) = project_id {
            if let Some(project) = self
                .config
                .projects
                .iter()
                .find(|p| p.project_id == project_id)
            {
                return format!("https://{}", project.account_host);
            }
        }

        format!("https://{}", self.config.context.active_host)
    }
}

pub struct ProfileSetup {
    pub profiles_dir: String,
}

/// Merge cloud config with existing config, with existing config taking priority
fn merge_configs(
    cloud_config: Option<ConfigMap>,
    existing_config: Option<&ConfigMap>,
) -> Option<ConfigMap> {
    match (cloud_config, existing_config) {
        (Some(mut cloud), Some(existing)) => {
            // Start with cloud config as base, then override with existing values
            for (key, value) in existing {
                cloud.insert(key.clone(), value.clone());
            }
            Some(cloud)
        }
        (Some(cloud), None) => Some(cloud),
        (None, Some(existing)) => Some(existing.clone()),
        (None, None) => None,
    }
}

impl ProfileSetup {
    pub fn new(profiles_dir: String) -> Self {
        Self { profiles_dir }
    }

    pub fn get_available_adapters() -> Vec<&'static str> {
        vec![
            "snowflake",
            "databricks",
            "bigquery",
            "redshift",
            // "postgres", // TODO (Elias): Add back once we have Postgres support
        ]
    }

    pub fn ask_for_adapter_choice(default_adapter: Option<&str>) -> FsResult<String> {
        let adapters = Self::get_available_adapters();
        let default_index = default_adapter
            .and_then(|d| adapters.iter().position(|a| a.eq_ignore_ascii_case(d)))
            .unwrap_or(0);

        let selection = Select::new()
            .with_prompt("Which adapter would you like to use?")
            .items(&adapters)
            .default(default_index)
            .interact()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get adapter selection: {}", e))?;

        Ok(adapters[selection].to_string())
    }

    pub fn create_profile_for_adapter(
        &self,
        adapter: &str,
        _profile_name: &str,
        existing_config: Option<&ConfigMap>,
    ) -> FsResult<Profile> {
        let config = match adapter {
            "snowflake" => setup_snowflake_profile(existing_config)?,
            "postgres" => setup_postgres_profile(existing_config)?,
            "redshift" => setup_redshift_profile(existing_config)?,
            "bigquery" => setup_bigquery_profile(existing_config)?,
            "databricks" => setup_databricks_profile(existing_config)?,
            _ => {
                return Err(fs_err!(
                    ErrorCode::InvalidArgument,
                    "Unsupported adapter: {}",
                    adapter
                ));
            }
        };

        let output = ProfileOutput {
            adapter_type: adapter.to_string(),
            config,
        };

        let mut outputs = HashMap::new();
        outputs.insert("dev".to_string(), output);

        Ok(Profile {
            target: "dev".to_string(),
            outputs,
        })
    }

    /// Write or update a single profile block in the appropriate profiles.yml,
    /// preserving existing content, order, and comments.
    pub fn write_profile(&self, profile_name: &str, profile: &Profile) -> FsResult<()> {
        // Determine target profiles.yml path:
        // 1) If ./profiles.yml exists, prefer writing there
        // 2) Else write to self.profiles_dir/profiles.yml (creating directory if needed)
        let local_profiles = PathBuf::from("profiles.yml");
        let target_file: PathBuf = if local_profiles.exists() {
            local_profiles
        } else {
            let profiles_dir = Path::new(&self.profiles_dir);
            if !profiles_dir.exists() {
                fs::create_dir_all(profiles_dir)?;
            }
            profiles_dir.join("profiles.yml")
        };

        let mut existing = if target_file.exists() {
            fs::read_to_string(&target_file)?
        } else {
            String::new()
        };

        if has_top_level_key_parsed_file(&target_file, profile_name)? {
            let overwrite = Confirm::new()
                .with_prompt(format!(
                    "The profile '{}' already exists in {}. Continue and overwrite it?",
                    profile_name,
                    target_file.display()
                ))
                .default(false)
                .interact()
                .map_err(|e| {
                    fs_err!(
                        ErrorCode::IoError,
                        "Failed to get overwrite confirmation: {}",
                        e
                    )
                })?;

            if !overwrite {
                return Err(fs_err!(ErrorCode::IoError, "Profile setup cancelled"));
            }

            if target_file.exists() && !existing.is_empty() {
                let backup_file = if target_file.file_name().is_some() {
                    target_file.with_extension("yml.bkp")
                } else {
                    target_file.with_extension("bkp")
                };
                fs::write(&backup_file, &existing)?;
                log::info!(
                    "{} Backup created at {}",
                    GREEN.apply_to("Info"),
                    backup_file.display()
                );
            }
        }

        existing = remove_top_level_key_from_str(existing, profile_name);

        if !existing.is_empty() && !existing.ends_with('\n') {
            existing.push('\n');
        }

        let mut top: HashMap<String, Profile> = HashMap::new();
        top.insert(profile_name.to_string(), profile.clone());
        let new_block = dbt_serde_yaml::to_string(&top).map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to serialize profile block: {}",
                e
            )
        })?;

        existing.push_str(&new_block);

        // Ensure trailing newline for cleanliness
        if !existing.ends_with('\n') {
            existing.push('\n');
        }

        fs::write(&target_file, existing)?;

        log::info!(
            "{} Profile written to {}",
            GREEN.apply_to("Success"),
            target_file.display()
        );
        Ok(())
    }

    fn profile_exists_in_str(content: &str, profile_name: &str) -> bool {
        content.contains(&format!("{profile_name}:"))
    }

    async fn handle_cloud_project_selection(project_store: &ProjectStore) -> FsResult<String> {
        let active_project = project_store.get_active_project();
        let all_projects = project_store.get_all_projects();

        if let Some(active) = active_project {
            log::info!(
                "Found active project: {} (ID: {})",
                active.project_name,
                active.project_id
            );

            let use_active = Confirm::new()
                .with_prompt(format!(
                    "Use active project '{}' from dbt_cloud.yml?",
                    active.project_name
                ))
                .default(true)
                .interact()
                .map_err(|e| {
                    fs_err!(ErrorCode::IoError, "Failed to get project selection: {}", e)
                })?;

            if use_active {
                return Ok(active.project_id.clone());
            }
        }

        if all_projects.is_empty() {
            return Err(fs_err!(
                ErrorCode::IoError,
                "No projects found in dbt_cloud.yml"
            ));
        }

        let project_names: Vec<String> = all_projects
            .iter()
            .map(|p| format!("{} (ID: {})", p.project_name, p.project_id))
            .collect();

        let selection = Select::new()
            .with_prompt("Select a project from dbt Cloud:")
            .items(&project_names)
            .interact()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get project selection: {}", e))?;

        Ok(all_projects[selection].project_id.clone())
    }

    async fn fetch_cloud_config_map(
        project_store: &ProjectStore,
        project_id: &str,
        adapter: &str,
    ) -> FsResult<Option<ConfigMap>> {
        let base_url = project_store.get_base_url(Some(project_id));

        match DbtCloudClient::get_credential_config_map(&base_url, Some(project_id), Some(adapter))
            .await
        {
            Ok(Some(config_map)) => Ok(Some(config_map)),
            Ok(None) => {
                log::warn!("No {adapter} credentials found for project {project_id}");
                Ok(None)
            }
            Err(e) => {
                log::warn!("Failed to fetch config: {e}");
                Ok(None)
            }
        }
    }

    pub async fn setup_profile(&self, profile_name: &str) -> FsResult<()> {
        log::info!("{} Setting up your profile...", GREEN.apply_to("Info"));

        let profile_exists = Self::profile_exists_in_file(Path::new("profiles.yml"), profile_name)
            || Self::profile_exists_in_file(
                &Path::new(&self.profiles_dir).join("profiles.yml"),
                profile_name,
            );

        if profile_exists {
            log::info!(
                "Profile '{profile_name}' already exists. You can choose to overwrite it in the next step."
            );

            use dialoguer::Confirm;
            let should_overwrite = Confirm::new()
                .with_prompt(format!(
                    "Do you want to overwrite the existing profile '{profile_name}'?"
                ))
                .default(false)
                .interact()
                .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get confirmation: {}", e))?;

            if !should_overwrite {
                log::info!("{} Profile setup cancelled.", GREEN.apply_to("Info"));
                return Ok(());
            }
        }

        let existing_defaults = self.load_existing_defaults_typed(profile_name)?;
        let adapter =
            Self::ask_for_adapter_choice(existing_defaults.as_ref().map(|d| d.adapter_type()))?;

        // Try to get cloud config for pre-population (regardless of existing profile)
        let cloud_config = if let Some(project_store) = ProjectStore::from_dbt_cloud_yml()? {
            log::info!("Found dbt_cloud.yml configuration");
            let project_id = Self::handle_cloud_project_selection(&project_store).await?;

            match Self::fetch_cloud_config_map(&project_store, &project_id, &adapter).await? {
                Some(config) => Some(config),
                None => {
                    log::info!("No cloud config found for this adapter/project");
                    None
                }
            }
        } else {
            log::info!("No dbt_cloud.yml found - proceeding without cloud pre-population");
            None
        };

        let should_use_existing_config = existing_defaults
            .as_ref()
            .map(|d| d.adapter_type().eq_ignore_ascii_case(&adapter))
            .unwrap_or(false);

        let existing_config = if should_use_existing_config {
            existing_defaults.as_ref().map(|d| d.config())
        } else {
            if existing_defaults.is_some() {
                log::info!(
                    "Adapter type changed from '{}' to '{}' - starting with fresh configuration",
                    existing_defaults.as_ref().unwrap().adapter_type(),
                    adapter
                );
            }
            None
        };

        // Merge cloud config with existing config (existing takes priority)
        let merged_config = merge_configs(cloud_config, existing_config);

        let profile =
            self.create_profile_for_adapter(&adapter, profile_name, merged_config.as_ref())?;
        self.write_profile(profile_name, &profile)?;

        Ok(())
    }

    fn load_existing_defaults_typed(
        &self,
        profile_name: &str,
    ) -> FsResult<Option<ProfileDefaults>> {
        let local_profiles = PathBuf::from("profiles.yml");
        let target_file: PathBuf = if local_profiles.exists() {
            local_profiles
        } else {
            Path::new(&self.profiles_dir).join("profiles.yml")
        };

        if !target_file.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&target_file)?;
        let root: dbt_serde_yaml::Value = dbt_serde_yaml::from_str(&content)
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to parse profiles.yml: {}", e))?;

        let Some(profiles_map) = root.as_mapping() else {
            return Ok(None);
        };

        let Some((_, profile_val)) = profiles_map
            .iter()
            .find(|(k, _)| k.as_str() == Some(profile_name))
        else {
            log::info!("Profile '{profile_name}' not found in profiles.yml");
            return Ok(None);
        };

        let profile_map = profile_val.as_mapping();
        let target_name = profile_map.and_then(|map| {
            map.iter()
                .find(|(k, _)| k.as_str() == Some("target"))
                .and_then(|(_, v)| v.as_str())
        });

        DefaultProfileParser::parse_profile_yaml(profile_val, target_name)
    }

    fn profile_exists_in_file(path: &Path, profile_name: &str) -> bool {
        if !path.exists() {
            return false;
        }
        let Ok(content) = fs::read_to_string(path) else {
            return false;
        };
        Self::profile_exists_in_str(&content, profile_name)
    }

    /// Get access to the dbt Cloud client for fetching environments and other cloud operations
    pub fn cloud_client() -> &'static DbtCloudClient {
        &DbtCloudClient
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter_config::common::FieldValue;

    #[test]
    fn test_merge_configs() {
        let mut cloud_config = ConfigMap::new();
        cloud_config.insert(
            "user".to_string(),
            FieldValue::String("cloud_user".to_string()),
        );
        cloud_config.insert("threads".to_string(), FieldValue::Number(8));
        cloud_config.insert(
            "schema".to_string(),
            FieldValue::String("cloud_schema".to_string()),
        );

        let mut existing_config = ConfigMap::new();
        existing_config.insert(
            "user".to_string(),
            FieldValue::String("existing_user".to_string()),
        );
        existing_config.insert(
            "password".to_string(),
            FieldValue::String("existing_password".to_string()),
        );

        // Test merging with existing taking priority
        let merged = merge_configs(Some(cloud_config.clone()), Some(&existing_config));

        assert!(merged.is_some());
        let merged = merged.unwrap();

        // Existing should override cloud
        assert_eq!(
            merged.get("user"),
            Some(&FieldValue::String("existing_user".to_string()))
        );
        // Cloud should provide values not in existing
        assert_eq!(merged.get("threads"), Some(&FieldValue::Number(8)));
        assert_eq!(
            merged.get("schema"),
            Some(&FieldValue::String("cloud_schema".to_string()))
        );
        // Existing-only fields should be preserved
        assert_eq!(
            merged.get("password"),
            Some(&FieldValue::String("existing_password".to_string()))
        );

        // Test cloud-only
        let cloud_only = merge_configs(Some(cloud_config.clone()), None);
        assert_eq!(cloud_only, Some(cloud_config));

        // Test existing-only
        let existing_only = merge_configs(None, Some(&existing_config));
        assert_eq!(existing_only, Some(existing_config));

        // Test both None
        let neither = merge_configs(None, None);
        assert_eq!(neither, None);
    }

    #[test]
    fn test_profile_output_serialization() {
        let mut config = ConfigMap::new();
        config.insert(
            "account".to_string(),
            FieldValue::String("ska67070".to_string()),
        );
        config.insert(
            "database".to_string(),
            FieldValue::String("test_db".to_string()),
        );
        config.insert(
            "warehouse".to_string(),
            FieldValue::String("test_warehouse".to_string()),
        );
        config.insert("threads".to_string(), FieldValue::Number(16));

        let output = ProfileOutput {
            adapter_type: "snowflake".to_string(),
            config,
        };

        let serialized = dbt_serde_yaml::to_string(&output).unwrap();

        // Verify that the fields are at the top level (not under __config__)
        assert!(serialized.contains("type: snowflake"));
        assert!(serialized.contains("account: ska67070"));
        assert!(serialized.contains("database: test_db"));
        assert!(serialized.contains("warehouse: test_warehouse"));
        assert!(serialized.contains("threads: 16"));

        // Verify that __config__ is NOT present
        assert!(!serialized.contains("__config__"));

        println!("Serialized ProfileOutput:\n{serialized}");
    }
}
