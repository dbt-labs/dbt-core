use crate::adapter_config::{
    setup_bigquery_profile, setup_databricks_profile, setup_postgres_profile,
    setup_redshift_profile, setup_snowflake_profile,
};
use crate::dbt_cloud_client::{CloudProject, DbtCloudClient, DbtCloudYml};
use crate::yaml_utils::{has_top_level_key_parsed_file, remove_top_level_key_from_str};
use dbt_common::pretty_string::GREEN;
use dbt_common::{ErrorCode, FsResult, fs_err, io_args::IoArgs};
use dbt_jinja_utils::phases::load::init::initialize_load_profile_jinja_environment;
use dbt_jinja_utils::serde::{into_typed_with_jinja, value_from_file};
use dbt_loader::{args::LoadArgs, load_profiles};
use dbt_schemas::schemas::profiles::DbConfig;
use dbt_schemas::schemas::project::DbtProjectSimplified;

use dialoguer::{Confirm, Select};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileTarget {
    pub target: String,
    pub outputs: HashMap<String, DbConfig>,
}

pub type Profiles = HashMap<String, ProfileTarget>;

/// Load profile using the standard dbt-loader infrastructure
fn load_profile_with_loader(
    profiles_dir: Option<&str>,
    profile_name: &str,
    target: Option<&str>,
) -> FsResult<DbConfig> {
    let current_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

    let io_args = IoArgs {
        in_dir: current_dir,
        command: "init".to_string(),
        ..Default::default()
    };

    let load_args = LoadArgs {
        command: "init".to_string(),
        io: io_args,
        profiles_dir: profiles_dir.map(PathBuf::from),
        profile: Some(profile_name.to_string()),
        target: target.map(String::from),
        ..Default::default()
    };

    let dbt_project = DbtProjectSimplified {
        packages_install_path: Some("dbt_packages".to_string()),
        profile: Some(profile_name.to_string()),
        dbt_cloud: None,
        data_paths: Default::default(),
        source_paths: Default::default(),
        log_path: Default::default(),
        target_path: Default::default(),
        __ignored__: Default::default(),
    };

    let jinja_env = initialize_load_profile_jinja_environment();
    let empty_context = HashMap::<String, String>::new();

    let dbt_profile = load_profiles(&load_args, &dbt_project, &jinja_env, &empty_context)?;
    Ok(dbt_profile.db_config)
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

        let io_args = IoArgs::default();
        let yaml_value = value_from_file(&io_args, &dbt_cloud_config_path, true, None)?;

        let env = initialize_load_profile_jinja_environment();
        let empty_context = HashMap::<String, String>::new();

        let config: DbtCloudYml =
            into_typed_with_jinja(&io_args, yaml_value, false, &env, &empty_context, &[], None)?;

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

    pub fn try_load_profile(&self, profiles_dir: &str, profile_name: &str) -> Option<DbConfig> {
        load_profile_with_loader(Some(profiles_dir), profile_name, None).ok()
    }
}

pub struct ProfileSetup {
    pub profiles_dir: String,
    pub project_store: Option<ProjectStore>,
}

impl ProfileSetup {
    pub fn new(profiles_dir: String) -> Self {
        let project_store = ProjectStore::from_dbt_cloud_yml().unwrap_or(None);
        Self {
            profiles_dir,
            project_store,
        }
    }

    pub fn get_available_adapters() -> Vec<&'static str> {
        vec![
            "snowflake",
            "databricks",
            "bigquery",
            "postgres",
            "redshift",
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
        existing_config: Option<&DbConfig>,
    ) -> FsResult<ProfileTarget> {
        let db_config = match adapter {
            "snowflake" => {
                let snowflake_config = match existing_config {
                    Some(DbConfig::Snowflake(config)) => Some(config),
                    _ => None,
                };
                DbConfig::Snowflake(setup_snowflake_profile(snowflake_config.map(Box::as_ref))?)
            }
            "bigquery" => {
                let bigquery_config = match existing_config {
                    Some(DbConfig::Bigquery(config)) => Some(config),
                    _ => None,
                };
                DbConfig::Bigquery(setup_bigquery_profile(bigquery_config.map(Box::as_ref))?)
            }
            "databricks" => {
                let databricks_config = match existing_config {
                    Some(DbConfig::Databricks(config)) => Some(config),
                    _ => None,
                };
                DbConfig::Databricks(setup_databricks_profile(
                    databricks_config.map(Box::as_ref),
                )?)
            }
            "postgres" => {
                let postgres_config = match existing_config {
                    Some(DbConfig::Postgres(config)) => Some(config),
                    _ => None,
                };
                DbConfig::Postgres(setup_postgres_profile(postgres_config.map(Box::as_ref))?)
            }
            "redshift" => {
                let redshift_config = match existing_config {
                    Some(DbConfig::Redshift(config)) => Some(config),
                    _ => None,
                };
                DbConfig::Redshift(setup_redshift_profile(redshift_config.map(Box::as_ref))?)
            }
            _ => {
                return Err(fs_err!(
                    ErrorCode::InvalidArgument,
                    "Unsupported adapter: {}",
                    adapter
                ));
            }
        };

        let mut outputs = HashMap::new();
        outputs.insert("dev".to_string(), db_config);

        Ok(ProfileTarget {
            target: "dev".to_string(),
            outputs,
        })
    }

    /// Write or update a single profile block in the appropriate profiles.yml,
    /// preserving existing content, order, and comments.
    pub fn write_profile(&self, profile_name: &str, profile: &ProfileTarget) -> FsResult<()> {
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

        let mut top: HashMap<String, ProfileTarget> = HashMap::new();
        top.insert(profile_name.to_string(), profile.clone());
        let new_block = dbt_serde_yaml::to_string(&top).map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to serialize profile block: {}",
                e
            )
        })?;

        existing.push_str(&new_block);

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
            .with_prompt("Select a project from dbt platform:")
            .items(&project_names)
            .interact()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get project selection: {}", e))?;

        Ok(all_projects[selection].project_id.clone())
    }

    async fn fetch_cloud_config(
        project_store: &ProjectStore,
        project_id: &str,
        adapter: &str,
    ) -> FsResult<Option<DbConfig>> {
        let base_url = project_store.get_base_url(Some(project_id));

        match DbtCloudClient::get_credential_db_config(&base_url, Some(project_id), Some(adapter))
            .await
        {
            Ok(db_config) => Ok(db_config),
            Err(e) => {
                log::warn!("Failed to fetch cloud config: {e}");
                Ok(None)
            }
        }
    }

    pub async fn setup_profile(&self, profile_name: &str) -> FsResult<()> {
        log::info!("{} Setting up your profile...", GREEN.apply_to("Info"));

        // Load the profile once at the beginning and cache the result
        let existing_config = if let Some(store) = &self.project_store {
            store.try_load_profile(&self.profiles_dir, profile_name)
        } else {
            load_profile_with_loader(Some(&self.profiles_dir), profile_name, None).ok()
        };

        let profile_exists = existing_config.is_some();

        let profile_action = if profile_exists {
            log::info!("Profile '{profile_name}' already exists. You can choose how to proceed.");

            use dialoguer::Select;
            let options = vec![
                "Edit existing profile (update fields interactively)",
                "Overwrite completely (start fresh)",
                "Cancel (keep existing profile as-is)",
            ];

            Select::new()
                .with_prompt("What would you like to do?")
                .items(&options)
                .default(0)
                .interact()
                .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get selection: {}", e))?
        } else {
            1 // New profile, equivalent to "overwrite"
        };

        match profile_action {
            0 => {
                log::info!("{} Editing existing profile...", GREEN.apply_to("Info"));
            }
            1 => {
                log::info!("{} Creating new profile...", GREEN.apply_to("Info"));
            }
            2 => {
                log::info!("{} Profile setup cancelled.", GREEN.apply_to("Info"));
                return Ok(());
            }
            _ => unreachable!(),
        }

        let adapter_type = existing_config.as_ref().map(|d| d.adapter_type());
        let adapter = Self::ask_for_adapter_choice(adapter_type)?;

        let cloud_config = if profile_action == 1 {
            if let Some(project_store) = &self.project_store {
                log::info!("Found dbt_cloud.yml configuration");
                let project_id = Self::handle_cloud_project_selection(project_store).await?;

                match Self::fetch_cloud_config(project_store, &project_id, &adapter).await? {
                    Some(config) => Some(config),
                    None => {
                        log::info!("No cloud config found for this adapter/project");
                        None
                    }
                }
            } else {
                log::info!("No dbt_cloud.yml found - proceeding without cloud pre-population");
                None
            }
        } else {
            log::info!("Editing existing profile - skipping cloud config fetch");
            None
        };

        let should_use_existing_config = existing_config
            .as_ref()
            .map(|d| d.adapter_type().eq_ignore_ascii_case(&adapter))
            .unwrap_or(false);

        let final_existing_config = if should_use_existing_config {
            existing_config.as_ref()
        } else {
            if let Some(existing_config) = existing_config.as_ref() {
                log::info!(
                    "Adapter type changed from '{}' to '{}' - starting with fresh configuration",
                    existing_config.adapter_type(),
                    adapter
                );
            }
            None
        };

        let merged_config = cloud_config.or_else(|| final_existing_config.cloned());

        let profile =
            self.create_profile_for_adapter(&adapter, profile_name, merged_config.as_ref())?;
        self.write_profile(profile_name, &profile)?;

        Ok(())
    }

    pub fn cloud_client() -> &'static DbtCloudClient {
        &DbtCloudClient
    }
}
