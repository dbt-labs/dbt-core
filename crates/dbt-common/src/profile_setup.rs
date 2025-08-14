use crate::adapter_config::{
    ConfigMap, DefaultProfileParser, ProfileDefaults, ProfileParser, setup_bigquery_profile,
    setup_databricks_profile, setup_postgres_profile, setup_redshift_profile,
    setup_snowflake_profile,
};
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

pub struct ProfileSetup {
    pub profiles_dir: String,
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

    pub fn setup_profile(&self, profile_name: &str) -> FsResult<()> {
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
        } else if let Ok(home) = std::env::var("HOME") {
            let cloud_cfg = PathBuf::from(home).join(".dbt").join("dbt_cloud.yml");
            if cloud_cfg.exists() {
                log::info!(
                    "Found dbt_cloud.yml at {}. We'll attempt to pre-fill prompts using your dbt Cloud project where possible.",
                    cloud_cfg.display()
                );
            }
        }

        let existing_defaults = self.load_existing_defaults_typed(profile_name)?;
        if let Some(ref defaults) = existing_defaults {
            log::info!(
                "Found existing profile '{}' with adapter '{}'",
                profile_name,
                defaults.adapter_type()
            );
            log::info!("Pre-filling configuration from existing profile");
        }

        let adapter =
            Self::ask_for_adapter_choice(existing_defaults.as_ref().map(|d| d.adapter_type()))?;

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

        let profile = self.create_profile_for_adapter(&adapter, profile_name, existing_config)?;
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
}
