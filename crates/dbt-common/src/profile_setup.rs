use crate::pretty_string::GREEN;
use crate::{ErrorCode, FsResult, fs_err};
use dialoguer::{Confirm, Input, Password, Select};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileOutput {
    #[serde(rename = "type")]
    pub adapter_type: String,
    #[serde(flatten)]
    pub config: HashMap<String, serde_json::Value>,
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

    /// Get list of available adapters
    pub fn get_available_adapters() -> Vec<&'static str> {
        vec![
            "snowflake",
            "databricks", // TODO (Elias): Removing "coming soon" once Databricks goes live
            "bigquery",   // TODO (Elias): Removing "coming soon" once BigQuery goes live
            "redshift",
            // "postgres", // TODO (Elias): Add back once we have Postgres support
        ]
    }

    /// Ask user to choose an adapter
    pub fn ask_for_adapter_choice() -> FsResult<String> {
        let adapters = Self::get_available_adapters();

        let selection = Select::new()
            .with_prompt("Which adapter would you like to use?")
            .items(&adapters)
            .default(0)
            .interact()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get adapter selection: {}", e))?;

        Ok(adapters[selection].to_string())
    }

    /// Create profile configuration based on adapter type
    pub fn create_profile_for_adapter(
        &self,
        adapter: &str,
        _profile_name: &str,
    ) -> FsResult<Profile> {
        let mut config = HashMap::new();

        match adapter {
            "snowflake" => self.setup_snowflake_profile(&mut config)?,
            "postgres" => self.setup_postgres_profile(&mut config)?,
            "redshift" => self.setup_redshift_profile(&mut config)?,
            "bigquery" => self.setup_bigquery_profile(&mut config)?,
            "databricks" => self.setup_databricks_profile(&mut config)?,
            _ => {
                return Err(fs_err!(
                    ErrorCode::InvalidArgument,
                    "Unsupported adapter: {}",
                    adapter
                ));
            }
        }

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

    fn setup_snowflake_profile(
        &self,
        config: &mut HashMap<String, serde_json::Value>,
    ) -> FsResult<()> {
        let account: String = Input::new()
            .with_prompt("account (account id)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get account: {}", e))?;
        config.insert("account".to_string(), serde_json::Value::String(account));

        let user: String = Input::new()
            .with_prompt("user (username)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get user: {}", e))?;
        config.insert("user".to_string(), serde_json::Value::String(user));

        // Choose authentication method
        let auth_methods = vec![
            "password",
            "externalbrowser (browser SSO)",
            "keypair",
            "oauth",
        ];
        let auth_choice = Select::new()
            .with_prompt("Choose authentication method")
            .items(&auth_methods)
            .default(0)
            .interact()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get auth method: {}", e))?;

        match auth_choice {
            0 => {
                // Password authentication (with optional MFA flag)
                let password: String = Password::new()
                    .with_prompt("password")
                    .interact()
                    .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get password: {}", e))?;
                config.insert("password".to_string(), serde_json::Value::String(password));

                // Ask if MFA is required
                let needs_mfa = Confirm::new()
                    .with_prompt("Do you use MFA (username_password_mfa)?")
                    .default(false)
                    .interact()
                    .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get MFA choice: {}", e))?;

                if needs_mfa {
                    config.insert(
                        "authenticator".to_string(),
                        serde_json::Value::String("username_password_mfa".to_string()),
                    );
                }
            }
            1 => {
                // SSO authentication
                config.insert(
                    "authenticator".to_string(),
                    serde_json::Value::String("externalbrowser".to_string()),
                );
            }
            2 => {
                // Keypair authentication
                // Ask for path vs inline
                let use_path = Confirm::new()
                    .with_prompt("Do you want to provide a private_key_path instead of embedding the key inline?")
                    .default(true)
                    .interact()
                    .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get key choice: {}", e))?;

                if use_path {
                    let pk_path: String = Input::new()
                        .with_prompt("private_key_path (/path/to/private.key)")
                        .interact_text()
                        .map_err(|e| {
                            fs_err!(ErrorCode::IoError, "Failed to get private_key_path: {}", e)
                        })?;
                    config.insert(
                        "private_key_path".to_string(),
                        serde_json::Value::String(pk_path),
                    );
                } else {
                    let pk_inline: String = Password::new()
                        .with_prompt("private_key (paste the key)")
                        .interact()
                        .map_err(|e| {
                            fs_err!(ErrorCode::IoError, "Failed to get private_key: {}", e)
                        })?;
                    config.insert(
                        "private_key".to_string(),
                        serde_json::Value::String(pk_inline),
                    );
                }

                let passphrase_needed = Confirm::new()
                    .with_prompt("Is your private key encrypted with a passphrase?")
                    .default(false)
                    .interact()
                    .map_err(|e| {
                        fs_err!(ErrorCode::IoError, "Failed to get passphrase choice: {}", e)
                    })?;

                if passphrase_needed {
                    let passphrase: String = Password::new()
                        .with_prompt("private_key_passphrase")
                        .interact()
                        .map_err(|e| {
                            fs_err!(ErrorCode::IoError, "Failed to get passphrase: {}", e)
                        })?;
                    config.insert(
                        "private_key_passphrase".to_string(),
                        serde_json::Value::String(passphrase),
                    );
                }
            }
            3 => {
                // OAuth authentication
                config.insert(
                    "authenticator".to_string(),
                    serde_json::Value::String("oauth".to_string()),
                );

                let client_id: String = Input::new()
                    .with_prompt("oauth_client_id")
                    .interact_text()
                    .map_err(|e| {
                        fs_err!(ErrorCode::IoError, "Failed to get oauth_client_id: {}", e)
                    })?;
                config.insert(
                    "oauth_client_id".to_string(),
                    serde_json::Value::String(client_id),
                );

                let client_secret: String = Password::new()
                    .with_prompt("oauth_client_secret")
                    .interact()
                    .map_err(|e| {
                        fs_err!(
                            ErrorCode::IoError,
                            "Failed to get oauth_client_secret: {}",
                            e
                        )
                    })?;
                config.insert(
                    "oauth_client_secret".to_string(),
                    serde_json::Value::String(client_secret),
                );

                let token: String = Password::new()
                    .with_prompt("token (OAuth refresh token)")
                    .interact()
                    .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get token: {}", e))?;
                config.insert("token".to_string(), serde_json::Value::String(token));
            }
            _ => unreachable!(),
        }

        let role: String = Input::new()
            .with_prompt("role (user role)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get role: {}", e))?;
        config.insert("role".to_string(), serde_json::Value::String(role));

        let database: String = Input::new()
            .with_prompt("database (database name)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get database: {}", e))?;
        config.insert("database".to_string(), serde_json::Value::String(database));

        let warehouse: String = Input::new()
            .with_prompt("warehouse (warehouse name)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get warehouse: {}", e))?;
        config.insert(
            "warehouse".to_string(),
            serde_json::Value::String(warehouse),
        );

        let schema: String = Input::new()
            .with_prompt("schema (dbt schema)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get schema: {}", e))?;
        config.insert("schema".to_string(), serde_json::Value::String(schema));

        config.insert(
            "threads".to_string(),
            serde_json::Value::Number(serde_json::Number::from(16)),
        );

        Ok(())
    }

    fn setup_postgres_profile(
        &self,
        config: &mut HashMap<String, serde_json::Value>,
    ) -> FsResult<()> {
        let host: String = Input::new()
            .with_prompt("host (hostname)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get host: {}", e))?;
        config.insert("host".to_string(), serde_json::Value::String(host));

        let user: String = Input::new()
            .with_prompt("user (username)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get user: {}", e))?;
        config.insert("user".to_string(), serde_json::Value::String(user));

        let password: String = Password::new()
            .with_prompt("password")
            .interact()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get password: {}", e))?;
        config.insert("password".to_string(), serde_json::Value::String(password));

        let port: u32 = Input::new()
            .with_prompt("port")
            .default(5432)
            .interact()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get port: {}", e))?;
        config.insert("port".to_string(), serde_json::Value::Number(port.into()));

        let dbname: String = Input::new()
            .with_prompt("dbname (database name)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get dbname: {}", e))?;
        config.insert("dbname".to_string(), serde_json::Value::String(dbname));

        let schema: String = Input::new()
            .with_prompt("schema (dbt schema)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get schema: {}", e))?;
        config.insert("schema".to_string(), serde_json::Value::String(schema));

        Ok(())
    }

    fn setup_redshift_profile(
        &self,
        config: &mut HashMap<String, serde_json::Value>,
    ) -> FsResult<()> {
        let host: String = Input::new()
            .with_prompt("host (hostname.region.redshift.amazonaws.com)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get host: {}", e))?;
        config.insert("host".to_string(), serde_json::Value::String(host));

        let user: String = Input::new()
            .with_prompt("user (username)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get user: {}", e))?;
        config.insert("user".to_string(), serde_json::Value::String(user));

        let password: String = Password::new()
            .with_prompt("password")
            .interact()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get password: {}", e))?;
        config.insert("password".to_string(), serde_json::Value::String(password));

        let dbname: String = Input::new()
            .with_prompt("dbname (database name)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get dbname: {}", e))?;
        config.insert("dbname".to_string(), serde_json::Value::String(dbname));

        let schema: String = Input::new()
            .with_prompt("schema (dbt schema)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get schema: {}", e))?;
        config.insert("schema".to_string(), serde_json::Value::String(schema));

        Ok(())
    }

    fn setup_bigquery_profile(
        &self,
        config: &mut HashMap<String, serde_json::Value>,
    ) -> FsResult<()> {
        let methods = vec!["oauth", "service-account"];
        let method_choice = Select::new()
            .with_prompt("Choose authentication method")
            .items(&methods)
            .default(0)
            .interact()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get auth method: {}", e))?;

        match method_choice {
            0 => {
                config.insert(
                    "method".to_string(),
                    serde_json::Value::String("oauth".to_string()),
                );
            }
            1 => {
                config.insert(
                    "method".to_string(),
                    serde_json::Value::String("service-account".to_string()),
                );
                let keyfile: String = Input::new()
                    .with_prompt("keyfile (/path/to/bigquery/keyfile.json)")
                    .interact_text()
                    .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get keyfile: {}", e))?;
                config.insert("keyfile".to_string(), serde_json::Value::String(keyfile));
            }
            _ => unreachable!(),
        }

        let project: String = Input::new()
            .with_prompt("project (gcp_project_id)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get project: {}", e))?;
        config.insert("project".to_string(), serde_json::Value::String(project));

        let dataset: String = Input::new()
            .with_prompt("dataset (dbt_dataset_name)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get dataset: {}", e))?;
        config.insert("dataset".to_string(), serde_json::Value::String(dataset));

        config.insert(
            "threads".to_string(),
            serde_json::Value::Number(serde_json::Number::from(16)),
        );

        Ok(())
    }

    fn setup_databricks_profile(
        &self,
        config: &mut HashMap<String, serde_json::Value>,
    ) -> FsResult<()> {
        let schema: String = Input::new()
            .with_prompt("schema (schema_name)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get schema: {}", e))?;
        config.insert("schema".to_string(), serde_json::Value::String(schema));

        let host: String = Input::new()
            .with_prompt("host (yourorg.databrickshost.com)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get host: {}", e))?;
        config.insert("host".to_string(), serde_json::Value::String(host));

        let http_path: String = Input::new()
            .with_prompt("http_path (/sql/your/http/path)")
            .interact_text()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get http_path: {}", e))?;
        config.insert(
            "http_path".to_string(),
            serde_json::Value::String(http_path),
        );

        let has_catalog = Confirm::new()
            .with_prompt("Do you want to specify a catalog? (Unity Catalog)")
            .default(false)
            .interact()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get catalog choice: {}", e))?;

        if has_catalog {
            let catalog: String = Input::new()
                .with_prompt("catalog (catalog_name)")
                .interact_text()
                .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get catalog: {}", e))?;
            config.insert("catalog".to_string(), serde_json::Value::String(catalog));
        }

        // Choose authentication method
        let auth_methods = vec!["token", "oauth"];
        let auth_choice = Select::new()
            .with_prompt("Choose authentication method")
            .items(&auth_methods)
            .default(0)
            .interact()
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get auth method: {}", e))?;

        match auth_choice {
            0 => {
                // Token authentication
                let token: String = Password::new()
                    .with_prompt("token (personal access token)")
                    .interact()
                    .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get token: {}", e))?;
                config.insert("token".to_string(), serde_json::Value::String(token));
            }
            1 => {
                // OAuth authentication
                config.insert(
                    "auth_type".to_string(),
                    serde_json::Value::String("oauth".to_string()),
                );
                let client_id: String = Input::new()
                    .with_prompt("client_id (oauth_client_id)")
                    .interact_text()
                    .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to get client_id: {}", e))?;
                config.insert(
                    "client_id".to_string(),
                    serde_json::Value::String(client_id),
                );

                let client_secret: String = Password::new()
                    .with_prompt("client_secret")
                    .interact()
                    .map_err(|e| {
                        fs_err!(ErrorCode::IoError, "Failed to get client_secret: {}", e)
                    })?;
                config.insert(
                    "client_secret".to_string(),
                    serde_json::Value::String(client_secret),
                );
            }
            _ => unreachable!(),
        }

        config.insert(
            "threads".to_string(),
            serde_json::Value::Number(serde_json::Number::from(16)),
        );

        Ok(())
    }

    /// Write profile to profiles.yml
    pub fn write_profile(&self, profile_name: &str, profile: &Profile) -> FsResult<()> {
        let profiles_dir = Path::new(&self.profiles_dir);

        // Create profiles directory if it doesn't exist
        if !profiles_dir.exists() {
            fs::create_dir_all(profiles_dir)?;
        }

        let profiles_file = profiles_dir.join("profiles.yml");

        // Read existing profiles if the file exists
        let mut all_profiles: HashMap<String, Profile> = if profiles_file.exists() {
            let content = fs::read_to_string(&profiles_file)?;
            dbt_serde_yaml::from_str(&content).unwrap_or_default()
        } else {
            HashMap::new()
        };

        // Check if profile already exists and confirm overwrite
        if all_profiles.contains_key(profile_name) {
            let overwrite = Confirm::new()
                .with_prompt(format!(
                    "The profile '{}' already exists in {}. Continue and overwrite it?",
                    profile_name,
                    profiles_file.display()
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
        }

        // Add/update the profile
        all_profiles.insert(profile_name.to_string(), profile.clone());

        // Write back to file
        let yaml_content = dbt_serde_yaml::to_string(&all_profiles)
            .map_err(|e| fs_err!(ErrorCode::IoError, "Failed to serialize profiles: {}", e))?;

        fs::write(&profiles_file, yaml_content)?;

        log::info!(
            "{} Profile written to {}",
            GREEN.apply_to("Success"),
            profiles_file.display()
        );
        Ok(())
    }

    /// Full profile setup workflow
    pub fn setup_profile(&self, profile_name: &str) -> FsResult<()> {
        log::info!("{} Setting up your profile...", GREEN.apply_to("Info"));

        let adapter = Self::ask_for_adapter_choice()?;
        let profile = self.create_profile_for_adapter(&adapter, profile_name)?;
        self.write_profile(profile_name, &profile)?;

        Ok(())
    }
}
