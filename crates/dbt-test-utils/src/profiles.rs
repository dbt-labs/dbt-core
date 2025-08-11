use dbt_common::FsResult;
use dbt_common::io_args::IoArgs;
use dbt_jinja_utils::phases::load::LoadContext;
use dbt_jinja_utils::phases::load::init::initialize_load_profile_jinja_environment;
use dbt_jinja_utils::serde::yaml_to_fs_error;
use dbt_loader::args::LoadArgs;
use dbt_loader::utils::read_profiles_and_extract_db_config;
use dbt_schemas::schemas::profiles::{DbConfig, DbTargets};

use dbt_serde_yaml;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};

const TEST_PROFILE: &str = "fusion_tests";

/// Load the db config from a 'test' profiles.yml at the default profile path (~/.dbt)
/// and set schema and database values
pub fn load_db_config_from_test_profile_with_database(
    target: &str,
    schema: &str,
    database: &str,
) -> FsResult<DbConfig> {
    let mut db_config = load_db_config_from_test_profile(target, schema)?;
    match &mut db_config {
        DbConfig::Postgres(pg) => {
            pg.database = Some(database.to_string());
        }
        DbConfig::Snowflake(sf) => {
            sf.database = Some(database.to_string());
        }
        DbConfig::Redshift(rs) => {
            rs.database = Some(database.to_string());
        }
        DbConfig::Bigquery(bq) => {
            bq.database = Some(database.to_string());
        }
        DbConfig::Databricks(db) => {
            db.database = Some(database.to_string());
        }
        _ => {}
    }

    Ok(db_config)
}

/// Load the db config from a 'test' profiles.yml at the default profile path (~/.dbt)
/// and set schema value
pub fn load_db_config_from_test_profile(target: &str, schema: &str) -> FsResult<DbConfig> {
    let home_dir = dirs::home_dir().expect("home dir exists");
    // ! This must be consistent with what's written out from the init_creds.rs (from xtask crate)
    let profile_path = home_dir.join(".dbt").join("profiles.yml");
    load_db_config(target, schema, &profile_path)
}

/// Load the target db config from a profiles.yml at a give directory
pub fn load_db_config<P: AsRef<Path>>(
    target: &str,
    schema: &str,
    profile_path: P,
) -> FsResult<DbConfig> {
    let arg = LoadArgs::default();

    // Get all the profiles
    let env = initialize_load_profile_jinja_environment();

    let load_context = LoadContext::new(arg.vars);

    let (_, mut db_config) = read_profiles_and_extract_db_config(
        &IoArgs::default(),
        &Some(target.to_string()),
        &env,
        &load_context,
        TEST_PROFILE,
        profile_path.as_ref().to_path_buf(),
    )?;

    match &mut db_config {
        DbConfig::Postgres(pg) => {
            pg.schema = Some(schema.to_string());
        }
        DbConfig::Snowflake(sf) => {
            sf.schema = Some(schema.to_string());
        }
        DbConfig::Redshift(rs) => {
            rs.schema = Some(schema.to_string());
        }
        DbConfig::Bigquery(bq) => {
            bq.schema = Some(schema.to_string());
        }
        DbConfig::Databricks(db) => {
            db.schema = Some(schema.to_string());
        }
        _ => {}
    }

    Ok(db_config)
}

/// Write the db config to a 'test' profiles.yml at a give directory
pub fn write_db_config_to_test_profile(
    db_config: DbConfig,
    profile_dir: &Path,
) -> FsResult<PathBuf> {
    let profile_path = profile_dir.join("profiles.yml");
    let mut file = File::create(&profile_path)?;

    let adapter_type = db_config.adapter_type();

    let profile = HashMap::from([(
        TEST_PROFILE,
        DbTargets {
            default_target: adapter_type.to_string(),
            outputs: HashMap::from([(adapter_type, serde_json::to_value(db_config)?)]),
        },
    )]);
    dbt_serde_yaml::to_writer(&mut file, &profile)
        .map_err(|e| yaml_to_fs_error(e, Some(&profile_path)))?;
    Ok(profile_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    #[ignore = "This test is for local debugging but unnecessary for CI, since the functions are covered in other tests"]
    fn test_load_and_write_profile_roundtrip() -> FsResult<()> {
        // load the test profile
        let db_config = load_db_config_from_test_profile("postgres", "test_schema")?;

        let temp_dir = tempdir()?;
        let profile_path = write_db_config_to_test_profile(db_config, temp_dir.path())?;

        assert!(profile_path.exists());
        let profile_contents = fs::read_to_string(&profile_path)?;
        assert!(profile_contents.contains("test_schema"));
        assert!(profile_contents.contains("postgres"));
        assert!(profile_contents.contains("test:"));
        assert!(!profile_contents.is_empty());

        // Clean up
        temp_dir.close()?;

        Ok(())
    }
}
