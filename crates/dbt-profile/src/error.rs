use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("{}", not_found_message(.searched, .explicit_profiles_dir))]
    NotFound {
        searched: Vec<PathBuf>,
        /// True when `--profiles-dir` was explicitly set, restricting the search.
        explicit_profiles_dir: bool,
    },

    #[error("no profile name specified and no dbt_project.yml found to infer it")]
    NoProfileName,

    #[error("Profile '{}' not found in profiles.yml", profile)]
    ProfileMissing { profile: String, path: PathBuf },

    #[error("no 'outputs' key found in profile '{profile}'")]
    NoOutputs { profile: String },

    #[error("target '{target}' not found in profile '{profile}'")]
    TargetMissing { profile: String, target: String },

    #[error("YAML parse error in {}: {source}", path.display())]
    Yaml {
        path: PathBuf,
        source: dbt_yaml::Error,
    },

    #[error("Jinja render error: {0}")]
    Jinja(#[from] minijinja::Error),

    #[error("missing 'type' field in resolved profile output")]
    NoAdapterType,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, ProfileError>;

fn not_found_message(searched: &[PathBuf], explicit_profiles_dir: &bool) -> String {
    if searched.len() == 1 {
        let path = &searched[0];
        let mut msg = format!("No profiles.yml found at `{}`.", path.display());
        if *explicit_profiles_dir {
            msg.push_str(
                "\nTry running without the --profiles-dir flag to check the default locations.",
            );
        }
        msg
    } else {
        format!(
            "no profiles.yml found (searched: {:?}). Run `dbt init` to create one",
            searched
        )
    }
}
