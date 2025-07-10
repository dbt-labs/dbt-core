use dbt_common::{err, fs_err, ErrorCode, FsResult};
use dbt_schemas::schemas::packages::PrivatePackage;
use serde::{Deserialize, Serialize};
use serde_json;
use std::{collections::HashMap, ops::Deref};

#[derive(Debug, Deserialize, Serialize)]
pub struct ProviderDetail {
    url: String,
    token: String,
    org: String,
    provider: Option<String>,
}

impl ProviderDetail {
    fn resolved_url(&self, repo: &str) -> String {
        self.url
            .replace("{token}", &self.token)
            .replace("{repo}", repo)
    }
}

pub fn get_provider_info() -> HashMap<String, Vec<ProviderDetail>> {
    let git_providers_str =
        std::env::var("DBT_ENV_PRIVATE_GIT_PROVIDER_INFO").unwrap_or_else(|_| "[]".to_string());

    let mut provider_info = HashMap::new();
    let provider_json: Vec<ProviderDetail> =
        serde_json::from_str(&git_providers_str).expect("Failed to parse git providers JSON");

    for provider_detail in provider_json {
        let org = provider_detail.org.clone();
        provider_info
            .entry(org)
            .or_insert_with(Vec::new)
            .push(provider_detail);
    }
    provider_info
}

pub fn get_resolved_url(private_package: &PrivatePackage) -> FsResult<String> {
    let provider_info = get_provider_info();
    let parts: Vec<&str> = private_package.private.split('/').collect();

    // If we did not get any provider information then we run locally and default to ssh git.
    if provider_info.is_empty() {
        match private_package.provider.as_deref().unwrap_or_default() {
            "github" if parts.len() == 2 => {
                return Ok(format!(
                    "git@github.com:{}.git",
                    private_package.private.deref()
                ))
            }
            "gitlab" if parts.len() > 1 => {
                return Ok(format!(
                    "git@gitlab.com:{}.git",
                    private_package.private.deref()
                ))
            }
            "ado" if parts.len() == 3 => {
                return Ok(format!(
                    "git@ssh.dev.azure.com:v3/{}",
                    private_package.private.deref()
                ))
            }
            _ => {
                return err!(
                    ErrorCode::InvalidConfig,
                    r#"Invalid private package configuration: '{}' provider: '{}'"#,
                    private_package.private.deref(),
                    private_package.provider.as_deref().unwrap_or_default()
                )
            }
        };
    }

    if parts.len() != 2 {
        return err!(
            ErrorCode::InvalidConfig,
            "Invalid private package definition"
        );
    }

    let (org_name, repo) = (parts[0], parts[1]);

    match provider_info.get(org_name) {
        None => err!(
            ErrorCode::InvalidConfig,
            "Private Package Org {} not configured",
            org_name
        ),
        Some(providers) => match &private_package.provider {
            None => {
                if providers.len() != 1 {
                    return err!(
                        ErrorCode::InvalidConfig,
                        "Do not know which provider to use"
                    );
                }
                Ok(providers[0].resolved_url(repo))
            }
            Some(provider_name) => providers
                .iter()
                .find(|p| p.provider.as_deref() == Some(provider_name))
                .map(|p| p.resolved_url(repo))
                .ok_or(fs_err!(
                    ErrorCode::InvalidConfig,
                    "Requested provider not provided"
                )),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_private_packages() {
        // Test get_provider_info()
        let json_provider_str = r#"
            [
                {
                    "org": "dbt-labs",
                    "url": "https://{token}@github.com/dbt-labs/{repo}.git",
                    "token": "a_token",
                    "provider": "github"
                }
            ]"#;

        #[allow(clippy::disallowed_methods)]
        std::env::set_var("DBT_ENV_PRIVATE_GIT_PROVIDER_INFO", json_provider_str);

        let provider_info = get_provider_info();
        let provider = &provider_info.get("dbt-labs").unwrap()[0];
        assert_eq!(provider.org, "dbt-labs".to_string());

        // Test get_resolved_url()
        let private_package = PrivatePackage {
            private: "dbt-labs/dbt-integration-project".to_string().into(),
            revision: Some("dbt/1.0.0".to_string()),
            provider: None,
            subdirectory: None,
            warn_unpinned: None,
            unrendered: HashMap::new(),
        };

        let resolved_url = get_resolved_url(&private_package).unwrap();
        assert_eq!(
            resolved_url,
            "https://a_token@github.com/dbt-labs/dbt-integration-project.git".to_string()
        );
    }

    #[test]
    fn test_local_private_packages_github() {
        // Make sure the variable is not set as we want to simulate a local run.
        #[allow(clippy::disallowed_methods)]
        std::env::remove_var("DBT_ENV_PRIVATE_GIT_PROVIDER_INFO");

        let private_package = PrivatePackage {
            private: "dbt-labs/dbt-integration-project".to_string().into(),
            revision: None,
            provider: Some("github".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            unrendered: HashMap::new(),
        };

        let resolved_url = get_resolved_url(&private_package).unwrap();
        assert_eq!(
            resolved_url,
            "git@github.com:dbt-labs/dbt-integration-project.git".to_string()
        );
    }

    #[test]
    fn test_local_private_packages_gitlab() {
        // Make sure the variable is not set as we want to simulate a local run.
        #[allow(clippy::disallowed_methods)]
        std::env::remove_var("DBT_ENV_PRIVATE_GIT_PROVIDER_INFO");

        let private_package = PrivatePackage {
            private: "dbt-labs/unrestricted/nesting/allowed/dbt-integration-project"
                .to_string()
                .into(),
            revision: None,
            provider: Some("gitlab".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            unrendered: HashMap::new(),
        };

        let resolved_url = get_resolved_url(&private_package).unwrap();
        assert_eq!(
            resolved_url,
            "git@gitlab.com:dbt-labs/unrestricted/nesting/allowed/dbt-integration-project.git"
                .to_string()
        );
    }
    #[test]
    fn test_local_private_packages_azure() {
        // Make sure the variable is not set as we want to simulate a local run.
        #[allow(clippy::disallowed_methods)]
        std::env::remove_var("DBT_ENV_PRIVATE_GIT_PROVIDER_INFO");

        let private_package = PrivatePackage {
            private: "dbt-labs/dbt-integration-project/some-repo"
                .to_string()
                .into(),
            revision: None,
            provider: Some("ado".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            unrendered: HashMap::new(),
        };

        let resolved_url = get_resolved_url(&private_package).unwrap();
        assert_eq!(
            resolved_url,
            "git@ssh.dev.azure.com:v3/dbt-labs/dbt-integration-project/some-repo".to_string()
        );
    }

    #[test]
    fn test_local_private_packages_error() {
        // Make sure the variable is not set as we want to simulate a local run.
        #[allow(clippy::disallowed_methods)]
        std::env::remove_var("DBT_ENV_PRIVATE_GIT_PROVIDER_INFO");

        let private_package = PrivatePackage {
            private: "dbt-labs/dbt-integration-project/some-repo"
                .to_string()
                .into(),
            revision: None,
            provider: Some("unknown".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            unrendered: HashMap::new(),
        };

        let resolved_url = get_resolved_url(&private_package);
        assert!(resolved_url.is_err_and(|e| e.code == ErrorCode::InvalidConfig));
    }
}
