use dbt_common::{ErrorCode, FsResult, err};
use dbt_schemas::schemas::packages::PrivatePackage;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use serde_json;
use std::ops::Deref;
use url::Url;

#[derive(Debug, Deserialize, Serialize)]
pub struct ProviderDetail {
    url: String,
    token: String,
    org: String,
    provider: Option<String>,
}

impl ProviderDetail {
    fn resolved_url(&self, repo: &str) -> String {
        if self.provider.as_deref() == Some("azure_active_directory") {
            let git_url = ADOGitURL::new(self.url.clone());
            git_url.resolve(&self.token, repo)
        } else {
            let git_url = GitURL::new(self.url.clone());
            git_url.resolve(&self.token, repo)
        }
    }

    fn matches_private_definition(
        &self,
        private_def: &PrivateDefinition,
        provider: Option<&str>,
    ) -> bool {
        // Check if provider matches (if specified)
        if let Some(requested_provider) = provider {
            if self.provider.as_deref() != Some(requested_provider) {
                return false;
            }
        }

        // Use appropriate GitURL type based on provider
        if self.provider.as_deref() == Some("azure_active_directory") {
            let git_url = ADOGitURL::new(self.url.clone());
            git_url.can_resolve(private_def)
        } else {
            let git_url = GitURL::new(self.url.clone());
            git_url.can_resolve(private_def)
        }
    }
}

#[derive(Debug, Clone)]
pub struct PrivateDefinition {
    pub org_name: String,
    pub groups: Vec<String>,
    pub repo_name: String,
}

impl PrivateDefinition {
    pub fn build(s: &str) -> Self {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() < 2 {
            panic!("Private definition must have at least org/repo format");
        }

        let org_name = parts[0].to_string();
        let repo_name = parts[parts.len() - 1].to_string();
        let groups = if parts.len() > 2 {
            parts[1..parts.len() - 1]
                .iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            Vec::new()
        };

        Self {
            org_name,
            groups,
            repo_name,
        }
    }

    pub fn to_path_string(&self) -> String {
        if self.groups.is_empty() {
            format!("{}/{}", self.org_name, self.repo_name)
        } else {
            let groups_str = self.groups.join("/");
            format!("{}/{}/{}", self.org_name, groups_str, self.repo_name)
        }
    }

    pub fn is_repo_wildcard(&self) -> bool {
        self.repo_name == "{repo}"
    }
}

fn extract_path_from_url(url: String) -> String {
    // 1) parse
    let parsed =
        Url::parse(&url).unwrap_or_else(|e| panic!("Failed to parse URL `{}`: {}", &url, e));

    // 2) grab the raw path (no leading slash)
    let raw = parsed.path().trim_start_matches('/');

    // 3) percent-decode it back to "{repo}.git"
    let decoded = percent_decode_str(raw)
        .decode_utf8()
        .expect("URL path was not valid UTF-8");

    // 4) drop the ".git" suffix if present
    decoded.trim_end_matches(".git").to_string()
}

#[derive(Debug)]
pub struct GitURL {
    url: String,
}

impl GitURL {
    pub fn new(url: String) -> Self {
        Self { url }
    }

    pub fn get_definition(&self) -> PrivateDefinition {
        // Extract the path part and remove .git suffix
        let path = extract_path_from_url(self.url.clone());
        PrivateDefinition::build(&path)
    }

    pub fn can_resolve(&self, private_def: &PrivateDefinition) -> bool {
        let url_def = self.get_definition();

        // Compare org names
        if url_def.org_name != private_def.org_name {
            return false;
        }

        // Compare groups (for multi-level paths)
        if url_def.groups != private_def.groups {
            return false;
        }

        // Compare repo names (allowing for {repo} wildcard)
        if url_def.is_repo_wildcard() || url_def.repo_name == private_def.repo_name {
            return true;
        }

        false
    }

    pub fn resolve(&self, token: &str, repo: &str) -> String {
        self.url.replace("{token}", token).replace("{repo}", repo)
    }
}

#[derive(Debug)]
pub struct ADOGitURL {
    url: String,
}

impl ADOGitURL {
    pub fn new(url: String) -> Self {
        Self { url }
    }

    pub fn get_definition(&self) -> PrivateDefinition {
        // Extract the path part and remove .git suffix
        let path = extract_path_from_url(self.url.clone());

        // Handle ADO's _git path structure
        let path = if path.contains("/_git/") {
            path.replace("/_git/", "/")
        } else {
            path
        };

        PrivateDefinition::build(&path)
    }

    pub fn can_resolve(&self, private_def: &PrivateDefinition) -> bool {
        let url_def = self.get_definition();

        // For ADO, we only compare org and repo, not groups (project is ignored)
        if url_def.org_name != private_def.org_name {
            return false;
        }

        // Compare repo names (allowing for {repo} wildcard)
        if url_def.is_repo_wildcard() || url_def.repo_name == private_def.repo_name {
            return true;
        }

        false
    }

    pub fn resolve(&self, token: &str, repo: &str) -> String {
        self.url.replace("{token}", token).replace("{repo}", repo)
    }
}

/// Retrieves Git provider configuration from environment variable
pub fn get_provider_info() -> Vec<ProviderDetail> {
    let git_providers_str =
        std::env::var("DBT_ENV_PRIVATE_GIT_PROVIDER_INFO").unwrap_or_else(|_| "[]".to_string());

    let provider_json: Vec<ProviderDetail> =
        serde_json::from_str(&git_providers_str).expect("Failed to parse git providers JSON");

    provider_json
}

/// Resolves a private package definition to its Git clone URL
pub fn get_resolved_url(private_package: &PrivatePackage) -> FsResult<String> {
    let provider_info = get_provider_info();
    let private_def = PrivateDefinition::build(&private_package.private);

    // If we did not get any provider information then we run locally and default to ssh git.
    if provider_info.is_empty() {
        return get_local_resolved_url(private_package);
    }

    // Iterate over all providers and try to match each one
    for provider in provider_info {
        if provider.matches_private_definition(&private_def, private_package.provider.as_deref()) {
            return Ok(provider.resolved_url(&private_def.repo_name));
        }
    }

    // No matching provider found
    err!(
        ErrorCode::InvalidConfig,
        "No matching provider found for private definition '{}' with provider {:?}",
        private_package.private.deref(),
        private_package.provider
    )
}

fn get_local_resolved_url(private_package: &PrivatePackage) -> FsResult<String> {
    match private_package.provider.as_deref().unwrap_or_default() {
        "github" => Ok(format!(
            "git@github.com:{}.git",
            private_package.private.deref()
        )),
        "gitlab" => Ok(format!(
            "git@gitlab.com:{}.git",
            private_package.private.deref()
        )),
        "ado" => Ok(format!(
            "git@ssh.dev.azure.com:v3/{}",
            private_package.private.deref()
        )),
        _ => {
            err!(
                ErrorCode::InvalidConfig,
                r#"Invalid private package configuration: '{}' provider: '{}'"#,
                private_package.private.deref(),
                private_package.provider.as_deref().unwrap_or_default()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn clear_env() {
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::remove_var("DBT_ENV_PRIVATE_GIT_PROVIDER_INFO");
        }
    }

    fn set_env(json_provider_str: &str) {
        unsafe {
            #[allow(clippy::disallowed_methods)]
            std::env::set_var("DBT_ENV_PRIVATE_GIT_PROVIDER_INFO", json_provider_str);
        }
    }

    #[test]
    fn test_private_packages() {
        clear_env();

        // Test get_provider_info() with multiple providers
        // GitHub clone URLs have Org + Interpolated Repo Name
        // Azure DevOps clone URLs have Org + Project + Interpolated Repo Name
        // Gitlab clone URLs have Org + Group + Fixed Repo Name
        let json_provider_str = r#"
            [
                {
                    "org": "github-labs",
                    "url": "https://{token}@github.com/github-labs/{repo}.git",
                    "token": "the_github_token",
                    "provider": "github"
                },
                {
                    "org": "ado-labs",
                    "url": "https://{token}@dev.azure.com/ado-labs/ado-project/_git/{repo}.git",
                    "token": "the_ado_token",
                    "provider": "azure_active_directory"
                },
                {
                    "org": "gitlab-labs",
                    "url": "https://{token}@gitlab.com/gitlab-labs/gitlab-group/my-repo.git",
                    "token": "the_gitlab_token",
                    "provider": "gitlab"
                }
            ]"#;

        set_env(json_provider_str);

        let provider_info = get_provider_info();
        assert_eq!(provider_info[0].org, "github-labs");
        assert_eq!(provider_info[1].org, "ado-labs");
        assert_eq!(provider_info[2].org, "gitlab-labs");

        // Test get_resolved_url()
        let private_package = PrivatePackage {
            private: "github-labs/my-repo".to_string().into(),
            revision: Some("dbt/1.0.0".to_string()),
            provider: Some("github".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };
        let resolved_url_github = get_resolved_url(&private_package).unwrap();
        assert_eq!(
            resolved_url_github,
            "https://the_github_token@github.com/github-labs/my-repo.git"
        );

        // Test get_resolved_url() for Azure DevOps
        let private_package_ado = PrivatePackage {
            private: "ado-labs/ado-repo".to_string().into(),
            revision: Some("dbt/1.0.0".to_string()),
            provider: Some("azure_active_directory".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };
        let resolved_url_ado = get_resolved_url(&private_package_ado).unwrap();
        assert_eq!(
            resolved_url_ado,
            "https://the_ado_token@dev.azure.com/ado-labs/ado-project/_git/ado-repo.git"
        );

        // Test get_resolved_url() for GitLab
        let private_package_gitlab = PrivatePackage {
            private: "gitlab-labs/gitlab-group/my-repo".to_string().into(),
            revision: Some("dbt/1.0.0".to_string()),
            provider: Some("gitlab".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };
        let resolved_url_gitlab = get_resolved_url(&private_package_gitlab).unwrap();
        assert_eq!(
            resolved_url_gitlab,
            "https://the_gitlab_token@gitlab.com/gitlab-labs/gitlab-group/my-repo.git"
        );

        clear_env();
    }

    #[test]
    fn test_private_package_github() {
        clear_env();

        // Test GitHub matching: should match any repo in the same org
        let json_provider_str = r#"
            [
                {
                    "org": "github-labs",
                    "url": "https://{token}@github.com/github-labs/{repo}.git",
                    "token": "the_github_token",
                    "provider": "github"
                }
            ]"#;

        set_env(json_provider_str);

        // Should match: same org, different repo
        let private_package_1 = PrivatePackage {
            private: "github-labs/repo1".to_string().into(),
            revision: Some("main".to_string()),
            provider: Some("github".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };
        let resolved_url_1 = get_resolved_url(&private_package_1).unwrap();
        assert_eq!(
            resolved_url_1,
            "https://the_github_token@github.com/github-labs/repo1.git"
        );

        // Should match: same org, another repo
        let private_package_2 = PrivatePackage {
            private: "github-labs/repo2".to_string().into(),
            revision: Some("main".to_string()),
            provider: Some("github".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };
        let resolved_url_2 = get_resolved_url(&private_package_2).unwrap();
        assert_eq!(
            resolved_url_2,
            "https://the_github_token@github.com/github-labs/repo2.git"
        );

        // Should fail: different org
        let private_package_3 = PrivatePackage {
            private: "other-org/repo1".to_string().into(),
            revision: Some("main".to_string()),
            provider: Some("github".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };
        let resolved_url_3 = get_resolved_url(&private_package_3);
        assert!(resolved_url_3.is_err_and(|e| e.code == ErrorCode::InvalidConfig));

        clear_env();
    }

    #[test]
    fn test_private_package_azure() {
        clear_env();

        // Test ADO matching: should match any repo under the same org
        // Provider info uses org+project+repo, but private definition only uses org+repo
        let json_provider_str = r#"
            [
                {
                    "org": "ado-labs",
                    "url": "https://{token}@dev.azure.com/ado-labs/ado-project/_git/{repo}.git",
                    "token": "the_ado_token",
                    "provider": "azure_active_directory"
                }
            ]"#;

        set_env(json_provider_str);

        // Should match: same org, different repo
        let private_package_1 = PrivatePackage {
            private: "ado-labs/repo1".to_string().into(),
            revision: Some("main".to_string()),
            provider: Some("azure_active_directory".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };
        let resolved_url_1 = get_resolved_url(&private_package_1).unwrap();
        assert_eq!(
            resolved_url_1,
            "https://the_ado_token@dev.azure.com/ado-labs/ado-project/_git/repo1.git"
        );

        // Should match: same org, another repo
        let private_package_2 = PrivatePackage {
            private: "ado-labs/repo2".to_string().into(),
            revision: Some("main".to_string()),
            provider: Some("azure_active_directory".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };
        let resolved_url_2 = get_resolved_url(&private_package_2).unwrap();
        assert_eq!(
            resolved_url_2,
            "https://the_ado_token@dev.azure.com/ado-labs/ado-project/_git/repo2.git"
        );

        // Should fail: different org
        let private_package_3 = PrivatePackage {
            private: "other-org/repo1".to_string().into(),
            revision: Some("main".to_string()),
            provider: Some("azure_active_directory".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };
        let resolved_url_3 = get_resolved_url(&private_package_3);
        assert!(resolved_url_3.is_err_and(|e| e.code == ErrorCode::InvalidConfig));

        clear_env();
    }

    #[test]
    fn test_private_package_gitlab() {
        clear_env();

        // Test GitLab matching: should only match exact org+group+repo combination
        let json_provider_str = r#"
            [
                {
                    "org": "gitlab-labs",
                    "url": "https://{token}@gitlab.com/gitlab-labs/gitlab-group/my-repo.git",
                    "token": "the_gitlab_token",
                    "provider": "gitlab"
                }
            ]"#;

        set_env(json_provider_str);

        // Should match: exact org+group+repo combination
        let private_package_1 = PrivatePackage {
            private: "gitlab-labs/gitlab-group/my-repo".to_string().into(),
            revision: Some("main".to_string()),
            provider: Some("gitlab".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };
        let resolved_url_1 = get_resolved_url(&private_package_1).unwrap();
        assert_eq!(
            resolved_url_1,
            "https://the_gitlab_token@gitlab.com/gitlab-labs/gitlab-group/my-repo.git"
        );

        // Should fail: same org+group, different repo
        let private_package_2 = PrivatePackage {
            private: "gitlab-labs/gitlab-group/other-repo".to_string().into(),
            revision: Some("main".to_string()),
            provider: Some("gitlab".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };
        let resolved_url_2 = get_resolved_url(&private_package_2);
        assert!(resolved_url_2.is_err_and(|e| e.code == ErrorCode::InvalidConfig));

        // Should fail: different org
        let private_package_3 = PrivatePackage {
            private: "other-org/gitlab-group/my-repo".to_string().into(),
            revision: Some("main".to_string()),
            provider: Some("gitlab".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };
        let resolved_url_3 = get_resolved_url(&private_package_3);
        assert!(resolved_url_3.is_err_and(|e| e.code == ErrorCode::InvalidConfig));

        clear_env();
    }

    #[test]
    fn test_local_private_packages_gitlab() {
        clear_env();

        let private_package = PrivatePackage {
            private: "dbt-labs/unrestricted/nesting/allowed/dbt-integration-project"
                .to_string()
                .into(),
            revision: None,
            provider: Some("gitlab".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };

        let resolved_url = get_resolved_url(&private_package).unwrap();
        assert_eq!(
            resolved_url,
            "git@gitlab.com:dbt-labs/unrestricted/nesting/allowed/dbt-integration-project.git"
        );

        clear_env();
    }

    #[test]
    fn test_local_private_packages_azure() {
        clear_env();

        let private_package = PrivatePackage {
            private: "dbt-labs/dbt-integration-project/some-repo"
                .to_string()
                .into(),
            revision: None,
            provider: Some("ado".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };

        let resolved_url = get_resolved_url(&private_package).unwrap();
        assert_eq!(
            resolved_url,
            "git@ssh.dev.azure.com:v3/dbt-labs/dbt-integration-project/some-repo"
        );

        clear_env();
    }

    #[test]
    fn test_local_private_packages_error() {
        clear_env();

        let private_package = PrivatePackage {
            private: "dbt-labs/dbt-integration-project/some-repo"
                .to_string()
                .into(),
            revision: None,
            provider: Some("unknown".to_string()),
            subdirectory: None,
            warn_unpinned: None,
            __unrendered__: HashMap::new(),
        };

        let resolved_url = get_resolved_url(&private_package);
        assert!(resolved_url.is_err_and(|e| e.code == ErrorCode::InvalidConfig));

        clear_env();
    }
}
