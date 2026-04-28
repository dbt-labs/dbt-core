use dbt_common::{ErrorCode, FsResult, err};
use dbt_schemas::schemas::ResolvedCloudConfig;
use dbt_schemas::schemas::packages::PrivatePackage;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use serde_json;
use std::ops::Deref;
use url::Url;
use vortex_events::private_package_usage_event;

#[derive(Debug, Deserialize, Serialize)]
pub struct ProviderDetail {
    url: String,
    token: String,
    org: String,
    provider: Option<String>,
}

impl ProviderDetail {
    fn resolved_url(&self, private_def: &PrivateDefinition) -> String {
        if self.is_azure_devops() {
            let git_url = ADOGitURL::new(self.url.clone());
            if self.is_ado() && !private_def.groups.is_empty() {
                let project = private_def.groups.join("/");
                git_url.resolve_with_project(&self.token, &project, &private_def.repo_name)
            } else {
                git_url.resolve(&self.token, &private_def.repo_name)
            }
        } else {
            let git_url = GitURL::new(self.url.clone());
            git_url.resolve(&self.token, &private_def.repo_name)
        }
    }

    fn is_ado(&self) -> bool {
        self.provider.as_deref() == Some("ado")
    }

    fn is_azure_active_directory(&self) -> bool {
        self.provider.as_deref() == Some("azure_active_directory")
    }

    fn is_azure_devops(&self) -> bool {
        self.is_ado() || self.is_azure_active_directory()
    }

    fn matches_private_definition(
        &self,
        private_def: &PrivateDefinition,
        provider: Option<&str>,
    ) -> bool {
        // Check if provider matches (if specified)
        // "ado" and "azure_active_directory" are distinct providers with different path requirements
        if let Some(requested_provider) = provider
            && self.provider.as_deref() != Some(requested_provider)
        {
            return false;
        }

        // Validate path structure based on provider type
        // "ado" expects 3-part names (org/project/repo)
        // "azure_active_directory" expects 2-part names (org/repo) for backward compatibility
        if self.is_ado() {
            // For "ado", the private definition should have groups (project)
            // URL template contains the project, so we match org and repo
            let git_url = ADOGitURL::new(self.url.clone());
            git_url.can_resolve_ado(private_def)
        } else if self.is_azure_active_directory() {
            // For "azure_active_directory", backward compatibility with 2-part names
            let git_url = ADOGitURL::new(self.url.clone());
            git_url.can_resolve_azure_active_directory(private_def)
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
                .map(|s| (*s).to_string())
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
        // Default behavior for backward compatibility
        self.can_resolve_azure_active_directory(private_def)
    }

    pub fn can_resolve_azure_active_directory(&self, private_def: &PrivateDefinition) -> bool {
        let url_def = self.get_definition();

        // For azure_active_directory, we only compare org and repo, not groups (project is in URL template)
        // Private definition should be 2-part: org/repo
        if url_def.org_name != private_def.org_name {
            return false;
        }

        // Compare repo names (allowing for {repo} wildcard)
        if url_def.is_repo_wildcard() || url_def.repo_name == private_def.repo_name {
            return true;
        }

        false
    }

    pub fn can_resolve_ado(&self, private_def: &PrivateDefinition) -> bool {
        // "ado" requires 3-part names: org/project/repo
        if private_def.groups.is_empty() {
            return false;
        }

        let url_def = self.get_definition();

        // The project in private_def is informational — the URL template contains the actual project.
        // We only match on org and repo.
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

    pub fn resolve_with_project(&self, token: &str, project: &str, repo: &str) -> String {
        self.url
            .replace("{token}", token)
            .replace("{project}", project)
            .replace("{repo}", repo)
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
pub fn get_resolved_url(
    private_package: &PrivatePackage,
    cloud_config: &Option<ResolvedCloudConfig>,
) -> FsResult<String> {
    let provider_info = get_provider_info();
    let private_def = PrivateDefinition::build(&private_package.private);

    // If we did not get any provider information then we run locally and default to ssh git.
    if provider_info.is_empty() {
        return get_local_resolved_url(private_package);
    }

    // Iterate over all providers and try to match each one
    for provider in provider_info {
        if provider.matches_private_definition(&private_def, private_package.provider.as_deref()) {
            private_package_usage_event(
                cloud_config,
                private_package.private.deref(),
                private_package.provider.as_deref(),
                true,
                provider.provider.as_deref(),
            );
            return Ok(provider.resolved_url(&private_def));
        }
    }

    // No matching provider found
    private_package_usage_event(
        cloud_config,
        private_package.private.deref(),
        private_package.provider.as_deref(),
        false,
        None,
    );
    err!(
        ErrorCode::InvalidConfig,
        "No matching provider found for private definition '{}' with provider {:?}",
        private_package.private.deref(),
        private_package.provider
    )
}

fn get_local_resolved_url(private_package: &PrivatePackage) -> FsResult<String> {
    // Default to "github" when provider is unspecified, matching dbt-core's behavior
    match private_package.provider.as_deref().unwrap_or("github") {
        "github" => Ok(format!(
            "git@github.com:{}.git",
            private_package.private.deref()
        )),
        "gitlab" => Ok(format!(
            "git@gitlab.com:{}.git",
            private_package.private.deref()
        )),
        "ado" | "azure_devops" => {
            // "ado"/"azure_devops" requires 3-part names: org/project/repo
            let def = PrivateDefinition::build(private_package.private.deref());
            if def.groups.is_empty() {
                return err!(
                    ErrorCode::InvalidConfig,
                    "The '{}' provider requires org/project/repo format (3 parts), got: '{}'",
                    private_package.provider.as_deref().unwrap_or_default(),
                    private_package.private.deref()
                );
            }
            Ok(format!(
                "git@ssh.dev.azure.com:v3/{}",
                private_package.private.deref()
            ))
        }
        _ => {
            err!(
                ErrorCode::InvalidConfig,
                r#"Invalid private package configuration: '{}' provider: '{}'. Valid providers are: github, gitlab, ado, azure_active_directory"#,
                private_package.private.deref(),
                private_package.provider.as_deref().unwrap_or_default()
            )
        }
    }
}
