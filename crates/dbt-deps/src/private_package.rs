use dbt_common::{ErrorCode, FsResult, err};
use dbt_schemas::schemas::ResolvedCloudConfig;
use dbt_schemas::schemas::packages::{PrivatePackage, PrivatePackageProvider};
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
    #[serde(default)]
    provider: Option<String>,
}

impl ProviderDetail {
    /// Parses the raw provider string, emitting a deprecation warning if needed.
    fn parsed_provider(&self) -> Option<PrivatePackageProvider> {
        let raw = self.provider.as_deref()?;
        if raw == "azure_devops" {
            tracing::warn!(
                "Provider 'azure_devops' is deprecated; use 'ado' instead. \
                 The 'ado' provider supports both 2-part (org/repo) and 3-part (org/project/repo) paths."
            );
        }
        PrivatePackageProvider::deserialize(serde_json::Value::String(raw.to_string())).ok()
    }
}

/// `azure_active_directory` has no SSH clone form, so it's excluded here.
const VALID_FOR_SSH: &str = "github, gitlab, ado, azure_devops";

fn providers_equivalent(
    entry: Option<PrivatePackageProvider>,
    requested: Option<PrivatePackageProvider>,
) -> bool {
    match (entry, requested) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(entry), Some(requested)) => entry.equivalent(requested),
    }
}

impl ProviderDetail {
    fn is_ado(&self) -> bool {
        self.parsed_provider()
            .is_some_and(PrivatePackageProvider::is_ado)
    }

    fn resolved_url(&self, private_def: &PrivateDefinition) -> String {
        if self.is_ado() {
            ADOGitURL::new(self.url.clone()).resolve(
                &self.token,
                &private_def.groups.join("/"),
                &private_def.repo_name,
            )
        } else {
            let git_url = GitURL::new(self.url.clone());
            git_url.resolve(&self.token, &private_def.repo_name)
        }
    }

    fn matches_private_definition(
        &self,
        private_def: &PrivateDefinition,
        provider: Option<PrivatePackageProvider>,
    ) -> bool {
        let parsed_provider = self.parsed_provider();
        if !providers_equivalent(parsed_provider, provider) {
            return false;
        }

        if !self.is_ado() {
            let git_url = GitURL::new(self.url.clone());
            return git_url.can_resolve(private_def);
        }

        // For Azure DevOps entries, compare the project segment unless one side omits it.
        // A 2-part package (no project) can match a concrete-project entry but not a {project} placeholder.
        let git_url = ADOGitURL::new(self.url.clone());
        let url_has_project = !git_url.get_definition().groups.is_empty();
        let pkg_has_project = !private_def.groups.is_empty();

        // If the URL has a {project} or {group} placeholder and the package has no project,
        // that's an unfilled placeholder — don't match.
        if url_has_project && !pkg_has_project {
            let url_def = git_url.get_definition();
            let url_group = url_def.groups.join("/");
            if url_group == "{project}" || url_group == "{group}" {
                return false;
            }
        }

        // Compare the project if both sides have one, or neither has one.
        let compare_project =
            (url_has_project && pkg_has_project) || (!url_has_project && !pkg_has_project);
        git_url.can_resolve(private_def, compare_project)
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

fn path_component_eq(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn groups_eq(left: &[String], right: &[String]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left_group, right_group)| path_component_eq(left_group, right_group))
}

/// Matches a URL template's project segment against the requested one. A lone
/// `{project}`/`{group}` placeholder accepts any project; concrete segments must match.
fn ado_project_matches(url_groups: &[String], def_groups: &[String]) -> bool {
    match url_groups {
        [placeholder] if placeholder == "{project}" || placeholder == "{group}" => {
            !def_groups.is_empty()
        }
        _ => groups_eq(url_groups, def_groups),
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
        if !path_component_eq(&url_def.org_name, &private_def.org_name) {
            return false;
        }

        // Compare groups (for multi-level paths)
        if !groups_eq(&url_def.groups, &private_def.groups) {
            return false;
        }

        // Compare repo names (allowing for {repo} wildcard)
        if url_def.is_repo_wildcard()
            || path_component_eq(&url_def.repo_name, &private_def.repo_name)
        {
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

    /// Matches a package definition against this URL template. `compare_project` is false for
    /// 2-part packages, where the project comes from the template rather than the package.
    pub fn can_resolve(&self, private_def: &PrivateDefinition, compare_project: bool) -> bool {
        let url_def = self.get_definition();

        if !path_component_eq(&url_def.org_name, &private_def.org_name) {
            return false;
        }

        // Entries sharing an org differ only by project, so the project selects the entry.
        if compare_project && !ado_project_matches(&url_def.groups, &private_def.groups) {
            return false;
        }

        // Compare repo names (allowing for {repo} wildcard)
        if url_def.is_repo_wildcard()
            || path_component_eq(&url_def.repo_name, &private_def.repo_name)
        {
            return true;
        }

        false
    }

    pub fn resolve(&self, token: &str, project: &str, repo: &str) -> String {
        self.url
            .replace("{token}", token)
            .replace("{project}", project)
            .replace("{group}", project)
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
        if provider.matches_private_definition(&private_def, private_package.provider) {
            private_package_usage_event(
                cloud_config,
                private_package.private.deref(),
                private_package.provider.map(PrivatePackageProvider::as_str),
                true,
                provider
                    .parsed_provider()
                    .map(PrivatePackageProvider::as_str),
            );
            return Ok(provider.resolved_url(&private_def));
        }
    }

    // No matching provider found
    private_package_usage_event(
        cloud_config,
        private_package.private.deref(),
        private_package.provider.map(PrivatePackageProvider::as_str),
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
    let provider = private_package
        .provider
        .unwrap_or(PrivatePackageProvider::Github);
    match provider {
        PrivatePackageProvider::Github => Ok(format!(
            "git@github.com:{}.git",
            private_package.private.deref()
        )),
        PrivatePackageProvider::Gitlab => Ok(format!(
            "git@gitlab.com:{}.git",
            private_package.private.deref()
        )),
        // An SSH clone URL requires the project for ado (whether spelled as "ado" or "azure_devops").
        PrivatePackageProvider::Ado => {
            let def = PrivateDefinition::build(private_package.private.deref());
            if def.groups.is_empty() {
                return err!(
                    ErrorCode::InvalidConfig,
                    "The '{}' provider requires org/project/repo format (3 parts), got: '{}'",
                    provider.as_str(),
                    private_package.private.deref()
                );
            }
            Ok(format!(
                "git@ssh.dev.azure.com:v3/{}",
                private_package.private.deref()
            ))
        }
        // `azure_active_directory` is only meaningful for platform-hosted runs.
        PrivatePackageProvider::AzureActiveDirectory => err!(
            ErrorCode::InvalidConfig,
            r#"Invalid private package configuration: '{}' provider: '{}'. Valid providers are: {}"#,
            private_package.private.deref(),
            provider.as_str(),
            VALID_FOR_SSH
        ),
    }
}
