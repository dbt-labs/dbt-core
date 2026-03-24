use crate::DbtCloudConfig;
use dbt_schemas::schemas::project::ProjectDbtCloudConfig;

pub use dbt_schemas::schemas::{CloudCredentials, ResolvedCloudConfig};

/// Returns the value of the environment variable `name`, or `None` if it is
/// unset or empty. This prevents empty env vars from overriding valid config
/// file values.
fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// Build a fully-resolved cloud config by applying precedence:
/// env var > dbt_project.yml > dbt_cloud.yml.
pub fn resolve_cloud_config(
    dbt_cloud_yml: Option<&DbtCloudConfig>,
    project_dbt_cloud: Option<&ProjectDbtCloudConfig>,
) -> Option<ResolvedCloudConfig> {
    resolve_cloud_config_with_env_reader(dbt_cloud_yml, project_dbt_cloud, non_empty_env)
}

/// Internal: same as [`resolve_cloud_config`] but with an injectable env reader
/// for testing.
fn resolve_cloud_config_with_env_reader(
    dbt_cloud_yml: Option<&DbtCloudConfig>,
    project_dbt_cloud: Option<&ProjectDbtCloudConfig>,
    env_reader: impl Fn(&str) -> Option<String>,
) -> Option<ResolvedCloudConfig> {
    // Find the active project from dbt_cloud.yml by matching project_id from
    // dbt_project.yml, or falling back to context.active_project.
    let active_project = dbt_cloud_yml.and_then(|config| {
        let lookup_id = project_dbt_cloud
            .and_then(|p| p.project_id.as_ref())
            .map(|v| v.to_string())
            .unwrap_or_else(|| config.context.active_project.clone());
        config.get_project_by_id(&lookup_id)
    });

    // Resolve project_id first — needed for credential mixing guard.
    let project_id = env_reader("DBT_CLOUD_PROJECT_ID").or_else(|| {
        project_dbt_cloud
            .and_then(|p| p.project_id.as_ref())
            .map(|v| v.to_string())
            .or_else(|| active_project.map(|p| p.project_id.clone()))
    });

    // Credential mixing guard: if env var overrides project_id to a different
    // project than dbt_cloud.yml, don't use dbt_cloud.yml credentials.
    let project_id_matches = active_project.is_some_and(|p| {
        project_id
            .as_ref()
            .is_some_and(|resolved| resolved == &p.project_id)
    });
    let safe_project = if project_id_matches {
        active_project
    } else {
        None
    };

    let account_id = env_reader("DBT_CLOUD_ACCOUNT_ID").or_else(|| {
        project_dbt_cloud
            .and_then(|p| p.account_id.as_ref())
            .map(|v| v.to_string())
            .or_else(|| safe_project.map(|p| p.account_id.clone()))
    });

    let token =
        env_reader("DBT_CLOUD_TOKEN").or_else(|| safe_project.map(|p| p.token_value.clone()));

    let host = env_reader("DBT_CLOUD_ACCOUNT_HOST").or_else(|| {
        project_dbt_cloud
            .and_then(|p| {
                p.tenant_hostname
                    .clone()
                    .filter(|h| !h.is_empty())
                    .or_else(|| p.account_host.clone())
            })
            .or_else(|| safe_project.map(|p| p.account_host.clone()))
    });

    // Build credentials only when all 3 required fields are present.
    let credentials = match (&account_id, &host, &token) {
        (Some(aid), Some(h), Some(t)) => Some(CloudCredentials {
            account_id: aid.clone(),
            host: h.clone(),
            token: t.clone(),
        }),
        _ => None,
    };

    let account_identifier = env_reader("DBT_CLOUD_ACCOUNT_IDENTIFIER");

    let environment_id = env_reader("DBT_CLOUD_ENVIRONMENT_ID");

    let defer_env_id = env_reader("DBT_CLOUD_DEFER_ENV_ID").or_else(|| {
        project_dbt_cloud
            .and_then(|p| p.defer_env_id.as_ref())
            .map(|v| v.to_string())
            .or_else(|| dbt_cloud_yml.and_then(|c| c.context.defer_env_id.clone()))
    });

    let resolved = ResolvedCloudConfig {
        credentials,
        project_id,
        account_identifier,
        environment_id,
        defer_env_id,
    };

    // Exhaustive match ensures new fields aren't forgotten.
    if let ResolvedCloudConfig {
        credentials: None,
        project_id: None,
        account_identifier: None,
        environment_id: None,
        defer_env_id: None,
    } = &resolved
    {
        None
    } else {
        Some(resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::schemas::serde::StringOrInteger;
    use dbt_schemas::schemas::{DbtCloudContext, DbtCloudProject};
    use std::collections::HashMap;

    /// Test env reader that filters empty strings (like non_empty_env).
    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |name: &str| map.get(name).filter(|v| !v.is_empty()).cloned()
    }

    fn cloud_yml(project_id: &str, host: &str, token: &str) -> DbtCloudConfig {
        DbtCloudConfig {
            version: "1".to_string(),
            context: DbtCloudContext {
                active_project: project_id.to_string(),
                active_host: host.to_string(),
                defer_env_id: None,
            },
            projects: vec![DbtCloudProject {
                project_name: "test".to_string(),
                project_id: project_id.to_string(),
                account_name: "acme".to_string(),
                account_id: "111".to_string(),
                account_host: host.to_string(),
                token_name: "pat".to_string(),
                token_value: token.to_string(),
            }],
        }
    }

    fn project_cloud(
        pid: Option<&str>,
        host: Option<&str>,
        defer: Option<&str>,
    ) -> ProjectDbtCloudConfig {
        ProjectDbtCloudConfig {
            project_id: pid.map(|s| StringOrInteger::String(s.to_string())),
            account_host: host.map(|s| s.to_string()),
            defer_env_id: defer.map(|s| StringOrInteger::String(s.to_string())),
            account_id: None,
            job_id: None,
            run_id: None,
            api_key: None,
            application: None,
            environment: None,
            tenant_hostname: None,
        }
    }

    #[test]
    fn no_config_returns_none() {
        assert!(resolve_cloud_config_with_env_reader(None, None, env(&[])).is_none());
    }

    #[test]
    fn env_vars_only_partial_no_credentials() {
        // Only project_id — not enough for credentials, but project_id is a top-level field
        let r = resolve_cloud_config_with_env_reader(
            None,
            None,
            env(&[("DBT_CLOUD_PROJECT_ID", "123")]),
        )
        .unwrap();
        assert!(r.credentials.is_none());
        assert_eq!(r.project_id.as_deref(), Some("123"));
    }

    #[test]
    fn env_vars_full_credentials() {
        let r = resolve_cloud_config_with_env_reader(
            None,
            None,
            env(&[
                ("DBT_CLOUD_PROJECT_ID", "123"),
                ("DBT_CLOUD_ACCOUNT_ID", "456"),
                ("DBT_CLOUD_ACCOUNT_HOST", "cloud.getdbt.com"),
                ("DBT_CLOUD_TOKEN", "tok"),
            ]),
        )
        .unwrap();
        assert_eq!(r.project_id.as_deref(), Some("123"));
        let creds = r.credentials.unwrap();
        assert_eq!(creds.account_id, "456");
        assert_eq!(creds.host, "cloud.getdbt.com");
        assert_eq!(creds.token, "tok");
    }

    #[test]
    fn cloud_yml_provides_full_credentials() {
        let yml = cloud_yml("456", "cloud.getdbt.com", "secret");
        let r = resolve_cloud_config_with_env_reader(Some(&yml), None, env(&[])).unwrap();
        assert_eq!(r.project_id.as_deref(), Some("456"));
        let creds = r.credentials.unwrap();
        assert_eq!(creds.host, "cloud.getdbt.com");
        assert_eq!(creds.token, "secret");
        assert_eq!(creds.account_id, "111");
    }

    #[test]
    fn dbt_project_yml_only() {
        let pc = project_cloud(Some("789"), Some("proj.dbt.com"), Some("def1"));
        let r = resolve_cloud_config_with_env_reader(None, Some(&pc), env(&[])).unwrap();
        // No token source → no credentials
        assert!(r.credentials.is_none());
        assert_eq!(r.defer_env_id.as_deref(), Some("def1"));
    }

    #[test]
    fn env_overrides_cloud_yml_host() {
        let yml = cloud_yml("456", "cloud.getdbt.com", "secret");
        let r = resolve_cloud_config_with_env_reader(
            Some(&yml),
            None,
            env(&[
                ("DBT_CLOUD_PROJECT_ID", "456"),
                ("DBT_CLOUD_ACCOUNT_HOST", "emea.dbt.com"),
            ]),
        )
        .unwrap();
        let creds = r.credentials.unwrap();
        assert_eq!(creds.host, "emea.dbt.com");
        assert_eq!(creds.token, "secret"); // still from yml since project_id matches
    }

    #[test]
    fn dbt_project_yml_overrides_cloud_yml_host() {
        let yml = cloud_yml("456", "cloud.getdbt.com", "secret");
        let pc = project_cloud(Some("456"), Some("proj-override.dbt.com"), None);
        let r = resolve_cloud_config_with_env_reader(Some(&yml), Some(&pc), env(&[])).unwrap();
        let creds = r.credentials.unwrap();
        assert_eq!(creds.host, "proj-override.dbt.com");
        assert_eq!(creds.token, "secret");
    }

    #[test]
    fn credential_mixing_guard() {
        let yml = cloud_yml("456", "cloud.getdbt.com", "secret-for-456");
        // project_id mismatch → config exists (project_id is top-level) but no credentials
        let r = resolve_cloud_config_with_env_reader(
            Some(&yml),
            None,
            env(&[("DBT_CLOUD_PROJECT_ID", "999")]),
        )
        .unwrap();
        assert!(r.credentials.is_none());
        assert_eq!(r.project_id.as_deref(), Some("999"));

        // With an environment_id present, we get a config but no credentials
        let r = resolve_cloud_config_with_env_reader(
            Some(&yml),
            None,
            env(&[
                ("DBT_CLOUD_PROJECT_ID", "999"),
                ("DBT_CLOUD_ENVIRONMENT_ID", "e1"),
            ]),
        )
        .unwrap();
        assert!(r.credentials.is_none());
        assert_eq!(r.environment_id.as_deref(), Some("e1"));
    }

    #[test]
    fn empty_env_var_does_not_override() {
        let yml = cloud_yml("456", "cloud.getdbt.com", "secret");
        let r = resolve_cloud_config_with_env_reader(
            Some(&yml),
            None,
            env(&[("DBT_CLOUD_PROJECT_ID", "")]),
        )
        .unwrap();
        assert_eq!(r.project_id.as_deref(), Some("456"));
    }

    #[test]
    fn defer_env_id_three_tier_precedence() {
        let mut yml = cloud_yml("456", "cloud.getdbt.com", "secret");
        yml.context.defer_env_id = Some("context-defer".to_string());
        let pc = project_cloud(Some("456"), None, Some("proj-defer"));

        // Env var wins over both
        let r = resolve_cloud_config_with_env_reader(
            Some(&yml),
            Some(&pc),
            env(&[("DBT_CLOUD_DEFER_ENV_ID", "env-defer")]),
        )
        .unwrap();
        assert_eq!(r.defer_env_id.as_deref(), Some("env-defer"));

        // dbt_project.yml wins over dbt_cloud.yml context
        let r = resolve_cloud_config_with_env_reader(Some(&yml), Some(&pc), env(&[])).unwrap();
        assert_eq!(r.defer_env_id.as_deref(), Some("proj-defer"));

        // dbt_cloud.yml context is last fallback
        let r = resolve_cloud_config_with_env_reader(Some(&yml), None, env(&[])).unwrap();
        assert_eq!(r.defer_env_id.as_deref(), Some("context-defer"));
    }

    #[test]
    fn account_identifier_is_separate_from_account_id() {
        // account_identifier is a separate field, not a fallback for account_id
        let yml = cloud_yml("456", "cloud.getdbt.com", "secret");
        let r = resolve_cloud_config_with_env_reader(
            Some(&yml),
            None,
            env(&[("DBT_CLOUD_ACCOUNT_IDENTIFIER", "my-org")]),
        )
        .unwrap();
        // account_id comes from dbt_cloud.yml, account_identifier from env
        assert_eq!(r.credentials.as_ref().unwrap().account_id, "111");
        assert_eq!(r.account_identifier.as_deref(), Some("my-org"));
    }

    #[test]
    fn host_prefers_tenant_hostname() {
        let mut pc = project_cloud(Some("123"), None, None);
        pc.account_host = Some("account-host.dbt.com".to_string());
        pc.tenant_hostname = Some("tenant.dbt.com".to_string());

        // Use cloud_yml to provide full credentials so we can verify host
        let yml = cloud_yml("123", "yml-host.dbt.com", "secret");
        let r = resolve_cloud_config_with_env_reader(Some(&yml), Some(&pc), env(&[])).unwrap();
        let creds = r.credentials.unwrap();
        assert_eq!(creds.host, "tenant.dbt.com"); // tenant_hostname preferred
    }
}
