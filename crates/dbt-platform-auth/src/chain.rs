use crate::{
    AuthError, Credential,
    resolver::{
        AuthResolver, CloudYamlResolver, EnvVarResolver, OAuthInteractiveResolver,
        OAuthPassiveResolver, ResolverKind,
    },
};
use std::collections::HashSet;

/// OAuth client ID registered with dbt platform.
/// See lsp/src/registration/webAuth/constants.ts
pub const OAUTH_CLIENT_ID: &str = "854ad54c885f03bbe6ca7eb1e75593fb";

/// Returns the effective OAuth client ID, preferring `DBT_OAUTH_CLIENT_ID` if set.
fn effective_client_id() -> String {
    std::env::var("DBT_OAUTH_CLIENT_ID").unwrap_or_else(|_| OAUTH_CLIENT_ID.to_owned())
}

/// An ordered chain of credential resolvers tried in sequence.
///
/// `resolve` walks the chain until a resolver returns credentials. `NotAuthenticated`
/// from a resolver is silently skipped (try next). Any other error is recorded and the
/// chain continues; if no credentials are found, the first non-`NotAuthenticated` error
/// is returned — otherwise `NotAuthenticated`.
pub struct AuthChain {
    resolvers: Vec<AuthResolver>,
}

impl Default for AuthChain {
    fn default() -> Self {
        AuthChainBuilder::default().build()
    }
}

impl AuthChain {
    /// Creates an interactive auth chain that appends an [`OAuthInteractiveResolver`]
    /// after the standard non-interactive resolvers. Uses [`OAUTH_CLIENT_ID`].
    ///
    /// Use this when the caller can prompt the user — e.g. a CLI `login` command.
    /// For headless contexts, use [`AuthChain::default`] instead.
    pub fn interactive() -> Self {
        let client_id = effective_client_id();
        AuthChain {
            resolvers: vec![
                AuthResolver::EnvVar(EnvVarResolver),
                AuthResolver::OAuthPassive(OAuthPassiveResolver::new(&client_id)),
                AuthResolver::CloudYaml(CloudYamlResolver::default()),
                AuthResolver::OAuthInteractive(OAuthInteractiveResolver::new(client_id)),
            ],
        }
    }

    pub async fn resolve(&self) -> Result<Credential, AuthError> {
        self.resolve_with_source().await.map(|(c, _)| c)
    }

    /// Like [`resolve`], but also returns which resolver produced the credential.
    pub async fn resolve_with_source(&self) -> Result<(Credential, ResolverKind), AuthError> {
        let mut first_error: Option<AuthError> = None;
        for resolver in &self.resolvers {
            match resolver.resolve().await {
                Ok(cred) => return Ok((cred, resolver.kind())),
                Err(AuthError::NotAuthenticated) => {}
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }
        Err(first_error.unwrap_or(AuthError::NotAuthenticated))
    }
}

/// Builds an [`AuthChain`] with an allowlist or denylist filter over the default
/// resolver set, or with an entirely custom resolver list.
///
/// **Default resolver order:** `EnvVar` → `OAuthPassive` → `CloudYaml`
pub struct AuthChainBuilder {
    resolvers: Vec<AuthResolver>,
}

impl Default for AuthChainBuilder {
    fn default() -> Self {
        Self::new(effective_client_id())
    }
}

impl AuthChainBuilder {
    /// Start with the default resolver chain using the given OAuth `client_id`.
    pub fn new(client_id: impl Into<String>) -> Self {
        let client_id = client_id.into();
        Self {
            resolvers: vec![
                AuthResolver::EnvVar(EnvVarResolver),
                AuthResolver::OAuthPassive(OAuthPassiveResolver::new(client_id)),
                AuthResolver::CloudYaml(CloudYamlResolver::default()),
            ],
        }
    }

    /// Start with a fully custom resolver list instead of the defaults.
    pub fn with_resolvers(resolvers: Vec<AuthResolver>) -> Self {
        Self { resolvers }
    }

    /// Retain only resolvers whose kind is in `kinds` (allowlist).
    pub fn allow_only(mut self, kinds: &[ResolverKind]) -> Self {
        let allowed: HashSet<ResolverKind> = kinds.iter().copied().collect();
        self.resolvers.retain(|r| allowed.contains(&r.kind()));
        self
    }

    /// Remove resolvers whose kind is in `kinds` (denylist).
    pub fn deny(mut self, kinds: &[ResolverKind]) -> Self {
        let denied: HashSet<ResolverKind> = kinds.iter().copied().collect();
        self.resolvers.retain(|r| !denied.contains(&r.kind()));
        self
    }

    pub fn build(self) -> AuthChain {
        AuthChain {
            resolvers: self.resolvers,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::CloudYamlResolver;
    use std::io::Write as _;
    use std::path::PathBuf;
    use tempfile::NamedTempFile;

    fn write_yaml(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    fn valid_yaml() -> &'static str {
        r#"
version: "1"
context:
  active-project: "proj-1"
  active-host: "ab123.us1.dbt.com"
projects:
  - project-name: "My Project"
    project-id: "proj-1"
    account-name: "acme"
    account-id: "42"
    account-host: "ab123.us1.dbt.com"
    token-name: "my-token"
    token-value: "dbtc_abc123"
"#
    }

    #[tokio::test]
    async fn chain_returns_first_successful_credential() {
        let f = write_yaml(valid_yaml());
        // EnvVar won't fire (no env vars set in this test), CloudYaml succeeds.
        let chain =
            AuthChainBuilder::with_resolvers(vec![AuthResolver::CloudYaml(CloudYamlResolver {
                dbt_project_path: None,
                path: Some(f.path().to_path_buf()),
            })])
            .build();

        let cred = chain.resolve().await.unwrap();
        assert_eq!(cred.token(), "dbtc_abc123");
        assert_eq!(cred.account_id(), 42);
    }

    #[tokio::test]
    async fn chain_returns_not_authenticated_when_all_fail() {
        let chain =
            AuthChainBuilder::with_resolvers(vec![AuthResolver::CloudYaml(CloudYamlResolver {
                dbt_project_path: None,
                path: Some(PathBuf::from("/nonexistent/dbt_cloud.yml")),
            })])
            .build();

        let err = chain.resolve().await.unwrap_err();
        assert!(matches!(err, AuthError::NotAuthenticated));
    }

    #[tokio::test]
    async fn chain_continues_past_errors_and_returns_first_error_if_no_credentials() {
        let bad = write_yaml("not: valid: yaml: [[[");

        // Chain: malformed YAML (error) → nonexistent file (not-authenticated)
        // Expected: InaccessibleSource/Malformed from the first resolver, since no
        // credentials were found and that was the first non-NotAuthenticated error.
        let chain = AuthChainBuilder::with_resolvers(vec![
            AuthResolver::CloudYaml(CloudYamlResolver {
                dbt_project_path: None,
                path: Some(bad.path().to_path_buf()),
            }),
            AuthResolver::CloudYaml(CloudYamlResolver {
                dbt_project_path: None,
                path: Some(PathBuf::from("/nonexistent/dbt_cloud.yml")),
            }),
        ])
        .build();

        let err = chain.resolve().await.unwrap_err();
        assert!(matches!(err, AuthError::Malformed(_)));
    }

    #[tokio::test]
    async fn chain_continues_past_error_and_succeeds_on_next_resolver() {
        let bad = write_yaml("not: valid: yaml: [[[");
        let good = write_yaml(valid_yaml());

        let chain = AuthChainBuilder::with_resolvers(vec![
            AuthResolver::CloudYaml(CloudYamlResolver {
                dbt_project_path: None,
                path: Some(bad.path().to_path_buf()),
            }),
            AuthResolver::CloudYaml(CloudYamlResolver {
                dbt_project_path: None,
                path: Some(good.path().to_path_buf()),
            }),
        ])
        .build();

        let cred = chain.resolve().await.unwrap();
        assert_eq!(cred.token(), "dbtc_abc123");
    }

    #[test]
    fn builder_allow_only_filters_correctly() {
        let chain = AuthChainBuilder::default()
            .allow_only(&[ResolverKind::EnvVar])
            .build();
        assert_eq!(chain.resolvers.len(), 1);
        assert!(matches!(chain.resolvers[0], AuthResolver::EnvVar(_)));
    }

    #[test]
    fn builder_deny_filters_correctly() {
        let chain = AuthChainBuilder::default()
            .deny(&[ResolverKind::OAuthPassive])
            .build();
        assert_eq!(chain.resolvers.len(), 2);
        assert!(
            !chain
                .resolvers
                .iter()
                .any(|r| matches!(r, AuthResolver::OAuthPassive(_)))
        );
    }

    #[test]
    fn interactive_chain_includes_interactive_resolver() {
        let chain = AuthChain::interactive();
        assert!(
            chain
                .resolvers
                .iter()
                .any(|r| matches!(r, AuthResolver::OAuthInteractive(_)))
        );
    }

    #[test]
    fn default_chain_excludes_interactive_resolver() {
        let chain = AuthChain::default();
        assert!(
            !chain
                .resolvers
                .iter()
                .any(|r| matches!(r, AuthResolver::OAuthInteractive(_)))
        );
    }
}
