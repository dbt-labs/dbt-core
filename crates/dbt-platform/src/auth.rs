use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use dbt_platform_auth::{AuthChainBuilder, Credential, ResolverKind};

use crate::error::{PlatformError, Result};

/// Identifies this SDK to the dbt platform in the OAuth `_dbtsrc` parameter and
/// the HTTP `User-Agent` header.
pub(crate) const SOURCE_APPLICATION: &str = "dbt-platform-sdk";

/// How long before an OAuth token's stated expiry we treat it as already
/// expired, so a refresh happens ahead of the request rather than racing it.
const EXPIRY_SKEW: Duration = Duration::from_secs(60);

/// Resolver knobs captured from [`crate::PlatformClientBuilder`], reused every
/// time a credential is (re-)resolved for refresh.
#[derive(Clone, Default)]
pub(crate) struct ResolverConfig {
    pub interactive: bool,
    pub allow: Option<Vec<ResolverKind>>,
    pub deny: Option<Vec<ResolverKind>>,
}

/// How a [`PlatformAuth`] obtains and renews credentials.
enum Mode {
    /// Re-resolvable via the credential chain. `primary` is the account the
    /// client first connected to; its credential may come from any source
    /// (env var, `dbt_cloud.yml`, or an OAuth session). Other accounts are only
    /// reachable through account-scoped OAuth sessions.
    Chain {
        primary: u64,
        config: ResolverConfig,
    },
    /// A caller-supplied credential with no resolver behind it — cannot refresh.
    Static,
}

/// Owns credential resolution and renewal for one or more accounts, shared by
/// every [`crate::PlatformClient`] derived from the same connection.
///
/// A credential is cached per account and reused until it is within
/// [`EXPIRY_SKEW`] of expiry (OAuth) — PATs and service tokens never expire, so
/// they are cached indefinitely. Re-resolving an expired OAuth credential drives
/// the auth module's refresh-token exchange transparently.
pub(crate) struct PlatformAuth {
    mode: Mode,
    cache: Mutex<HashMap<u64, Credential>>,
}

impl PlatformAuth {
    /// Build a refreshing authenticator seeded with the credential just resolved
    /// for the primary account.
    pub(crate) fn chain(primary: Credential, config: ResolverConfig) -> Self {
        let primary_account = primary.account_id();
        let mut cache = HashMap::new();
        cache.insert(primary_account, primary);
        Self {
            mode: Mode::Chain {
                primary: primary_account,
                config,
            },
            cache: Mutex::new(cache),
        }
    }

    /// Resolve the primary credential when first connecting, optionally scoping
    /// to `account`. Used to seed [`chain`](Self::chain); unlike a follow-up
    /// [`credential`](Self::credential) lookup there is no cache or primary
    /// account yet, so every resolver source is eligible.
    pub(crate) async fn resolve_initial(
        account: Option<u64>,
        config: &ResolverConfig,
    ) -> Result<Credential> {
        let mut chain = AuthChainBuilder::default().source_application(SOURCE_APPLICATION);
        if let Some(account) = account {
            chain = chain.account_id(account.to_string());
        }
        if config.interactive {
            chain = chain.interactive();
        }
        if let Some(allow) = &config.allow {
            chain = chain.allow_only(allow);
        }
        if let Some(deny) = &config.deny {
            chain = chain.deny(deny);
        }
        let credential = chain.build().resolve().await?;
        if let Some(account) = account {
            if credential.account_id() != account {
                return Err(PlatformError::AccountUnavailable {
                    requested: account,
                    resolved: Some(credential.account_id()),
                });
            }
        }
        Ok(credential)
    }

    /// Build a non-refreshing authenticator around a caller-supplied credential.
    pub(crate) fn static_credential(credential: Credential) -> Self {
        let mut cache = HashMap::new();
        cache.insert(credential.account_id(), credential);
        Self {
            mode: Mode::Static,
            cache: Mutex::new(cache),
        }
    }

    /// Return a currently-valid credential for `account`, refreshing or
    /// re-resolving if the cached one is missing or near expiry. Set
    /// `force_refresh` to bypass the cache (used to recover from a `401`).
    pub(crate) async fn credential(&self, account: u64, force_refresh: bool) -> Result<Credential> {
        if !force_refresh {
            if let Some(cred) = self.cached_valid(account) {
                return Ok(cred);
            }
        }
        let cred = self.resolve(account).await?;
        self.cache.lock().unwrap().insert(account, cred.clone());
        Ok(cred)
    }

    /// The cached credential for `account`, if present and not near expiry.
    fn cached_valid(&self, account: u64) -> Option<Credential> {
        let cache = self.cache.lock().unwrap();
        cache.get(&account).filter(|c| is_valid(c)).cloned()
    }

    /// Resolve a fresh credential for `account` through the credential chain.
    async fn resolve(&self, account: u64) -> Result<Credential> {
        let config = match &self.mode {
            Mode::Chain { config, primary } => {
                // The primary account may be served by any resolver. A different
                // account can only come from an account-scoped OAuth session, so
                // exclude the ambient single-account sources to avoid returning
                // the wrong account.
                let mut config = config.clone();
                if account != *primary {
                    config = config.deny_ambient_sources();
                }
                config
            }
            Mode::Static => {
                // No chain to re-run: the only credential we have is whatever was
                // supplied up front, already cached. If it is gone or expired we
                // cannot recover it.
                return Err(PlatformError::AccountUnavailable {
                    requested: account,
                    resolved: None,
                });
            }
        };

        let mut chain = AuthChainBuilder::default()
            .source_application(SOURCE_APPLICATION)
            .account_id(account.to_string());
        if config.interactive {
            chain = chain.interactive();
        }
        if let Some(allow) = &config.allow {
            chain = chain.allow_only(allow);
        }
        if let Some(deny) = &config.deny {
            chain = chain.deny(deny);
        }

        let credential = chain.build().resolve().await?;
        if credential.account_id() != account {
            return Err(PlatformError::AccountUnavailable {
                requested: account,
                resolved: Some(credential.account_id()),
            });
        }
        Ok(credential)
    }
}

impl ResolverConfig {
    /// Drop the single-account ambient sources (env var, `dbt_cloud.yml`) from an
    /// allow/deny set, leaving only the account-aware OAuth resolvers.
    fn deny_ambient_sources(mut self) -> Self {
        let ambient = [ResolverKind::EnvVar, ResolverKind::CloudYaml];
        match &mut self.allow {
            Some(allow) => allow.retain(|k| !ambient.contains(k)),
            None => {
                let deny = self.deny.get_or_insert_with(Vec::new);
                for kind in ambient {
                    if !deny.contains(&kind) {
                        deny.push(kind);
                    }
                }
            }
        }
        self
    }
}

/// Whether a cached credential is still safe to use. OAuth tokens are checked
/// against their expiry (minus [`EXPIRY_SKEW`]); other credential kinds do not
/// expire.
fn is_valid(credential: &Credential) -> bool {
    match credential {
        Credential::OAuth(session) => session.expires_at > SystemTime::now() + EXPIRY_SKEW,
        Credential::Pat { .. } | Credential::ServiceToken { .. } => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_platform_auth::OAuthSession;

    fn pat() -> Credential {
        Credential::Pat {
            token: "dbtu_secret".to_owned(),
            account_host: "ab123.us1.dbt.com".to_owned(),
            account_id: 1,
        }
    }

    fn oauth(expires_at: SystemTime) -> Credential {
        Credential::OAuth(OAuthSession {
            access_token: "tok".to_owned(),
            refresh_token: None,
            id_token: None,
            scopes: vec![],
            expires_at,
            account_host: "ab123.us1.dbt.com".to_owned(),
            account_id: 1,
            user_id: 7,
            client_id: "client".to_owned(),
        })
    }

    #[test]
    fn pats_never_expire() {
        assert!(is_valid(&pat()));
    }

    #[test]
    fn oauth_within_skew_of_expiry_is_invalid() {
        // Expires in 30s — inside the 60s skew, so treat as already expired.
        assert!(!is_valid(&oauth(
            SystemTime::now() + Duration::from_secs(30)
        )));
    }

    #[test]
    fn oauth_well_before_expiry_is_valid() {
        assert!(is_valid(&oauth(
            SystemTime::now() + Duration::from_secs(3600)
        )));
    }

    #[test]
    fn static_auth_caches_the_supplied_credential() {
        let auth = PlatformAuth::static_credential(pat());
        assert!(auth.cached_valid(1).is_some());
        assert!(auth.cached_valid(2).is_none());
    }

    #[test]
    fn deny_ambient_sources_adds_env_and_yaml_to_empty_denylist() {
        let config = ResolverConfig::default().deny_ambient_sources();
        let deny = config.deny.unwrap();
        assert!(deny.contains(&ResolverKind::EnvVar));
        assert!(deny.contains(&ResolverKind::CloudYaml));
    }

    #[test]
    fn deny_ambient_sources_prunes_them_from_an_allowlist() {
        let config = ResolverConfig {
            allow: Some(vec![
                ResolverKind::EnvVar,
                ResolverKind::OAuthPassive,
                ResolverKind::CloudYaml,
            ]),
            ..Default::default()
        }
        .deny_ambient_sources();
        assert_eq!(config.allow.unwrap(), vec![ResolverKind::OAuthPassive]);
    }

    #[dbt_runtime::test]
    async fn static_auth_cannot_resolve_an_uncached_account() {
        let auth = PlatformAuth::static_credential(pat());
        let err = auth.credential(999, false).await.unwrap_err();
        assert!(matches!(
            err,
            PlatformError::AccountUnavailable {
                requested: 999,
                resolved: None
            }
        ));
    }
}
