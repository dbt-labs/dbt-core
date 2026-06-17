use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;

use dbt_platform_auth::{AuthChain, AuthChainBuilder, AuthError, Credential, OAUTH_CLIENT_ID};

use crate::auth::browser_flow::{
    BrowserFlow, INTERACTIVE_TIMEOUT, InteractiveFlow, LOOPBACK_PORT, ORGS_SCOPE, TokenResponse,
};
use crate::auth::scope::{Scope, determine_org_id, jwt_claims};
use crate::auth::token_store::{StoredToken, TokenStore};
use crate::service_client::RunCacheServiceError;
use crate::service_config::RunCacheServiceConfig;

const TOKEN_REFRESH_WINDOW: Duration = Duration::from_secs(300);

#[derive(Clone, Debug)]
pub struct CachedToken {
    pub id_token: String,
    pub scope: String,
    pub expires_at: Option<SystemTime>,
    pub refresh_token: Option<String>,
}

impl CachedToken {
    pub fn is_fresh(&self) -> bool {
        match self.expires_at {
            Some(expires_at) => expires_at > SystemTime::now() + TOKEN_REFRESH_WINDOW,
            None => true,
        }
    }
}

pub struct OAuthTokenSource {
    http: reqwest::Client,
    token_url: String,
    client_id: String,
    client_secret: Option<String>,
    configured_org_id: Option<String>,
    store: TokenStore,
    cached: Arc<Mutex<Option<CachedToken>>>,
    disk_loaded: Arc<AtomicBool>,
    interactive_flow: Arc<dyn InteractiveFlow>,
    auth_chain: AuthChain,
}

impl std::fmt::Debug for OAuthTokenSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthTokenSource")
            .field("token_url", &self.token_url)
            .field("client_id", &self.client_id)
            .field("configured_org_id", &self.configured_org_id)
            .finish()
    }
}

impl OAuthTokenSource {
    pub fn new(config: &RunCacheServiceConfig) -> Result<Self, RunCacheServiceError> {
        let http = reqwest::Client::builder()
            .connect_timeout(config.timeout)
            .timeout(config.timeout)
            .build()?;

        let store = TokenStore::discover().ok_or_else(|| {
            RunCacheServiceError::Auth(
                "could not resolve home directory for dbt State auth; \
                 set DBT_ENGINE_STATE_HOME to override"
                    .to_string(),
            )
        })?;

        let interactive_flow: Arc<dyn InteractiveFlow> = Arc::new(BrowserFlow {
            http: http.clone(),
            auth_url: config.oauth_auth_url.clone(),
            token_url: config.oauth_token_url.clone(),
            client_id: config.oauth_client_id.clone(),
            scope: ORGS_SCOPE.to_string(),
            timeout: INTERACTIVE_TIMEOUT,
            redirect_port: LOOPBACK_PORT,
            opener: BrowserFlow::default_opener(),
            abort_signal: std::sync::Mutex::new(None),
        });

        Ok(Self {
            http,
            token_url: config.oauth_token_url.clone(),
            client_id: config.oauth_client_id.clone(),
            client_secret: config.oauth_client_secret.clone(),
            configured_org_id: config.org_id.clone(),
            store,
            cached: Arc::new(Mutex::new(None)),
            disk_loaded: Arc::new(AtomicBool::new(false)),
            interactive_flow,
            auth_chain: AuthChainBuilder::new(OAUTH_CLIENT_ID).build(),
        })
    }

    /// Construct with explicit components. Used by tests and the integration suite.
    pub fn with_components(
        config: &RunCacheServiceConfig,
        store: TokenStore,
        interactive_flow: Arc<dyn InteractiveFlow>,
        auth_chain: AuthChain,
    ) -> Result<Self, RunCacheServiceError> {
        let http = reqwest::Client::builder()
            .connect_timeout(config.timeout)
            .timeout(config.timeout)
            .build()?;
        Ok(Self {
            http,
            token_url: config.oauth_token_url.clone(),
            client_id: config.oauth_client_id.clone(),
            client_secret: config.oauth_client_secret.clone(),
            configured_org_id: config.org_id.clone(),
            store,
            cached: Arc::new(Mutex::new(None)),
            disk_loaded: Arc::new(AtomicBool::new(false)),
            interactive_flow,
            auth_chain,
        })
    }

    pub async fn token(&self) -> Result<CachedToken, RunCacheServiceError> {
        let mut guard = self.cached.lock().await;

        if let Some(token) = guard.as_ref() {
            if token.is_fresh() {
                return Ok(token.clone());
            }
        }

        if let Some(token) = self.get_or_refresh_from_disk().await? {
            *guard = Some(token.clone());
            return Ok(token);
        }

        let token = self.acquire_fresh_token().await?;
        let stored: StoredToken = (&token).into_stored(&self.token_type_or_default());
        if let Err(err) = self.store.save(&stored).await {
            tracing::warn!("failed to persist dbt State auth token: {err}");
        }
        *guard = Some(token.clone());
        Ok(token)
    }

    async fn get_or_refresh_from_disk(&self) -> Result<Option<CachedToken>, RunCacheServiceError> {
        if self.disk_loaded.swap(true, Ordering::AcqRel) {
            return Ok(None);
        }
        let Some(stored) = self.store.load().await? else {
            return Ok(None);
        };
        let cached = stored.clone().into_cached();
        let is_fresh = cached.is_fresh();

        // If a configured org_id isn't in the cached scope, force a refresh in case the
        // user's permissions changed since the cache was last saved.
        let needs_scope_refresh = !self.scope_satisfies_config(&cached.scope);

        if is_fresh && !needs_scope_refresh {
            return Ok(Some(cached));
        }

        let Some(refresh_token) = stored.refresh_token.clone() else {
            // Without a refresh token we can't pick up scope changes. If the cached token
            // is still valid, return it and let resolve_org_id surface any scope mismatch;
            // otherwise fall through to re-login.
            return Ok(if is_fresh { Some(cached) } else { None });
        };

        match self.fetch_refresh(&refresh_token).await {
            Ok(token) => Ok(Some(token)),
            Err(err) => {
                if is_fresh {
                    // The token itself is still valid; return it and let resolve_org_id
                    // surface any scope mismatch as a proper error instead of triggering
                    // browser re-auth.
                    Ok(Some(cached))
                } else {
                    // The token is expired and refresh failed; clear the cache and fall
                    // through to re-login.
                    tracing::warn!(
                        "dbt State token refresh failed, falling back to re-login: {err}"
                    );
                    let _ = self.store.delete().await;
                    Ok(None)
                }
            }
        }
    }

    async fn acquire_fresh_token(&self) -> Result<CachedToken, RunCacheServiceError> {
        let response = if self.client_secret.is_some() {
            self.fetch_client_credentials().await?
        } else {
            match self.auth_chain.resolve().await {
                Ok(credential) => self.fetch_platform_token_exchange(&credential).await?,
                // No platform credentials available — fall back to dbt State
                // standalone authentication via the interactive browser flow.
                Err(AuthError::NotAuthenticated) => self.interactive_flow.run().await?,
                Err(err) => {
                    return Err(RunCacheServiceError::Auth(format!(
                        "failed to resolve dbt Platform credential for dbt State token exchange: {err}"
                    )));
                }
            }
        };
        self.process_response(response)
    }

    /// Whether a token whose scope is `scope_str` can serve the configured org_id.
    /// When no org_id is configured this is always true — disambiguation is then
    /// deferred to `resolve_org_id`.
    fn scope_satisfies_config(&self, scope_str: &str) -> bool {
        match self.configured_org_id.as_deref() {
            None => true,
            Some(configured) => Scope::from_string(scope_str)
                .map(|scope| scope.is_org_id_in_scope(configured))
                .unwrap_or(false),
        }
    }

    /// Resolve the organization ID for a token from its scope and the configured
    /// org_id. The org_id is always derived from the live token scope rather than a
    /// value cached at write time, so the result is consistent with the current
    /// configuration. Ambiguous or disabled scopes surface here as errors instead of
    /// being treated as token acquisition failures.
    pub fn resolve_org_id(&self, token: &CachedToken) -> Result<String, RunCacheServiceError> {
        let scope = Scope::from_string(&token.scope)?;
        determine_org_id(&scope, self.configured_org_id.as_deref())
    }

    fn token_type_or_default(&self) -> String {
        "Bearer".to_string()
    }

    async fn fetch_client_credentials(&self) -> Result<TokenResponse, RunCacheServiceError> {
        let client_secret = self
            .client_secret
            .as_deref()
            .ok_or_else(|| RunCacheServiceError::Auth("missing client secret".to_string()))?;
        let response = self
            .http
            .post(&self.token_url)
            .basic_auth(&self.client_id, Some(client_secret))
            .form(&[("grant_type", "client_credentials"), ("scope", ORGS_SCOPE)])
            .send()
            .await
            .map_err(RunCacheServiceError::from)?
            .error_for_status()
            .map_err(RunCacheServiceError::from)?;
        let body = response.text().await.map_err(RunCacheServiceError::from)?;
        serde_json::from_str(&body).map_err(|err| {
            RunCacheServiceError::Auth(format!("invalid OAuth token response: {err}"))
        })
    }

    async fn fetch_platform_token_exchange(
        &self,
        credential: &Credential,
    ) -> Result<TokenResponse, RunCacheServiceError> {
        let response = self
            .http
            .post(&self.token_url)
            .form(&[
                (
                    "grant_type",
                    "urn:ietf:params:oauth:grant-type:token-exchange",
                ),
                ("subject_token_type", "dbt"),
                ("subject_token", credential.token()),
                ("dbt_hostname", credential.account_host()),
                ("client_id", self.client_id.as_str()),
            ])
            .send()
            .await
            .map_err(RunCacheServiceError::from)?
            .error_for_status()
            .map_err(RunCacheServiceError::from)?;
        let body = response.text().await.map_err(RunCacheServiceError::from)?;
        serde_json::from_str(&body).map_err(|err| {
            RunCacheServiceError::Auth(format!(
                "Unable to exchange dbt platform token for a dbt State authentication token: {err}"
            ))
        })
    }

    async fn fetch_refresh(
        &self,
        refresh_token: &str,
    ) -> Result<CachedToken, RunCacheServiceError> {
        let mut form: Vec<(&str, &str)> = vec![
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", self.client_id.as_str()),
        ];
        if let Some(secret) = self.client_secret.as_deref() {
            form.push(("client_secret", secret));
        }
        let response = self
            .http
            .post(&self.token_url)
            .form(&form)
            .send()
            .await
            .map_err(RunCacheServiceError::from)?
            .error_for_status()
            .map_err(RunCacheServiceError::from)?;
        let body = response.text().await.map_err(RunCacheServiceError::from)?;
        let token_resp: TokenResponse = serde_json::from_str(&body).map_err(|err| {
            RunCacheServiceError::Auth(format!("invalid OAuth token response: {err}"))
        })?;
        let cached = self.process_response(token_resp)?;
        let stored = (&cached).into_stored(&self.token_type_or_default());
        if let Err(err) = self.store.save(&stored).await {
            tracing::warn!("failed to persist refreshed dbt State auth token: {err}");
        }
        Ok(cached)
    }

    fn process_response(
        &self,
        response: TokenResponse,
    ) -> Result<CachedToken, RunCacheServiceError> {
        let claims = jwt_claims(&response.id_token)?;
        let scope_str = claims.scope.ok_or_else(|| {
            RunCacheServiceError::Auth("OAuth token is missing scope".to_string())
        })?;
        // Validate the scope is well-formed. Org resolution is deferred to
        // `resolve_org_id` so that an ambiguous or disabled scope surfaces a proper
        // error at request time instead of being treated as a token acquisition
        // failure (which would delete the cache and re-open the browser).
        Scope::from_string(&scope_str)?;
        let expires_at = expires_at_from(&response);

        Ok(CachedToken {
            id_token: response.id_token,
            scope: scope_str,
            expires_at,
            refresh_token: response.refresh_token,
        })
    }
}

fn expires_at_from(response: &TokenResponse) -> Option<SystemTime> {
    if let Some(seconds) = response.expires_at {
        return Some(epoch_seconds_to_system_time(seconds));
    }
    response
        .expires_in
        .map(|secs| SystemTime::now() + duration_from_seconds(secs))
}

fn epoch_seconds_to_system_time(seconds: f64) -> SystemTime {
    UNIX_EPOCH + duration_from_seconds(seconds)
}

fn duration_from_seconds(seconds: f64) -> Duration {
    if seconds.is_finite() && seconds > 0.0 {
        Duration::from_secs_f64(seconds)
    } else {
        Duration::ZERO
    }
}

trait IntoStored {
    fn into_stored(self, token_type: &str) -> StoredToken;
}

impl IntoStored for &CachedToken {
    fn into_stored(self, token_type: &str) -> StoredToken {
        StoredToken {
            scope: self.scope.clone(),
            token_type: token_type.to_string(),
            id_token: self.id_token.clone(),
            expires_at: self
                .expires_at
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs_f64()),
            access_token: None,
            refresh_token: self.refresh_token.clone(),
        }
    }
}

trait FromStored {
    fn into_cached(self) -> CachedToken;
}

impl FromStored for StoredToken {
    fn into_cached(self) -> CachedToken {
        CachedToken {
            id_token: self.id_token,
            scope: self.scope,
            expires_at: self.expires_at.map(epoch_seconds_to_system_time),
            refresh_token: self.refresh_token,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service_config::DEFAULT_OAUTH_AUTH_URL;
    use async_trait::async_trait;
    use dbt_platform_auth::resolver::{AuthResolver, EnvVarResolver};
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde::Serialize;
    use std::sync::Mutex as StdMutex;
    use tempfile::TempDir;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Builds an `AuthChain` with no resolvers — `resolve()` deterministically
    /// returns `NotAuthenticated`. Used by tests that exercise the
    /// interactive-flow or disk-cache paths and must not be influenced by the
    /// test process's env vars or `~/.dbt/*` files.
    fn empty_auth_chain() -> AuthChain {
        AuthChain::new(vec![])
    }

    /// Builds an `AuthChain` containing only `EnvVarResolver`. Pair with a
    /// `DbtCloudEnvGuard` so the env vars consumed by the resolver are scoped
    /// to the test.
    fn env_var_auth_chain() -> AuthChain {
        AuthChain::new(vec![AuthResolver::EnvVar(EnvVarResolver)])
    }

    /// Serializes any test that mutates `DBT_CLOUD_*` env vars.
    static TEST_ENV_LOCK: StdMutex<()> = StdMutex::new(());

    /// RAII helper that sets `DBT_CLOUD_*` env vars (consumed by
    /// `EnvVarResolver`) for the duration of a test, holding `TEST_ENV_LOCK`
    /// to prevent races, and restoring any prior values on drop so the host
    /// process's environment is left untouched.
    struct DbtCloudEnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        prior: [(&'static str, Option<String>); 3],
    }

    impl DbtCloudEnvGuard {
        fn new(token: &str, host: &str, account_id: &str) -> Self {
            let lock = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let prior = [
                ("DBT_CLOUD_TOKEN", std::env::var("DBT_CLOUD_TOKEN").ok()),
                (
                    "DBT_CLOUD_ACCOUNT_HOST",
                    std::env::var("DBT_CLOUD_ACCOUNT_HOST").ok(),
                ),
                (
                    "DBT_CLOUD_ACCOUNT_ID",
                    std::env::var("DBT_CLOUD_ACCOUNT_ID").ok(),
                ),
            ];
            unsafe {
                #[allow(clippy::disallowed_methods)]
                std::env::set_var("DBT_CLOUD_TOKEN", token);
                #[allow(clippy::disallowed_methods)]
                std::env::set_var("DBT_CLOUD_ACCOUNT_HOST", host);
                #[allow(clippy::disallowed_methods)]
                std::env::set_var("DBT_CLOUD_ACCOUNT_ID", account_id);
            }
            Self { _lock: lock, prior }
        }
    }

    impl Drop for DbtCloudEnvGuard {
        fn drop(&mut self) {
            for (name, value) in &self.prior {
                unsafe {
                    match value {
                        #[allow(clippy::disallowed_methods)]
                        Some(v) => std::env::set_var(name, v),
                        #[allow(clippy::disallowed_methods)]
                        None => std::env::remove_var(name),
                    }
                }
            }
        }
    }

    fn make_jwt(scope: &str) -> String {
        #[derive(Serialize)]
        struct Claims<'a> {
            scope: &'a str,
        }
        encode(
            &Header::default(),
            &Claims { scope },
            &EncodingKey::from_secret(b"test-secret"),
        )
        .unwrap()
    }

    fn config_with(
        server_url: &str,
        secret: Option<&str>,
        org_id: Option<&str>,
    ) -> RunCacheServiceConfig {
        let mut config = RunCacheServiceConfig::disabled();
        config.enabled = true;
        config.oauth_token_url = format!("{server_url}/token");
        config.oauth_auth_url = DEFAULT_OAUTH_AUTH_URL.to_string();
        config.oauth_client_id = "test-client".to_string();
        config.oauth_client_secret = secret.map(str::to_string);
        config.org_id = org_id.map(str::to_string);
        config.timeout = Duration::from_secs(5);
        config
    }

    fn token_store_in(dir: &TempDir) -> TokenStore {
        let auth_home = dir.path().join(".dbt");
        TokenStore::discover_from(Some(auth_home.to_string_lossy().into_owned()), None).unwrap()
    }

    struct FakeFlow {
        responses: StdMutex<Vec<TokenResponse>>,
    }

    impl FakeFlow {
        fn new(responses: Vec<TokenResponse>) -> Arc<Self> {
            Arc::new(Self {
                responses: StdMutex::new(responses),
            })
        }
    }

    #[async_trait]
    impl InteractiveFlow for FakeFlow {
        async fn run(&self) -> Result<TokenResponse, RunCacheServiceError> {
            let mut q = self.responses.lock().unwrap();
            if q.is_empty() {
                return Err(RunCacheServiceError::Auth("FakeFlow drained".to_string()));
            }
            Ok(q.remove(0))
        }
    }

    fn token_response(scope: &str, expires_in: f64, refresh: Option<&str>) -> TokenResponse {
        TokenResponse {
            id_token: make_jwt(scope),
            access_token: Some("access".to_string()),
            refresh_token: refresh.map(str::to_string),
            scope: Some(scope.to_string()),
            token_type: Some("Bearer".to_string()),
            expires_at: None,
            expires_in: Some(expires_in),
        }
    }

    #[tokio::test]
    async fn m2m_happy_path_persists_token_and_sends_basic_auth() {
        let server = MockServer::start().await;
        let scope = "runcache:scope:org:dev:admin";
        let token_resp = serde_json::json!({
            "id_token": make_jwt(scope),
            "scope": scope,
            "token_type": "Bearer",
            "expires_in": 3600,
        });
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=client_credentials"))
            .respond_with(ResponseTemplate::new(200).set_body_json(token_resp))
            .mount(&server)
            .await;

        let dir = TempDir::new().unwrap();
        let config = config_with(&server.uri(), Some("secret-x"), Some("dev"));
        let source = OAuthTokenSource::with_components(
            &config,
            token_store_in(&dir),
            FakeFlow::new(vec![]),
            empty_auth_chain(),
        )
        .unwrap();

        let token = source.token().await.unwrap();
        assert_eq!(source.resolve_org_id(&token).unwrap(), "dev");

        let stored = token_store_in(&dir).load().await.unwrap().unwrap();
        assert_eq!(stored.scope, scope);

        // Inspect server requests to confirm Basic auth header.
        let requests = server.received_requests().await.unwrap();
        let auth_header = requests[0]
            .headers
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth_header.starts_with("Basic "));
    }

    #[tokio::test]
    async fn browser_happy_path_includes_client_id_in_form() {
        let server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let config = config_with(&server.uri(), None, Some("dev"));

        let scope = "runcache:scope:org:dev:admin";
        let fake = FakeFlow::new(vec![token_response(scope, 3600.0, Some("refresh-xyz"))]);
        let source = OAuthTokenSource::with_components(
            &config,
            token_store_in(&dir),
            fake,
            empty_auth_chain(),
        )
        .unwrap();

        let token = source.token().await.unwrap();
        assert_eq!(source.resolve_org_id(&token).unwrap(), "dev");
        assert_eq!(token.refresh_token.as_deref(), Some("refresh-xyz"));

        let stored = token_store_in(&dir).load().await.unwrap().unwrap();
        assert_eq!(stored.refresh_token.as_deref(), Some("refresh-xyz"));
    }

    #[tokio::test]
    async fn refresh_path_replaces_stored_token() {
        let server = MockServer::start().await;
        let scope = "runcache:scope:org:dev:admin";
        let new_token = make_jwt(scope);
        let token_resp = serde_json::json!({
            "id_token": new_token,
            "scope": scope,
            "token_type": "Bearer",
            "expires_in": 3600,
            "refresh_token": "refresh-new",
        });
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(token_resp))
            .mount(&server)
            .await;

        let dir = TempDir::new().unwrap();
        let store = token_store_in(&dir);
        // Seed disk with a stale token.
        let stale = StoredToken {
            scope: scope.to_string(),
            token_type: "Bearer".to_string(),
            id_token: make_jwt(scope),
            expires_at: Some(1.0), // ancient
            access_token: None,
            refresh_token: Some("refresh-old".to_string()),
        };
        store.save(&stale).await.unwrap();

        let config = config_with(&server.uri(), None, Some("dev"));
        let source = OAuthTokenSource::with_components(
            &config,
            store,
            FakeFlow::new(vec![]),
            empty_auth_chain(),
        )
        .unwrap();

        let token = source.token().await.unwrap();
        assert_eq!(token.id_token, new_token);
        assert_eq!(token.refresh_token.as_deref(), Some("refresh-new"));
    }

    #[tokio::test]
    async fn org_disabled_returns_dedicated_error() {
        let server = MockServer::start().await;
        let scope = "runcache:scope:app:dev:developer"; // only app, no org
        let token_resp = serde_json::json!({
            "id_token": make_jwt(scope),
            "scope": scope,
            "token_type": "Bearer",
            "expires_in": 3600,
        });
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(token_resp))
            .mount(&server)
            .await;

        let dir = TempDir::new().unwrap();
        let config = config_with(&server.uri(), Some("secret"), Some("dev"));
        let source = OAuthTokenSource::with_components(
            &config,
            token_store_in(&dir),
            FakeFlow::new(vec![]),
            empty_auth_chain(),
        )
        .unwrap();

        // The token is acquired and persisted successfully; the disabled-org error
        // surfaces from resolve_org_id, not from token acquisition.
        let token = source.token().await.unwrap();
        let err = source.resolve_org_id(&token).unwrap_err();
        assert!(matches!(err, RunCacheServiceError::OrgDisabled { ref org_id } if org_id == "dev"));
    }

    #[tokio::test]
    async fn disk_token_with_mismatched_org_is_refreshed() {
        // A configured org_id that isn't in the cached scope forces a refresh so a
        // permissions change is picked up without deleting the cache or re-opening the
        // browser.
        let server = MockServer::start().await;
        let scope_new = "runcache:scope:org:primary:admin";
        let token_resp = serde_json::json!({
            "id_token": make_jwt(scope_new),
            "scope": scope_new,
            "token_type": "Bearer",
            "expires_in": 3600,
            "refresh_token": "refresh-new",
        });
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(token_resp))
            .mount(&server)
            .await;

        let dir = TempDir::new().unwrap();
        let store = token_store_in(&dir);
        // Disk grants only "other"; the configured org "primary" isn't in scope yet.
        store
            .save(&StoredToken {
                scope: "runcache:scope:org:other:admin".to_string(),
                token_type: "Bearer".to_string(),
                id_token: make_jwt("runcache:scope:org:other:admin"),
                expires_at: Some(9_999_999_999.0),
                access_token: None,
                refresh_token: Some("refresh-old".to_string()),
            })
            .await
            .unwrap();

        let config = config_with(&server.uri(), Some("secret"), Some("primary"));
        let source = OAuthTokenSource::with_components(
            &config,
            token_store_in(&dir),
            FakeFlow::new(vec![]),
            empty_auth_chain(),
        )
        .unwrap();

        let token = source.token().await.unwrap();
        assert_eq!(source.resolve_org_id(&token).unwrap(), "primary");
    }

    #[tokio::test]
    async fn fresh_disk_token_is_used_without_network_call() {
        let server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let store = token_store_in(&dir);
        let scope = "runcache:scope:org:dev:admin";
        store
            .save(&StoredToken {
                scope: scope.to_string(),
                token_type: "Bearer".to_string(),
                id_token: make_jwt(scope),
                expires_at: Some(9_999_999_999.0),
                access_token: None,
                refresh_token: None,
            })
            .await
            .unwrap();

        let config = config_with(&server.uri(), None, Some("dev"));
        let source = OAuthTokenSource::with_components(
            &config,
            token_store_in(&dir),
            FakeFlow::new(vec![]),
            empty_auth_chain(),
        )
        .unwrap();

        let token = source.token().await.unwrap();
        assert_eq!(source.resolve_org_id(&token).unwrap(), "dev");
        // No mock was set up; the FakeFlow would have errored. Implicit assertion.
    }

    #[tokio::test]
    async fn ambiguous_fresh_cached_token_raises_when_no_org_configured() {
        // A previous run used a configured org_id, so a valid multi-org token was cached.
        // A subsequent run without a configured org_id must fail rather than silently
        // reusing the org_id picked at write time.
        let dir = TempDir::new().unwrap();
        let store = token_store_in(&dir);
        let scope = "runcache:scope:org:org1:admin runcache:scope:org:org2:admin";
        store
            .save(&StoredToken {
                scope: scope.to_string(),
                token_type: "Bearer".to_string(),
                id_token: make_jwt(scope),
                expires_at: Some(9_999_999_999.0),
                access_token: None,
                refresh_token: Some("refresh".to_string()),
            })
            .await
            .unwrap();

        let config = config_with("http://127.0.0.1:1", None, None); // no org configured
        let source = OAuthTokenSource::with_components(
            &config,
            token_store_in(&dir),
            FakeFlow::new(vec![]),
            empty_auth_chain(),
        )
        .unwrap();

        let token = source.token().await.unwrap();
        let err = source.resolve_org_id(&token).unwrap_err();
        assert!(err.to_string().contains("state-org-id"));
    }

    #[tokio::test]
    async fn ambiguity_during_refresh_raises_without_browser_flow() {
        // A near-expiry token is refreshed and the refresh returns a multi-org scope.
        // With no configured org_id the ambiguity must surface from resolve_org_id, not
        // be treated as a token acquisition failure that re-opens the browser.
        let server = MockServer::start().await;
        let multi_org = "runcache:scope:org:org1:admin runcache:scope:org:org2:admin";
        let token_resp = serde_json::json!({
            "id_token": make_jwt(multi_org),
            "scope": multi_org,
            "token_type": "Bearer",
            "expires_in": 3600,
            "refresh_token": "new-refresh",
        });
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(token_resp))
            .mount(&server)
            .await;

        let dir = TempDir::new().unwrap();
        let store = token_store_in(&dir);
        let near_expiry = (SystemTime::now() + Duration::from_secs(200))
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        store
            .save(&StoredToken {
                scope: "runcache:scope:org:org1:admin".to_string(),
                token_type: "Bearer".to_string(),
                id_token: make_jwt("runcache:scope:org:org1:admin"),
                expires_at: Some(near_expiry),
                access_token: None,
                refresh_token: Some("old-refresh".to_string()),
            })
            .await
            .unwrap();

        let config = config_with(&server.uri(), None, None);
        // FakeFlow is empty: if the browser flow were reached it would error.
        let source = OAuthTokenSource::with_components(
            &config,
            token_store_in(&dir),
            FakeFlow::new(vec![]),
            empty_auth_chain(),
        )
        .unwrap();

        let token = source.token().await.unwrap();
        let err = source.resolve_org_id(&token).unwrap_err();
        assert!(err.to_string().contains("state-org-id"));

        // The refreshed token was persisted (refresh_token rotated) — cache must remain.
        let saved = token_store_in(&dir).load().await.unwrap().unwrap();
        assert_eq!(saved.refresh_token.as_deref(), Some("new-refresh"));
    }

    #[tokio::test]
    async fn disabled_org_disk_token_surfaces_error_without_reauth() {
        // When the cached token grants no active access to the configured org (the user
        // was disabled), the cache must be kept and the disabled error surfaced from
        // resolve_org_id rather than re-triggering the browser flow.
        let dir = TempDir::new().unwrap();
        let store = token_store_in(&dir);
        let scope = "runcache:scope:app:org1:developer"; // app-only: org access disabled
        store
            .save(&StoredToken {
                scope: scope.to_string(),
                token_type: "Bearer".to_string(),
                id_token: make_jwt(scope),
                expires_at: Some(9_999_999_999.0),
                access_token: None,
                refresh_token: None,
            })
            .await
            .unwrap();

        let config = config_with("http://127.0.0.1:1", None, Some("org1"));
        let source = OAuthTokenSource::with_components(
            &config,
            token_store_in(&dir),
            FakeFlow::new(vec![]),
            empty_auth_chain(),
        )
        .unwrap();

        let token = source.token().await.unwrap();
        let err = source.resolve_org_id(&token).unwrap_err();
        assert!(
            matches!(err, RunCacheServiceError::OrgDisabled { ref org_id } if org_id == "org1")
        );

        // The cache must still be present — no deletion, no browser re-auth.
        assert!(token_store_in(&dir).load().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn platform_token_exchange_succeeds_when_no_client_secret() {
        let server = MockServer::start().await;
        let scope = "runcache:scope:org:dev:admin";
        let token_resp = serde_json::json!({
            "id_token": make_jwt(scope),
            "scope": scope,
            "token_type": "Bearer",
            "expires_in": 3600,
        });
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains(
                "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange",
            ))
            .and(body_string_contains("subject_token_type=dbt"))
            .and(body_string_contains("subject_token=dbtc_platform_token"))
            .and(body_string_contains("dbt_hostname=ab123.us1.dbt.com"))
            .respond_with(ResponseTemplate::new(200).set_body_json(token_resp))
            .mount(&server)
            .await;

        let _env = DbtCloudEnvGuard::new("dbtc_platform_token", "ab123.us1.dbt.com", "42");

        let dir = TempDir::new().unwrap();
        let config = config_with(&server.uri(), None, Some("dev"));
        let source = OAuthTokenSource::with_components(
            &config,
            token_store_in(&dir),
            FakeFlow::new(vec![]),
            env_var_auth_chain(),
        )
        .unwrap();

        let token = source.token().await.unwrap();
        assert_eq!(source.resolve_org_id(&token).unwrap(), "dev");
    }

    #[tokio::test]
    async fn platform_token_exchange_skipped_when_client_secret_set() {
        let server = MockServer::start().await;
        let scope = "runcache:scope:org:dev:admin";
        let token_resp = serde_json::json!({
            "id_token": make_jwt(scope),
            "scope": scope,
            "token_type": "Bearer",
            "expires_in": 3600,
        });
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(token_resp))
            .mount(&server)
            .await;

        let dir = TempDir::new().unwrap();
        let config = config_with(&server.uri(), Some("client-secret"), Some("dev"));
        let source = OAuthTokenSource::with_components(
            &config,
            token_store_in(&dir),
            FakeFlow::new(vec![]),
            empty_auth_chain(),
        )
        .unwrap();

        let token = source.token().await.unwrap();
        assert_eq!(source.resolve_org_id(&token).unwrap(), "dev");

        // Verify only client_credentials was used — no token-exchange.
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let body = std::str::from_utf8(&requests[0].body).unwrap();
        assert!(body.contains("grant_type=client_credentials"));
        assert!(!body.contains("token-exchange"));
    }

    #[tokio::test]
    async fn platform_token_exchange_propagates_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("token-exchange"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let _env = DbtCloudEnvGuard::new("dbtc_platform_token", "ab123.us1.dbt.com", "42");

        let dir = TempDir::new().unwrap();
        let config = config_with(&server.uri(), None, Some("dev"));
        // FakeFlow would error if reached — exchange failure must propagate, not fall back.
        let source = OAuthTokenSource::with_components(
            &config,
            token_store_in(&dir),
            FakeFlow::new(vec![]),
            env_var_auth_chain(),
        )
        .unwrap();

        let err = source.token().await.unwrap_err();
        assert!(matches!(err, RunCacheServiceError::AuthRequest(_)));
    }
}
