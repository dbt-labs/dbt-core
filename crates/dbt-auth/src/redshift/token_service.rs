use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use once_cell::sync::Lazy;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

#[derive(Clone, Debug)]
struct CachedToken {
    value: String,
    expires_at: Instant,
}

static TOKEN_CACHE: Lazy<Arc<RwLock<Option<CachedToken>>>> =
    Lazy::new(|| Arc::new(RwLock::new(None)));

static TOKEN_FETCH_LOCK: Lazy<Arc<Mutex<()>>> = Lazy::new(|| Arc::new(Mutex::new(())));

#[derive(Debug, Error)]
pub enum TokenServiceError {
    #[error("Missing required key in token_endpoint: {0}")]
    MissingKey(String),

    #[error("Rate limit reached. Consider lowering concurrency or increasing IdP limits.")]
    RateLimited,

    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Invalid header name: {0}")]
    InvalidHeaderName(String),

    #[error("Invalid header value for '{0}': {1}")]
    InvalidHeaderValue(String, String),

    #[error("Unsupported identity provider type: {0}. Select 'okta' or 'entra'.")]
    UnsupportedProvider(String),

    #[error("Token not found in response body")]
    MissingToken,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenEndpoint {
    pub r#type: String,
    pub request_url: String,
    pub request_data: String,
    #[serde(flatten)]
    pub other_params: HashMap<String, String>,
}

impl TokenEndpoint {
    pub fn validate(&self) -> Result<(), TokenServiceError> {
        for key in ["type", "request_url", "request_data"] {
            if (key == "type" && self.r#type.is_empty())
                || (key == "request_url" && self.request_url.is_empty())
                || (key == "request_data" && self.request_data.is_empty())
            {
                return Err(TokenServiceError::MissingKey(key.to_string()));
            }
        }
        Ok(())
    }
}

#[async_trait]
pub trait TokenService: Send + Sync {
    async fn handle_request(&self) -> Result<String, TokenServiceError>;
    fn build_headers(&self) -> Result<HashMap<String, String>, TokenServiceError>;
}

pub struct BaseTokenService {
    client: Client,
    endpoint: TokenEndpoint,
}

impl BaseTokenService {
    pub fn new(endpoint: TokenEndpoint) -> Result<Self, TokenServiceError> {
        endpoint.validate()?;
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(TokenServiceError::Http)?;
        Ok(Self { client, endpoint })
    }

    pub fn header_map(headers: &HashMap<String, String>) -> Result<HeaderMap, TokenServiceError> {
        let mut map = HeaderMap::new();
        for (k, v) in headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|_| TokenServiceError::InvalidHeaderName(k.clone()))?;
            let value = HeaderValue::from_str(v)
                .map_err(|_| TokenServiceError::InvalidHeaderValue(k.clone(), v.clone()))?;
            map.insert(name, value);
        }
        Ok(map)
    }

    pub async fn post(
        &self,
        headers: HashMap<String, String>,
    ) -> Result<String, TokenServiceError> {
        // --- Check cache first ---
        {
            let cache = TOKEN_CACHE.read().await;
            if let Some(ref token) = *cache {
                if Instant::now() < token.expires_at {
                    return Ok(token.value.clone());
                }
            }
        }

        let _lock = TOKEN_FETCH_LOCK.lock().await;

        {
            let cache = TOKEN_CACHE.read().await;
            if let Some(ref token) = *cache {
                if Instant::now() < token.expires_at {
                    return Ok(token.value.clone());
                }
            }
        }

        // --- Perform the request ---
        let header_map = Self::header_map(&headers)?;
        let resp = self
            .client
            .post(&self.endpoint.request_url)
            .headers(header_map)
            .body(self.endpoint.request_data.clone())
            .send()
            .await?;

        if resp.status() == StatusCode::TOO_MANY_REQUESTS {
            return Err(TokenServiceError::RateLimited);
        }

        resp.error_for_status_ref()?;

        let bytes = resp.bytes().await?;
        let json: Value =
            serde_json::from_slice(&bytes).map_err(|_| TokenServiceError::MissingToken)?;

        let token = json
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or(TokenServiceError::MissingToken)?
            .to_string();

        let expires_in = json
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(3600);

        let mut cache = TOKEN_CACHE.write().await;
        *cache = Some(CachedToken {
            value: token.clone(),
            expires_at: Instant::now() + Duration::from_secs(expires_in),
        });

        Ok(token)
    }
}

pub struct OktaIdpTokenService {
    base: BaseTokenService,
}

#[async_trait]
impl TokenService for OktaIdpTokenService {
    async fn handle_request(&self) -> Result<String, TokenServiceError> {
        let headers = self.build_headers()?;
        self.base.post(headers).await
    }

    fn build_headers(&self) -> Result<HashMap<String, String>, TokenServiceError> {
        let creds = self
            .base
            .endpoint
            .other_params
            .get("idp_auth_credentials")
            .ok_or_else(|| {
                TokenServiceError::MissingKey(
                    "idp_auth_credentials (Base64 client_id:client_secret)".into(),
                )
            })?
            .trim();

        Ok(HashMap::from([
            ("accept".into(), "application/json".into()),
            ("authorization".into(), format!("Basic {}", creds)),
            (
                "content-type".into(),
                "application/x-www-form-urlencoded".into(),
            ),
        ]))
    }
}

pub struct EntraIdpTokenService {
    base: BaseTokenService,
}

#[async_trait]
impl TokenService for EntraIdpTokenService {
    async fn handle_request(&self) -> Result<String, TokenServiceError> {
        let headers = self.build_headers()?;
        self.base.post(headers).await
    }

    fn build_headers(&self) -> Result<HashMap<String, String>, TokenServiceError> {
        Ok(HashMap::from([
            ("accept".into(), "application/json".into()),
            (
                "content-type".into(),
                "application/x-www-form-urlencoded".into(),
            ),
        ]))
    }
}

pub fn create_token_service_client(
    endpoint: TokenEndpoint,
) -> Result<Box<dyn TokenService + Send + Sync>, TokenServiceError> {
    match endpoint.r#type.to_lowercase().as_str() {
        "okta" => Ok(Box::new(OktaIdpTokenService {
            base: BaseTokenService::new(endpoint)?,
        })),
        "entra" => Ok(Box::new(EntraIdpTokenService {
            base: BaseTokenService::new(endpoint)?,
        })),
        _ => Err(TokenServiceError::UnsupportedProvider(endpoint.r#type)),
    }
}
