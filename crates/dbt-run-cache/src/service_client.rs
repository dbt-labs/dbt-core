use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dbt_common::ErrorCode;
use dbt_common::tracing::emit::emit_warn_log_message;
use thiserror::Error;
use tonic::{
    Request,
    metadata::MetadataValue,
    transport::{Channel, ClientTlsConfig, Endpoint},
};

use crate::auth::OAuthTokenSource;
use crate::proto::query_cache::{
    CloneRequest, CloneResponse, ConfirmExecutionRequest, ConfirmExecutionResponse,
    GetExplainMessagesRequest, GetExplainMessagesResponse, RecordExecutionsRequest,
    RecordExecutionsResponse, SubmitEnrichedSqlRequest, SubmitSqlResponse,
    SubmitTelemetryBatchRequest, SubmitTelemetryBatchResponse, SubmitValuesRequest,
    ValidateClientVersionRequest, client_validation_client::ClientValidationClient,
    execution_client::ExecutionClient, explain_client::ExplainClient, sql_client::SqlClient,
};
use crate::proto::query_cache::{client_telemetry_client::ClientTelemetryClient, clone_client};
use crate::service_config::{RunCacheServiceConfig, RunCacheServiceConfigError};

const AUTHORIZATION_HEADER: &str = "authorization";
const ORG_ID_HEADER: &str = "x-organization-id";
const OS_NAME_HEADER: &str = "x-os-name";
const REQUEST_ID_HEADER: &str = "x-request-id";
const SESSION_ID_HEADER: &str = "x-session-id";
const SUBMITTED_AT_EPOCH_HEADER: &str = "x-submitted-at-epoch";
const SYSTEM_USER_ID_HEADER: &str = "x-system-user-id";
const HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
const HTTP2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientVersionStatus {
    Supported,
    Unsupported,
    Skipped,
}

impl ClientVersionStatus {
    pub fn allows_service_use(self) -> bool {
        matches!(self, Self::Supported)
    }
}

#[derive(Debug, Error)]
pub enum RunCacheServiceError {
    #[error("dbt State service is disabled")]
    Disabled,
    #[error(transparent)]
    Config(#[from] RunCacheServiceConfigError),
    #[error("dbt State service transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("dbt State service authentication request failed: {0}")]
    AuthRequest(#[from] reqwest::Error),
    #[error("dbt State service authentication failed: {0}")]
    Auth(String),
    #[error("Access to organization '{org_id}' has been disabled")]
    OrgDisabled { org_id: String },
    #[error("dbt State service metadata failed: {0}")]
    Metadata(String),
    #[error("dbt State service RPC failed: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("dbt State authentication was cancelled")]
    Aborted,
    #[error("dbt State authentication timed out after {0}s")]
    Timeout(u64),
}

impl RunCacheServiceError {
    pub fn is_transient_transport_rpc(&self) -> bool {
        match self {
            Self::Rpc(status) => {
                status.code() == tonic::Code::Unknown && status.message() == "transport error"
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunCacheClientMetadata {
    pub session_id: String,
    pub system_user_id: String,
    pub os_name: String,
}

impl RunCacheClientMetadata {
    pub fn new(
        session_id: impl Into<String>,
        system_user_id: impl Into<String>,
        os_name: impl Into<String>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            system_user_id: system_user_id.into(),
            os_name: os_name.into(),
        }
    }

    fn attach<T>(&self, mut request: Request<T>) -> Result<Request<T>, RunCacheServiceError> {
        let metadata = request.metadata_mut();
        insert_metadata(metadata, REQUEST_ID_HEADER, &new_request_id())?;
        insert_metadata(metadata, SESSION_ID_HEADER, &self.session_id)?;
        insert_metadata(
            metadata,
            SUBMITTED_AT_EPOCH_HEADER,
            &current_epoch_millis().to_string(),
        )?;
        insert_metadata(metadata, SYSTEM_USER_ID_HEADER, &self.system_user_id)?;
        insert_metadata(metadata, OS_NAME_HEADER, &self.os_name)?;
        Ok(request)
    }
}

impl Default for RunCacheClientMetadata {
    fn default() -> Self {
        Self {
            session_id: new_request_id(),
            system_user_id: String::new(),
            os_name: std::env::consts::OS.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
enum RunCacheAuth {
    None,
    OAuth(Arc<OAuthTokenSource>),
}

impl RunCacheAuth {
    fn from_config(config: &RunCacheServiceConfig) -> Result<Self, RunCacheServiceError> {
        if !config.secure {
            return Ok(Self::None);
        }
        Ok(Self::OAuth(Arc::new(OAuthTokenSource::new(config)?)))
    }

    async fn attach<T>(&self, request: Request<T>) -> Result<Request<T>, RunCacheServiceError> {
        match self {
            Self::None => Ok(request),
            Self::OAuth(source) => {
                let token = source.token().await?;
                with_auth_metadata(request, &token.id_token, &token.org_id)
            }
        }
    }
}

fn with_auth_metadata<T>(
    mut request: Request<T>,
    id_token: &str,
    org_id: &str,
) -> Result<Request<T>, RunCacheServiceError> {
    let authorization = format!("Bearer {id_token}").parse().map_err(|err| {
        RunCacheServiceError::Auth(format!("invalid authorization metadata: {err}"))
    })?;
    let org_id = org_id
        .parse()
        .map_err(|err| RunCacheServiceError::Auth(format!("invalid org metadata: {err}")))?;

    let metadata = request.metadata_mut();
    metadata.insert(AUTHORIZATION_HEADER, authorization);
    metadata.insert(ORG_ID_HEADER, org_id);
    Ok(request)
}

pub type SharedRunCacheServiceClient = Arc<dyn RunCacheServiceClient>;

pub fn shared_run_cache_service_client<C>(client: C) -> SharedRunCacheServiceClient
where
    C: RunCacheServiceClient + 'static,
{
    Arc::new(client)
}

#[async_trait]
pub trait RunCacheServiceClient: Send + Sync {
    async fn validate_client_version(&self) -> Result<ClientVersionStatus, RunCacheServiceError>;

    async fn submit_enriched_sql(
        &self,
        request: SubmitEnrichedSqlRequest,
    ) -> Result<SubmitSqlResponse, RunCacheServiceError>;

    async fn submit_values(
        &self,
        request: SubmitValuesRequest,
    ) -> Result<SubmitSqlResponse, RunCacheServiceError>;

    async fn confirm_execution(
        &self,
        request: ConfirmExecutionRequest,
    ) -> Result<ConfirmExecutionResponse, RunCacheServiceError>;

    async fn record_executions(
        &self,
        _request: RecordExecutionsRequest,
    ) -> Result<RecordExecutionsResponse, RunCacheServiceError> {
        Err(RunCacheServiceError::Disabled)
    }

    async fn register_clone(
        &self,
        _request: CloneRequest,
    ) -> Result<CloneResponse, RunCacheServiceError> {
        Err(RunCacheServiceError::Disabled)
    }

    async fn submit_telemetry_batch(
        &self,
        _request: SubmitTelemetryBatchRequest,
    ) -> Result<SubmitTelemetryBatchResponse, RunCacheServiceError> {
        Err(RunCacheServiceError::Disabled)
    }

    async fn get_explain_messages(
        &self,
        _request: GetExplainMessagesRequest,
    ) -> Result<GetExplainMessagesResponse, RunCacheServiceError> {
        Err(RunCacheServiceError::Disabled)
    }
}

#[derive(Debug, Clone)]
pub struct GrpcRunCacheServiceClient {
    sql: SqlClient<Channel>,
    clone: clone_client::CloneClient<Channel>,
    execution: ExecutionClient<Channel>,
    client_telemetry: ClientTelemetryClient<Channel>,
    client_validation: ClientValidationClient<Channel>,
    explain: ExplainClient<Channel>,
    auth: RunCacheAuth,
    metadata: RunCacheClientMetadata,
}

impl GrpcRunCacheServiceClient {
    pub async fn connect(config: RunCacheServiceConfig) -> Result<Self, RunCacheServiceError> {
        Self::connect_with_metadata(config, RunCacheClientMetadata::default()).await
    }

    pub async fn connect_with_metadata(
        config: RunCacheServiceConfig,
        metadata: RunCacheClientMetadata,
    ) -> Result<Self, RunCacheServiceError> {
        if !config.enabled {
            return Err(RunCacheServiceError::Disabled);
        }

        let auth = RunCacheAuth::from_config(&config)?;
        let mut endpoint = Endpoint::from_shared(config.endpoint_uri())?
            .connect_timeout(config.timeout)
            .timeout(config.timeout)
            .http2_keep_alive_interval(HTTP2_KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(HTTP2_KEEP_ALIVE_TIMEOUT)
            .keep_alive_while_idle(true);
        if config.secure {
            endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
        }
        let channel = endpoint.connect().await?;

        Ok(Self {
            sql: SqlClient::new(channel.clone()),
            clone: clone_client::CloneClient::new(channel.clone()),
            execution: ExecutionClient::new(channel.clone()),
            client_telemetry: ClientTelemetryClient::new(channel.clone()),
            client_validation: ClientValidationClient::new(channel.clone()),
            explain: ExplainClient::new(channel),
            auth,
            metadata,
        })
    }

    async fn attach<T>(&self, request: Request<T>) -> Result<Request<T>, RunCacheServiceError> {
        let request = self.auth.attach(request).await?;
        self.metadata.attach(request)
    }
}

#[async_trait]
impl RunCacheServiceClient for GrpcRunCacheServiceClient {
    async fn validate_client_version(&self) -> Result<ClientVersionStatus, RunCacheServiceError> {
        let request = self
            .attach(Request::new(ValidateClientVersionRequest {
                dbt_run_cache_version: env!("CARGO_PKG_VERSION").to_string(),
            }))
            .await?;
        let response = self
            .client_validation
            .clone()
            .validate_client_version(request)
            .await?
            .into_inner();

        if response.is_supported {
            Ok(ClientVersionStatus::Supported)
        } else {
            Ok(ClientVersionStatus::Unsupported)
        }
    }

    async fn submit_enriched_sql(
        &self,
        request: SubmitEnrichedSqlRequest,
    ) -> Result<SubmitSqlResponse, RunCacheServiceError> {
        let request = self.attach(Request::new(request)).await?;
        Ok(self
            .sql
            .clone()
            .submit_enriched_sql(request)
            .await?
            .into_inner())
    }

    async fn submit_values(
        &self,
        request: SubmitValuesRequest,
    ) -> Result<SubmitSqlResponse, RunCacheServiceError> {
        let request = self.attach(Request::new(request)).await?;
        Ok(self.sql.clone().submit_values(request).await?.into_inner())
    }

    async fn confirm_execution(
        &self,
        request: ConfirmExecutionRequest,
    ) -> Result<ConfirmExecutionResponse, RunCacheServiceError> {
        let request = self.attach(Request::new(request)).await?;
        Ok(self
            .execution
            .clone()
            .confirm_execution(request)
            .await?
            .into_inner())
    }

    async fn record_executions(
        &self,
        request: RecordExecutionsRequest,
    ) -> Result<RecordExecutionsResponse, RunCacheServiceError> {
        let request = self.attach(Request::new(request)).await?;
        Ok(self
            .execution
            .clone()
            .record_executions(request)
            .await?
            .into_inner())
    }

    async fn register_clone(
        &self,
        request: CloneRequest,
    ) -> Result<CloneResponse, RunCacheServiceError> {
        let request = self.attach(Request::new(request)).await?;
        Ok(self
            .clone
            .clone()
            .register_clone(request)
            .await?
            .into_inner())
    }

    async fn submit_telemetry_batch(
        &self,
        request: SubmitTelemetryBatchRequest,
    ) -> Result<SubmitTelemetryBatchResponse, RunCacheServiceError> {
        let request = self.attach(Request::new(request)).await?;
        Ok(self
            .client_telemetry
            .clone()
            .submit_telemetry_batch(request)
            .await?
            .into_inner())
    }

    async fn get_explain_messages(
        &self,
        request: GetExplainMessagesRequest,
    ) -> Result<GetExplainMessagesResponse, RunCacheServiceError> {
        let request = self.attach(Request::new(request)).await?;
        Ok(self
            .explain
            .clone()
            .get_explain_messages(request)
            .await?
            .into_inner())
    }
}

fn new_request_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn current_epoch_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

fn insert_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    key: &'static str,
    value: &str,
) -> Result<(), RunCacheServiceError> {
    let value = MetadataValue::try_from(value)
        .map_err(|err| RunCacheServiceError::Metadata(format!("invalid {key} metadata: {err}")))?;
    metadata.insert(key, value);
    Ok(())
}

pub async fn validate_client_version_fail_open<C>(client: &C) -> ClientVersionStatus
where
    C: RunCacheServiceClient + ?Sized,
{
    match client.validate_client_version().await {
        Ok(status) => status,
        Err(err) => {
            emit_warn_log_message(
                ErrorCode::StateServiceWarn,
                format!(
                    "dbt State client validation failed: {}; executing normally",
                    format_error_chain(&err)
                ),
                None,
            );
            ClientVersionStatus::Skipped
        }
    }
}

/// Render an error with its full `source()` chain joined by `: `.
///
/// Several upstream error types (notably `tonic::transport::Error`) have a
/// terse `Display` that hides the actual cause. Walking the chain surfaces it.
pub fn format_error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    use std::fmt::Write;
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(s) = source {
        let _ = write!(out, ": {s}");
        source = s.source();
    }
    out
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::service_config::{DEFAULT_API_URL, DEFAULT_OAUTH_AUTH_URL, DEFAULT_OAUTH_TOKEN_URL};

    #[test]
    fn only_supported_validation_allows_service_use() {
        assert!(ClientVersionStatus::Supported.allows_service_use());
        assert!(!ClientVersionStatus::Skipped.allows_service_use());
        assert!(!ClientVersionStatus::Unsupported.allows_service_use());
    }

    struct FailingValidationClient;

    #[async_trait]
    impl RunCacheServiceClient for FailingValidationClient {
        async fn validate_client_version(
            &self,
        ) -> Result<ClientVersionStatus, RunCacheServiceError> {
            Err(RunCacheServiceError::Rpc(tonic::Status::unavailable(
                "service unavailable",
            )))
        }

        async fn submit_enriched_sql(
            &self,
            _request: SubmitEnrichedSqlRequest,
        ) -> Result<SubmitSqlResponse, RunCacheServiceError> {
            unreachable!("validation test should not submit SQL")
        }

        async fn submit_values(
            &self,
            _request: SubmitValuesRequest,
        ) -> Result<SubmitSqlResponse, RunCacheServiceError> {
            unreachable!("validation test should not submit values")
        }

        async fn confirm_execution(
            &self,
            _request: ConfirmExecutionRequest,
        ) -> Result<ConfirmExecutionResponse, RunCacheServiceError> {
            unreachable!("validation test should not confirm execution")
        }
    }

    #[tokio::test]
    async fn validation_rpc_error_fails_open_to_skipped() {
        let client = FailingValidationClient;

        assert_eq!(
            validate_client_version_fail_open(&client).await,
            ClientVersionStatus::Skipped
        );
    }

    #[tokio::test]
    async fn disabled_config_does_not_connect() {
        let err = GrpcRunCacheServiceClient::connect(RunCacheServiceConfig::disabled())
            .await
            .unwrap_err();

        assert!(matches!(err, RunCacheServiceError::Disabled));
    }

    #[test]
    fn auth_metadata_contains_bearer_token_and_org_context() {
        let request = with_auth_metadata(Request::new(()), "test-token", "test-org").unwrap();

        assert_eq!(
            request
                .metadata()
                .get(AUTHORIZATION_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer test-token"
        );
        assert_eq!(
            request
                .metadata()
                .get(ORG_ID_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            "test-org"
        );
    }

    #[test]
    fn secure_auth_without_client_secret_constructs_browser_source() {
        let config = RunCacheServiceConfig {
            enabled: true,
            api_url: DEFAULT_API_URL.to_string(),
            secure: true,
            org_id: Some("test-org".to_string()),
            oauth_client_id: "client-id".to_string(),
            oauth_client_secret: None,
            oauth_token_url: DEFAULT_OAUTH_TOKEN_URL.to_string(),
            oauth_auth_url: DEFAULT_OAUTH_AUTH_URL.to_string(),
            timeout: Duration::from_secs(60),
            ..RunCacheServiceConfig::disabled()
        };

        RunCacheAuth::from_config(&config)
            .expect("construction should succeed without a client secret");
    }

    #[test]
    fn org_disabled_error_renders_with_org_id() {
        let err = RunCacheServiceError::OrgDisabled {
            org_id: "test-org".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Access to organization 'test-org' has been disabled"
        );
    }

    #[test]
    fn transient_transport_rpc_is_identified() {
        let err = RunCacheServiceError::Rpc(tonic::Status::unknown("transport error"));
        assert!(err.is_transient_transport_rpc());

        let unavailable = RunCacheServiceError::Rpc(tonic::Status::unavailable("transport error"));
        assert!(!unavailable.is_transient_transport_rpc());

        let application_unknown =
            RunCacheServiceError::Rpc(tonic::Status::unknown("application error"));
        assert!(!application_unknown.is_transient_transport_rpc());
    }

    #[test]
    fn request_metadata_matches_python_client_headers() {
        let metadata = RunCacheClientMetadata::new("session-1", "system-user-1", "test-os");
        let request = metadata.attach(Request::new(())).unwrap();

        assert!(
            request
                .metadata()
                .get(REQUEST_ID_HEADER)
                .unwrap()
                .to_str()
                .unwrap()
                .len()
                > 20
        );
        assert_eq!(
            request
                .metadata()
                .get(SESSION_ID_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            "session-1"
        );
        assert!(
            request
                .metadata()
                .get(SUBMITTED_AT_EPOCH_HEADER)
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<i64>()
                .unwrap()
                > 0
        );
        assert_eq!(
            request
                .metadata()
                .get(SYSTEM_USER_ID_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            "system-user-1"
        );
        assert_eq!(
            request
                .metadata()
                .get(OS_NAME_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            "test-os"
        );
    }
}
