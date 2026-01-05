use crate::typed_adapter::TypedBaseAdapter;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use std::cell::RefCell;
use std::io::Read;
use std::thread;
use std::time::{Duration, Instant};
use ureq::http;
use ureq::{self, Agent, Body, Error as UreqError, RequestBuilder};

const DEFAULT_SHARED_NOTEBOOK_ROOT: &str = "/Shared/dbt_python_model";
const USER_AGENT: &str = "dbt-fs";
const REQUEST_TIMEOUT_SECS: u64 = 60;
const POLL_INTERVAL_SECS: u64 = 10;

pub(crate) struct DatabricksApiClient {
    agent: Agent,
    base_url: String,
    auth_header: String,
    use_user_folder: bool,
    cached_user: RefCell<Option<String>>,
}

impl DatabricksApiClient {
    pub(crate) fn new(
        adapter: &dyn TypedBaseAdapter,
        use_user_folder: bool,
    ) -> AdapterResult<Self> {
        let host = adapter.get_db_config("host").ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                "Databricks profile is missing 'host'.",
            )
        })?;
        let token = adapter.get_db_config("token").ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Configuration,
                "Databricks profile is missing 'token'. A personal access token is required to submit Python models.",
            )
        })?;

        let base_url = Self::normalize_host(host.as_ref());
        let agent: Agent = Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(REQUEST_TIMEOUT_SECS)))
            .build()
            .into();

        Ok(Self {
            agent,
            base_url,
            auth_header: format!("Bearer {}", token),
            use_user_folder,
            cached_user: RefCell::new(None),
        })
    }

    pub(crate) fn notebook_path(
        &self,
        catalog: &str,
        schema: &str,
        identifier: &str,
    ) -> AdapterResult<String> {
        let base_folder = if self.use_user_folder {
            let user = self.current_user()?;
            format!("/Users/{}/dbt_python_models/{}/{}", user, catalog, schema)
        } else {
            format!("{}/{schema}", DEFAULT_SHARED_NOTEBOOK_ROOT)
        };

        Ok(format!("{}/{}", base_folder, identifier))
    }

    pub(crate) fn ensure_directory(&self, path: &str) -> AdapterResult<()> {
        let body = json!({ "path": path });
        self.post_json_noop("/api/2.0/workspace/mkdirs", body)
    }

    pub(crate) fn import_notebook(&self, path: &str, compiled_code: &str) -> AdapterResult<()> {
        let encoded = BASE64_STANDARD.encode(compiled_code);
        let body = json!({
            "path": path,
            "language": "PYTHON",
            "format": "SOURCE",
            "overwrite": true,
            "content": encoded,
        });
        self.post_json_noop("/api/2.0/workspace/import", body)
    }

    pub(crate) fn submit_job_run(
        &self,
        run_name: &str,
        job_spec: &serde_json::Value,
        timeout_seconds: u64,
    ) -> AdapterResult<String> {
        let capped_timeout = timeout_seconds.min(i64::MAX as u64);
        let payload = json!({
            "run_name": run_name,
            "timeout_seconds": capped_timeout as i64,
            "tasks": [job_spec.clone()],
            "queue": {
                "enabled": true
            }
        });

        let response: SubmitRunResponse = self.post_json("/api/2.1/jobs/runs/submit", payload)?;
        Ok(response.run_id.to_string())
    }

    pub(crate) fn poll_job_completion(
        &self,
        run_id: &str,
        timeout_seconds: u64,
    ) -> AdapterResult<()> {
        let start = Instant::now();
        let timeout_duration = if timeout_seconds == 0 {
            None
        } else {
            Some(Duration::from_secs(timeout_seconds))
        };

        loop {
            let state = self.run_state(run_id)?;
            if let Some(life_cycle_state) = state.life_cycle_state.as_deref() {
                if matches!(
                    life_cycle_state,
                    "TERMINATED" | "SKIPPED" | "INTERNAL_ERROR"
                ) {
                    return Self::handle_terminal_state(life_cycle_state, &state);
                }
            }

            if let Some(limit) = timeout_duration {
                if start.elapsed() >= limit {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Driver,
                        format!(
                            "Databricks job {run_id} timed out after {timeout_seconds} seconds"
                        ),
                    ));
                }
            }

            thread::sleep(Duration::from_secs(POLL_INTERVAL_SECS));
        }
    }

    fn run_state(&self, run_id: &str) -> AdapterResult<RunStatePayload> {
        let response: GetRunResponse =
            self.get("/api/2.1/jobs/runs/get", Some(&[("run_id", run_id)]))?;
        Ok(response.state)
    }

    fn handle_terminal_state(life_cycle_state: &str, state: &RunStatePayload) -> AdapterResult<()> {
        if life_cycle_state == "TERMINATED"
            && state
                .result_state
                .as_deref()
                .is_some_and(|result| result == "SUCCESS")
        {
            return Ok(());
        }

        let result_state = state.result_state.as_deref().unwrap_or("UNKNOWN");
        let message = state
            .state_message
            .as_deref()
            .unwrap_or("Databricks reported a failure without additional context");

        Err(AdapterError::new(
            AdapterErrorKind::Driver,
            format!(
                "Databricks job failed (life_cycle_state={life_cycle_state}, result_state={result_state}): {message}"
            ),
        ))
    }

    fn current_user(&self) -> AdapterResult<String> {
        if !self.use_user_folder {
            return Err(AdapterError::new(
                AdapterErrorKind::Internal,
                "current_user() should not be called when user folders are disabled",
            ));
        }

        if let Some(user) = self.cached_user.borrow().clone() {
            return Ok(user);
        }

        let response: CurrentUserResponse = self.get("/api/2.0/preview/scim/v2/Me", None)?;
        if response.user_name.trim().is_empty() {
            return Err(AdapterError::new(
                AdapterErrorKind::Driver,
                "Unable to determine Databricks user name for notebook uploads",
            ));
        }

        self.cached_user
            .borrow_mut()
            .replace(response.user_name.clone());
        Ok(response.user_name)
    }

    fn post_json<T: DeserializeOwned>(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> AdapterResult<T> {
        let url = self.full_url(path);
        let request = self.configure_request(self.agent.post(&url), true);
        let response = match request.send_json(body) {
            Ok(resp) => resp,
            Err(err) => return Err(self.map_ureq_error(err)),
        };
        self.parse_json_response(response)
    }

    fn post_json_noop(&self, path: &str, body: serde_json::Value) -> AdapterResult<()> {
        let url = self.full_url(path);
        let request = self.configure_request(self.agent.post(&url), true);
        let response = match request.send_json(body) {
            Ok(resp) => resp,
            Err(err) => return Err(self.map_ureq_error(err)),
        };
        self.consume_response(response)
    }

    fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        query: Option<&[(&str, &str)]>,
    ) -> AdapterResult<T> {
        let url = self.full_url(path);
        let mut request = self.configure_request(self.agent.get(&url), false);
        if let Some(params) = query {
            for (key, value) in params {
                request = request.query(key, value);
            }
        }
        let response = match request.call() {
            Ok(resp) => resp,
            Err(err) => return Err(self.map_ureq_error(err)),
        };
        self.parse_json_response(response)
    }

    fn full_url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn configure_request<B>(
        &self,
        mut request: RequestBuilder<B>,
        include_content_type: bool,
    ) -> RequestBuilder<B> {
        request = request
            .header("Authorization", &self.auth_header)
            .header("User-Agent", USER_AGENT);
        if include_content_type {
            request = request.header("Content-Type", "application/json");
        }
        request
    }

    fn parse_json_response<T: DeserializeOwned>(
        &self,
        response: http::Response<Body>,
    ) -> AdapterResult<T> {
        let mut response = self.ensure_success(response)?;
        response.body_mut().read_json::<T>().map_err(|err| {
            AdapterError::new(
                AdapterErrorKind::Driver,
                format!("Failed to parse Databricks API response: {err}"),
            )
        })
    }

    fn consume_response(&self, response: http::Response<Body>) -> AdapterResult<()> {
        let response = self.ensure_success(response)?;
        Self::body_to_string(response)?;
        Ok(())
    }

    fn ensure_success(
        &self,
        response: http::Response<Body>,
    ) -> AdapterResult<http::Response<Body>> {
        let status = response.status().as_u16();
        if status >= 400 {
            let body = Self::body_to_string(response)?;
            Err(Self::http_error(status, body))
        } else {
            Ok(response)
        }
    }

    fn body_to_string(response: http::Response<Body>) -> AdapterResult<String> {
        let mut reader = response.into_body().into_reader();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).map_err(|err| {
            AdapterError::new(
                AdapterErrorKind::Driver,
                format!("Failed to read Databricks API response: {err}"),
            )
        })?;
        Ok(String::from_utf8_lossy(&buffer).to_string())
    }

    fn map_ureq_error(&self, err: UreqError) -> AdapterError {
        match err {
            UreqError::StatusCode(status) => Self::http_error(status, String::new()),
            UreqError::Timeout(_) => AdapterError::new(
                AdapterErrorKind::Driver,
                "Databricks API request timed out".to_string(),
            ),
            UreqError::Io(io_err) => AdapterError::new(
                AdapterErrorKind::Driver,
                format!("Databricks API I/O error: {io_err}"),
            ),
            other => AdapterError::new(
                AdapterErrorKind::Driver,
                format!("Databricks API error: {other:?}"),
            ),
        }
    }

    fn http_error(status: u16, body: String) -> AdapterError {
        if let Ok(err) = serde_json::from_str::<DatabricksErrorResponse>(&body) {
            let code = err.error_code.unwrap_or_else(|| "Unknown".to_string());
            let message = err
                .message
                .unwrap_or_else(|| "Databricks API request failed".to_string());
            AdapterError::new(
                AdapterErrorKind::Driver,
                format!("Databricks API (status {status}, code {code}): {message}"),
            )
        } else {
            let trimmed = body.trim();
            let rendered = if trimmed.is_empty() {
                "<empty response>"
            } else {
                trimmed
            };
            AdapterError::new(
                AdapterErrorKind::Driver,
                format!("Databricks API (status {status}) returned an error: {rendered}"),
            )
        }
    }

    fn normalize_host(host: &str) -> String {
        let trimmed = host.trim().trim_end_matches('/');
        if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            trimmed.to_string()
        } else {
            format!("https://{trimmed}")
        }
    }
}

#[derive(Deserialize)]
struct SubmitRunResponse {
    run_id: u64,
}

#[derive(Deserialize)]
struct GetRunResponse {
    state: RunStatePayload,
}

#[derive(Deserialize)]
struct RunStatePayload {
    #[serde(rename = "life_cycle_state")]
    life_cycle_state: Option<String>,
    #[serde(rename = "result_state")]
    result_state: Option<String>,
    #[serde(rename = "state_message")]
    state_message: Option<String>,
}

#[derive(Deserialize)]
struct CurrentUserResponse {
    #[serde(rename = "userName")]
    user_name: String,
}

#[derive(Deserialize)]
struct DatabricksErrorResponse {
    #[serde(rename = "error_code")]
    error_code: Option<String>,
    message: Option<String>,
}
