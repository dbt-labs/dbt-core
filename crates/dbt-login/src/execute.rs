use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use dbt_common::cancellation::CancellationToken;
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_platform_auth::resolver::{INTERACTIVE_TIMEOUT, OAuthInteractiveResolver};
use dbt_platform_auth::{AuthError, OAUTH_CLIENT_ID};
use dbt_run_cache::auth::{
    BrowserFlow, InteractiveFlow, LOOPBACK_PORT, ORGS_SCOPE, StoredToken, TokenStore,
};
use dbt_run_cache::service_client::RunCacheServiceError;
use dbt_run_cache::service_config::{
    DEFAULT_OAUTH_AUTH_URL, DEFAULT_OAUTH_CLIENT_ID, DEFAULT_OAUTH_TOKEN_URL,
};

use crate::LicenseFetcher;
use crate::state_guidance::{run_state_guidance, run_state_guidance_after_state_login};

pub async fn execute_login(
    fetcher: Arc<dyn LicenseFetcher>,
    token: &CancellationToken,
) -> FsResult<()> {
    // Each opener captures its URL via a oneshot and returns immediately.
    // A separate task joins both URLs, combines them into a single browser open.
    let (state_url_tx, state_url_rx) = tokio::sync::oneshot::channel::<String>();
    let (platform_url_tx, platform_url_rx) = tokio::sync::oneshot::channel::<String>();

    let state_url_tx = Arc::new(Mutex::new(Some(state_url_tx)));
    let platform_url_tx = Arc::new(Mutex::new(Some(platform_url_tx)));

    let state_opener: dbt_run_cache::auth::Opener = {
        let tx = state_url_tx.clone();
        Box::new(move |url: &str| {
            if let Some(sender) = tx.lock().unwrap().take() {
                let _ = sender.send(url.to_string());
            }
        })
    };

    let platform_opener: dbt_platform_auth::resolver::Opener = {
        let tx = platform_url_tx.clone();
        Box::new(move |url: &str| {
            if let Some(sender) = tx.lock().unwrap().take() {
                let _ = sender.send(url.to_string());
            }
        })
    };

    // Wait for both authorize URLs (with timeout), combine them into a single browser open:
    // the platform-auth URL with the base64-encoded state URL as a query param.
    let url_timeout = tokio::time::Duration::from_secs(30);
    tokio::spawn(async move {
        let state_url = match tokio::time::timeout(url_timeout, state_url_rx).await {
            Ok(Ok(url)) => url,
            _ => {
                tracing::warn!("timed out waiting for dbt State authorize URL");
                return;
            }
        };
        let platform_url = match tokio::time::timeout(url_timeout, platform_url_rx).await {
            Ok(Ok(url)) => url,
            _ => {
                tracing::warn!("timed out waiting for dbt platform authorize URL");
                return;
            }
        };

        let encoded_state = URL_SAFE_NO_PAD.encode(state_url.as_bytes());
        let combined = match url::Url::parse(&platform_url) {
            Ok(mut u) => {
                u.query_pairs_mut()
                    .append_pair("dbt_state_oauth", &encoded_state);
                u.to_string()
            }
            Err(_) => format!("{platform_url}&dbt_state_oauth={encoded_state}"),
        };

        println!("Opening your browser to complete login...");
        println!("{}", console::style(&combined).bold());
        if let Err(_err) = open::that_detached(&combined) {
            println!(
                "Cannot open browser. Please paste the URL above into your browser to authorize \
                the dbt CLI."
            );
        }
        println!();
        println!(
            "If you need to reset your password, complete the reset, then re-run {} to finish \
            authenticating.",
            console::style("dbt login").bold()
        );
        println!("\nWaiting for authentication.");
    });

    let (state_abort_tx, state_abort_rx) = tokio::sync::oneshot::channel::<()>();
    let (platform_abort_tx, platform_abort_rx) = tokio::sync::oneshot::channel::<()>();

    let state_flow = BrowserFlow {
        http: reqwest::Client::new(),
        auth_url: DEFAULT_OAUTH_AUTH_URL.to_string(),
        token_url: DEFAULT_OAUTH_TOKEN_URL.to_string(),
        client_id: DEFAULT_OAUTH_CLIENT_ID.to_string(),
        scope: ORGS_SCOPE.to_string(),
        timeout: INTERACTIVE_TIMEOUT,
        redirect_port: LOOPBACK_PORT,
        opener: state_opener,
        abort_signal: Mutex::new(Some(state_abort_rx)),
    };

    let platform_resolver = OAuthInteractiveResolver::builder(OAUTH_CLIENT_ID)
        .opener(platform_opener)
        .abort_signal(platform_abort_rx)
        .build();

    let state_result = async {
        match state_flow.run().await {
            Ok(r) => Some(Ok(r)),
            Err(RunCacheServiceError::Aborted | RunCacheServiceError::Timeout(_)) => None,
            Err(e) => Some(Err(e)),
        }
    };

    tokio::select! {
        _ = async {
            loop {
                if token.is_cancelled() { break; }
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
        } => {
            let _ = state_abort_tx.send(());
            let _ = platform_abort_tx.send(());
            return Ok(());
        }
        Some(result) = state_result => {
            let response = result.map_err(|e| fs_err!(ErrorCode::AuthFailed, "{e}"))?;
            let stored = StoredToken::from_token_response(response, None)
                .map_err(|e| fs_err!(ErrorCode::AuthFailed, "{e}"))?;
            let store = TokenStore::discover().ok_or_else(|| {
                fs_err!(
                    ErrorCode::AuthFailed,
                    "could not resolve home directory for dbt State auth"
                )
            })?;
            store
                .save(&stored)
                .await
                .map_err(|e| fs_err!(ErrorCode::AuthFailed, "{e}"))?;
            run_state_guidance_after_state_login()?;
            println!("dbt State login successful (org: {}).", stored.org_id);
        }
        result = platform_resolver.resolve() => {
            let cred = match result {
                Ok(c) => c,
                Err(AuthError::Aborted) => return Ok(()),
                Err(e) => {
                    eprintln!(
                        "Authentication failed. Re-run {} to try again.\n\n{e}",
                        console::style("dbt login").bold()
                    );
                    return Err(fs_err!(ErrorCode::AuthFailed, "authentication failed"));
                }
            };

            // Fire off license fetch in background; join it after state guidance so
            // the process doesn't exit before it completes.
            let license_handle = {
                let f = Arc::clone(&fetcher);
                tokio::spawn(async move {
                    if let Err(e) = f.fetch_and_cache_license().await {
                        tracing::warn!("license fetch failed: {e}");
                    }
                })
            };

            let http = reqwest::Client::new();
            run_state_guidance(&cred, &http).await?;

            let _ = license_handle.await;

            println!("Congratulations! You are now signed in.");
        }
    }

    Ok(())
}
