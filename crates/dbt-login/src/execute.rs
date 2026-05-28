use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use dbt_common::{ErrorCode, FsResult, fs_err};
use dbt_platform_auth::OAUTH_CLIENT_ID;
use dbt_platform_auth::resolver::OAuthInteractiveResolver;
use dbt_run_cache::auth::{
    BrowserFlow, INTERACTIVE_TIMEOUT, InteractiveFlow, LOOPBACK_PORT, ORGS_SCOPE, StoredToken,
    TokenStore,
};
use dbt_run_cache::service_config::{
    DEFAULT_OAUTH_AUTH_URL, DEFAULT_OAUTH_CLIENT_ID, DEFAULT_OAUTH_TOKEN_URL,
};

use crate::LicenseFetcher;
use crate::state_guidance::run_state_guidance;

fn auth_spinner() -> indicatif::ProgressBar {
    let pb = indicatif::ProgressBar::new_spinner();
    pb.set_style(
        indicatif::ProgressStyle::default_spinner()
            .template("{spinner} {msg}")
            .expect("valid spinner template"),
    );
    pb.set_message("Waiting for authentication.");
    pb
}

pub async fn execute_login(fetcher: Arc<dyn LicenseFetcher>) -> FsResult<()> {
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

    // The spinner is created up-front so both the spawn (which starts it) and the
    // select! arms (which clear it) can share the same handle via clone.
    let spinner = auth_spinner();

    // Wait for both authorize URLs (with timeout), combine them into a single browser open:
    // the platform-auth URL with the base64-encoded state URL as a query param.
    let url_timeout = tokio::time::Duration::from_secs(30);
    let spinner_clone = spinner.clone();
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
        spinner_clone.enable_steady_tick(std::time::Duration::from_millis(80));
    });

    let state_flow = BrowserFlow {
        http: reqwest::Client::new(),
        auth_url: DEFAULT_OAUTH_AUTH_URL.to_string(),
        token_url: DEFAULT_OAUTH_TOKEN_URL.to_string(),
        client_id: DEFAULT_OAUTH_CLIENT_ID.to_string(),
        scope: ORGS_SCOPE.to_string(),
        timeout: INTERACTIVE_TIMEOUT,
        redirect_port: LOOPBACK_PORT,
        opener: state_opener,
        abort_signal: Mutex::new(None),
    };

    let platform_resolver = OAuthInteractiveResolver::builder(OAUTH_CLIENT_ID)
        .opener(platform_opener)
        .build();

    tokio::select! {
        result = state_flow.run() => {
            spinner.finish_and_clear();
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
            println!("dbt State login successful (org: {}).", stored.org_id);
        }
        result = platform_resolver.resolve() => {
            spinner.finish_and_clear();
            let cred = match result {
                Ok(c) => c,
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
