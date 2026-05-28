use dbt_common::constants::DBT_CDN_URL;
use reqwest;
use serde_json::Value;
use std::env;

const VERSION_CHECK_DISABLED_ENV: &str = "DBT_DISABLE_VERSION_CHECK";

pub async fn check_version(current_version: &str, cdn_url: Option<&str>) -> Option<String> {
    // Skip check if disabled via env var
    #[allow(clippy::disallowed_methods)]
    if env::var(VERSION_CHECK_DISABLED_ENV).is_ok() {
        return None;
    }

    // Get latest version
    if let Ok(latest_version) = fetch_latest_version(cdn_url).await
        && compare_versions(current_version, &latest_version)
    {
        return Some(latest_version);
    }
    None
}

// Returns true if latest_version is newer than current_version
fn compare_versions(current_version: &str, latest_version: &str) -> bool {
    let current_parts: Vec<&str> = current_version.split('.').collect();
    let latest_parts: Vec<&str> = latest_version.split('.').collect();

    if current_parts.len() != latest_parts.len() {
        return false;
    }

    // Compare numeric parts first
    for (current, latest) in current_parts.iter().zip(latest_parts.iter()) {
        // Split into numeric and pre-release parts
        let (current_num, current_pre) = split_version_part(current);
        let (latest_num, latest_pre) = split_version_part(latest);

        match (current_num.parse::<u32>(), latest_num.parse::<u32>()) {
            (Ok(c), Ok(l)) if l > c => return true,
            (Ok(c), Ok(l)) if l < c => return false,
            (Ok(c), Ok(l)) if l == c => {
                // If numeric parts are equal, compare pre-release parts
                if let (Some(cp), Some(lp)) = (current_pre, latest_pre) {
                    // alpha < beta < rc < no suffix
                    match (cp, lp) {
                        ("alpha", _) if lp != "alpha" => return true,
                        ("beta", "rc") | ("beta", "") => return true,
                        ("rc", "") => return true,
                        (c, l) if c == l => continue,
                        _ => return false,
                    }
                } else if current_pre.is_some() && latest_pre.is_none() {
                    // Pre-release version is older than release version
                    return true;
                } else if current_pre.is_none() && latest_pre.is_some() {
                    return false;
                }
                continue;
            }
            _ => continue,
        }
    }
    false
}

// Helper function to split version part into numeric and pre-release parts
fn split_version_part(part: &str) -> (&str, Option<&str>) {
    if let Some((num, pre)) = part.split_once('-') {
        (num, Some(pre))
    } else {
        (part, None)
    }
}

async fn fetch_latest_version(cdn_url: Option<&str>) -> Result<String, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let base_url = if let Some(url) = cdn_url {
        url.to_string()
    } else {
        #[allow(clippy::disallowed_methods)]
        env::var("DBT_CDN_URL").unwrap_or_else(|_| DBT_CDN_URL.to_string())
    };
    let url = format!("{base_url}/versions.json");
    let response = client
        .get(&url)
        .header("User-Agent", "dbt-fusion")
        .send()
        .await?;

    let body = response.text().await?;
    let versions: Value = serde_json::from_str(&body)?;
    let latest = versions["latest"]["tag"]
        .as_str()
        .ok_or("Invalid version format")?
        .trim_start_matches('v')
        .to_string();

    Ok(latest)
}
