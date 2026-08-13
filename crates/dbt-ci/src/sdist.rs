//! Assembly of the download-at-install sdist: a tiny `.tar.gz` carrying the
//! embedded PEP 517 backend and an `assets.json` manifest of per-platform wheel
//! filenames + sha256s. See `templates/sdist_build_backend.py`.

use crate::pack::{normalize_wheel_name, render_metadata, target_to_platform_tag, wheel_filename};
use crate::pyproject::Spec;
use crate::release_version::semver_to_pep440;
use crate::utils::{backoff, is_transient, require_https, sha256_hex};
use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::File;
use std::path::{Path, PathBuf};

/// Embedded PEP 517 backend baked into every sdist.
const SDIST_BACKEND_PY: &str = include_str!("../templates/sdist_build_backend.py");

/// Import package name of the embedded backend; shared by the bundled paths and
/// the pyproject `build-backend` line.
const SDIST_BACKEND_PKG: &str = "_dbt_sa_build";

/// A prebuilt wheel pinned by the sdist manifest.
#[derive(Debug)]
pub(crate) struct WheelAsset {
    /// PyPI platform tag (e.g. `manylinux_2_28_x86_64`); the manifest key.
    pub(crate) platform_tag: String,
    pub(crate) filename: String,
    pub(crate) sha256_hex: String,
}

#[derive(serde::Serialize)]
struct AssetsManifest<'a> {
    name: &'a str,
    version: &'a str,
    base_url: &'a str,
    wheels: BTreeMap<&'a str, AssetEntry<'a>>,
}

#[derive(serde::Serialize)]
struct AssetEntry<'a> {
    filename: &'a str,
    sha256: &'a str,
}

/// Builds the sdist `{dist}-{version}.tar.gz`; `wheels` pins the per-platform
/// filename + sha256 the manifest references.
pub(crate) fn build_sdist(
    spec: &Spec,
    version_pep440: &str,
    wheels: &[WheelAsset],
    base_url: &str,
    out_dir: &Path,
) -> Result<PathBuf> {
    require_https(base_url)?;
    let dist = normalize_wheel_name(&spec.wheel_name);
    let root = format!("{dist}-{version_pep440}");
    let sdist_path = out_dir.join(format!("{root}.tar.gz"));

    let pyproject = render_sdist_pyproject(spec, version_pep440);
    let pkg_info = render_metadata(spec, version_pep440);
    let assets = render_assets_json(spec, version_pep440, base_url, wheels)?;

    let entries: Vec<(String, Vec<u8>)> = vec![
        (format!("{root}/pyproject.toml"), pyproject.into_bytes()),
        (format!("{root}/PKG-INFO"), pkg_info.into_bytes()),
        (
            format!("{root}/{SDIST_BACKEND_PKG}/__init__.py"),
            SDIST_BACKEND_PY.as_bytes().to_vec(),
        ),
        (
            format!("{root}/{SDIST_BACKEND_PKG}/assets.json"),
            assets.into_bytes(),
        ),
    ];

    write_targz(&sdist_path, &entries)?;
    Ok(sdist_path)
}

/// Builds the release sdist by fetching each `--target`'s wheel from `base_url`
/// and hashing the live bytes, so the manifest matches what installers fetch.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_release_sdist(
    http: &reqwest::Client,
    spec: &Spec,
    version: &str,
    base_url: &str,
    targets: &[String],
    python_tag: &str,
    abi_tag: &str,
    out_dir: &Path,
) -> Result<PathBuf> {
    if targets.is_empty() {
        bail!("--download-base-url requires at least one --target");
    }
    // Reject a non-https base url before any fetch, so we never pull wheels over
    // an insecure transport (the assembly-time check in `build_sdist` is too late).
    require_https(base_url)?;
    let version_pep440 = semver_to_pep440(version)?;
    let dist = normalize_wheel_name(&spec.wheel_name);
    let base = base_url.trim_end_matches('/');

    let mut wheels = Vec::with_capacity(targets.len());
    for triple in targets {
        let platform_tag = target_to_platform_tag(triple)
            .ok_or_else(|| anyhow!("unsupported --target {triple:?}"))?;
        let filename = wheel_filename(&dist, &version_pep440, python_tag, abi_tag, &platform_tag);
        let url = format!("{base}/{filename}");
        eprintln!("→ GET {url}");
        let bytes = download(http, &url).await?;
        let digest = sha256_hex(bytes.as_ref());
        eprintln!("✓ {filename} ({} bytes, sha256={digest})", bytes.len());
        wheels.push(WheelAsset {
            platform_tag,
            filename,
            sha256_hex: digest,
        });
    }

    build_sdist(spec, &version_pep440, &wheels, base_url, out_dir)
}

/// GETs `url`, retrying transient failures and 5xx. A 4xx (e.g. a missing wheel)
/// is fatal — better to fail than ship an sdist pointing at a missing wheel.
async fn download(http: &reqwest::Client, url: &str) -> Result<Bytes> {
    let max_attempts: u32 = 4;
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match http.get(url).send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    return resp
                        .bytes()
                        .await
                        .with_context(|| format!("read body from {url}"));
                }
                if status.is_server_error() && attempt < max_attempts {
                    let delay = backoff(attempt);
                    eprintln!(
                        "warning: GET {url} got {status}; retrying in {}ms",
                        delay.as_millis()
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                bail!("GET {url} failed: {status}");
            }
            Err(e) if is_transient(&e) && attempt < max_attempts => {
                let delay = backoff(attempt);
                eprintln!(
                    "warning: GET {url} failed: {e}; retrying in {}ms",
                    delay.as_millis()
                );
                tokio::time::sleep(delay).await;
                continue;
            }
            Err(e) => return Err(anyhow::Error::new(e).context(format!("GET {url}"))),
        }
    }
}

/// Minimal pyproject wiring up the embedded backend; rich metadata lives in PKG-INFO.
fn render_sdist_pyproject(spec: &Spec, version_pep440: &str) -> String {
    let mut out = String::new();
    out.push_str("[build-system]\n");
    out.push_str("requires = [\"packaging>=24\"]\n");
    let _ = writeln!(out, "build-backend = {SDIST_BACKEND_PKG:?}");
    out.push_str("backend-path = [\".\"]\n\n");
    out.push_str("[project]\n");
    // `{:?}` yields quoted, escaped TOML basic strings.
    let _ = writeln!(out, "name = {:?}", spec.wheel_name);
    let _ = writeln!(out, "version = {version_pep440:?}");
    if let Some(rp) = &spec.requires_python {
        let _ = writeln!(out, "requires-python = {rp:?}");
    }
    out
}

fn render_assets_json(
    spec: &Spec,
    version_pep440: &str,
    base_url: &str,
    wheels: &[WheelAsset],
) -> Result<String> {
    let mut map: BTreeMap<&str, AssetEntry> = BTreeMap::new();
    for w in wheels {
        let entry = AssetEntry {
            filename: &w.filename,
            sha256: &w.sha256_hex,
        };
        if map.insert(w.platform_tag.as_str(), entry).is_some() {
            bail!(
                "two binaries map to platform tag {:?}; the sdist manifest can only \
                 reference one wheel per platform",
                w.platform_tag
            );
        }
    }
    let manifest = AssetsManifest {
        name: &spec.wheel_name,
        version: version_pep440,
        base_url,
        wheels: map,
    };
    let mut json = serde_json::to_string_pretty(&manifest).context("serialize assets.json")?;
    json.push('\n');
    Ok(json)
}

fn write_targz(out_path: &Path, entries: &[(String, Vec<u8>)]) -> Result<()> {
    let file = File::create(out_path).with_context(|| format!("create {}", out_path.display()))?;
    let enc = flate2::write::GzEncoder::new(file, flate2::Compression::best());
    let mut builder = tar::Builder::new(enc);
    for (path, bytes) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        // Fixed mtime keeps the sdist byte-reproducible across builds.
        header.set_mtime(0);
        builder
            .append_data(&mut header, path, bytes.as_slice())
            .with_context(|| format!("tar append {path}"))?;
    }
    builder
        .into_inner()
        .context("finish tar")?
        .finish()
        .context("finish gzip")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn sample_spec(dir: &Path) -> Spec {
        Spec {
            wheel_name: "dbt-sa-cli".to_string(),
            pyproject_dir: dir.to_path_buf(),
            summary: Some("dbt fusion standalone analyzer CLI".to_string()),
            requires_python: Some(">=3.9".to_string()),
            classifiers: vec![],
            urls: vec![],
            authors: vec![],
            license: None,
            description: None,
            description_content_type: None,
        }
    }

    fn read_targz(path: &Path) -> BTreeMap<String, String> {
        let file = File::open(path).unwrap();
        let dec = flate2::read::GzDecoder::new(file);
        let mut ar = tar::Archive::new(dec);
        let mut out = BTreeMap::new();
        for entry in ar.entries().unwrap() {
            let mut entry = entry.unwrap();
            let name = entry.path().unwrap().to_string_lossy().into_owned();
            let mut body = String::new();
            entry.read_to_string(&mut body).unwrap();
            out.insert(name, body);
        }
        out
    }

    #[test]
    fn build_sdist_bundles_backend_and_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let spec = sample_spec(dir);
        let wheels = vec![
            WheelAsset {
                platform_tag: "manylinux_2_28_x86_64".to_string(),
                filename: "dbt_sa_cli-2.0.0a1-py3-none-manylinux_2_28_x86_64.whl".to_string(),
                sha256_hex: "aa".repeat(32),
            },
            WheelAsset {
                platform_tag: "macosx_11_0_arm64".to_string(),
                filename: "dbt_sa_cli-2.0.0a1-py3-none-macosx_11_0_arm64.whl".to_string(),
                sha256_hex: "bb".repeat(32),
            },
        ];

        let path =
            build_sdist(&spec, "2.0.0a1", &wheels, "https://example.com/dl/v2/", dir).unwrap();
        assert_eq!(
            path.file_name().unwrap().to_str().unwrap(),
            "dbt_sa_cli-2.0.0a1.tar.gz"
        );

        let files = read_targz(&path);
        for expected in [
            "dbt_sa_cli-2.0.0a1/pyproject.toml",
            "dbt_sa_cli-2.0.0a1/PKG-INFO",
            "dbt_sa_cli-2.0.0a1/_dbt_sa_build/__init__.py",
            "dbt_sa_cli-2.0.0a1/_dbt_sa_build/assets.json",
        ] {
            assert!(files.contains_key(expected), "missing {expected}");
        }

        let pyproject = &files["dbt_sa_cli-2.0.0a1/pyproject.toml"];
        assert!(pyproject.contains("build-backend = \"_dbt_sa_build\""));
        assert!(pyproject.contains("name = \"dbt-sa-cli\""));
        assert!(pyproject.contains("version = \"2.0.0a1\""));
        assert!(pyproject.contains("requires-python = \">=3.9\""));

        let backend = &files["dbt_sa_cli-2.0.0a1/_dbt_sa_build/__init__.py"];
        assert!(backend.contains("def build_wheel("));

        let assets: serde_json::Value =
            serde_json::from_str(&files["dbt_sa_cli-2.0.0a1/_dbt_sa_build/assets.json"]).unwrap();
        assert_eq!(assets["name"], "dbt-sa-cli");
        assert_eq!(assets["version"], "2.0.0a1");
        assert_eq!(assets["base_url"], "https://example.com/dl/v2/");
        assert_eq!(
            assets["wheels"]["manylinux_2_28_x86_64"]["filename"],
            "dbt_sa_cli-2.0.0a1-py3-none-manylinux_2_28_x86_64.whl"
        );
        assert_eq!(
            assets["wheels"]["macosx_11_0_arm64"]["sha256"],
            "bb".repeat(32)
        );
    }

    #[test]
    fn build_sdist_rejects_duplicate_platform_tag() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let spec = sample_spec(dir);
        // Both windows-msvc and windows-gnu map to win_amd64.
        let wheels = vec![
            WheelAsset {
                platform_tag: "win_amd64".to_string(),
                filename: "a.whl".to_string(),
                sha256_hex: "aa".repeat(32),
            },
            WheelAsset {
                platform_tag: "win_amd64".to_string(),
                filename: "b.whl".to_string(),
                sha256_hex: "bb".repeat(32),
            },
        ];
        let err = build_sdist(&spec, "2.0.0", &wheels, "https://example.com", dir).unwrap_err();
        assert!(err.to_string().contains("win_amd64"));
    }

    #[test]
    fn build_sdist_rejects_non_https_base_url() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let spec = sample_spec(dir);
        let wheels = vec![WheelAsset {
            platform_tag: "win_amd64".to_string(),
            filename: "a.whl".to_string(),
            sha256_hex: "aa".repeat(32),
        }];
        let err = build_sdist(&spec, "2.0.0", &wheels, "http://example.com", dir).unwrap_err();
        assert!(err.to_string().contains("https"));
    }
}
