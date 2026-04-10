use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::OnceLock;

use minijinja::Environment;
use minijinja::listener::RenderingEventListener;
use regex::Regex;
use serde::Serialize;

use crate::error::{ProfileError, Result};
use crate::jinja::{ProfileContext, profile_environment};

// ---------------------------------------------------------------------------
// ProfileEnvironment — the shared Jinja env + context
// ---------------------------------------------------------------------------

/// The Jinja environment and rendering context needed for profile resolution.
/// Self-contained — no dependency on `dbt-jinja-utils` or `dbt-common`.
pub struct ProfileEnvironment {
    pub env: Environment<'static>,
    pub ctx: ProfileContext,
}

impl ProfileEnvironment {
    /// Build the profile-resolution environment.
    ///
    /// `vars` are the CLI `--vars` values, exposed as `var(...)` in Jinja.
    /// `env_var(...)` reads from `std::env` (real values, no secret placeholders).
    pub fn new(vars: BTreeMap<String, dbt_yaml::Value>) -> Self {
        Self {
            env: profile_environment(),
            ctx: ProfileContext::new(vars),
        }
    }
}

// ---------------------------------------------------------------------------
// ResolveArgs / ResolvedProfile — public types
// ---------------------------------------------------------------------------

/// Arguments for resolving a dbt profile.
#[derive(Debug, Clone, Default)]
pub struct ResolveArgs {
    /// Override the profiles directory (equivalent to `--profiles-dir`).
    /// Searched first; if unset falls back to `project_dir` then `~/.dbt/`.
    pub profiles_dir: Option<PathBuf>,

    /// Path to the dbt project directory. Used to:
    /// 1. Find a `profiles.yml` in the project root (second search location).
    /// 2. Read `dbt_project.yml` for the default profile name (standalone mode only).
    pub project_dir: Option<PathBuf>,

    /// Explicit profile name (equivalent to `--profile`).
    /// If unset in standalone mode, read from `dbt_project.yml`.
    pub profile: Option<String>,

    /// Explicit target name (equivalent to `--target` / `DBT_TARGET`).
    /// If unset, uses the profile's `target:` field, defaulting to `"default"`.
    pub target: Option<String>,

    /// Variables to inject into the Jinja context as `var(...)`.
    /// Equivalent to `--vars '{"key": "value"}'`.
    pub vars: BTreeMap<String, dbt_yaml::Value>,
}

/// A fully-resolved dbt profile ready for use.
#[derive(Debug, Clone)]
pub struct ResolvedProfile {
    /// The profile name that was resolved.
    pub profile_name: String,
    /// The target name that was selected.
    pub target_name: String,
    /// The adapter type string (e.g. `"snowflake"`, `"postgres"`, `"duckdb"`).
    pub adapter_type: String,
    /// The rendered target output as a YAML mapping — the source of truth for
    /// connection configuration. Flows directly to `AdapterConfig` / `dbt-auth`.
    pub credentials: dbt_yaml::Mapping,
    /// Path to the `profiles.yml` file that was loaded.
    pub profile_path: PathBuf,
}

impl ResolvedProfile {
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.credentials.get(key).and_then(|v| v.as_str())
    }

    pub fn database(&self) -> Option<&str> {
        self.get_str("database")
            .or_else(|| self.get_str("dbname"))
            .or_else(|| self.get_str("project"))
    }

    pub fn schema(&self) -> Option<&str> {
        self.get_str("schema").or_else(|| self.get_str("dataset"))
    }

    pub fn credentials_json(&self) -> std::result::Result<String, serde_json::Error> {
        let json_map: BTreeMap<String, serde_json::Value> = self
            .credentials
            .iter()
            .filter_map(|(k, v)| {
                let key = k.as_str()?.to_owned();
                let val = yaml_value_to_json(v);
                Some((key, val))
            })
            .collect();
        serde_json::to_string(&json_map)
    }
}

// ---------------------------------------------------------------------------
// High-level: resolve() — self-contained entrypoint
// ---------------------------------------------------------------------------

/// Resolve a dbt profile from `profiles.yml` in one call.
///
/// Creates its own `ProfileEnvironment` internally. For callers that need
/// to share the environment (e.g. `dbt-loader` rendering `dbt_project.yml`
/// with the same env), use the building blocks directly:
/// [`ProfileEnvironment::new`], [`find_profiles_path`], [`resolve_target`],
/// [`render_target`].
pub fn resolve(args: &ResolveArgs) -> Result<ResolvedProfile> {
    let penv = ProfileEnvironment::new(args.vars.clone());
    let profile_path =
        find_profiles_path(args.profiles_dir.as_deref(), args.project_dir.as_deref())?;
    let profile_name = resolve_profile_name(args)?;

    resolve_with_env(&penv, &profile_path, &profile_name, args.target.as_deref())
}

/// Resolve a profile using an externally-provided [`ProfileEnvironment`].
pub fn resolve_with_env(
    penv: &ProfileEnvironment,
    profile_path: &Path,
    profile_name: &str,
    target_override: Option<&str>,
) -> Result<ResolvedProfile> {
    let raw_yaml = std::fs::read_to_string(profile_path)?;
    let sanitized = sanitize_yml(&raw_yaml);
    let mut doc: dbt_yaml::Value =
        dbt_yaml::from_str(sanitized).map_err(|e| ProfileError::Yaml {
            path: profile_path.to_path_buf(),
            source: e,
        })?;
    doc.apply_merge().map_err(|e| ProfileError::Yaml {
        path: profile_path.to_path_buf(),
        source: e,
    })?;

    // dbt-core allows a top-level `config` key (for `send_anonymous_usage_stats`).
    // We reject it to match DbtProfilesIntermediate validation in dbt-schemas.
    if doc.get("config").is_some() {
        return Err(ProfileError::Other(
            "Unexpected 'config' key in profiles.yml".to_string(),
        ));
    }

    let profile_val = doc
        .get(profile_name)
        .ok_or_else(|| ProfileError::ProfileMissing {
            profile: profile_name.to_owned(),
            path: profile_path.to_path_buf(),
        })?;

    let target_name = resolve_target(target_override, profile_val, penv)?;

    let outputs = profile_val
        .get("outputs")
        .ok_or_else(|| ProfileError::NoOutputs {
            profile: profile_name.to_owned(),
        })?;

    let target_raw = outputs
        .get(&target_name)
        .ok_or_else(|| ProfileError::TargetMissing {
            profile: profile_name.to_owned(),
            target: target_name.clone(),
        })?;

    let credentials = render_target(target_raw, penv)?;

    let adapter_type = credentials
        .get("type")
        .and_then(|v| v.as_str())
        .map(|s| s.to_owned())
        .ok_or(ProfileError::NoAdapterType)?;

    Ok(ResolvedProfile {
        profile_name: profile_name.to_owned(),
        target_name,
        adapter_type,
        credentials,
        profile_path: profile_path.to_path_buf(),
    })
}

// ---------------------------------------------------------------------------
// Building blocks — exposed for dbt-loader composition
// ---------------------------------------------------------------------------

const PROFILES_YML: &str = "profiles.yml";

/// Find the `profiles.yml` file by searching standard locations.
///
/// When `profiles_dir` is set (e.g. from `--profiles-dir`), only that directory
/// is searched — no fallback to project_dir or ~/.dbt. This matches dbt semantics
/// where an explicit `--profiles-dir` that lacks profiles.yml must error.
pub fn find_profiles_path(
    profiles_dir: Option<&Path>,
    project_dir: Option<&Path>,
) -> Result<PathBuf> {
    let mut searched = Vec::new();

    if let Some(dir) = profiles_dir {
        let p = dir.join(PROFILES_YML);
        if p.exists() {
            return Ok(p);
        }
        searched.push(p);
        // When profiles_dir is explicitly set, do NOT fall back — error instead
        return Err(ProfileError::NotFound {
            searched,
            explicit_profiles_dir: true,
        });
    }

    if let Some(dir) = project_dir {
        let p = dir.join(PROFILES_YML);
        if p.exists() {
            return Ok(p);
        }
        searched.push(p);
    }

    if let Some(home) = dirs::home_dir() {
        let p = home.join(".dbt").join(PROFILES_YML);
        if p.exists() {
            return Ok(p);
        }
        searched.push(p);
    }

    Err(ProfileError::NotFound {
        searched,
        explicit_profiles_dir: false,
    })
}

/// Resolve the target name from overrides, `DBT_TARGET`, or the profile's
/// `target:` field (which may itself contain Jinja).
pub fn resolve_target(
    target_override: Option<&str>,
    profile_val: &dbt_yaml::Value,
    penv: &ProfileEnvironment,
) -> Result<String> {
    if let Some(t) = target_override {
        return Ok(t.to_owned());
    }

    if let Some(t) = std::env::var("DBT_TARGET").ok().filter(|s| !s.is_empty()) {
        return Ok(t);
    }

    if let Some(raw_str) = profile_val.get("target").and_then(|v| v.as_str()) {
        if raw_str.contains("{{") || raw_str.contains("{%") {
            let rendered = penv
                .env
                .render_str(raw_str, &penv.ctx, &[])
                .map_err(ProfileError::Jinja)?;
            return Ok(rendered);
        }
        return Ok(raw_str.to_owned());
    }

    Ok("default".to_owned())
}

/// Render a profile target output mapping through Jinja, returning the
/// resolved credentials as a loose `dbt_yaml::Mapping`.
pub fn render_target(
    target_val: &dbt_yaml::Value,
    penv: &ProfileEnvironment,
) -> Result<dbt_yaml::Mapping> {
    let rendered = render_value_recursive(&penv.env, &penv.ctx, target_val)?;
    match rendered {
        dbt_yaml::Value::Mapping(m, _) => Ok(m),
        _ => Err(ProfileError::Other(
            "rendered target output is not a mapping".to_owned(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Profile name resolution (private — standalone mode only)
// ---------------------------------------------------------------------------

fn resolve_profile_name(args: &ResolveArgs) -> Result<String> {
    if let Some(name) = &args.profile {
        return Ok(name.clone());
    }

    if let Some(name) = args
        .project_dir
        .as_ref()
        .and_then(|d| read_profile_from_project(d))
    {
        return Ok(name);
    }

    Err(ProfileError::NoProfileName)
}

fn read_profile_from_project(project_dir: &Path) -> Option<String> {
    let path = project_dir.join("dbt_project.yml");
    let raw = std::fs::read_to_string(path).ok()?;
    let doc: dbt_yaml::Value = dbt_yaml::from_str(&raw).ok()?;
    doc.get("profile")?.as_str().map(String::from)
}

// ---------------------------------------------------------------------------
// Recursive Jinja rendering over YAML values
// ---------------------------------------------------------------------------

fn render_value_recursive<S: Serialize>(
    env: &Environment<'_>,
    ctx: &S,
    value: &dbt_yaml::Value,
) -> Result<dbt_yaml::Value> {
    let listeners: &[Rc<dyn RenderingEventListener>] = &[];

    match value {
        dbt_yaml::Value::String(s, span) => {
            let has_jinja = s.contains("{{") || s.contains("{%");
            let rendered = if has_jinja {
                env.render_str(s, ctx, listeners)
                    .map_err(ProfileError::Jinja)?
            } else {
                s.clone()
            };
            let resolved = render_secrets(&rendered)?;
            if !has_jinja && resolved == rendered {
                Ok(value.clone())
            } else {
                match dbt_yaml::from_str::<dbt_yaml::Value>(&resolved) {
                    Ok(parsed) => match &parsed {
                        dbt_yaml::Value::String(_, _) => {
                            Ok(dbt_yaml::Value::String(resolved, span.clone()))
                        }
                        _ => Ok(parsed),
                    },
                    Err(_) => Ok(dbt_yaml::Value::String(resolved, span.clone())),
                }
            }
        }
        dbt_yaml::Value::Mapping(map, span) => {
            let mut new_map = dbt_yaml::Mapping::new();
            for (k, v) in map.iter() {
                new_map.insert(k.clone(), render_value_recursive(env, ctx, v)?);
            }
            Ok(dbt_yaml::Value::Mapping(new_map, span.clone()))
        }
        dbt_yaml::Value::Sequence(seq, span) => {
            let rendered: std::result::Result<Vec<_>, _> = seq
                .iter()
                .map(|v| render_value_recursive(env, ctx, v))
                .collect();
            Ok(dbt_yaml::Value::Sequence(rendered?, span.clone()))
        }
        _ => Ok(value.clone()),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve `DBT_ENV_SECRET_*` sentinel placeholders inserted by the Jinja
/// `env_var` function (when `placeholder_on_secret_access = true`) to their
/// real environment variable values.
///
/// Mirrors `dbt_jinja_utils::phases::load::secret_renderer::render_secrets`
/// but is duplicated here to keep `dbt-profile` free of `dbt-jinja-utils`.
fn render_secrets(s: &str) -> Result<String> {
    use dbt_jinja_vars::{SECRET_ENV_VAR_PREFIX, SECRET_PLACEHOLDER};

    if !s.contains(SECRET_ENV_VAR_PREFIX) {
        return Ok(s.to_owned());
    }

    static RE: OnceLock<Regex> = OnceLock::new();
    let pattern = SECRET_PLACEHOLDER
        .replace("{}", &format!("({SECRET_ENV_VAR_PREFIX}(.*))"))
        .replace("$", r"\$");
    let re = RE.get_or_init(|| Regex::new(&pattern).expect("valid secret placeholder regex"));

    let mut result = s.to_owned();
    for caps in re.captures_iter(s) {
        let var_name = &caps[1];
        let full_match = &caps[0];
        match std::env::var(var_name) {
            Ok(value) => result = result.replace(full_match, &value),
            Err(_) => {
                return Err(ProfileError::Other(format!(
                    "Secret environment variable '{var_name}' not found"
                )));
            }
        }
    }
    Ok(result)
}

// FIXME(jason): Duplicated from dbt_jinja_utils::serde::dbt_sanitize_yml because
// IoArgs poisons that module's public API.
fn sanitize_yml(input: &str) -> &str {
    // Remove any UTF-8 BOM from the beginning of the input string -- the YAML
    // parser confuses it with a document separator.
    let input = input.strip_prefix('\u{feff}').unwrap_or(input);

    // Trim trailing whitespace.
    let input = input.trim_end();

    // If the first non-empty line has leading whitespace, trim leading
    // whitespace as well.
    if let Some(first_non_empty_line) = input.lines().find(|line| !line.trim().is_empty())
        && first_non_empty_line
            .chars()
            .next()
            .map(|c| c.is_whitespace())
            .unwrap_or(false)
    {
        input.trim_start()
    } else {
        input
    }
}

fn yaml_value_to_json(v: &dbt_yaml::Value) -> serde_json::Value {
    match v {
        dbt_yaml::Value::Null(_) => serde_json::Value::Null,
        dbt_yaml::Value::Bool(b, _) => serde_json::Value::Bool(*b),
        dbt_yaml::Value::Number(n, _) => {
            if let Some(i) = n.as_i64() {
                serde_json::Value::Number(i.into())
            } else if let Some(u) = n.as_u64() {
                serde_json::Value::Number(u.into())
            } else if let Some(f) = n.as_f64() {
                serde_json::json!(f)
            } else {
                serde_json::Value::String(n.to_string())
            }
        }
        dbt_yaml::Value::String(s, _) => serde_json::Value::String(s.clone()),
        dbt_yaml::Value::Sequence(seq, _) => {
            serde_json::Value::Array(seq.iter().map(yaml_value_to_json).collect())
        }
        dbt_yaml::Value::Mapping(map, _) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .filter_map(|(k, v)| Some((k.as_str()?.to_owned(), yaml_value_to_json(v))))
                .collect();
            serde_json::Value::Object(obj)
        }
        dbt_yaml::Value::Tagged(tagged, _) => yaml_value_to_json(&tagged.value),
    }
}
