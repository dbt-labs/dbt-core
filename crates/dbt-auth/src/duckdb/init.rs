//! Generates DuckDB initialization SQL from an [`AdapterConfig`].
//!
//! Produces statements in the same order as upstream dbt-duckdb:
//! 1. `INSTALL` + `LOAD` for each extension (including auto-injected `motherduck`)
//! 2. `CREATE OR REPLACE SECRET` for each secret
//! 3. `SET motherduck_token` (for MotherDuck paths, when resolved)
//! 4. `SET` for each setting
//! 5. `ATTACH IF NOT EXISTS` for each attachment
//!
//! FIXME: this module has nothing to do with authentication — it is DuckDB session
//! setup logic that ended up here because `dbt-auth` was the first crate that needed
//! it. `dbt-index` depends on `dbt-auth` solely to reach this function. It should
//! move to `dbt-adapter` or a small shared crate, but that requires untangling the
//! `dbt-index` → `dbt-adapter` dependency graph first.

use crate::AuthError;
use crate::config::{AdapterConfig, YmlValue};

// ---------------------------------------------------------------------------
// Parsed types
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Attachment {
    path: String,
    alias: Option<String>,
    #[serde(rename = "type")]
    db_type: Option<String>,
    #[serde(default)]
    read_only: bool,
}

enum DuckDbPath {
    Memory,
    Local,
    MotherDuck {
        /// Attach path with query parameters stripped.
        path: String,
        /// Database alias (from `database` config or derived from path).
        alias: String,
        /// Token, if resolved.
        token: Option<String>,
    },
}

impl DuckDbPath {
    fn resolve(config: &AdapterConfig) -> Self {
        let raw = config
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        if raw.is_empty() || raw == ":memory:" {
            return DuckDbPath::Memory;
        }

        if !is_motherduck_path(raw) {
            return DuckDbPath::Local;
        }

        let path = Self::attach_path(raw);

        let alias = {
            let from_config = config
                .get("database")
                .and_then(|v| v.as_str())
                .map(sanitize_identifier)
                .unwrap_or_default();
            if from_config.is_empty() {
                let derived = sanitize_identifier(&Self::database_name(raw));
                if derived.is_empty() {
                    "my_db".to_owned()
                } else {
                    derived
                }
            } else {
                from_config
            }
        };

        let token = Self::resolve_token(raw, config);

        DuckDbPath::MotherDuck { path, alias, token }
    }

    fn resolve_token(path: &str, config: &AdapterConfig) -> Option<String> {
        if let Some(token) = config
            .get("settings")
            .and_then(|v| match v {
                YmlValue::Mapping(map, _) => map.get("motherduck_token"),
                _ => None,
            })
            .and_then(|v| v.as_str())
        {
            if !token.is_empty() {
                return Some(token.to_owned());
            }
        }

        let from_path = path.split_once('?').and_then(|(_, query)| {
            query.split('&').find_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                (key == "motherduck_token" && !value.is_empty()).then(|| value.to_owned())
            })
        });
        from_path.or_else(|| std::env::var("MOTHERDUCK_TOKEN").ok())
    }

    /// Derive a database name from a MotherDuck path.
    fn database_name(path: &str) -> String {
        let stripped = if let Some(rest) = path.strip_prefix("motherduck:").or_else(|| {
            let lower = path.to_lowercase();
            if lower.starts_with("motherduck:") {
                Some(&path["motherduck:".len()..])
            } else {
                None
            }
        }) {
            rest
        } else if let Some(rest) = path.strip_prefix("md:").or_else(|| {
            let lower = path.to_lowercase();
            if lower.starts_with("md:") {
                Some(&path["md:".len()..])
            } else {
                None
            }
        }) {
            rest
        } else {
            path
        };

        let name = stripped.split('?').next().unwrap_or("");
        if name.is_empty() {
            "my_db".to_owned()
        } else {
            name.to_owned()
        }
    }

    /// Strip URL query parameters from a MotherDuck attach path.
    fn attach_path(path: &str) -> String {
        path.split_once('?')
            .map(|(base, _)| base.to_owned())
            .unwrap_or_else(|| path.to_owned())
    }
}

/// All parsed inputs needed to generate init SQL.
struct DuckDbInitInputs {
    path: DuckDbPath,
    extensions: Vec<String>,
    secrets: Vec<String>,
    // FIXME: settings is raw YAML key/value pairs with no validation — same problem
    // secrets had before. Should be replaced with a typed struct of known DuckDB
    // settings so unknown keys are rejected at parse time.
    settings: Vec<(String, YmlValue)>,
    attachments: Vec<Attachment>,
}

// ---------------------------------------------------------------------------
// Parse
// ---------------------------------------------------------------------------

fn read_init_inputs(config: &AdapterConfig) -> Result<DuckDbInitInputs, AuthError> {
    let path = DuckDbPath::resolve(config);

    let extensions = {
        match config.get("extensions") {
            None => vec![],
            Some(YmlValue::Sequence(seq, _)) => {
                let mut result = Vec::with_capacity(seq.len());
                for (i, item) in seq.iter().enumerate() {
                    match item.as_str() {
                        Some(s) => result.push(s.to_owned()),
                        None => {
                            return Err(AuthError::config(format!(
                                "extensions: item {i} must be a string, got {item:?}"
                            )));
                        }
                    }
                }
                result
            }
            Some(other) => {
                return Err(AuthError::config(format!(
                    "extensions: expected a sequence, got {other:?}"
                )));
            }
        }
    };

    let secrets = {
        match config.get("secrets") {
            None => vec![],
            Some(YmlValue::Sequence(seq, _)) => seq
                .iter()
                .enumerate()
                .filter_map(|(i, item)| render_secret_untyped(item, i))
                .collect(),
            Some(other) => {
                return Err(AuthError::config(format!(
                    "secrets: expected a sequence, got {other:?}"
                )));
            }
        }
    };

    let settings = {
        match config.get("settings") {
            None => vec![],
            Some(YmlValue::Mapping(map, _)) => {
                let mut result = Vec::with_capacity(map.len());
                for (k, v) in map.iter() {
                    let key = match k.as_str() {
                        Some(s) => s,
                        None => continue,
                    };
                    // motherduck_token is emitted separately as a SET statement
                    // only for MotherDuck paths; skip it from the general settings.
                    if key == "motherduck_token" {
                        continue;
                    }
                    result.push((key.to_owned(), v.clone()));
                }
                result
            }
            Some(other) => {
                return Err(AuthError::config(format!(
                    "settings: expected a mapping, got {other:?}"
                )));
            }
        }
    };

    let attachments = {
        match config.get("attach") {
            None => vec![],
            Some(YmlValue::Sequence(seq, _)) => {
                let mut result = Vec::with_capacity(seq.len());
                for (i, item) in seq.iter().enumerate() {
                    let attachment: Attachment = dbt_yaml::from_value(item.clone())
                        .map_err(|e| AuthError::config(format!("attach: item {i}: {e}")))?;
                    result.push(attachment);
                }
                result
            }
            Some(other) => {
                return Err(AuthError::config(format!(
                    "attach: expected a sequence, got {other:?}"
                )));
            }
        }
    };

    Ok(DuckDbInitInputs {
        path,
        extensions,
        secrets,
        settings,
        attachments,
    })
}

// ---------------------------------------------------------------------------
// Statement types
// ---------------------------------------------------------------------------

struct ExtensionStatements {
    names: Vec<String>,
}

impl ExtensionStatements {
    /// Build from an explicit list of extension names (local/memory paths).
    fn from_config(extensions: &[String]) -> Self {
        let names = extensions
            .iter()
            .map(|s| sanitize_identifier(s))
            .filter(|s| !s.is_empty())
            .collect();
        Self { names }
    }

    /// Build with auto-injected `motherduck` (MotherDuck paths).
    fn with_motherduck(extensions: &[String]) -> Self {
        let has_motherduck = extensions
            .iter()
            .any(|s| s.eq_ignore_ascii_case("motherduck"));

        let mut names: Vec<String> = if has_motherduck {
            vec![]
        } else {
            vec!["motherduck".to_owned()]
        };
        for ext in extensions {
            let sanitized = sanitize_identifier(ext);
            if !sanitized.is_empty() {
                names.push(sanitized);
            }
        }
        Self { names }
    }

    fn render(&self) -> Vec<String> {
        let mut out = Vec::with_capacity(self.names.len() * 2);
        for name in &self.names {
            out.push(format!("INSTALL {name}"));
            out.push(format!("LOAD {name}"));
        }
        out
    }
}

// FIXME: replace with typed Secret variants per issue #7834 (S3, GCS, R2, Azure, HuggingFace).
// Currently passes all unknown fields through as SQL params, same as the original main logic.
fn render_secret_untyped(item: &YmlValue, i: usize) -> Option<String> {
    let YmlValue::Mapping(map, _) = item else {
        return None;
    };
    let secret_type = sanitize_identifier(map.get("type").and_then(|v| v.as_str())?);
    let name = map
        .get("name")
        .and_then(|v| v.as_str())
        .map(sanitize_identifier)
        .unwrap_or_else(|| format!("__dbt_secret_{i}"));
    let persistent = map
        .get("persistent")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let persist_kw = if persistent { " PERSISTENT" } else { "" };

    let mut params = vec![format!("TYPE {secret_type}")];
    if let Some(provider) = map.get("provider").and_then(|v| v.as_str()) {
        params.push(format!("PROVIDER {}", sanitize_identifier(provider)));
    }
    if let Some(scope) = map.get("scope").and_then(|v| v.as_str()) {
        params.push(format!("SCOPE '{}'", escape_single_quotes(scope)));
    }
    const RESERVED: &[&str] = &["type", "name", "persistent", "provider", "scope"];
    for (k, v) in map.iter() {
        if let Some(key) = k.as_str().filter(|k| !RESERVED.contains(k)) {
            let key_upper = sanitize_identifier(key).to_uppercase();
            if !key_upper.is_empty() {
                params.push(format!("{key_upper} {}", yml_value_to_sql_literal(v)));
            }
        }
    }
    Some(format!(
        "CREATE OR REPLACE{persist_kw} SECRET {name} ({})",
        params.join(", ")
    ))
}

struct SecretStatements {
    secrets: Vec<String>,
}

impl SecretStatements {
    fn new(secrets: Vec<String>) -> Self {
        Self { secrets }
    }

    fn render(&self) -> Vec<String> {
        self.secrets.clone()
    }
}

struct TokenStatement {
    token: String,
}

impl TokenStatement {
    fn new(token: String) -> Self {
        Self { token }
    }

    fn render(&self) -> Vec<String> {
        vec![format!(
            "SET motherduck_token = '{}'",
            escape_single_quotes(&self.token)
        )]
    }
}

struct SettingStatements {
    settings: Vec<(String, YmlValue)>,
}

impl SettingStatements {
    fn new(settings: Vec<(String, YmlValue)>) -> Self {
        Self { settings }
    }

    fn render(&self) -> Vec<String> {
        let mut out = Vec::with_capacity(self.settings.len());
        for (key, value) in &self.settings {
            let k = sanitize_identifier(key);
            if k.is_empty() {
                continue;
            }
            out.push(format!("SET {k} = {}", yml_value_to_sql_literal(value)));
        }
        out
    }
}

struct MotherDuckAttach {
    path: String,
    alias: String,
}

impl MotherDuckAttach {
    fn new(path: String, alias: String) -> Self {
        Self { path, alias }
    }

    fn render(&self) -> Vec<String> {
        vec![
            format!(
                "ATTACH IF NOT EXISTS '{}' AS {}",
                escape_single_quotes(&self.path),
                self.alias,
            ),
            format!("USE {}", self.alias),
        ]
    }
}

struct AttachmentStatements {
    attachments: Vec<Attachment>,
}

impl AttachmentStatements {
    fn new(attachments: Vec<Attachment>) -> Self {
        Self { attachments }
    }

    fn render(&self) -> Vec<String> {
        let mut out = Vec::with_capacity(self.attachments.len());
        for attachment in &self.attachments {
            let path_escaped = escape_single_quotes(&attachment.path);
            let mut sql = format!("ATTACH IF NOT EXISTS '{path_escaped}'");

            if let Some(alias) = &attachment.alias {
                let alias = sanitize_identifier(alias);
                if !alias.is_empty() {
                    sql.push_str(&format!(" AS {alias}"));
                }
            }

            let db_type = attachment.db_type.as_deref();
            let read_only = attachment.read_only;

            if db_type.is_some() || read_only {
                let mut opts = Vec::new();
                if let Some(t) = db_type {
                    opts.push(format!("TYPE {}", sanitize_identifier(t)));
                }
                if read_only {
                    opts.push("READ_ONLY".to_owned());
                }
                sql.push_str(&format!(" ({})", opts.join(", ")));
            }

            out.push(sql);
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Top-level entry point
// ---------------------------------------------------------------------------

/// Generate DuckDB initialization SQL statements from the adapter config.
///
/// Returns an ordered list of SQL strings ready for sequential execution.
/// When the path is a MotherDuck connection (`md:` / `motherduck:`), the
/// `motherduck` extension is auto-installed/loaded and the token is injected.
pub fn generate_duckdb_init_sql(config: &AdapterConfig) -> Result<Vec<String>, AuthError> {
    let params = read_init_inputs(config)?;

    match params.path {
        DuckDbPath::MotherDuck { path, alias, token } => {
            let extensions = ExtensionStatements::with_motherduck(&params.extensions);
            let secrets = SecretStatements::new(params.secrets);
            let token_stmt = token.map(TokenStatement::new);
            let settings = SettingStatements::new(params.settings);
            let md_attach = MotherDuckAttach::new(path, alias);
            let attachments = AttachmentStatements::new(params.attachments);

            let mut out = Vec::new();
            out.extend(extensions.render());
            out.extend(secrets.render());
            if let Some(t) = token_stmt {
                out.extend(t.render());
            }
            out.extend(settings.render());
            out.extend(md_attach.render());
            out.extend(attachments.render());
            Ok(out)
        }

        DuckDbPath::Local | DuckDbPath::Memory => {
            let extensions = ExtensionStatements::from_config(&params.extensions);
            let secrets = SecretStatements::new(params.secrets);
            let settings = SettingStatements::new(params.settings);
            let attachments = AttachmentStatements::new(params.attachments);

            let mut out = Vec::new();
            out.extend(extensions.render());
            out.extend(secrets.render());
            out.extend(settings.render());
            out.extend(attachments.render());
            Ok(out)
        }
    }
}

// ---------------------------------------------------------------------------
// MotherDuck helpers (private — consumed by DuckDbPath::resolve)
// ---------------------------------------------------------------------------

/// Returns `true` if `path` is a MotherDuck connection string (`md:` or `motherduck:` prefix).
pub fn is_motherduck_path(path: &str) -> bool {
    let lower = path.to_lowercase();
    lower.starts_with("md:") || lower.starts_with("motherduck:")
}

// ---------------------------------------------------------------------------
// SQL literal helpers
// ---------------------------------------------------------------------------

/// Keep only ASCII alphanumeric and underscore characters (SQL injection prevention).
fn sanitize_identifier(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect()
}

/// Escape single quotes for SQL string literals (`'` → `''`).
fn escape_single_quotes(s: &str) -> String {
    s.replace('\'', "''")
}

/// Convert a [`YmlValue`] to a SQL literal.
///
/// - Strings → `'escaped'`
/// - Numbers / Bools → bare
/// - Null → `NULL`
/// - Sequences / Mappings → serialized as string
fn yml_value_to_sql_literal(v: &YmlValue) -> String {
    match v {
        YmlValue::String(s, _) => format!("'{}'", escape_single_quotes(s)),
        YmlValue::Number(n, _) => n.to_string(),
        YmlValue::Bool(b, _) => b.to_string(),
        YmlValue::Null(_) => "NULL".to_owned(),
        _ => {
            // Fallback: serialize as a quoted string.
            // All YmlValue variants are serializable, so this should never fail.
            let s = match dbt_yaml::to_string(v) {
                Ok(s) => s,
                Err(e) => {
                    debug_assert!(false, "YmlValue serialization failed: {e}");
                    return "NULL".to_owned();
                }
            };
            let s = s.trim_end_matches('\n');
            format!("'{}'", escape_single_quotes(s))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn config_from_yaml(yaml: &str) -> AdapterConfig {
        let value: YmlValue = dbt_yaml::from_str(yaml).unwrap();
        let mapping = match value {
            YmlValue::Mapping(m, _) => m,
            _ => panic!("expected mapping"),
        };
        AdapterConfig::new(mapping)
    }

    #[test]
    fn test_empty_config() {
        let config = AdapterConfig::default();
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert!(stmts.is_empty());
    }

    #[test]
    fn test_extensions() {
        let config = config_from_yaml(
            r#"
extensions:
  - httpfs
  - parquet
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(
            stmts,
            vec![
                "INSTALL httpfs",
                "LOAD httpfs",
                "INSTALL parquet",
                "LOAD parquet",
            ]
        );
    }

    #[test]
    fn test_settings_string_and_number() {
        let config = config_from_yaml(
            r#"
settings:
  memory_limit: "2GB"
  threads: 4
  enable_progress_bar: true
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 3);
        assert!(stmts.contains(&"SET memory_limit = '2GB'".to_string()));
        assert!(stmts.contains(&"SET threads = 4".to_string()));
        assert!(stmts.contains(&"SET enable_progress_bar = true".to_string()));
    }

    #[test]
    fn test_secret_with_name_and_provider() {
        let config = config_from_yaml(
            r#"
secrets:
  - type: s3
    name: my_s3_secret
    provider: credential_chain
    scope: "s3://my-bucket"
    region: us-east-1
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(sql.starts_with("CREATE OR REPLACE SECRET my_s3_secret ("));
        assert!(sql.contains("TYPE s3"));
        assert!(sql.contains("PROVIDER credential_chain"));
        assert!(sql.contains("SCOPE 's3://my-bucket'"));
        assert!(sql.contains("REGION 'us-east-1'"));
    }

    #[test]
    fn test_secret_without_name() {
        let config = config_from_yaml(
            r#"
secrets:
  - type: s3
    key_id: fake_key
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(sql.contains("SECRET __dbt_secret_0"));
        assert!(sql.contains("TYPE s3"));
        assert!(sql.contains("KEY_ID 'fake_key'"));
    }

    #[test]
    fn test_persistent_secret() {
        let config = config_from_yaml(
            r#"
secrets:
  - type: s3
    persistent: true
    key_id: my_key
    secret: my_secret
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(sql.starts_with("CREATE OR REPLACE PERSISTENT SECRET __dbt_secret_0 ("));
        assert!(sql.contains("TYPE s3"));
        assert!(sql.contains("KEY_ID 'my_key'"));
        assert!(sql.contains("SECRET 'my_secret'"));
    }

    #[test]
    fn test_secret_sql_injection_in_scope() {
        let config = config_from_yaml(
            r#"
secrets:
  - type: s3
    scope: "s3://bucket'; DROP TABLE users; --"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        // Single quotes should be escaped
        assert!(sql.contains("SCOPE 's3://bucket''; DROP TABLE users; --'"));
    }

    #[test]
    fn test_attachment_minimal() {
        let config = config_from_yaml(
            r#"
attach:
  - path: ":memory:"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts, vec!["ATTACH IF NOT EXISTS ':memory:'"]);
    }

    #[test]
    fn test_attachment_all_options() {
        let config = config_from_yaml(
            r#"
attach:
  - path: /data/external.db
    alias: ext
    type: duckdb
    read_only: true
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(
            stmts[0],
            "ATTACH IF NOT EXISTS '/data/external.db' AS ext (TYPE duckdb, READ_ONLY)"
        );
    }

    #[test]
    fn test_attachment_path_escaping() {
        let config = config_from_yaml(
            r#"
attach:
  - path: "/data/it's a db.duckdb"
    alias: weird
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(
            stmts[0],
            "ATTACH IF NOT EXISTS '/data/it''s a db.duckdb' AS weird"
        );
    }

    #[test]
    fn test_ordering_extensions_secrets_settings_attachments() {
        let config = config_from_yaml(
            r#"
extensions:
  - httpfs
settings:
  memory_limit: "2GB"
secrets:
  - type: s3
    key_id: k
attach:
  - path: ":memory:"
    alias: scratch
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        // Order: extensions, secrets, settings, attachments
        assert_eq!(stmts[0], "INSTALL httpfs");
        assert_eq!(stmts[1], "LOAD httpfs");
        assert!(stmts[2].starts_with("CREATE OR REPLACE"));
        assert!(stmts[3].starts_with("SET memory_limit"));
        assert!(stmts[4].starts_with("ATTACH IF NOT EXISTS"));
    }

    #[test]
    fn test_full_config() {
        let config = config_from_yaml(
            r#"
path: /tmp/test.db
extensions:
  - httpfs
  - parquet
settings:
  memory_limit: "2GB"
secrets:
  - type: s3
    key_id: fake_key
    secret: fake_secret
    region: us-east-1
attach:
  - path: ":memory:"
    alias: scratch
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        // 2 extensions * 2 stmts + 1 secret + 1 setting + 1 attachment = 7
        assert_eq!(stmts.len(), 7);
        assert_eq!(stmts[0], "INSTALL httpfs");
        assert_eq!(stmts[1], "LOAD httpfs");
        assert_eq!(stmts[2], "INSTALL parquet");
        assert_eq!(stmts[3], "LOAD parquet");
        assert!(stmts[4].contains("TYPE s3"));
        assert!(stmts[5].starts_with("SET memory_limit"));
        assert!(stmts[6].starts_with("ATTACH IF NOT EXISTS"));
    }

    #[test]
    fn test_sanitize_identifier() {
        assert_eq!(sanitize_identifier("normal_name"), "normal_name");
        assert_eq!(sanitize_identifier("has spaces"), "hasspaces");
        assert_eq!(sanitize_identifier("has;semicolons"), "hassemicolons");
        assert_eq!(sanitize_identifier("DROP TABLE--"), "DROPTABLE");
    }

    #[test]
    fn test_escape_single_quotes() {
        assert_eq!(escape_single_quotes("no quotes"), "no quotes");
        assert_eq!(escape_single_quotes("it's"), "it''s");
        assert_eq!(escape_single_quotes("a''b"), "a''''b");
    }

    #[test]
    fn test_empty_extension_name_skipped() {
        let config = config_from_yaml(
            r#"
extensions:
  - ""
  - httpfs
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts, vec!["INSTALL httpfs", "LOAD httpfs"]);
    }

    #[test]
    fn test_extension_sql_injection_sanitized() {
        let config = config_from_yaml(
            r#"
extensions:
  - "httpfs; DROP TABLE users"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        // Semicolons and spaces stripped by sanitize_identifier
        assert_eq!(stmts[0], "INSTALL httpfsDROPTABLEusers");
        assert_eq!(stmts[1], "LOAD httpfsDROPTABLEusers");
    }

    #[test]
    fn test_secret_type_only() {
        let config = config_from_yaml(
            r#"
secrets:
  - type: s3
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(
            stmts[0],
            "CREATE OR REPLACE SECRET __dbt_secret_0 (TYPE s3)"
        );
    }

    #[test]
    fn test_multiple_attachments_ordering() {
        let config = config_from_yaml(
            r#"
attach:
  - path: /data/first.db
    alias: first
  - path: /data/second.db
    alias: second
  - path: ":memory:"
    alias: scratch
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0], "ATTACH IF NOT EXISTS '/data/first.db' AS first");
        assert_eq!(stmts[1], "ATTACH IF NOT EXISTS '/data/second.db' AS second");
        assert_eq!(stmts[2], "ATTACH IF NOT EXISTS ':memory:' AS scratch");
    }

    #[test]
    fn test_setting_value_with_single_quotes() {
        let config = config_from_yaml(
            r#"
settings:
  custom_setting: "it's a value"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "SET custom_setting = 'it''s a value'");
    }

    #[test]
    fn test_settings_only_no_extensions() {
        let config = config_from_yaml(
            r#"
settings:
  memory_limit: "4GB"
  threads: 8
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(stmts.contains(&"SET memory_limit = '4GB'".to_owned()));
        assert!(stmts.contains(&"SET threads = 8".to_owned()));
    }

    // -----------------------------------------------------------------------
    // MotherDuck helpers
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_motherduck_path() {
        assert!(is_motherduck_path("md:"));
        assert!(is_motherduck_path("md:my_db"));
        assert!(is_motherduck_path("MD:my_db"));
        assert!(is_motherduck_path("motherduck:"));
        assert!(is_motherduck_path("MotherDuck:my_db"));
        assert!(!is_motherduck_path("/tmp/local.duckdb"));
        assert!(!is_motherduck_path(":memory:"));
    }

    #[test]
    fn test_database_name() {
        assert_eq!(DuckDbPath::database_name("md:my_db"), "my_db");
        assert_eq!(DuckDbPath::database_name("md:"), "my_db");
        assert_eq!(DuckDbPath::database_name("motherduck:sales"), "sales");
        assert_eq!(
            DuckDbPath::database_name("md:my_db?motherduck_token=tok123"),
            "my_db"
        );
    }

    #[test]
    fn test_attach_path_strips_query() {
        assert_eq!(
            DuckDbPath::attach_path("md:my_db?motherduck_token=tok"),
            "md:my_db"
        );
        assert_eq!(
            DuckDbPath::attach_path("motherduck:sales?user=1"),
            "motherduck:sales"
        );
        assert_eq!(DuckDbPath::attach_path("md:plain"), "md:plain");
    }

    #[test]
    fn test_motherduck_auto_extension() {
        let config = config_from_yaml(
            r#"
path: "md:my_db"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts[0], "INSTALL motherduck");
        assert_eq!(stmts[1], "LOAD motherduck");
    }

    #[test]
    fn test_motherduck_auto_extension_not_duplicated() {
        let config = config_from_yaml(
            r#"
path: "md:my_db"
extensions:
  - motherduck
  - httpfs
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        // Should not have duplicate INSTALL/LOAD motherduck
        let install_count = stmts.iter().filter(|s| *s == "INSTALL motherduck").count();
        assert_eq!(install_count, 1);
    }

    #[test]
    fn test_motherduck_path_is_auto_attached() {
        let config = config_from_yaml(
            r#"
path: "md:stocks_dev"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert!(stmts.contains(&"ATTACH IF NOT EXISTS 'md:stocks_dev' AS stocks_dev".to_owned()));
        assert!(stmts.contains(&"USE stocks_dev".to_owned()));
    }

    #[test]
    fn test_motherduck_path_uses_explicit_database_alias() {
        let config = config_from_yaml(
            r#"
path: "md:stocks_dev"
database: "analytics"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert!(stmts.contains(&"ATTACH IF NOT EXISTS 'md:stocks_dev' AS analytics".to_owned()));
        assert!(stmts.contains(&"USE analytics".to_owned()));
    }

    #[test]
    fn test_motherduck_token_set_in_init_sql() {
        let config = config_from_yaml(
            r#"
path: "md:my_db"
settings:
  motherduck_token: "my_secret_token"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert!(
            stmts.contains(&"SET motherduck_token = 'my_secret_token'".to_owned()),
            "motherduck_token should be emitted in init SQL for MotherDuck paths"
        );
    }

    #[test]
    fn test_motherduck_token_settings_wins_in_init_sql() {
        let config = config_from_yaml(
            r#"
path: "md:my_db?motherduck_token=tok_from_path"
settings:
  motherduck_token: "tok_from_settings"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert!(stmts.contains(&"SET motherduck_token = 'tok_from_settings'".to_owned()));
    }

    #[test]
    fn test_motherduck_token_set_before_attach() {
        let config = config_from_yaml(
            r#"
path: "md:my_db?motherduck_token=tok_from_path"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        let token_idx = stmts
            .iter()
            .position(|s| s == "SET motherduck_token = 'tok_from_path'")
            .expect("expected motherduck token SET statement");
        let attach_idx = stmts
            .iter()
            .position(|s| s == "ATTACH IF NOT EXISTS 'md:my_db' AS my_db")
            .expect("expected ATTACH statement");
        let use_idx = stmts
            .iter()
            .position(|s| s == "USE my_db")
            .expect("expected USE statement");
        assert!(token_idx < attach_idx);
        assert!(attach_idx < use_idx);
    }

    #[test]
    fn test_motherduck_token_not_set_for_local_path() {
        let config = config_from_yaml(
            r#"
path: "/tmp/local.duckdb"
settings:
  motherduck_token: "my_secret_token"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert!(
            !stmts.iter().any(|s| s.starts_with("SET motherduck_token")),
            "motherduck_token should not be emitted for local DuckDB paths"
        );
    }

    #[test]
    fn test_resolve_motherduck_token_from_settings() {
        let config = config_from_yaml(
            r#"
path: "md:my_db"
settings:
  motherduck_token: "my_secret_token"
"#,
        );
        assert_eq!(
            DuckDbPath::resolve_token(
                config
                    .get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default(),
                &config
            ),
            Some("my_secret_token".to_owned())
        );
    }

    #[test]
    fn test_resolve_motherduck_token_from_path_query() {
        let config = config_from_yaml(
            r#"
path: "md:my_db?motherduck_token=tok_from_path"
"#,
        );
        assert_eq!(
            DuckDbPath::resolve_token(
                config
                    .get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default(),
                &config
            ),
            Some("tok_from_path".to_owned())
        );
    }

    #[test]
    fn test_resolve_motherduck_token_settings_wins() {
        // When token is in both settings and path, settings wins
        let config = config_from_yaml(
            r#"
path: "md:my_db?motherduck_token=tok_from_path"
settings:
  motherduck_token: "tok_from_settings"
"#,
        );
        assert_eq!(
            DuckDbPath::resolve_token(
                config
                    .get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default(),
                &config
            ),
            Some("tok_from_settings".to_owned())
        );
    }

    // -----------------------------------------------------------------------
    // yml_value_to_sql_literal untested paths
    // -----------------------------------------------------------------------

    #[test]
    fn test_null_setting_value_emits_null_literal() {
        let config = config_from_yaml(
            r#"
settings:
  my_null: ~
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts, vec!["SET my_null = NULL"]);
    }

    #[test]
    fn test_sequence_setting_value_uses_fallback_literal() {
        // Exercises the Sequence/Mapping fallback path in yml_value_to_sql_literal.
        let config = config_from_yaml(
            r#"
settings:
  my_list:
    - foo
    - bar
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 1);
        // Should be a quoted string (not crash, not NULL)
        assert!(
            stmts[0].starts_with("SET my_list = '"),
            "expected quoted fallback literal: {}",
            stmts[0]
        );
    }

    #[test]
    fn test_mapping_setting_value_uses_fallback_literal() {
        let config = config_from_yaml(
            r#"
settings:
  my_map:
    key: value
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(
            stmts[0].starts_with("SET my_map = '"),
            "expected quoted fallback literal: {}",
            stmts[0]
        );
    }

    // -----------------------------------------------------------------------
    // Empty token in settings falls through to other sources
    // -----------------------------------------------------------------------

    #[test]
    fn test_empty_settings_token_falls_through_to_path_query() {
        let config = config_from_yaml(
            r#"
path: "md:my_db?motherduck_token=tok_from_path"
settings:
  motherduck_token: ""
"#,
        );
        // Empty settings token should be skipped; path query should win
        assert_eq!(
            DuckDbPath::resolve_token(
                config
                    .get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default(),
                &config
            ),
            Some("tok_from_path".to_owned())
        );
    }

    #[test]
    fn test_empty_settings_token_with_no_other_source_returns_none() {
        let config = config_from_yaml(
            r#"
path: "md:my_db"
settings:
  motherduck_token: ""
"#,
        );
        assert_eq!(
            DuckDbPath::resolve_token(
                config
                    .get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default(),
                &config
            ),
            None
        );
    }

    // -----------------------------------------------------------------------
    // Secret edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_non_mapping_secret_item_is_skipped() {
        // A non-mapping secret item now causes an error in parse()
        let config = config_from_yaml(
            r#"
secrets:
  - "not a mapping"
  - type: s3
    key_id: real_key
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("KEY_ID 'real_key'"));
    }

    #[test]
    fn test_secret_missing_type_is_skipped() {
        let config = config_from_yaml(
            r#"
secrets:
  - key_id: some_key
"#,
        );
        assert!(generate_duckdb_init_sql(&config).unwrap().is_empty());
    }

    // -----------------------------------------------------------------------
    // Attachment edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_non_mapping_attach_item_is_skipped() {
        // A non-mapping attach item now causes an error in parse()
        let config = config_from_yaml(
            r#"
attach:
  - "not a mapping"
  - path: /data/real.db
    alias: real
"#,
        );
        assert!(generate_duckdb_init_sql(&config).is_err());
    }

    #[test]
    fn test_attach_missing_path_is_skipped() {
        // Missing `path` is now a hard error from serde
        let config = config_from_yaml(
            r#"
attach:
  - alias: scratch
"#,
        );
        assert!(generate_duckdb_init_sql(&config).is_err());
    }

    #[test]
    fn test_attach_read_only_false_no_type_produces_no_parens() {
        let config = config_from_yaml(
            r#"
attach:
  - path: /data/real.db
    read_only: false
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "ATTACH IF NOT EXISTS '/data/real.db'");
    }

    // -----------------------------------------------------------------------
    // Malformed top-level shapes
    // -----------------------------------------------------------------------

    #[test]
    fn test_extensions_as_scalar_produces_no_statements() {
        // extensions as scalar is now a hard error
        let config = config_from_yaml(
            r#"
extensions: httpfs
"#,
        );
        assert!(generate_duckdb_init_sql(&config).is_err());
    }

    #[test]
    fn test_settings_as_sequence_produces_no_statements() {
        // settings as sequence is now a hard error
        let config = config_from_yaml(
            r#"
settings:
  - foo
  - bar
"#,
        );
        assert!(generate_duckdb_init_sql(&config).is_err());
    }

    #[test]
    fn test_secrets_as_scalar_produces_no_statements() {
        // secrets as scalar is now a hard error
        let config = config_from_yaml(
            r#"
secrets: my_secret
"#,
        );
        assert!(generate_duckdb_init_sql(&config).is_err());
    }

    // -----------------------------------------------------------------------
    // MotherDuck alias edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_motherduck_database_alias_sanitizes_to_empty_falls_back_to_my_db() {
        let config = config_from_yaml(
            r#"
path: "md:my_db"
database: "!@#$%"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert!(
            stmts.contains(&"ATTACH IF NOT EXISTS 'md:my_db' AS my_db".to_owned()),
            "expected fallback alias 'my_db': {stmts:?}"
        );
        assert!(stmts.contains(&"USE my_db".to_owned()));
    }

    #[test]
    fn test_bare_md_path_attach_alias_is_my_db() {
        let config = config_from_yaml(
            r#"
path: "md:"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config).unwrap();
        assert!(
            stmts.contains(&"ATTACH IF NOT EXISTS 'md:' AS my_db".to_owned()),
            "expected alias 'my_db' for bare md: path: {stmts:?}"
        );
        assert!(stmts.contains(&"USE my_db".to_owned()));
    }
}
