//! Generates DuckDB initialization SQL from an [`AdapterConfig`].
//!
//! Produces statements in the same order as upstream dbt-duckdb:
//! 1. `INSTALL` + `LOAD` for each extension (including auto-injected `motherduck`)
//! 2. `CREATE OR REPLACE SECRET` for each secret
//! 3. `SET motherduck_token` (for MotherDuck paths, when resolved)
//! 4. `SET` for each setting
//! 5. `ATTACH IF NOT EXISTS` for each attachment

use crate::config::{AdapterConfig, YmlValue};

// ---------------------------------------------------------------------------
// MotherDuck helpers
// ---------------------------------------------------------------------------

/// Returns `true` if `path` is a MotherDuck connection string (`md:` or `motherduck:` prefix).
pub fn is_motherduck_path(path: &str) -> bool {
    let lower = path.to_lowercase();
    lower.starts_with("md:") || lower.starts_with("motherduck:")
}

/// Derive a database name from a MotherDuck path.
///
/// - `md:my_db` → `"my_db"`
/// - `md:my_db?motherduck_token=tok` → `"my_db"`
/// - `md:` / `motherduck:` (bare) → `"my_db"` (MotherDuck default)
pub fn motherduck_database_name(path: &str) -> String {
    let stripped = if let Some(rest) = path.strip_prefix("motherduck:").or_else(|| {
        // case-insensitive strip
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

    // Strip query parameters
    let name = stripped.split('?').next().unwrap_or("");
    if name.is_empty() {
        "my_db".to_owned()
    } else {
        name.to_owned()
    }
}

/// Normalize a MotherDuck attach path by dropping URL query parameters.
///
/// Examples:
/// - `md:my_db?motherduck_token=tok` -> `md:my_db`
/// - `motherduck:sales?user=1` -> `motherduck:sales`
fn motherduck_attach_path(path: &str) -> String {
    path.split_once('?')
        .map(|(base, _)| base.to_owned())
        .unwrap_or_else(|| path.to_owned())
}

/// Resolve the MotherDuck token from config, path query string, or environment.
///
/// Priority: explicit settings > path query string > `MOTHERDUCK_TOKEN` env var.
/// Returns `None` if the path is not a MotherDuck connection or no token is found.
pub fn resolve_motherduck_token(config: &AdapterConfig) -> Option<String> {
    let path = config
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if !is_motherduck_path(path) {
        return None;
    }

    // Check explicit settings first
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

    // Then path query string, then env var
    extract_token_from_path(path).or_else(|| std::env::var("MOTHERDUCK_TOKEN").ok())
}

/// Extract a `motherduck_token` from a path query string (e.g. `md:db?motherduck_token=tok`).
fn extract_token_from_path(path: &str) -> Option<String> {
    let query = path.split_once('?')?.1;
    for pair in query.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            if key == "motherduck_token" && !value.is_empty() {
                return Some(value.to_owned());
            }
        }
    }
    None
}

/// Generate DuckDB initialization SQL statements from the adapter config.
///
/// Returns an ordered list of SQL strings ready for sequential execution.
/// When the path is a MotherDuck connection (`md:` / `motherduck:`), the
/// `motherduck` extension is auto-installed/loaded and the token is injected.
pub fn generate_duckdb_init_sql(config: &AdapterConfig) -> Vec<String> {
    let mut stmts = Vec::new();

    let path = config
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_owned();
    let is_md = is_motherduck_path(&path);

    // Auto-inject `motherduck` extension for MotherDuck paths
    if is_md {
        let already_listed = config
            .get("extensions")
            .and_then(|v| match v {
                YmlValue::Sequence(seq, _) => Some(seq.iter().any(|item| {
                    item.as_str()
                        .is_some_and(|s| s.eq_ignore_ascii_case("motherduck"))
                })),
                _ => None,
            })
            .unwrap_or(false);

        if !already_listed {
            stmts.push("INSTALL motherduck".to_owned());
            stmts.push("LOAD motherduck".to_owned());
        }
    }

    generate_extension_sql(config, &mut stmts);
    generate_secret_sql(config, &mut stmts);
    generate_motherduck_token_sql(config, &mut stmts);
    generate_setting_sql(config, &mut stmts);
    generate_motherduck_path_attach_sql(config, &mut stmts);
    generate_attachment_sql(config, &mut stmts);
    stmts
}

// ---------------------------------------------------------------------------
// Helpers
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
// Extensions
// ---------------------------------------------------------------------------

fn generate_extension_sql(config: &AdapterConfig, stmts: &mut Vec<String>) {
    let Some(val) = config.get("extensions") else {
        return;
    };
    if let YmlValue::Sequence(seq, _) = val {
        for item in seq {
            if let Some(ext) = item.as_str() {
                let ext = sanitize_identifier(ext);
                if !ext.is_empty() {
                    stmts.push(format!("INSTALL {ext}"));
                    stmts.push(format!("LOAD {ext}"));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------

/// Reserved keys in a secret mapping that are not passed as parameters.
const SECRET_RESERVED_KEYS: &[&str] = &["type", "name", "provider", "scope", "persistent"];

fn generate_secret_sql(config: &AdapterConfig, stmts: &mut Vec<String>) {
    let Some(val) = config.get("secrets") else {
        return;
    };
    let YmlValue::Sequence(seq, _) = val else {
        return;
    };
    for (i, item) in seq.iter().enumerate() {
        let YmlValue::Mapping(map, _) = item else {
            continue;
        };

        // `type` is required
        let Some(secret_type) = map.get("type").and_then(|v| v.as_str()) else {
            continue;
        };
        let secret_type = sanitize_identifier(secret_type);

        // Name: optional, default to `__dbt_secret_{i}`
        let name = map
            .get("name")
            .and_then(|v| v.as_str())
            .map(sanitize_identifier)
            .unwrap_or_else(|| format!("__dbt_secret_{i}"));

        // Persistent?
        let persistent = map
            .get("persistent")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let persist_kw = if persistent { " PERSISTENT" } else { "" };

        // Build the inner parameters
        let mut params = Vec::new();
        params.push(format!("TYPE {secret_type}"));

        if let Some(provider) = map.get("provider").and_then(|v| v.as_str()) {
            params.push(format!("PROVIDER {}", sanitize_identifier(provider)));
        }

        if let Some(scope) = map.get("scope").and_then(|v| v.as_str()) {
            params.push(format!("SCOPE '{}'", escape_single_quotes(scope)));
        }

        // All other keys become KEY 'value' pairs
        for (k, v) in map.iter() {
            let Some(key_str) = k.as_str() else {
                continue;
            };
            if SECRET_RESERVED_KEYS.contains(&key_str) {
                continue;
            }
            let key_upper = sanitize_identifier(key_str).to_uppercase();
            if key_upper.is_empty() {
                continue;
            }
            params.push(format!("{key_upper} {}", yml_value_to_sql_literal(v)));
        }

        let params_str = params.join(", ");
        stmts.push(format!(
            "CREATE OR REPLACE{persist_kw} SECRET {name} ({params_str})"
        ));
    }
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

fn generate_setting_sql(config: &AdapterConfig, stmts: &mut Vec<String>) {
    let Some(val) = config.get("settings") else {
        return;
    };
    if let YmlValue::Mapping(map, _) = val {
        for (k, v) in map.iter() {
            if let Some(key_str) = k.as_str() {
                // motherduck_token must be set at database init, not via SET
                if key_str == "motherduck_token" {
                    continue;
                }
                let key = sanitize_identifier(key_str);
                if !key.is_empty() {
                    stmts.push(format!("SET {key} = {}", yml_value_to_sql_literal(v)));
                }
            }
        }
    }
}

fn generate_motherduck_token_sql(config: &AdapterConfig, stmts: &mut Vec<String>) {
    if let Some(token) = resolve_motherduck_token(config) {
        stmts.push(format!(
            "SET motherduck_token = '{}'",
            escape_single_quotes(&token)
        ));
    }
}

fn generate_motherduck_path_attach_sql(config: &AdapterConfig, stmts: &mut Vec<String>) {
    let Some(path) = config.get("path").and_then(|v| v.as_str()) else {
        return;
    };
    if !is_motherduck_path(path) {
        return;
    }

    let alias = config
        .get("database")
        .and_then(|v| v.as_str())
        .map(sanitize_identifier)
        .unwrap_or_else(|| sanitize_identifier(&motherduck_database_name(path)));
    let alias = if alias.is_empty() {
        "my_db".to_owned()
    } else {
        alias
    };

    let attach_path = motherduck_attach_path(path);
    stmts.push(format!(
        "ATTACH IF NOT EXISTS '{}' AS {alias}",
        escape_single_quotes(&attach_path),
    ));
    stmts.push(format!("USE {alias}"));
}

// ---------------------------------------------------------------------------
// Attachments
// ---------------------------------------------------------------------------

fn generate_attachment_sql(config: &AdapterConfig, stmts: &mut Vec<String>) {
    let Some(val) = config.get("attach") else {
        return;
    };
    let YmlValue::Sequence(seq, _) = val else {
        return;
    };
    for item in seq {
        let YmlValue::Mapping(map, _) = item else {
            continue;
        };

        let Some(path) = map.get("path").and_then(|v| v.as_str()) else {
            continue;
        };
        let path_escaped = escape_single_quotes(path);

        let mut sql = format!("ATTACH IF NOT EXISTS '{path_escaped}'");

        if let Some(alias) = map.get("alias").and_then(|v| v.as_str()) {
            let alias = sanitize_identifier(alias);
            if !alias.is_empty() {
                sql.push_str(&format!(" AS {alias}"));
            }
        }

        // Collect options in parentheses: TYPE and READ_ONLY
        let db_type = map.get("type").and_then(|v| v.as_str());
        let read_only = map
            .get("read_only")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

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

        stmts.push(sql);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
  - type: gcs
    key_id: fake_key
"#,
        );
        let stmts = generate_duckdb_init_sql(&config);
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(sql.contains("SECRET __dbt_secret_0"));
        assert!(sql.contains("TYPE gcs"));
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
    fn test_motherduck_database_name() {
        assert_eq!(motherduck_database_name("md:my_db"), "my_db");
        assert_eq!(motherduck_database_name("md:"), "my_db");
        assert_eq!(motherduck_database_name("motherduck:sales"), "sales");
        assert_eq!(
            motherduck_database_name("md:my_db?motherduck_token=tok123"),
            "my_db"
        );
    }

    #[test]
    fn test_motherduck_attach_path_strips_query() {
        assert_eq!(
            motherduck_attach_path("md:my_db?motherduck_token=tok"),
            "md:my_db"
        );
        assert_eq!(
            motherduck_attach_path("motherduck:sales?user=1"),
            "motherduck:sales"
        );
        assert_eq!(motherduck_attach_path("md:plain"), "md:plain");
    }

    #[test]
    fn test_motherduck_auto_extension() {
        let config = config_from_yaml(
            r#"
path: "md:my_db"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
        assert!(stmts.contains(&"SET motherduck_token = 'tok_from_settings'".to_owned()));
    }

    #[test]
    fn test_motherduck_token_set_before_attach() {
        let config = config_from_yaml(
            r#"
path: "md:my_db?motherduck_token=tok_from_path"
"#,
        );
        let stmts = generate_duckdb_init_sql(&config);
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
        let stmts = generate_duckdb_init_sql(&config);
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
            resolve_motherduck_token(&config),
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
            resolve_motherduck_token(&config),
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
            resolve_motherduck_token(&config),
            Some("tok_from_settings".to_owned())
        );
    }

    #[test]
    fn test_resolve_motherduck_token_none_for_local() {
        let config = config_from_yaml(
            r#"
path: "/tmp/local.duckdb"
"#,
        );
        assert_eq!(resolve_motherduck_token(&config), None);
    }
}
