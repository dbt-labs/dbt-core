pub mod init;

use std::sync::atomic::{AtomicBool, Ordering};

use crate::config::YmlValue;
use crate::{AdapterConfig, Auth, AuthError, AuthOutcome};

use dbt_xdbc::{Backend, database};

/// Tracks whether the `allow_unsigned_extensions` warning has already been
/// emitted in this process. `configure` runs once per connection/model, so this
/// guard keeps the warning to a single emission instead of one per model.
static ALLOW_UNSIGNED_EXTENSIONS_WARNED: AtomicBool = AtomicBool::new(false);

pub struct DuckDbAuth {
    backend: Backend,
}

impl DuckDbAuth {
    pub fn new(backend: Backend) -> Self {
        debug_assert!(matches!(backend, Backend::DuckDB | Backend::DuckDBExtended));
        Self { backend }
    }
}

impl Auth for DuckDbAuth {
    fn backend(&self) -> Backend {
        self.backend
    }

    fn configure(&self, config: &AdapterConfig) -> Result<AuthOutcome, AuthError> {
        let mut builder = database::Builder::new(self.backend());

        // DuckDB requires the database path to be specified
        // The path option from profiles.yml specifies where to store the database file
        if let Some(path) = config.get_string("path") {
            // MotherDuck paths must be attached after extension initialization.
            // Use an in-memory primary DB and let init SQL attach the md: database.
            let path = if init::is_motherduck_path(path.as_ref()) {
                ":memory:"
            } else {
                path.as_ref()
            };
            builder
                .with_named_option("path", path)
                .map_err(|e| AuthError::Config(e.to_string()))?;
        }

        let mut warnings = Vec::new();
        // `allow_unsigned_extensions` lets DuckDB load extensions that are not
        // signed by dbt Labs. It is an explicit, opt-in escape hatch for local
        // development against custom/unsigned extension builds; surface a warning
        // so it is never enabled silently against production data.
        if let Some(value) = duckdb_startup_setting(config, "allow_unsigned_extensions") {
            builder
                .with_named_option("allow_unsigned_extensions", value)
                .map_err(|e| AuthError::Config(e.to_string()))?;
            // Warn only once per process: `configure` runs once per
            // connection/model, so without this guard the warning is repeated
            // for every model in the run.
            if !ALLOW_UNSIGNED_EXTENSIONS_WARNED.swap(true, Ordering::Relaxed) {
                warnings.push(
                    "DuckDB `allow_unsigned_extensions` is enabled: this connection will load \
                     unsigned extensions. This is intended for local development only — do not \
                     enable it against production data."
                        .to_string(),
                );
            }
        }

        Ok(AuthOutcome { builder, warnings })
    }
}

fn duckdb_startup_setting(config: &AdapterConfig, key: &str) -> Option<String> {
    let settings = config.get("settings")?;
    let YmlValue::Mapping(map, _) = settings else {
        return None;
    };
    map.get(key).and_then(yml_value_to_duckdb_option)
}

fn yml_value_to_duckdb_option(value: &YmlValue) -> Option<String> {
    match value {
        YmlValue::Bool(value, _) => Some(value.to_string()),
        YmlValue::Number(value, _) => Some(value.to_string()),
        YmlValue::String(value, _) => Some(value.clone()),
        YmlValue::Tagged(tagged, _) => yml_value_to_duckdb_option(&tagged.value),
        YmlValue::Null(_) | YmlValue::Sequence(_, _) | YmlValue::Mapping(_, _) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::options::{OptionDatabase, OptionValue};

    fn config_from_yaml(yaml: &str) -> AdapterConfig {
        let value: dbt_yaml::Value = dbt_yaml::from_str(yaml).unwrap();
        let mapping = match value {
            dbt_yaml::Value::Mapping(m, _) => m,
            _ => panic!("expected mapping"),
        };
        AdapterConfig::new(mapping)
    }

    #[test]
    fn configure_preserves_duckdb_backend_variant() {
        for backend in [Backend::DuckDB, Backend::DuckDBExtended] {
            let auth = DuckDbAuth::new(backend);
            let builder = auth
                .configure(&AdapterConfig::new(Default::default()))
                .unwrap()
                .builder;

            assert_eq!(auth.backend(), backend);
            assert_eq!(builder.backend, backend);
        }
    }

    #[test]
    fn configure_uses_in_memory_path_for_motherduck() {
        let auth = DuckDbAuth::new(Backend::DuckDBExtended);
        let config = config_from_yaml(
            r#"
path: "md:stocks_dev"
"#,
        );

        let builder = auth.configure(&config).unwrap().builder;
        assert!(builder.other.iter().any(|(name, value)| {
            matches!(
                (name, value),
                (
                    OptionDatabase::Other(option_name),
                    OptionValue::String(option_value)
                ) if option_name == "path" && option_value == ":memory:"
            )
        }));
        assert!(!builder.other.iter().any(|(name, _)| {
            matches!(
                name,
                OptionDatabase::Other(option_name) if option_name == "motherduck_token"
            )
        }));
    }

    #[test]
    fn configure_keeps_local_path() {
        let auth = DuckDbAuth::new(Backend::DuckDBExtended);
        let config = config_from_yaml(
            r#"
path: "/tmp/local.duckdb"
"#,
        );

        let builder = auth.configure(&config).unwrap().builder;
        assert!(builder.other.iter().any(|(name, value)| {
            matches!(
                (name, value),
                (
                    OptionDatabase::Other(option_name),
                    OptionValue::String(option_value)
                ) if option_name == "path" && option_value == "/tmp/local.duckdb"
            )
        }));
    }

    #[test]
    fn configure_passes_allow_unsigned_extensions_as_database_option() {
        // The warning is gated by a process-wide latch; reset it so this test
        // observes the first (and only) emission regardless of test ordering.
        ALLOW_UNSIGNED_EXTENSIONS_WARNED.store(false, Ordering::Relaxed);
        let auth = DuckDbAuth::new(Backend::DuckDBExtended);
        let config = config_from_yaml(
            r#"
path: "/tmp/local.duckdb"
settings:
  allow_unsigned_extensions: true
"#,
        );

        let outcome = auth.configure(&config).unwrap();
        assert!(outcome.builder.other.iter().any(|(name, value)| {
            matches!(
                (name, value),
                (
                    OptionDatabase::Other(option_name),
                    OptionValue::String(option_value)
                ) if option_name == "allow_unsigned_extensions" && option_value == "true"
            )
        }));
        // Enabling the escape hatch must surface a warning so it is never silent.
        assert!(
            outcome
                .warnings
                .iter()
                .any(|w| w.contains("allow_unsigned_extensions")),
            "expected an allow_unsigned_extensions warning, got: {:?}",
            outcome.warnings
        );
    }
}
