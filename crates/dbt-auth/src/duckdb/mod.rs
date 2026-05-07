pub mod init;

use crate::{AdapterConfig, Auth, AuthError, AuthOutcome};

use dbt_xdbc::{Backend, database};

pub struct DuckDbAuth;

impl Auth for DuckDbAuth {
    fn backend(&self) -> Backend {
        Backend::DuckDBExtended
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

        Ok(AuthOutcome {
            builder,
            warnings: vec![],
        })
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
    fn configure_uses_in_memory_path_for_motherduck() {
        let auth = DuckDbAuth;
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
        let auth = DuckDbAuth;
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
}
