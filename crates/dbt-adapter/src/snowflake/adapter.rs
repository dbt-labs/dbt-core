use crate::adapter_engine::AdapterEngine;
use crate::typed_adapter::TypedBaseAdapter;
use crate::{AdapterTyping, metadata::*};

use std::fmt;
use std::sync::Arc;

/// An adapter for interacting with Snowflake.
#[derive(Clone)]
pub struct SnowflakeAdapter {
    pub engine: Arc<AdapterEngine>,
}

impl fmt::Debug for SnowflakeAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.adapter_type())
    }
}

impl SnowflakeAdapter {
    pub fn new(engine: Arc<AdapterEngine>) -> Self {
        Self { engine }
    }
}

impl AdapterTyping for SnowflakeAdapter {
    fn as_metadata_adapter(&self) -> Option<&dyn MetadataAdapter> {
        Some(self)
    }
    fn as_typed_base_adapter(&self) -> &dyn TypedBaseAdapter {
        self
    }

    fn engine(&self) -> &Arc<AdapterEngine> {
        &self.engine
    }
}

impl TypedBaseAdapter for SnowflakeAdapter {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AdapterType;
    use crate::sql_types::NaiveTypeOpsImpl;
    use crate::stmt_splitter::NaiveStmtSplitter;

    use crate::config::AdapterConfig;
    use crate::query_comment::QueryCommentConfig;
    use dbt_auth::auth_for_backend;
    use dbt_common::{AdapterResult, cancellation::never_cancels};
    use dbt_schemas::schemas::relations::SNOWFLAKE_RESOLVED_QUOTING;
    use dbt_schemas::schemas::relations::base::ComponentName;
    use dbt_serde_yaml::Mapping;
    use dbt_xdbc::Backend;

    use minijinja::{Environment, State};

    fn engine() -> Arc<AdapterEngine> {
        let config = Mapping::from_iter([
            ("user".into(), "U".into()),
            ("password".into(), "P".into()),
            ("account".into(), "A".into()),
            ("database".into(), "D".into()),
            ("schema".into(), "S".into()),
            ("role".into(), "role".into()),
            ("warehouse".into(), "warehouse".into()),
        ]);
        let auth = auth_for_backend(Backend::Snowflake);
        AdapterEngine::new(
            AdapterType::Snowflake,
            auth.into(),
            AdapterConfig::new(config),
            SNOWFLAKE_RESOLVED_QUOTING,
            Arc::new(NaiveStmtSplitter),
            None,
            QueryCommentConfig::from_query_comment(None, AdapterType::Snowflake, false),
            Box::new(NaiveTypeOpsImpl::new(AdapterType::Snowflake)), // XXX: NaiveTypeOpsImpl
            never_cancels(),
        )
    }

    #[test]
    fn test_adapter_type() {
        let adapter = SnowflakeAdapter::new(engine());
        assert_eq!(adapter.adapter_type(), AdapterType::Snowflake);
    }

    #[test]
    fn test_quote() {
        let adapter = SnowflakeAdapter::new(engine());
        let env = Environment::new();
        let state = State::new_for_env(&env);
        assert_eq!(adapter.quote(&state, "abc").unwrap(), "\"abc\"");
    }

    #[test]
    fn test_quote_seed_column() -> AdapterResult<()> {
        let adapter = SnowflakeAdapter::new(engine());
        let env = Environment::new();
        let state = State::new_for_env(&env);
        let quoted = adapter
            .quote_seed_column(&state, "my_column", None)
            .unwrap();
        assert_eq!(quoted, "my_column");
        let quoted = adapter
            .quote_seed_column(&state, "my_column", Some(false))
            .unwrap();
        assert_eq!(quoted, "my_column");
        let quoted = adapter
            .quote_seed_column(&state, "my_column", Some(true))
            .unwrap();
        assert_eq!(quoted, "\"my_column\"");
        Ok(())
    }

    #[test]
    fn test_quote_as_configured() -> AdapterResult<()> {
        let config = AdapterConfig::default();
        let auth = auth_for_backend(Backend::Snowflake);
        let engine = AdapterEngine::new(
            AdapterType::Snowflake,
            auth.into(),
            config,
            SNOWFLAKE_RESOLVED_QUOTING,
            Arc::new(NaiveStmtSplitter),
            None,
            QueryCommentConfig::from_query_comment(None, AdapterType::Snowflake, false),
            Box::new(NaiveTypeOpsImpl::new(AdapterType::Snowflake)),
            never_cancels(),
        );
        let adapter = SnowflakeAdapter::new(engine);

        let env = Environment::new();
        let state = State::new_for_env(&env);
        let quoted = adapter
            .quote_as_configured(&state, "my_schema", &ComponentName::Schema)
            .unwrap();
        assert_eq!(quoted, "my_schema");

        let quoted = adapter
            .quote_as_configured(&state, "my_database", &ComponentName::Database)
            .unwrap();
        assert_eq!(quoted, "my_database");

        let quoted = adapter
            .quote_as_configured(&state, "my_table", &ComponentName::Identifier)
            .unwrap();
        assert_eq!(quoted, "my_table");
        Ok(())
    }
}
