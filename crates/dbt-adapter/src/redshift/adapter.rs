use crate::adapter_engine::AdapterEngine;
use crate::base_adapter::AdapterTyping;
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use crate::metadata::*;
use crate::typed_adapter::TypedBaseAdapter;
use dbt_schemas::schemas::common::{ConstraintSupport, ConstraintType};
use minijinja::Value;

use std::borrow::Cow;
use std::fmt;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

/// An adapter for interacting with Redshift.
#[derive(Clone)]
pub struct RedshiftAdapter {
    engine: Arc<AdapterEngine>,
}

impl Debug for RedshiftAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.adapter_type())
    }
}

impl RedshiftAdapter {
    pub fn new(engine: Arc<AdapterEngine>) -> Self {
        Self { engine }
    }
}

impl AdapterTyping for RedshiftAdapter {
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

impl TypedBaseAdapter for RedshiftAdapter {
    fn verify_database(&self, database: String) -> AdapterResult<Value> {
        let ra3_node = self
            .engine
            .config("ra3_node")
            .unwrap_or(Cow::Borrowed("false"));

        // We have no guarantees that `database` is unquoted, but we do know that `configured_database` will be unquoted.
        // For the Redshift adapter, we can just trim the `"` character per `self.quote`.
        let database = database.trim_matches('\"');
        let configured_database = self.engine.config("database");

        if let Some(configured_database) = configured_database {
            let ra3_node: bool = FromStr::from_str(&ra3_node).map_err(|_| {
                AdapterError::new(
                    AdapterErrorKind::Configuration,
                    r#"Failed to parse ra3_node, expected "true" or "false""#,
                )
            })?;
            if !database.eq_ignore_ascii_case(&configured_database) && !ra3_node {
                return Err(AdapterError::new(
                    AdapterErrorKind::UnexpectedDbReference,
                    format!(
                        "Cross-db references allowed only in RA3.* node ({database} vs {configured_database})"
                    ),
                ));
            }
        }

        Ok(Value::from(()))
    }

    /// https://github.com/dbt-labs/dbt-adapters/blob/2a94cc75dba1f98fa5caff1f396f5af7ee444598/dbt-redshift/src/dbt/adapters/redshift/impl.py#L53
    fn get_constraint_support(&self, ct: ConstraintType) -> ConstraintSupport {
        match ct {
            ConstraintType::Check => ConstraintSupport::NotSupported,
            ConstraintType::NotNull => ConstraintSupport::Enforced,
            ConstraintType::Unique | ConstraintType::PrimaryKey | ConstraintType::ForeignKey => {
                ConstraintSupport::NotEnforced
            }
            _ => ConstraintSupport::NotSupported,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::AdapterConfig;
    use crate::query_comment::QueryCommentConfig;
    use crate::sql_types::NaiveTypeOpsImpl;
    use crate::stmt_splitter::NaiveStmtSplitter;
    use dbt_auth::auth_for_backend;
    use dbt_common::adapter::AdapterType;
    use dbt_common::cancellation::never_cancels;
    use dbt_schemas::schemas::relations::DEFAULT_RESOLVED_QUOTING;
    use dbt_serde_yaml::Mapping;
    use dbt_xdbc::Backend;
    use minijinja::State;

    fn engine() -> Arc<AdapterEngine> {
        let config = Mapping::new();
        let auth = auth_for_backend(Backend::Redshift);
        AdapterEngine::new(
            AdapterType::Redshift,
            auth.into(),
            AdapterConfig::new(config),
            DEFAULT_RESOLVED_QUOTING,
            Arc::new(NaiveStmtSplitter),
            None,
            QueryCommentConfig::from_query_comment(None, AdapterType::Redshift, false),
            Box::new(NaiveTypeOpsImpl::new(AdapterType::Redshift)), // XXX: NaiveTypeOpsImpl
            never_cancels(),
        )
    }

    #[test]
    fn test_quote() {
        let adapter = RedshiftAdapter::new(engine());
        let env = minijinja::Environment::new();
        let state = State::new_for_env(&env);
        assert_eq!(adapter.quote(&state, "abc").unwrap(), "\"abc\"");
    }
}
