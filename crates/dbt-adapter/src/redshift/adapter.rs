use crate::adapter_engine::AdapterEngine;
use crate::base_adapter::AdapterTyping;
use crate::metadata::*;
use crate::typed_adapter::TypedBaseAdapter;

use std::fmt;
use std::fmt::Debug;
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

impl TypedBaseAdapter for RedshiftAdapter {}

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
        assert_eq!(adapter.quote("abc"), "\"abc\"");
    }
}
