#[cfg(test)]
mod tests {
    use crate::sql_types::NaiveTypeOpsImpl;
    use crate::typed_adapter::ConcreteAdapter;
    use dbt_common::adapter::AdapterType;
    use dbt_common::cancellation::never_cancels;
    use dbt_schemas::schemas::relations::SNOWFLAKE_RESOLVED_QUOTING;

    use std::collections::BTreeMap;
    use std::sync::Arc;

    use crate::AdapterTyping;
    use crate::TypedBaseAdapter;

    #[test]
    fn test_adapter_type() {
        let adapter = ConcreteAdapter::new_mock(
            AdapterType::Snowflake,
            BTreeMap::new(),
            SNOWFLAKE_RESOLVED_QUOTING,
            Box::new(NaiveTypeOpsImpl::new(AdapterType::Snowflake)),
            Arc::new(crate::stmt_splitter::NaiveStmtSplitter),
            never_cancels(),
        );
        assert_eq!(adapter.adapter_type(), AdapterType::Snowflake);
    }

    #[test]
    fn test_quote() {
        let adapter = ConcreteAdapter::new_mock(
            AdapterType::Snowflake,
            BTreeMap::new(),
            SNOWFLAKE_RESOLVED_QUOTING,
            Box::new(NaiveTypeOpsImpl::new(AdapterType::Snowflake)),
            Arc::new(crate::stmt_splitter::NaiveStmtSplitter),
            never_cancels(),
        );
        assert_eq!(adapter.quote("abc"), "\"abc\"");
    }
}
