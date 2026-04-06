#[cfg(test)]
mod tests {
    use crate::adapter::adapter_impl::AdapterImpl;
    use crate::sql_types::SATypeOpsImpl;
    use dbt_adapter_core::AdapterType;

    use dbt_schemas::schemas::relations::SNOWFLAKE_RESOLVED_QUOTING;

    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[test]
    fn test_adapter_type() {
        let adapter = AdapterImpl::new_mock(
            AdapterType::Snowflake,
            BTreeMap::new(),
            SNOWFLAKE_RESOLVED_QUOTING,
            Box::new(SATypeOpsImpl::new(AdapterType::Snowflake)),
            Arc::new(crate::stmt_splitter::NaiveStmtSplitter),
        );
        assert_eq!(adapter.adapter_type(), AdapterType::Snowflake);
    }

    #[test]
    fn test_quote() {
        let adapter = AdapterImpl::new_mock(
            AdapterType::Snowflake,
            BTreeMap::new(),
            SNOWFLAKE_RESOLVED_QUOTING,
            Box::new(SATypeOpsImpl::new(AdapterType::Snowflake)),
            Arc::new(crate::stmt_splitter::NaiveStmtSplitter),
        );
        assert_eq!(adapter.quote("abc"), "\"abc\"");
    }
}
