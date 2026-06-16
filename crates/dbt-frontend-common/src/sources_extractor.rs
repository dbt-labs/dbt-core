//! The [SourcesExtractor] trait: parse a SQL statement and return the upstream
//! table references it depends on.

use dbt_adapter_core::AdapterType;

use crate::FullyQualifiedName;
use crate::error::FrontendResult;
use crate::named_reference::NamedReference;

pub trait SourcesExtractor: Send + Sync {
    /// Parse `sql` and return the upstream table references, qualified
    /// against `default_catalog`/`default_schema`. In-scope CTE aliases are
    /// excluded.
    fn extract_upstreams(
        &self,
        adapter_type: AdapterType,
        sql: &str,
        default_catalog: &str,
        default_schema: &str,
        quoted_name_ignore_case: bool,
    ) -> FrontendResult<Vec<NamedReference<FullyQualifiedName>>>;
}
