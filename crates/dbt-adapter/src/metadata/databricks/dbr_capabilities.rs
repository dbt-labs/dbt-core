//! DBR (Databricks Runtime) capability management system.
//!
//! Provides a centralized way to check for DBR version-gated features,
//! replacing scattered version comparisons with named capabilities.
//!
//! Reference: https://github.com/databricks/dbt-databricks/blob/25caa2a14ed0535f08f6fd92e29b39df1f453e4d/dbt/adapters/databricks/dbr_capabilities.py

use std::str::FromStr;

use crate::metadata::databricks::version::DbrVersion;

/// Named capabilities that depend on DBR version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DbrCapability {
    Timestampdiff,
    Iceberg,
    CommentOnColumn,
    JsonColumnMetadata,
    StreamingTableJsonMetadata,
    InsertByName,
    ReplaceOn,
}

impl DbrCapability {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Timestampdiff => "timestampdiff",
            Self::Iceberg => "iceberg",
            Self::CommentOnColumn => "comment_on_column",
            Self::JsonColumnMetadata => "json_column_metadata",
            Self::StreamingTableJsonMetadata => "streaming_table_json_metadata",
            Self::InsertByName => "insert_by_name",
            Self::ReplaceOn => "replace_on",
        }
    }

    pub fn valid_names() -> &'static [&'static str] {
        &[
            "timestampdiff",
            "iceberg",
            "comment_on_column",
            "json_column_metadata",
            "streaming_table_json_metadata",
            "insert_by_name",
            "replace_on",
        ]
    }
}

impl FromStr for DbrCapability {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "timestampdiff" => Ok(Self::Timestampdiff),
            "iceberg" => Ok(Self::Iceberg),
            "comment_on_column" => Ok(Self::CommentOnColumn),
            "json_column_metadata" => Ok(Self::JsonColumnMetadata),
            "streaming_table_json_metadata" => Ok(Self::StreamingTableJsonMetadata),
            "insert_by_name" => Ok(Self::InsertByName),
            "replace_on" => Ok(Self::ReplaceOn),
            _ => Err(format!(
                "Unknown DBR capability: '{}'. Valid capabilities are: {}",
                s,
                Self::valid_names().join(", ")
            )),
        }
    }
}

/// Specification for a DBR capability.
#[derive(Clone, Debug)]
pub struct CapabilitySpec {
    /// Minimum DBR version (major, minor) required. Use (0, 0) for "any version".
    pub min_version: (i64, i64),
    /// Whether SQL warehouses support this capability. Default true.
    pub sql_warehouse_supported: bool,
}

/// Capability specifications from dbt-databricks.
fn capability_spec(capability: DbrCapability) -> CapabilitySpec {
    match capability {
        DbrCapability::Timestampdiff => CapabilitySpec {
            min_version: (10, 4),
            sql_warehouse_supported: true,
        },
        DbrCapability::Iceberg => CapabilitySpec {
            min_version: (14, 3),
            sql_warehouse_supported: true,
        },
        DbrCapability::CommentOnColumn => CapabilitySpec {
            min_version: (16, 1),
            sql_warehouse_supported: true,
        },
        DbrCapability::JsonColumnMetadata => CapabilitySpec {
            min_version: (16, 2),
            sql_warehouse_supported: true,
        },
        DbrCapability::StreamingTableJsonMetadata => CapabilitySpec {
            min_version: (17, 1),
            sql_warehouse_supported: false,
        },
        DbrCapability::InsertByName => CapabilitySpec {
            min_version: (12, 2),
            sql_warehouse_supported: true,
        },
        DbrCapability::ReplaceOn => CapabilitySpec {
            min_version: (17, 1),
            sql_warehouse_supported: true,
        },
    }
}

/// Check if a capability is available for the given compute.
///
/// - `dbr_version`: The DBR version tuple (major, minor). Use `DbrVersion::Unset` for SQL warehouses
///   (treated as "latest" - all sql_warehouse_supported capabilities return true).
/// - `is_sql_warehouse`: Whether this is a SQL warehouse (vs cluster).
pub fn has_capability(
    capability: DbrCapability,
    dbr_version: DbrVersion,
    is_sql_warehouse: bool,
) -> bool {
    let spec = capability_spec(capability);

    if is_sql_warehouse {
        if !spec.sql_warehouse_supported {
            return false;
        }
        // SQL warehouses are treated as having the latest version
        return true;
    }

    // For clusters, we need a known version
    if matches!(dbr_version, DbrVersion::Unset) {
        return false;
    }

    let min_version = DbrVersion::Full(spec.min_version.0, spec.min_version.1);
    dbr_version >= min_version
}
