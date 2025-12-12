//! Semantic categorization of adapter operations.
//!
//! This module classifies adapter methods by their side effects,
//! enabling dependency analysis based validation during replay.

use serde::{Deserialize, Serialize};

/// Semantic category of an adapter operation.
///
/// These categories enable dependency analysis during replay:
/// - Reads can be re-ordered freely before/between/after write calls to the same relations
/// - Writes must preserve their relative order
/// - Writes depend on preceding reads for the same relations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticCategory {
    /// Read metadata without side effects (SELECT/SHOW queries).
    /// Examples: get_relation, get_columns_in_relation, list_schemas
    MetadataRead,

    /// Write/mutate operations (DDL/DML).
    /// Examples: drop_relation, create_schema
    /// Ambiguous calls like execute and add_query are also in this category
    Write,

    /// Cache operations with no DB interaction.
    /// Examples: cache_added, cache_dropped, cache_renamed
    Cache,

    /// Pure compute operations.
    /// Examples: quote, convert_type
    Pure,
}

impl SemanticCategory {
    /// Classify a BridgeAdapter method by its semantic category.
    ///
    /// This is derived from analyzing what each method does:
    /// - MetadataRead: DB queries (SELECT/SHOW), no mutations
    /// - Write: DB mutations (CREATE/DROP/ALTER/INSERT/DELETE)
    /// - Cache: Only touches adapter's in-memory relation cache
    /// - Pure: Local computation, no I/O at all
    pub fn from_adapter_method(method: &str) -> Self {
        match method {
            // Query database state without mutation
            "get_relation"
            | "get_columns_in_relation"
            | "list_schemas"
            | "check_schema_exists"
            | "list_relations_without_caching"
            | "get_relations_by_pattern"
            | "get_relations_without_caching"
            | "valid_snapshot_target"
            | "describe_relation"
            | "describe_dynamic_table"
            | "get_column_schema_from_query"
            | "get_columns_in_select_sql"
            | "get_partitions_metadata"
            | "get_bq_table"
            | "get_dataset_location"
            | "get_catalog_integration"
            | "get_relation_config"
            | "get_config_from_model"
            | "compare_dbr_version"
            | "get_missing_columns" => SemanticCategory::MetadataRead,

            // Mutate database state (DDL/DML)
            "execute"
            | "add_query"
            | "drop_relation"
            | "rename_relation"
            | "truncate_relation"
            | "create_schema"
            | "drop_schema"
            | "expand_target_column_types"
            | "load_dataframe"
            | "copy_table"
            | "update_columns"
            | "update_table_description"
            | "alter_table_add_columns"
            | "upload_file"
            | "grant_access_to"
            | "submit_python_job"
            | "assert_valid_snapshot_target_given_strategy"
            | "update_tblproperties_for_uniform_iceberg" => SemanticCategory::Write,

            // Internal bookkeeping only
            "cache_added" | "cache_dropped" | "cache_renamed" => SemanticCategory::Cache,

            // No I/O at all
            "quote"
            | "quote_as_configured"
            | "quote_seed_column"
            | "convert_type"
            | "dispatch"
            | "commit"
            | "type"
            | "render_raw_model_constraints"
            | "render_raw_columns_constraints"
            | "get_incremental_strategy_macro"
            | "standardize_grants_dict"
            | "verify_database"
            | "nest_column_data_types"
            | "add_time_ingestion_partition_column"
            | "parse_partition_by"
            | "is_replaceable"
            | "is_uniform"
            | "valid_incremental_strategies"
            | "get_persist_doc_columns"
            | "get_column_tags_from_model"
            | "generate_unique_temporary_table_suffix"
            | "parse_columns_and_constraints"
            | "clean_sql"
            | "get_common_options"
            | "get_table_options"
            | "get_view_options"
            | "parse_index"
            | "redact_credentials"
            | "compute_external_path"
            | "get_hard_deletes_behavior"
            | "is_cluster"
            | "build_catalog_relation" => SemanticCategory::Pure,

            _ => {
                debug_assert!(
                    false,
                    "time-machine: Unknown adapter method '{}', add it to SemanticCategory::from_adapter_method",
                    method
                );
                SemanticCategory::Write
            }
        }
    }

    /// Classify a MetadataAdapter method by its semantic category.
    pub fn from_metadata_method(method: &str) -> Self {
        match method {
            // All MetadataAdapter async methods are reads except one
            "list_relations_schemas"
            | "list_relations_sdf_schemas"
            | "list_relations_schemas_by_patterns"
            | "list_relations_in_parallel"
            | "freshness"
            | "list_user_defined_functions"
            | "build_schemas_from_stats_sql"
            | "build_columns_from_get_columns"
            | "create_relations_from_executed_nodes"
            | "is_permission_error" => SemanticCategory::MetadataRead,

            // This one actually mutates (CREATE SCHEMA)
            "create_schemas_if_not_exists" => SemanticCategory::Write,

            // Conservatively default to Write for unknown methods and assume side effects.
            _ => {
                debug_assert!(
                    false,
                    "time-machine: Unknown metadata method '{}', add it to SemanticCategory::from_metadata_method",
                    method
                );
                SemanticCategory::Write
            }
        }
    }

    /// Returns true if this category represents an operation that mutates state.
    pub fn is_mutating(&self) -> bool {
        matches!(self, SemanticCategory::Write)
    }

    /// Returns true if this category represents a database I/O operation.
    pub fn is_db_io(&self) -> bool {
        matches!(
            self,
            SemanticCategory::MetadataRead | SemanticCategory::Write
        )
    }
}

impl std::fmt::Display for SemanticCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SemanticCategory::MetadataRead => write!(f, "metadata_read"),
            SemanticCategory::Write => write!(f, "write"),
            SemanticCategory::Cache => write!(f, "cache"),
            SemanticCategory::Pure => write!(f, "pure"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adapter_method_classification() {
        // Metadata reads
        assert_eq!(
            SemanticCategory::from_adapter_method("get_relation"),
            SemanticCategory::MetadataRead
        );
        assert_eq!(
            SemanticCategory::from_adapter_method("get_columns_in_relation"),
            SemanticCategory::MetadataRead
        );
        assert_eq!(
            SemanticCategory::from_adapter_method("list_schemas"),
            SemanticCategory::MetadataRead
        );

        // Writes
        assert_eq!(
            SemanticCategory::from_adapter_method("execute"),
            SemanticCategory::Write
        );
        assert_eq!(
            SemanticCategory::from_adapter_method("drop_relation"),
            SemanticCategory::Write
        );
        assert_eq!(
            SemanticCategory::from_adapter_method("create_schema"),
            SemanticCategory::Write
        );

        // Cache ops
        assert_eq!(
            SemanticCategory::from_adapter_method("cache_added"),
            SemanticCategory::Cache
        );
        assert_eq!(
            SemanticCategory::from_adapter_method("cache_dropped"),
            SemanticCategory::Cache
        );

        // Pure compute
        assert_eq!(
            SemanticCategory::from_adapter_method("quote"),
            SemanticCategory::Pure
        );
        assert_eq!(
            SemanticCategory::from_adapter_method("convert_type"),
            SemanticCategory::Pure
        );
    }

    #[test]
    fn test_metadata_method_classification() {
        assert_eq!(
            SemanticCategory::from_metadata_method("list_relations_schemas"),
            SemanticCategory::MetadataRead
        );
        assert_eq!(
            SemanticCategory::from_metadata_method("freshness"),
            SemanticCategory::MetadataRead
        );
        assert_eq!(
            SemanticCategory::from_metadata_method("create_schemas_if_not_exists"),
            SemanticCategory::Write
        );
    }

    #[test]
    fn test_category_properties() {
        assert!(!SemanticCategory::MetadataRead.is_mutating());
        assert!(SemanticCategory::Write.is_mutating());
        assert!(!SemanticCategory::Cache.is_mutating());
        assert!(!SemanticCategory::Pure.is_mutating());

        assert!(SemanticCategory::MetadataRead.is_db_io());
        assert!(SemanticCategory::Write.is_db_io());
        assert!(!SemanticCategory::Cache.is_db_io());
        assert!(!SemanticCategory::Pure.is_db_io());
    }

    #[test]
    fn test_serialization() {
        let cat = SemanticCategory::MetadataRead;
        let json = serde_json::to_string(&cat).unwrap();
        assert_eq!(json, "\"metadata_read\"");

        let parsed: SemanticCategory = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, SemanticCategory::MetadataRead);
    }
}
