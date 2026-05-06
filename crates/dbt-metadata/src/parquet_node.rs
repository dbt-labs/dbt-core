use chrono::Utc;
use dbt_common::CodeLocationWithFile;
use dbt_common::FsResult;
use dbt_common::constants::NODES_RD;
use dbt_common::io_args::{EvalArgs, FsCommand, IoArgs, StaticAnalysisKind};
use dbt_metadata_parquet::utils::{
    generate_list_type, generate_map_type, generate_struct_type, read_parquet_file,
};
use dbt_schemas::schemas::IntrospectionKind;
use dbt_schemas::schemas::ResolvedCloudConfig;
use dbt_schemas::schemas::common::{
    Access, DbtBatchSize, DbtContract, DbtIncrementalStrategy, DbtMaterialization, DbtUniqueKey,
    DocsConfig, FreshnessDefinition, HardDeletes, OnConfigurationChange, OnError, OnSchemaChange,
    PersistDocsConfig, SchemaOrigin, Severity, StoreFailuresAs, SyncConfig,
};
use dbt_schemas::schemas::project::SnapshotMetaColumnNames;
use dbt_schemas::schemas::properties::ModelConstraint;
use dbt_schemas::schemas::properties::ModelFreshness;
use dbt_schemas::schemas::serde::{OmissibleGrantConfig, StringOrArrayOfStrings};
use dbt_schemas::state::{NodeExecutionState, NodeExecutionStatus, NodeStatus, ResolverState};
// for representing dbt metadata in Rust

use dbt_yaml::Spanned;
use serde::{Deserialize, Serialize};

use indexmap::IndexMap;
use std::collections::{BTreeMap, HashMap};

type YmlValue = dbt_yaml::Value;

// ...and writeing Parquet files

use arrow::datatypes::{DataType, Field, Fields};

use std::sync::Arc;

use crate::file_registry::CompleteStateWithKind;
use crate::parquet_node_serde::serialize_resolver_state_to_parquet_nodes;
use crate::parquet_project::ParquetProject;

// Helper functions for JSON serialization of complex types in Parquet format
mod json_serialize {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<T, S>(value: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Serialize,
        S: Serializer,
    {
        match value {
            Some(v) => {
                let mut json_value = serde_json::to_value(v).map_err(serde::ser::Error::custom)?;

                // Add a comment indicating the structure is subject to change
                if let serde_json::Value::Object(ref mut map) = json_value {
                    map.insert(
                        "_comment".to_string(),
                        serde_json::Value::String(
                            "This JSON structure is subject to change at any time".to_string(),
                        ),
                    );
                }

                let json_str =
                    serde_json::to_string(&json_value).map_err(serde::ser::Error::custom)?;
                serializer.serialize_some(&json_str)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
    where
        T: for<'a> Deserialize<'a>,
        D: Deserializer<'de>,
    {
        let opt_str: Option<String> = Option::deserialize(deserializer)?;
        match opt_str {
            Some(s) if !s.is_empty() => {
                let value = serde_json::from_str(&s).map_err(serde::de::Error::custom)?;
                Ok(Some(value))
            }
            _ => Ok(None),
        }
    }
}

// ParquetNode schema: Fusion's universal, forward-compatible manifest/caching format.
// Each row represents a logical DBT resource (model, macro, test, etc.), or a build Session.
// Designed for fast queries, robust cache, and simple evolution.

// ResourceType enumerates all recognized node/resource types in DBT/Fusion.
// The #[default] Session variant represents the build context (invocation/session root).
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default, Eq, Hash, Copy)]
#[serde(rename_all = "lowercase")]
pub enum ResourceType {
    Model,
    Seed,
    Snapshot,
    Source,
    Test,
    UnitTest,
    Macro,
    DocsMacro,
    Analysis, // TODO: Define use-cases for Analysis resource type.
    Operation,
    SemanticModel,
    SavedQuery,
    Group,
    Exposure,
    Function,
    #[default]
    Session, // The root node for a build/session/run.
}
impl ResourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResourceType::Model => "model",
            ResourceType::Seed => "seed",
            ResourceType::Snapshot => "snapshot",
            ResourceType::Source => "source",
            ResourceType::Test => "test",
            ResourceType::UnitTest => "unittest",
            ResourceType::Macro => "macro",
            ResourceType::DocsMacro => "docs_macro",
            ResourceType::Analysis => "analysis",
            ResourceType::Operation => "operation",
            ResourceType::SemanticModel => "semantic_model",
            ResourceType::SavedQuery => "saved_query",
            ResourceType::Group => "group",
            ResourceType::Exposure => "exposure",
            ResourceType::Function => "function",
            ResourceType::Session => "session",
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum InputKind {
    #[default]
    Code, // The primary code/SQL file for the node
    Config, // dbt_project.yml, schema.yml, etc. (non-patch config)
    Macro,  // A macro file used by this node
    Seed,   // Seed CSV (.csv)
    Doc,    // Documentation file (.md)
            // Other,  // Any other kind (refine as needed)
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct InputFile {
    pub path: String, // Only project-relative or package-relative file paths (the first component of the path could be a package path)
    pub file_cas_hash: String, // Content hash for CAS/cache/dedup
    pub input_kind: InputKind, // Describes the role of the file (always required from registry)
}

// The core row for nodes.parquet.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ParquetNode {
    // ---- Identity & core fields ----
    pub resource_type: ResourceType,
    pub unique_id: String,           // e.g., "model.jaffle_shop.orders"
    pub name: String,                // Simple resource name.
    pub input_files: Vec<InputFile>, // Every file this node depends on.
    // The first one is the source path, the rest are the patch paths, the doc, etc files.
    // The original_path is either the source path,
    // or if it is a package, it is the source path except for its  first component

    // ---- Common optional attributes ----
    pub is_enabled: Option<bool>, // Not all nodes support enable/disable.
    pub package_name: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub fqn: Option<Vec<String>>,

    // ---- Physical warehouse/SQL layer ----
    pub warehouse_details: Option<WarehouseDetails>,

    // ---- Phase results for cache & diagnostics ----
    pub latest_state: Option<NodeExecutionState>, // Parsed, Compiled, Run, etc.
    pub latest_status: Option<NodeExecutionStatus>, // Success, Error, Skipped, etc.
    pub latest_time: Option<String>,
    pub latest_message: Option<String>,

    // ---- Graph/dependency information ----
    pub depends_on_nodes: Vec<String>, // unique_id of node dependencies (models, sources, etc.)
    pub depends_on_macros: Vec<String>, // unique_id of macro dependencies.
    pub tested_by: Vec<String>,        // unique_id of test nodes that test this node.

    // ---- Metadata & resource-specific payloads ----
    pub meta: Option<MetaFields>, // User/system extensibility (dbt meta, classifiers, etc.).
    pub common_details: Option<CommonDetails>, // Shared metadata for governance, lifecycle, extensibility, etc.
    pub session_details: Option<SessionDetails>, // Only populated for Session node.
    pub macro_details: Option<MacroDetails>,   // Only for resource_type == Macro.
    pub model_details: Option<ModelDetails>,   // Only for resource_type == Model.
    pub source_details: Option<SourceDetails>, // Only for resource_type == Source.
    pub seed_details: Option<SeedDetails>,     // Only for resource_type == Seed.
    pub snapshot_details: Option<SnapshotDetails>, // Only for resource_type == Snapshot.
    pub test_details: Option<TestDetails>,     // Only for resource_type == Test.
    pub unit_test_details: Option<UnitTestDetails>, // Only for resource_type == UnitTest.
    pub exposure_details: Option<ExposureDetails>,
    pub semantic_model_details: Option<SemanticModelDetails>,
    pub saved_query_details: Option<SavedQueryDetails>,
    pub group_details: Option<GroupDetails>,
    #[serde(with = "json_serialize")]
    pub user_config_details: Option<UserConfigs>, // User configuration details for all node types.
}

pub type ParquetNodeRef = Arc<ParquetNode>;

// // Per-phase diagnostic/cache info (parse, compile, run).
// #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
// pub struct PhaseInfo {
//     pub input_hash: Option<String>, // Hash of all inputs for this phase.
//     pub input_cas_hash: Option<String>, // Combined CAS hash for input files.
//     pub cas_hash: Option<String>,   // Output artifact hash.
//     pub time: Option<String>,       // ISO8601 time for phase.
//     pub status: Option<PhaseStatus>, // Success, Error, Skipped, etc.
//     pub message: Option<String>,    // Diagnostic, error, or status message.
// }

// Only present for warehouse-backed nodes (models, seeds, snapshots, sources).
#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct WarehouseDetails {
    pub catalog: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub alias: Option<String>,
    pub relation_name: Option<String>, // Fully-qualified warehouse object.
    pub materialized: Option<DbtMaterialization>,
    // Compact encoding for quoting: 'q' = quoted, 'n' = not quoted. E.g., "q_q" for database/schema.
    pub quoting: Option<String>,
    pub static_analysis: Option<Spanned<StaticAnalysisKind>>,
    pub extended_model: Option<bool>,
}

// Cross-resource, governance, and rarely-indexed fields.
// Good place for access control, grants, freshness, meta, and hooks.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct CommonDetails {
    pub access: Option<Access>,       // Access control or classification.
    pub group: Option<String>,        // Group/owner.
    pub grants: Vec<String>,          // DB grants, if applicable.
    pub extended_model: Option<bool>, // True if model extends another model.
    pub hooks: Option<Hooks>,         // pre/post/session hooks.
}

// Hooks for lifecycle (before/after execution, for node or session).
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct Hooks {
    pub run_before: Vec<String>, // SQL or macro calls before node/session.
    pub run_before_as_transaction: Option<bool>, // Whether to run in transaction.
    pub run_after: Vec<String>,  // SQL or macro calls after node/session.
    pub run_after_as_transaction: Option<bool>,
}

// Unified, type-safe meta fields for extensibility.
// Matches dbt's meta, but allows multiple types.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct MetaFields {
    pub meta_str: Option<HashMap<String, String>>,
    pub meta_bool: Option<HashMap<String, bool>>,
    pub meta_num: Option<HashMap<String, f64>>,
    pub meta_blob: Option<HashMap<String, String>>,
    pub meta_list: Option<HashMap<String, Vec<String>>>,
}
// Meta key names use the following (prefix) conventions:
// - "foo": "bar" // User-defined -- no prefix
// - "__system_owner": "svc-account" // System-reserved -- __ prefix
// - "$pii": "true" // Policy/classification -- $ prefix

// Only set for the session/build root node.
// Contains everything needed to reconstruct the full run context.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct SessionDetails {
    // IDs (for robust joins and traceability)
    pub org_id: Option<String>,
    pub account_id: Option<String>,
    pub project_id: Option<String>,
    pub environment_id: Option<String>,
    pub user_id: Option<String>,

    // Names (for human readability)
    pub org_name: Option<String>,
    pub account_name: Option<String>,
    pub project_name: Option<String>,
    pub environment_name: Option<String>,
    pub user_name: Option<String>,

    // Command/run invocation
    pub invocation_id: Option<String>, // this should become a trace id
    pub invocation_started_at: Option<String>, // ISO8601
    pub generated_at: Option<String>,  // ISO8601, maybe skip...

    // Arguments for the run
    pub command: Option<String>, // relative path to packages installed by dbt, relative to the project root
    pub cli_args: Option<Vec<String>>,
    pub packages_install_path: Option<String>, // relative path to packages installed by dbt, relative to the project root
    pub target_dir: Option<String>, // relative path to packages installed by dbt, relative to the project root

    // Classic dbt config context
    pub profile_name: Option<String>, // dbt profile used
    pub target: Option<String>,       // dbt target/environment block used
    pub profile_hash: Option<String>, // Hash of rendered/selected profile block
    pub adapter_type: Option<String>,
    pub quoting: Option<String>,

    //  Environment context
    pub env_vars: Option<HashMap<String, String>>,
    pub env_vars_hash: Option<String>, // Hash of relevant env vars
    // pub vars: Option<HashMap<String, Value>>, // User-provided vars -- stored in meta
    // User-provided vars can be stored in meta.meta_blob or here in future

    // Versioning
    pub dbt_version: Option<String>,
    pub parquet_version: Option<String>, // Version of the Parquet schema used, start with 1.0
}

// Versioning rules for parquet

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct SourceFreshnessDetails {
    pub error_after_count: Option<i64>,
    pub error_after_period: Option<String>,
    pub warn_after_count: Option<i64>,
    pub warn_after_period: Option<String>,
    pub filter: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct ModelFreshnessDetails {
    pub build_after_count: Option<i64>,
    pub build_after_period: Option<String>,
    pub build_after_updates_on: Option<String>,
}

// ----- Resource-specific details below -----
// (All fields are Option or Vec so only the relevant struct is set for each node type.)

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct TestDetails {
    pub column_name: Option<String>,
    pub attached_node: Option<String>,
    pub test_metadata_name: Option<String>,
    pub test_metadata_namespace: Option<String>,
    pub file_key_name: Option<String>,
    pub introspection: IntrospectionKind,
    pub defined_at: Option<DefinedAtSpan>,
}

/// YAML span for a generic test's declaration. Persisted so warm-start reloads
/// can rebuild `defined_at` without losing the line/col/index.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct DefinedAtSpan {
    pub file: String,
    pub line: u32,
    pub col: u32,
    pub index: u32,
}

impl From<&CodeLocationWithFile> for DefinedAtSpan {
    fn from(loc: &CodeLocationWithFile) -> Self {
        Self {
            file: loc.file.to_string_lossy().into_owned(),
            line: loc.line,
            col: loc.col,
            index: loc.index,
        }
    }
}

impl From<&DefinedAtSpan> for CodeLocationWithFile {
    fn from(span: &DefinedAtSpan) -> Self {
        CodeLocationWithFile::new(
            span.line,
            span.col,
            span.index,
            std::path::PathBuf::from(&span.file),
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct MacroDetails {
    pub macro_sql_cas_hash: Option<String>,
    pub func_signature: Option<String>,
    pub arguments: Vec<MacroArgumentDetails>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct MacroArgumentDetails {
    pub name: String,
    pub is_optional: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct SeedDetails {
    pub quote_columns: Option<bool>, // Quoting config for seed columns
    pub column_types: Option<BTreeMap<Spanned<String>, String>>,
    pub delimiter: Option<String>,
    pub root_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct SourceDetails {
    pub identifier: String,
    pub source_name: String,
    pub loader: String,
    pub loaded_at_field: Option<String>,
    pub loaded_at_query: Option<String>,
    pub freshness_details: Option<SourceFreshnessDetails>, // Freshness/quality config.
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct UnitTestDetails {
    pub model: String,
    pub given_count: usize,            // Number of given cases
    pub expect_format: Option<String>, // e.g., "dict", "csv", "sql"
    pub expect_fixture: Option<String>,
    pub version: Option<String>,
    pub versions_include: Vec<String>,
    pub versions_exclude: Vec<String>,
    #[serde(rename = "_event_status")]
    pub field_event_status: Option<String>,
    #[serde(rename = "_pre_injected_sql")]
    pub field_pre_injected_sql: Option<String>,
    pub overrides: Option<String>,
    pub tested_node_unique_id: Option<String>,
    pub this_input_node_unique_id: Option<String>,
    pub given: Option<String>,
    pub expect: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct SnapshotDetails {
    pub dbt_scd_id: String,
    pub dbt_updated_at: String,
    pub dbt_valid_from: String,
    pub dbt_valid_to: String,
    pub dbt_is_deleted: String,
    pub compiled_sql_cas_hash: Option<String>,
    pub introspection: IntrospectionKind,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct ExposureDetails {
    pub owner_name: Option<String>,
    pub owner_emails: Vec<String>,
    pub label: Option<String>,
    pub maturity: Option<String>,
    pub exposure_type: String,
    pub url: Option<String>,
    pub unrendered_config: Option<String>,
    pub created_at: Option<f64>,
    pub root_path: Option<String>,
    pub original_file_path: String,
    pub name: String,
    pub description: Option<String>,
    pub depends_on: Option<String>,
    pub refs: Option<String>,
    pub sources: Option<String>,
    pub meta: Option<String>,
    pub tags: Option<Vec<Option<String>>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct GroupDetails {
    pub owner_name: Option<String>,
    pub owner_emails: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct SemanticModelDetails {
    pub resource_type: ResourceType,
    pub package_name: String,
    pub model: String,
    pub label: Option<String>,
    pub defaults: Option<String>,
    pub entities: Option<String>,
    pub dimensions: Option<String>,
    pub measures: Option<String>,
    pub metadata: Option<String>,
    pub primary_entity: Option<String>,
    pub node_relation: Option<String>,
    pub time_spine: Option<String>,
    pub refs: Option<String>,
    pub group: Option<String>,
    pub created_at: Option<f64>,
    pub unrendered_config: Option<String>,
    pub description: Option<String>,
    pub depends_on: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct SavedQueryDetails {
    pub label: Option<String>,
    pub group: Option<String>,
    pub description: Option<String>,
    pub query_params: Option<String>,
    pub exports: Option<String>,
    pub metadata: Option<String>,
    pub depends_on: Option<String>,
    pub refs: Option<String>,
    pub created_at: Option<f64>,
    pub unrendered_config: Option<String>,
    pub config: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ModelDetails {
    pub introspection: Option<IntrospectionKind>, // How was this model introspected/generated?
    pub compiled_sql_cas_hash: Option<String>,    // Hash of the compiled SQL
    pub contract_alias_types: Option<bool>,       // Whether contract alias types are enforced
    pub contract_enforced: Option<bool>,          // Whether contracts are enforced for the model
    pub contract_checksum: Option<String>,        // Hash of contract fields/config
    pub incremental_strategy: Option<DbtIncrementalStrategy>,
    pub version: Option<String>,
    pub latest_version: Option<String>,
    pub deprecation_date: Option<String>,
    pub primary_key: Vec<String>,
    pub constraints: Vec<ModelConstraint>,
    pub freshness: Option<ModelFreshnessDetails>,
    pub event_time: Option<String>, // For time-based incremental models
                                    // time_spine: Option<Value>,                // Uncomment/add as needed
}

/// A unified configuration struct that contains all fields from all node config types
/// This enables full round-trip serialization/deserialization of node configurations
/// Note: Fields that overlap with WarehouseDetails (database, schema, alias, materialized, quoting, static_analysis)
/// are excluded since they're already serialized in warehouse_details
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct UserConfigs {
    // Common fields across multiple configs (excluding those in WarehouseDetails)
    pub enabled: Option<bool>,
    pub tags: Option<StringOrArrayOfStrings>,
    pub meta: Option<IndexMap<String, YmlValue>>,
    pub group: Option<String>,
    pub description: Option<String>,
    pub event_time: Option<String>,

    // ModelConfig specific fields (excluding those in WarehouseDetails)
    pub catalog_name: Option<String>,
    pub incremental_strategy: Option<DbtIncrementalStrategy>,
    pub incremental_predicates: Option<Vec<String>>,
    pub batch_size: Option<DbtBatchSize>,
    pub lookback: Option<i32>,
    pub begin: Option<String>,
    pub persist_docs: Option<PersistDocsConfig>,
    pub post_hook: Option<Hooks>,
    pub pre_hook: Option<Hooks>,
    pub column_types: Option<BTreeMap<Spanned<String>, String>>,
    pub full_refresh: Option<bool>,
    pub unique_key: Option<DbtUniqueKey>,
    pub on_schema_change: Option<OnSchemaChange>,
    pub on_configuration_change: Option<OnConfigurationChange>,
    pub on_error: Option<OnError>,
    pub grants: OmissibleGrantConfig,
    pub packages: Option<StringOrArrayOfStrings>,
    pub python_version: Option<String>,
    pub imports: Option<StringOrArrayOfStrings>,
    pub secrets: Option<BTreeMap<String, YmlValue>>,
    pub external_access_integrations: Option<StringOrArrayOfStrings>,
    pub use_anonymous_sproc: Option<bool>,
    pub docs: Option<DocsConfig>,
    pub contract: Option<DbtContract>,
    pub concurrent_batches: Option<bool>,
    pub merge_update_columns: Option<StringOrArrayOfStrings>,
    pub merge_exclude_columns: Option<StringOrArrayOfStrings>,
    pub access: Option<Access>,
    pub table_format: Option<String>,
    pub freshness: Option<ModelFreshness>,
    pub sql_header: Option<String>,
    pub location: Option<String>,
    pub predicates: Option<Vec<String>>,
    pub submission_method: Option<String>,
    pub job_cluster_config: Option<BTreeMap<String, YmlValue>>,
    pub python_job_config: Option<BTreeMap<String, YmlValue>>,
    pub cluster_id: Option<String>,
    pub http_path: Option<String>,
    pub create_notebook: Option<bool>,
    pub index_url: Option<String>,
    pub additional_libs: Option<Vec<YmlValue>>,
    pub user_folder_for_python: Option<bool>,

    // SeedConfig specific fields
    pub quote_columns: Option<bool>,
    pub delimiter: Option<String>,

    // SnapshotConfig specific fields (excluding those in WarehouseDetails like materialized)
    pub target_database: Option<String>,
    pub target_schema: Option<String>,
    pub strategy: Option<String>,
    pub check_cols: Option<StringOrArrayOfStrings>,
    pub updated_at: Option<String>,
    pub dbt_valid_to_current: Option<String>,
    pub snapshot_meta_column_names: Option<SnapshotMetaColumnNames>,
    pub hard_deletes: Option<HardDeletes>,
    pub invalidate_hard_deletes: Option<bool>,

    // SourceConfig specific fields
    pub source_freshness: Option<FreshnessDefinition>,
    pub loaded_at_field: Option<String>,
    pub loaded_at_query: Option<String>,
    /// Specifies where the schema metadata originates: 'remote' (default) or 'local'
    pub schema_origin: Option<SchemaOrigin>,
    /// Schema synchronization configuration
    pub sync: Option<SyncConfig>,

    // DataTestConfig specific fields
    pub error_if: Option<String>,
    pub fail_calc: Option<String>,
    pub limit: Option<i32>,
    pub severity: Option<Severity>,
    pub store_failures: Option<bool>,
    pub store_failures_as: Option<StoreFailuresAs>,
    pub warn_if: Option<String>,
    pub where_: Option<String>,

    // Warehouse-specific configuration (shared by all config types) - serialized as JSON string
    pub warehouse_specific_config: Option<String>,
}

pub fn determine_resource_type(unique_id: &str) -> ResourceType {
    let prefix = unique_id.split('.').next().unwrap_or_default();
    match prefix {
        "model" => ResourceType::Model,
        "seed" => ResourceType::Seed,
        "snapshot" => ResourceType::Snapshot,
        "source" => ResourceType::Source,
        "test" => ResourceType::Test,
        "unit_test" => ResourceType::UnitTest,
        "macro" => ResourceType::Macro,
        "operation" => ResourceType::Operation,
        "exposure" => ResourceType::Exposure,
        "semantic_model" => ResourceType::SemanticModel,
        "saved_query" => ResourceType::SavedQuery,
        "group" => ResourceType::Group,
        "command" => ResourceType::Session,
        "analysis" => ResourceType::Analysis,
        "function" => ResourceType::Function,
        _ => ResourceType::Model, // Default fallback
    }
}
// ------------------------------------------------------------------------------------------------
// Write Helper Context

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct WriteContext {
    pub phase: NodeExecutionState,
    pub is_enabled: Option<bool>, // true if the node is enabled, false if disabled
    // pub datetime_utc: DateTime<Utc>,
    pub datetime_str: String, // short ISO8601 string representation: "2025-07-21T01:25:45"

                              // for inverse use:
                              // let naive = NaiveDateTime::parse_from_str(datetime_str, "%Y-%m-%dT%H:%M:%S")?;
                              // let datetime_utc = Utc.from_utc_datetime(&naive);
}

impl WriteContext {
    pub fn new(phase: NodeExecutionState) -> Self {
        WriteContext {
            phase,
            is_enabled: Some(false),
            // todo: if higher reolsution is needed chnage top SecondsFormat::Millis
            datetime_str: Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
        }
    }
    pub fn with_enabled(self: &WriteContext, is_enabled: bool) -> Self {
        WriteContext {
            is_enabled: Some(is_enabled),
            datetime_str: self.datetime_str.clone(),
            phase: self.phase,
        }
    }

    fn with_status(self, status: NodeExecutionState) -> WriteContext {
        WriteContext {
            phase: status,
            ..self
        }
    }
}
// ------------------------------------------------------------------------------------------------
// parquet Node
impl ParquetNode {
    pub fn new(
        resource_type: ResourceType,
        unique_id: String,
        name: String,
        fqn: Option<Vec<String>>,
    ) -> Self {
        ParquetNode {
            resource_type,
            unique_id,
            name,
            fqn,
            ..Default::default()
        }
    }

    pub fn with_ctx(self, ctx: WriteContext) -> Self {
        let WriteContext {
            is_enabled,
            phase,
            datetime_str,
        } = ctx;
        Self {
            is_enabled,
            latest_state: Some(phase),
            latest_time: Some(datetime_str),
            ..self
        }
    }
    pub fn _with_ctx(self, ctx: &WriteContext, cmd: FsCommand) -> Self {
        let ctx = ctx.clone();

        let state = match (cmd, &self.resource_type) {
            // Seed command
            (FsCommand::Seed, ResourceType::Seed) => NodeExecutionState::from_cmd(cmd),

            // Run command
            (FsCommand::Run, ResourceType::Model | ResourceType::Source) => {
                NodeExecutionState::from_cmd(cmd)
            }

            // Test command
            (FsCommand::Test, ResourceType::Test | ResourceType::UnitTest) => {
                NodeExecutionState::from_cmd(cmd)
            }

            // Snapshot command
            (FsCommand::Snapshot, ResourceType::Snapshot) => NodeExecutionState::from_cmd(cmd),

            // Build command (multiple resource types)
            (
                FsCommand::Build,
                ResourceType::Seed
                | ResourceType::Source
                | ResourceType::Test
                | ResourceType::UnitTest
                | ResourceType::Model,
            ) => NodeExecutionState::from_cmd(cmd),

            // Compile command
            (
                FsCommand::Compile,
                ResourceType::Model
                | ResourceType::Source
                | ResourceType::Seed
                | ResourceType::Test
                | ResourceType::UnitTest,
            ) => NodeExecutionState::from_cmd(cmd),

            // Parse command (most resource types)
            (
                FsCommand::Parse,
                ResourceType::Session
                | ResourceType::Model
                | ResourceType::Seed
                | ResourceType::Snapshot
                | ResourceType::Test
                | ResourceType::Source
                | ResourceType::Macro
                | ResourceType::UnitTest
                | ResourceType::DocsMacro
                | ResourceType::SemanticModel
                | ResourceType::SavedQuery
                | ResourceType::Group
                | ResourceType::Exposure
                | ResourceType::Analysis,
            ) => NodeExecutionState::from_cmd(cmd),

            // Parse is special - if it's "parse" but not a matching resource type, return self unchanged
            (FsCommand::Parse, _) => return self,

            // For all other cases, default to Parsed state
            _ => NodeExecutionState::Parsed,
        };

        self.with_ctx(ctx.with_status(state))
    }
}

// ------------------------------------------------------------------------------------------------
// used also by columns.parquet
pub fn generate_meta_fields() -> Vec<Field> {
    // MetaFields struct
    vec![
        Field::new("meta_str", generate_map_type(DataType::Utf8), true),
        Field::new("meta_bool", generate_map_type(DataType::Boolean), true),
        Field::new("meta_num", generate_map_type(DataType::Float64), true),
        Field::new("meta_blob", generate_map_type(DataType::Utf8), true),
        Field::new(
            "meta_list",
            generate_map_type(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            )))),
            true,
        ),
    ]
}
// used also by columns.parquet
pub fn generate_constraints_fields() -> Vec<Field> {
    vec![
        Field::new("type", DataType::Utf8, false),
        Field::new("expression", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("to", DataType::Utf8, true),
        Field::new("to_columns", generate_list_type(DataType::Utf8), true),
        Field::new("columns", generate_list_type(DataType::Utf8), true),
        Field::new("warn_unsupported", DataType::Boolean, true),
        Field::new("warn_unenforced", DataType::Boolean, true),
    ]
}

pub(crate) fn generate_pnode_arrow_schema() -> Vec<Field> {
    // InputFile struct
    let input_file_fields = Fields::from(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("file_cas_hash", DataType::Utf8, false),
        Field::new("input_kind", DataType::Utf8, true),
    ]);

    // WarehouseDetails struct
    let warehouse_details_fields = Fields::from(vec![
        Field::new("catalog", DataType::Utf8, true),
        Field::new("database", DataType::Utf8, true),
        Field::new("schema", DataType::Utf8, true),
        Field::new("alias", DataType::Utf8, true),
        Field::new("relation_name", DataType::Utf8, true),
        Field::new("materialized", DataType::Utf8, true),
        Field::new("quoting", DataType::Utf8, true),
        Field::new("static_analysis", DataType::Utf8, true),
        Field::new("extended_model", DataType::Boolean, true),
    ]);

    // // PhaseInfo struct
    // let phase_info_fields = Fields::from(vec![
    //     Field::new("input_hash", DataType::Utf8, true),
    //     Field::new("input_cas_hash", DataType::Utf8, true),
    //     Field::new("cas_hash", DataType::Utf8, true),
    //     Field::new("time", DataType::Utf8, true),
    //     Field::new("status", DataType::Utf8, true),
    //     Field::new("message", DataType::Utf8, true),
    // ]);

    // MetaFields struct
    let meta_fields = Fields::from(generate_meta_fields());

    // Hooks struct
    let hooks_fields = Fields::from(vec![
        Field::new("run_before", generate_list_type(DataType::Utf8), true),
        Field::new("run_before_as_transaction", DataType::Boolean, true),
        Field::new("run_after", generate_list_type(DataType::Utf8), true),
        Field::new("run_after_as_transaction", DataType::Boolean, true),
    ]);
    // CommonDetails struct
    let common_details_fields = Fields::from(vec![
        Field::new("access", DataType::Utf8, true),
        Field::new("group", DataType::Utf8, true),
        Field::new(
            "grants",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("hooks", DataType::Struct(hooks_fields), true),
    ]);

    // SessionDetails struct
    let session_details_fields = Fields::from(vec![
        Field::new("org_id", DataType::Utf8, true),
        Field::new("account_id", DataType::Utf8, true),
        Field::new("project_id", DataType::Utf8, true),
        Field::new("environment_id", DataType::Utf8, true),
        Field::new("user_id", DataType::Utf8, true),
        Field::new("org_name", DataType::Utf8, true),
        Field::new("account_name", DataType::Utf8, true),
        Field::new("project_name", DataType::Utf8, true),
        Field::new("environment_name", DataType::Utf8, true),
        Field::new("user_name", DataType::Utf8, true),
        Field::new("invocation_id", DataType::Utf8, true),
        Field::new("invocation_started_at", DataType::Utf8, true),
        Field::new("generated_at", DataType::Utf8, true),
        Field::new("command", DataType::Utf8, true),
        Field::new("cli_args", generate_list_type(DataType::Utf8), true),
        Field::new("packages_install_path", DataType::Utf8, true),
        Field::new("target_dir", DataType::Utf8, true),
        Field::new("profile_name", DataType::Utf8, true),
        Field::new("target", DataType::Utf8, true),
        Field::new("profile_hash", DataType::Utf8, true),
        Field::new("adapter_type", DataType::Utf8, true),
        Field::new("quoting", DataType::Utf8, true),
        Field::new("env_vars", generate_map_type(DataType::Utf8), true),
        Field::new("env_vars_hash", DataType::Utf8, true),
        Field::new("dbt_version", DataType::Utf8, true),
        Field::new("parquet_version", DataType::Utf8, true),
    ]);
    let macro_argument_fields = Fields::from(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("is_optional", DataType::Boolean, false),
    ]);
    let macro_details_fields = Fields::from(vec![
        Field::new("macro_sql_cas_hash", DataType::Utf8, true),
        Field::new("func_signature", DataType::Utf8, true),
        Field::new(
            "arguments",
            generate_list_type(DataType::Struct(macro_argument_fields)),
            true,
        ),
    ]);

    let model_freshness_details = Fields::from(vec![
        Field::new("build_after_count", DataType::Int64, true),
        Field::new("build_after_period", DataType::Utf8, true),
    ]);

    let constraint_details = Fields::from(generate_constraints_fields());

    let model_details_fields = Fields::from(vec![
        Field::new("introspection", DataType::Utf8, true),
        Field::new("compiled_sql_cas_hash", DataType::Utf8, true),
        Field::new("contract_alias_types", DataType::Boolean, true),
        Field::new("contract_enforced", DataType::Boolean, true),
        Field::new("contract_checksum", DataType::Utf8, true),
        Field::new("incremental_strategy", DataType::Utf8, true),
        Field::new("version", DataType::Utf8, true),
        Field::new("latest_version", DataType::Utf8, true),
        Field::new("deprecation_date", DataType::Utf8, true),
        Field::new(
            "primary_key",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "constraints",
            generate_list_type(DataType::Struct(constraint_details)),
            true,
        ), // Store as JSON string
        Field::new("freshness", DataType::Struct(model_freshness_details), true),
        Field::new("event_time", DataType::Utf8, true),
    ]);

    let source_freshness_details = Fields::from(vec![
        Field::new("error_after_count", DataType::Int64, true),
        Field::new("error_after_period", DataType::Utf8, true),
        Field::new("warn_after_count", DataType::Int64, true),
        Field::new("warn_after_period", DataType::Utf8, true),
        Field::new("filter", DataType::Utf8, true),
    ]);
    let source_details_fields = Fields::from(vec![
        Field::new("identifier", DataType::Utf8, false),
        Field::new("source_name", DataType::Utf8, false),
        Field::new("loader", DataType::Utf8, false),
        Field::new("loaded_at_field", DataType::Utf8, true),
        Field::new("loaded_at_query", DataType::Utf8, true),
        Field::new(
            "freshness_details",
            DataType::Struct(source_freshness_details),
            true,
        ),
    ]);

    let seed_details_fields = Fields::from(vec![
        Field::new("quote_columns", DataType::Boolean, true),
        Field::new("column_types", generate_map_type(DataType::Utf8), true),
        Field::new("delimiter", DataType::Utf8, true),
        Field::new("root_path", DataType::Utf8, true),
        Field::new("values", DataType::Utf8, true),
    ]);

    let snapshot_details_fields = Fields::from(vec![
        Field::new("dbt_scd_id", DataType::Utf8, true),
        Field::new("dbt_updated_at", DataType::Utf8, true),
        Field::new("dbt_valid_from", DataType::Utf8, true),
        Field::new("dbt_valid_to", DataType::Utf8, true),
        Field::new("dbt_is_deleted", DataType::Utf8, true),
        Field::new("compiled_sql_cas_hash", DataType::Utf8, true),
    ]);

    let test_details_fields = Fields::from(vec![
        Field::new("column_name", DataType::Utf8, true),
        Field::new("attached_node", DataType::Utf8, true),
        Field::new("test_metadata_name", DataType::Utf8, true),
        Field::new("test_metadata_namespace", DataType::Utf8, true),
        Field::new("file_key_name", DataType::Utf8, true),
    ]);

    let unit_test_details_fields = Fields::from(vec![
        Field::new("model", DataType::Utf8, false),
        Field::new("given_count", DataType::UInt64, false),
        Field::new("expect_format", DataType::Utf8, true),
        Field::new("expect_fixture", DataType::Utf8, true),
        Field::new("version", DataType::Utf8, true),
        Field::new(
            "versions_include",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "versions_exclude",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("_event_status", DataType::Utf8, true),
        Field::new("_pre_injected_sql", DataType::Utf8, true),
        Field::new("given", DataType::Utf8, true),
        Field::new("expect", DataType::Utf8, true),
    ]);

    let exposure_details_fields = Fields::from(vec![
        Field::new("owner_name", DataType::Utf8, true),
        Field::new(
            "owner_emails",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("label", DataType::Utf8, true),
        Field::new("maturity", DataType::Utf8, true),
        Field::new("exposure_type", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, true),
        Field::new("unrendered_config", DataType::Utf8, true),
        Field::new("created_at", DataType::Float64, true),
        Field::new("root_path", DataType::Utf8, true),
        Field::new("original_file_path", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("depends_on", DataType::Utf8, true),
        Field::new("refs", DataType::Utf8, true),
        Field::new("sources", DataType::Utf8, true),
        Field::new("meta", DataType::Utf8, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]);

    let group_details_fields = Fields::from(vec![
        Field::new("owner_name", DataType::Utf8, true),
        Field::new(
            "owner_emails",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]);

    let semantic_model_details_fields = Fields::from(vec![
        Field::new("resource_type", DataType::Utf8, false),
        Field::new("package_name", DataType::Utf8, false),
        Field::new("model", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("defaults", DataType::Utf8, true),
        Field::new("entities", DataType::Utf8, true),
        Field::new("dimensions", DataType::Utf8, true),
        Field::new("measures", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("primary_entity", DataType::Utf8, true),
        Field::new("node_relation", DataType::Utf8, true),
        Field::new("time_spine", DataType::Utf8, true),
        Field::new("refs", DataType::Utf8, true),
        Field::new("group", DataType::Utf8, true),
        Field::new("created_at", DataType::Float64, true),
        Field::new("unrendered_config", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("depends_on", DataType::Utf8, true),
    ]);

    let saved_query_details_fields = Fields::from(vec![
        Field::new("label", DataType::Utf8, true),
        Field::new("group", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("query_params", DataType::Utf8, true),
        Field::new("exports", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("depends_on", DataType::Utf8, true),
        Field::new("refs", DataType::Utf8, true),
        Field::new("created_at", DataType::Float64, true),
        Field::new("unrendered_config", DataType::Utf8, true),
        Field::new("config", DataType::Utf8, true),
    ]);

    let schema_fields = vec![
        // ---- Identity & core fields ----
        Field::new("resource_type", DataType::Utf8, false),
        Field::new("unique_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "input_files",
            generate_list_type(generate_struct_type(input_file_fields)),
            false,
        ),
        // ---- Common optional attributes ----
        Field::new("is_enabled", DataType::Boolean, true),
        Field::new("package_name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("tags", generate_list_type(DataType::Utf8), true),
        Field::new("fqn", generate_list_type(DataType::Utf8), true),
        // ---- Physical warehouse/SQL layer ----
        Field::new(
            "warehouse_details",
            DataType::Struct(warehouse_details_fields),
            true,
        ),
        // ---- Phase results for cache & diagnostics ----
        Field::new("latest_state", DataType::Utf8, true),
        Field::new("latest_status", DataType::Utf8, true),
        Field::new("latest_time", DataType::Utf8, true),
        Field::new("latest_message", DataType::Utf8, true),
        // ---- Graph/dependency information ----
        Field::new("depends_on_nodes", generate_list_type(DataType::Utf8), true),
        Field::new(
            "depends_on_macros",
            generate_list_type(DataType::Utf8),
            true,
        ),
        Field::new("tested_by", generate_list_type(DataType::Utf8), true),
        // ---- Metadata & resource-specific payloads ----
        Field::new(
            "common_details",
            DataType::Struct(common_details_fields),
            true,
        ),
        Field::new("meta", DataType::Struct(meta_fields), true),
        Field::new(
            "session_details",
            DataType::Struct(session_details_fields),
            true,
        ),
        Field::new(
            "macro_details",
            DataType::Struct(macro_details_fields),
            true,
        ),
        Field::new(
            "model_details",
            DataType::Struct(model_details_fields),
            true,
        ),
        Field::new(
            "source_details",
            DataType::Struct(source_details_fields),
            true,
        ),
        Field::new("seed_details", DataType::Struct(seed_details_fields), true),
        Field::new(
            "snapshot_details",
            DataType::Struct(snapshot_details_fields),
            true,
        ),
        Field::new("test_details", DataType::Struct(test_details_fields), true),
        Field::new(
            "unit_test_details",
            DataType::Struct(unit_test_details_fields),
            true,
        ),
        Field::new(
            "exposure_details",
            DataType::Struct(exposure_details_fields),
            true,
        ),
        Field::new(
            "semantic_model_details",
            DataType::Struct(semantic_model_details_fields),
            true,
        ),
        Field::new(
            "saved_query_details",
            DataType::Struct(saved_query_details_fields),
            true,
        ),
        Field::new(
            "group_details",
            DataType::Struct(group_details_fields),
            true,
        ),
        Field::new(
            "user_config_details",
            DataType::Utf8, // Now serialized as JSON string
            true,
        ),
    ];

    // No columns struct anymore, so just return empty Fields for columns
    schema_fields
}

// ------------------------------------------------------------------------------------------------

pub(crate) fn collect_projects(
    resolver_state: &ResolverState,
    cloud_config: &Option<ResolvedCloudConfig>,
) -> Vec<ParquetProject> {
    use std::collections::BTreeMap;

    let mut projects: BTreeMap<String, ParquetProject> = BTreeMap::new();
    let root_project_name = resolver_state.root_project_name.clone();
    let project_id = cloud_config.as_ref().and_then(|c| c.project_id.clone());
    let environment_id = cloud_config.as_ref().and_then(|c| c.environment_id.clone());
    projects.insert(
        root_project_name.clone(),
        ParquetProject {
            project_name: root_project_name.clone(),
            project_id,
            environment_id,
        },
    );

    let mut register_package = |package_name: &str| {
        if package_name.is_empty() || package_name == root_project_name {
            return;
        }
        projects
            .entry(package_name.to_string())
            .or_insert_with(|| ParquetProject {
                project_name: package_name.to_string(),
                project_id: None,
                environment_id: None,
            });
    };

    let mut collect_from_nodes = |nodes: &dbt_schemas::schemas::Nodes| {
        for node in nodes.models.values() {
            register_package(&node.__common_attr__.package_name);
        }
        for node in nodes.tests.values() {
            register_package(&node.__common_attr__.package_name);
        }
        for node in nodes.snapshots.values() {
            register_package(&node.__common_attr__.package_name);
        }
        for node in nodes.seeds.values() {
            register_package(&node.__common_attr__.package_name);
        }
        for node in nodes.sources.values() {
            register_package(&node.__common_attr__.package_name);
        }
        for node in nodes.unit_tests.values() {
            register_package(&node.__common_attr__.package_name);
        }
        for node in nodes.analyses.values() {
            register_package(&node.__common_attr__.package_name);
        }
        for node in nodes.exposures.values() {
            register_package(&node.__common_attr__.package_name);
        }
        for node in nodes.semantic_models.values() {
            register_package(&node.__common_attr__.package_name);
        }
        for node in nodes.saved_queries.values() {
            register_package(&node.__common_attr__.package_name);
        }
        for node in nodes.groups.values() {
            register_package(&node.__common_attr__.package_name);
        }
    };

    collect_from_nodes(&resolver_state.nodes);
    collect_from_nodes(&resolver_state.disabled_nodes);

    for macro_node in resolver_state.macros.macros.values() {
        register_package(&macro_node.package_name);
    }
    for docs_macro in resolver_state.macros.docs_macros.values() {
        register_package(&docs_macro.package_name);
    }

    projects.into_values().collect()
}

/// Read nodes metadata from a Parquet file at the given path.
pub fn read_nodes(io: &IoArgs) -> FsResult<Vec<ParquetNodeRef>> {
    read_parquet_file::<ParquetNodeRef>(io, "nodes.parquet", NODES_RD)
}

/// Generate `ParquetNode` rows and the CAS map without performing any writes.
#[allow(clippy::too_many_arguments)]
pub fn generate_parquet_nodes(
    io: &IoArgs,
    ctx: &WriteContext,
    eval_args: &EvalArgs,
    resolver_state: &ResolverState,
    registry: &CompleteStateWithKind,
    unchanged_node_statuses: &HashMap<String, NodeStatus>,
    cloud_config: &Option<ResolvedCloudConfig>,
) -> FsResult<(Vec<ParquetNodeRef>, HashMap<String, String>)> {
    // Pre-build CAS with all source files from the registry
    let mut cas = registry.build_cas(io)?;

    // Generate parquet_nodes
    let parquet_nodes = serialize_resolver_state_to_parquet_nodes(
        io,
        ctx,
        &mut cas,
        eval_args,
        resolver_state,
        registry,
        cloud_config,
    )?;

    let mut parquet_node_refs = Vec::new();

    // Apply unchanged node statuses if provided
    for mut node in parquet_nodes.into_iter() {
        if let Some(new_status) = unchanged_node_statuses.get(&node.unique_id) {
            node.latest_state = new_status.latest_state;
            node.latest_status = new_status.latest_status.clone();
            node.latest_time = new_status.latest_time.clone();
            node.latest_message = new_status.latest_message.clone();
        }
        parquet_node_refs.push(Arc::new(node));
    }

    Ok((parquet_node_refs, cas))
}

// ------------------------------------------------------------------------------------------------
// Helper API use of nodes

// give me all input_files.
pub fn get_all_input_files(nodes: &[ParquetNodeRef]) -> Vec<InputFile> {
    nodes.iter().flat_map(|n| n.input_files.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_test_primitives::assert_contains;
    use serde_arrow::to_record_batch;

    #[test]
    fn test_user_configs_json_serialization() {
        // Test that UserConfigs with complex types are serialized as JSON strings
        let user_configs = UserConfigs {
            post_hook: Some(Hooks {
                run_before: vec!["SELECT 1".to_string()],
                run_before_as_transaction: Some(true),
                run_after: vec!["SELECT 2".to_string()],
                ..Default::default()
            }),
            persist_docs: Some(PersistDocsConfig {
                relation: Some(true),
                columns: Some(false),
            }),
            batch_size: Some(DbtBatchSize::Day),
            unique_key: Some(DbtUniqueKey::Single("id".to_string())),
            ..Default::default()
        };

        // Create a ParquetNode with the user_config_details
        let node = ParquetNode {
            resource_type: ResourceType::Model,
            unique_id: "model.test.example".to_string(),
            name: "example".to_string(),
            user_config_details: Some(user_configs),
            ..Default::default()
        };

        // Test JSON serialization directly
        let serialized = serde_json::to_string(&node).unwrap();

        // The user_config_details should be serialized as a JSON string
        assert_contains!(serialized, "user_config_details");

        // Deserialize back to verify round-trip
        let deserialized: ParquetNode = serde_json::from_str(&serialized).unwrap();
        assert_eq!(node.unique_id, deserialized.unique_id);
        assert!(deserialized.user_config_details.is_some());

        // Test Arrow schema compatibility
        let schema = generate_pnode_arrow_schema();
        let schema_fields: Vec<Arc<Field>> = schema.into_iter().map(Arc::new).collect();
        let result = to_record_batch(&schema_fields, &vec![node]);

        // This should succeed with JSON serialization
        assert!(
            result.is_ok(),
            "Failed to serialize ParquetNode with JSON user_config_details: {:?}",
            result.err()
        );

        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_json_serialize_module() {
        // Test the json_serialize module behavior with UserConfigs
        let user_configs = UserConfigs {
            enabled: Some(true),
            post_hook: Some(Hooks {
                run_before: vec!["SELECT 1".to_string(), "SELECT 2".to_string()],
                run_before_as_transaction: Some(true),
                run_after: vec!["SELECT 3".to_string()],
                run_after_as_transaction: Some(false),
            }),
            persist_docs: Some(PersistDocsConfig {
                relation: Some(true),
                columns: Some(false),
            }),
            batch_size: Some(DbtBatchSize::Day),
            unique_key: Some(DbtUniqueKey::Single("id".to_string())),
            ..Default::default()
        };

        // Test serialization using the json_serialize module
        let mut serializer = serde_json::Serializer::new(Vec::new());
        json_serialize::serialize(&Some(user_configs.clone()), &mut serializer).unwrap();
        let serialized_bytes = serializer.into_inner();
        let serialized_string = String::from_utf8(serialized_bytes).unwrap();

        // Should be a JSON string containing the serialized UserConfigs
        assert!(serialized_string.starts_with('"'));
        assert!(serialized_string.ends_with('"'));

        // Parse the JSON string to verify it contains our data
        let json_content: String = serde_json::from_str(&serialized_string).unwrap();
        assert_contains!(json_content, "enabled");
        assert_contains!(json_content, "post_hook");
        assert_contains!(json_content, "SELECT 1");
        assert_contains!(json_content, "_comment");
        assert_contains!(
            json_content,
            "This JSON structure is subject to change at any time"
        );

        // Test deserialization
        let mut deserializer = serde_json::Deserializer::from_str(&serialized_string);
        let deserialized: Option<UserConfigs> =
            json_serialize::deserialize(&mut deserializer).unwrap();

        assert!(deserialized.is_some());
        let deserialized_configs = deserialized.unwrap();
        assert_eq!(user_configs.enabled, deserialized_configs.enabled);
        assert_eq!(user_configs.batch_size, deserialized_configs.batch_size);
        assert_eq!(user_configs.unique_key, deserialized_configs.unique_key);
    }

    #[test]
    fn test_parquet_node_json_roundtrip() {
        // Test complete round-trip serialization/deserialization of ParquetNode with user_config_details
        let user_configs = UserConfigs {
            enabled: Some(false),
            tags: Some(StringOrArrayOfStrings::ArrayOfStrings(vec![
                "tag1".to_string(),
                "tag2".to_string(),
            ])),
            meta: None, // Simplified to avoid complex YmlValue construction
            incremental_strategy: Some(DbtIncrementalStrategy::Append),
            unique_key: Some(DbtUniqueKey::Multiple(vec![
                "id".to_string(),
                "name".to_string(),
            ])),
            on_schema_change: Some(OnSchemaChange::Fail),
            contract: Some(DbtContract {
                enforced: true,
                alias_types: false,
                checksum: None,
            }),
            ..Default::default()
        };

        let original_node = ParquetNode {
            resource_type: ResourceType::Model,
            unique_id: "model.test.complex_example".to_string(),
            name: "complex_example".to_string(),
            description: Some("A complex test model".to_string()),
            tags: vec!["test".to_string(), "complex".to_string()],
            user_config_details: Some(user_configs),
            ..Default::default()
        };

        // Serialize to JSON
        let serialized_json = serde_json::to_string(&original_node).unwrap();

        // Verify the JSON contains the user_config_details as a JSON string
        assert_contains!(serialized_json, "user_config_details");

        // Deserialize from JSON
        let deserialized_node: ParquetNode = serde_json::from_str(&serialized_json).unwrap();

        // Verify core fields
        assert_eq!(original_node.resource_type, deserialized_node.resource_type);
        assert_eq!(original_node.unique_id, deserialized_node.unique_id);
        assert_eq!(original_node.name, deserialized_node.name);
        assert_eq!(original_node.description, deserialized_node.description);
        assert_eq!(original_node.tags, deserialized_node.tags);

        // Verify user_config_details was properly deserialized
        assert!(deserialized_node.user_config_details.is_some());
        let deserialized_config = deserialized_node.user_config_details.unwrap();
        assert_eq!(Some(false), deserialized_config.enabled);
        assert_eq!(
            Some(DbtIncrementalStrategy::Append),
            deserialized_config.incremental_strategy
        );
        assert_eq!(
            Some(OnSchemaChange::Fail),
            deserialized_config.on_schema_change
        );

        // Verify complex nested structures
        if let Some(DbtUniqueKey::Multiple(keys)) = &deserialized_config.unique_key {
            assert_eq!(vec!["id".to_string(), "name".to_string()], *keys);
        } else {
            panic!("Expected Multiple unique key");
        }

        if let Some(contract) = &deserialized_config.contract {
            assert!(contract.enforced);
            assert!(!contract.alias_types);
        } else {
            panic!("Expected contract to be present");
        }
    }

    #[test]
    fn test_json_serialize_none_handling() {
        // Test that None values are handled correctly by json_serialize module
        let node_with_none = ParquetNode {
            resource_type: ResourceType::Model,
            unique_id: "model.test.none_example".to_string(),
            name: "none_example".to_string(),
            user_config_details: None,
            ..Default::default()
        };

        // Serialize to JSON
        let serialized = serde_json::to_string(&node_with_none).unwrap();

        // Deserialize back
        let deserialized: ParquetNode = serde_json::from_str(&serialized).unwrap();

        // Verify None is preserved
        assert!(deserialized.user_config_details.is_none());
        assert_eq!(node_with_none.unique_id, deserialized.unique_id);
    }
}
