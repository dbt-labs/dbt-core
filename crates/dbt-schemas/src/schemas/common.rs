//! Common Definitions for use in serializing and deserializing dbt nodes

use indexmap::IndexMap;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::time::Duration;

/// Indicates where schema metadata originates from.
///
/// - `Remote` (default): Schema is fetched from the remote warehouse
/// - `Local`: Schema is derived from YAML column definitions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum SchemaOrigin {
    /// Schema metadata comes from the remote warehouse (default)
    #[default]
    Remote,
    /// Schema metadata is derived from YAML column definitions
    Local,
}

impl SchemaOrigin {
    /// Returns the string representation of the schema origin.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Remote => "remote",
            Self::Local => "local",
        }
    }
}

impl std::fmt::Display for SchemaOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for SchemaOrigin {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "remote" => Ok(Self::Remote),
            "local" => Ok(Self::Local),
            _ => Err(format!(
                "Invalid schema_origin '{}': expected 'remote' or 'local'",
                s
            )),
        }
    }
}

use dbt_common::adapter::AdapterType;
use dbt_common::{CodeLocationWithFile, ErrorCode, FsError, FsResult, err, fs_err};
use dbt_frontend_common::Dialect;
use dbt_serde_yaml::{JsonSchema, Spanned, UntaggedEnumDeserialize, Verbatim};
use dbt_telemetry::NodeMaterialization;
use hex;
use serde::{Deserialize, Deserializer, Serialize};
// Type alias for clarity
type YmlValue = dbt_serde_yaml::Value;
use serde_with::skip_serializing_none;
use sha2::{Digest, Sha256};
use strum::{Display, EnumIter, EnumString};

use crate::dbt_types::RelationType;
use crate::schemas::dbt_column::{ColumnPropertiesDimensionType, Granularity};
use crate::schemas::manifest::BigqueryPartitionConfig;
use crate::schemas::manifest::common::SourceFileMetadata;
use crate::schemas::semantic_layer::semantic_manifest::SemanticLayerElementConfig;

use super::serde::{
    StringOrArrayOfStrings, bool_or_string_bool, bool_or_string_bool_default, i64_or_string_i64,
};
#[derive(Default, Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
pub struct FreshnessRules {
    #[serde(deserialize_with = "i64_or_string_i64")]
    pub count: Option<i64>,
    pub period: Option<FreshnessPeriod>,
}

impl FreshnessRules {
    pub fn validate(rule: Option<&Self>) -> FsResult<()> {
        if rule.is_none() {
            return Ok(());
        }
        let rule = rule.expect("rule should be Some now");
        if rule.count.is_none() || rule.period.is_none() {
            return Err(fs_err!(
                ErrorCode::InvalidArgument,
                "count and period are required when freshness is provided, count: {:?}, period: {:?}",
                rule.count,
                rule.period
            ));
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.count.is_none() && self.period.is_none()
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum UpdatesOn {
    #[default]
    Any,
    All,
}

impl std::fmt::Display for UpdatesOn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdatesOn::Any => write!(f, "any"),
            UpdatesOn::All => write!(f, "all"),
        }
    }
}

impl FromStr for UpdatesOn {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "any" => Ok(UpdatesOn::Any),
            "all" => Ok(UpdatesOn::All),
            _ => Err(format!("Unknown UpdatesOn value: {s}")),
        }
    }
}

#[derive(Default, Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct ModelFreshnessRules {
    pub count: Option<i64>,
    pub period: Option<FreshnessPeriod>,
    pub updates_on: Option<UpdatesOn>,
}

impl PartialEq for ModelFreshnessRules {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count
            && self.period == other.period
            && updates_on_eq(&self.updates_on, &other.updates_on)
    }
}

impl Eq for ModelFreshnessRules {}

fn updates_on_eq(a: &Option<UpdatesOn>, b: &Option<UpdatesOn>) -> bool {
    match (a.as_ref(), b.as_ref()) {
        (None, None) => true,
        (Some(a_val), Some(b_val)) => a_val == b_val,
        (None, Some(b_val)) => b_val == &UpdatesOn::default(),
        (Some(a_val), None) => a_val == &UpdatesOn::default(),
    }
}

impl ModelFreshnessRules {
    pub fn validate(rule: Option<&Self>) -> FsResult<()> {
        if rule.is_none() {
            return Ok(());
        }
        let rule = rule.expect("rule should be Some now");
        let count_present = rule.count.is_some();
        let period_present = rule.period.is_some();

        if count_present != period_present {
            return Err(fs_err!(
                ErrorCode::InvalidArgument,
                "count and period are required when freshness is provided, count: {:?}, period: {:?}",
                rule.count,
                rule.period
            ));
        }
        Ok(())
    }

    /// Convert the freshness duration to seconds
    pub fn to_seconds(&self) -> i64 {
        let count = self.count.expect("count is required");
        let period = self.period.as_ref().expect("period is required");
        count
            * match period {
                FreshnessPeriod::minute => 60,
                FreshnessPeriod::hour => 60 * 60,
                FreshnessPeriod::day => 60 * 60 * 24,
            }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum FreshnessPeriod {
    minute,
    hour,
    day,
}
impl FromStr for FreshnessPeriod {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "minute" => Ok(FreshnessPeriod::minute),
            "hour" => Ok(FreshnessPeriod::hour),
            "day" => Ok(FreshnessPeriod::day),
            _ => Err(()),
        }
    }
}
impl std::fmt::Display for FreshnessPeriod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let period_str = match self {
            FreshnessPeriod::minute => "minute",
            FreshnessPeriod::hour => "hour",
            FreshnessPeriod::day => "day",
        };
        write!(f, "{period_str}")
    }
}

// We don't skip serializing none here because dbt project evaluator checks for the presence of either error_after or warn_after
// https://github.com/dbt-labs/dbt-project-evaluator/blob/94768b117573705e95a9456273de8e358efadb00/macros/unpack/get_source_values.sql#L27-L28
#[derive(Default, Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
pub struct FreshnessDefinition {
    #[serde(default, serialize_with = "serialize_freshness_rule")]
    pub error_after: Option<FreshnessRules>,
    #[serde(default, serialize_with = "serialize_freshness_rule")]
    pub warn_after: Option<FreshnessRules>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

/// Custom serializer to ensure FreshnessRules are always objects, never null
fn serialize_freshness_rule<S>(
    rule: &Option<FreshnessRules>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match rule {
        Some(rule) => rule.serialize(serializer),
        None => FreshnessRules::default().serialize(serializer),
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum FreshnessStatus {
    Pass,
    Warn,
    Error,
}

impl std::fmt::Display for FreshnessStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Error => "error",
        };
        write!(f, "{s}")
    }
}

/// Trait for types that can be merged, taking the last non-None value
pub trait Merge<T> {
    /// Merge with another instance, where the other's non-None values overwrite self
    fn merge(&self, other: &T) -> Self;
}

// Generic implementation for Option<T> where T: Clone
impl<T: Clone + Merge<T>> Merge<Option<T>> for Option<T> {
    fn merge(&self, other: &Option<T>) -> Self {
        match (self, other) {
            (Some(s), Some(o)) => Some(s.merge(o)),
            (None, Some(o)) => Some(o.clone()),
            (Some(s), None) => Some(s.clone()),
            (None, None) => None,
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, EnumIter, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DbtMaterialization {
    #[default]
    Snapshot,
    Seed,
    View,
    Table,
    Incremental,
    MaterializedView,
    External,
    Test,
    Ephemeral,
    Unit,
    Analysis,
    Function,
    /// only for databricks
    StreamingTable,
    /// only for snowflake
    DynamicTable,
    /// for inline SQL compilation
    Inline,
    #[serde(untagged)]
    Unknown(String),
}
impl FromStr for DbtMaterialization {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "view" => Ok(DbtMaterialization::View),
            "table" => Ok(DbtMaterialization::Table),
            "incremental" => Ok(DbtMaterialization::Incremental),
            "materialized_view" => Ok(DbtMaterialization::MaterializedView),
            "external" => Ok(DbtMaterialization::External),
            "test" => Ok(DbtMaterialization::Test),
            "ephemeral" => Ok(DbtMaterialization::Ephemeral),
            "unit" => Ok(DbtMaterialization::Unit),
            "analysis" => Ok(DbtMaterialization::Analysis),
            "function" => Ok(DbtMaterialization::Function),
            "streaming_table" => Ok(DbtMaterialization::StreamingTable),
            "dynamic_table" => Ok(DbtMaterialization::DynamicTable),
            "inline" => Ok(DbtMaterialization::Inline),
            other => Ok(DbtMaterialization::Unknown(other.to_string())),
        }
    }
}
impl From<DbtMaterialization> for String {
    fn from(materialization: DbtMaterialization) -> Self {
        materialization.to_string()
    }
}

impl std::fmt::Display for DbtMaterialization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let materialized_str = match self {
            DbtMaterialization::View => "view",
            DbtMaterialization::Table => "table",
            DbtMaterialization::Incremental => "incremental",
            DbtMaterialization::MaterializedView => "materialized_view",
            DbtMaterialization::External => "external",
            DbtMaterialization::Test => "test",
            DbtMaterialization::Ephemeral => "ephemeral",
            DbtMaterialization::Unit => "unit",
            DbtMaterialization::StreamingTable => "streaming_table",
            DbtMaterialization::DynamicTable => "dynamic_table",
            DbtMaterialization::Analysis => "analysis",
            DbtMaterialization::Function => "function",
            DbtMaterialization::Inline => "inline",
            DbtMaterialization::Unknown(s) => s.as_str(),
            DbtMaterialization::Snapshot => "snapshot",
            DbtMaterialization::Seed => "seed",
        };
        write!(f, "{materialized_str}")
    }
}

// Question (Ani): does this map correctly?
impl From<DbtMaterialization> for RelationType {
    fn from(materialization: DbtMaterialization) -> Self {
        match materialization {
            DbtMaterialization::Table => RelationType::Table,
            DbtMaterialization::View => RelationType::View,
            DbtMaterialization::MaterializedView => RelationType::MaterializedView,
            DbtMaterialization::Ephemeral => RelationType::Ephemeral,
            DbtMaterialization::External => RelationType::External,
            DbtMaterialization::Test => RelationType::External, // TODO Validate this
            DbtMaterialization::Incremental => RelationType::External, // TODO Validate this
            DbtMaterialization::Unit => RelationType::External, // TODO Validate this
            DbtMaterialization::StreamingTable => RelationType::StreamingTable,
            DbtMaterialization::DynamicTable => RelationType::DynamicTable,
            DbtMaterialization::Analysis => RelationType::External, // TODO Validate this
            DbtMaterialization::Inline => RelationType::Ephemeral, // Inline models don't materialize in DB
            DbtMaterialization::Unknown(_) => RelationType::External, // TODO Validate this
            DbtMaterialization::Snapshot => RelationType::Table,   // TODO Validate this
            DbtMaterialization::Seed => RelationType::Table,       // TODO Validate this
            DbtMaterialization::Function => RelationType::Function,
        }
    }
}

impl From<&DbtMaterialization> for NodeMaterialization {
    fn from(value: &DbtMaterialization) -> Self {
        match value {
            DbtMaterialization::Table => Self::Table,
            DbtMaterialization::View => Self::View,
            DbtMaterialization::MaterializedView => Self::MaterializedView,
            DbtMaterialization::Ephemeral => Self::Ephemeral,
            DbtMaterialization::External => Self::External,
            DbtMaterialization::Test => Self::Test,
            DbtMaterialization::Incremental => Self::Incremental,
            DbtMaterialization::Unit => Self::Unit,
            DbtMaterialization::StreamingTable => Self::StreamingTable,
            DbtMaterialization::DynamicTable => Self::DynamicTable,
            DbtMaterialization::Analysis => Self::Analysis,
            DbtMaterialization::Inline => Self::Ephemeral, // Inline is similar to ephemeral
            DbtMaterialization::Unknown(_) => Self::Custom,
            DbtMaterialization::Snapshot => Self::Snapshot,
            DbtMaterialization::Seed => Self::Seed,
            DbtMaterialization::Function => Self::Function,
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Display, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Access {
    Private,
    #[default]
    Protected,
    Public,
}

impl FromStr for Access {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "private" => Ok(Access::Private),
            "protected" => Ok(Access::Protected),
            "public" => Ok(Access::Public),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct NodeDependsOn {
    #[serde(default)]
    pub macros: Vec<String>,
    #[serde(default)]
    pub nodes: Vec<String>,
    #[serde(default)]
    pub nodes_with_ref_location: Vec<(String, CodeLocationWithFile)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Copy, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ResolvedQuoting {
    #[serde(deserialize_with = "bool_or_string_bool_default")]
    pub database: bool,
    #[serde(deserialize_with = "bool_or_string_bool_default")]
    pub identifier: bool,
    #[serde(deserialize_with = "bool_or_string_bool_default")]
    pub schema: bool,
}

impl Default for ResolvedQuoting {
    fn default() -> Self {
        // dbt rules
        Self::trues()
        // todo: however a much more sensible rule would be
        // Self::falses()
        // ... since SQL is case insensitive -- so let the dialect dictate and not the user...
    }
}

impl ResolvedQuoting {
    pub fn trues() -> Self {
        ResolvedQuoting {
            database: true,
            identifier: true,
            schema: true,
        }
    }
    pub fn falses() -> Self {
        ResolvedQuoting {
            database: false,
            identifier: false,
            schema: false,
        }
    }
}

impl TryFrom<DbtQuoting> for ResolvedQuoting {
    type Error = Box<FsError>;

    fn try_from(value: DbtQuoting) -> FsResult<Self> {
        Ok(ResolvedQuoting {
            database: value.database.ok_or_else(|| fs_err!(
                ErrorCode::InvalidArgument,
                "Missing database in dbt quoting config. Failed to convert to ResolvedQuoting."
            ))?,
            identifier: value.identifier.ok_or_else(|| fs_err!(
                ErrorCode::InvalidArgument,
                "Missing identifier in dbt quoting config. Failed to convert to ResolvedQuoting."
            ))?,
            schema: value.schema.ok_or_else(|| fs_err!(
                ErrorCode::InvalidArgument,
                "Missing schema in dbt quoting config. Failed to convert to ResolvedQuoting."
            ))?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Default, Deserialize, PartialEq, Eq, Copy, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DbtQuoting {
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub database: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub identifier: Option<bool>,
    #[serde(default, deserialize_with = "bool_or_string_bool")]
    pub schema: Option<bool>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "bool_or_string_bool"
    )]
    pub snowflake_ignore_case: Option<bool>,
}

impl DbtQuoting {
    pub fn is_default(&self) -> bool {
        self.database.is_none() && self.identifier.is_none() && self.schema.is_none()
    }

    pub fn default_to(&mut self, other: &DbtQuoting) {
        self.database = self.database.or(other.database);
        self.identifier = self.identifier.or(other.identifier);
        self.schema = self.schema.or(other.schema);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbtCheckColsSpec {
    /// A list of column names to be used for checking if a snapshot's source data set was updated
    Cols(Vec<String>),
    /// Use all columns to check whether a snapshot's source data set was updated
    All,
}

impl Serialize for DbtCheckColsSpec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            DbtCheckColsSpec::Cols(cols) => cols.serialize(serializer),
            DbtCheckColsSpec::All => "all".serialize(serializer),
        }
    }
}

impl TryFrom<StringOrArrayOfStrings> for DbtCheckColsSpec {
    type Error = Box<FsError>;
    fn try_from(value: StringOrArrayOfStrings) -> Result<Self, Self::Error> {
        match value {
            StringOrArrayOfStrings::String(all) => {
                if all == "all" {
                    Ok(DbtCheckColsSpec::All)
                } else {
                    err!(
                        ErrorCode::InvalidConfig,
                        "Invalid check_cols value: {}",
                        all
                    )
                }
            }
            StringOrArrayOfStrings::ArrayOfStrings(cols) => Ok(DbtCheckColsSpec::Cols(cols)),
        }
    }
}
impl<'de> Deserialize<'de> for DbtCheckColsSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = YmlValue::deserialize(deserializer)?;
        match value {
            YmlValue::String(all, _) => {
                // Validate that all is 'all'
                if all != "all" {
                    return Err(serde::de::Error::custom("Expected 'all'"));
                }
                Ok(DbtCheckColsSpec::All)
            }
            YmlValue::Sequence(col_list, _) => {
                let cols: Result<Vec<_>, D::Error> = col_list
                    .into_iter()
                    .map(|v| match v {
                        YmlValue::String(s, _) => Ok(s),
                        _ => Err(serde::de::Error::custom("Expected array of strings")),
                    })
                    .collect();
                match cols {
                    Ok(col_names) => Ok(DbtCheckColsSpec::Cols(col_names.into_iter().collect())),
                    Err(_) => Err(serde::de::Error::custom("Expected array of strings")),
                }
            }
            _ => Err(serde::de::Error::custom(format!(
                "Expected a string or array of strings for check_cols, got {value:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnumString, Display, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum DbtBatchSize {
    Hour,
    Day,
    Month,
    Year,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct DbtContract {
    #[serde(default = "default_alias_types")]
    pub alias_types: bool,
    #[serde(default)]
    pub enforced: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<YmlValue>,
}

fn default_alias_types() -> bool {
    true
}

// DO NOT REMOVE. Somehow, `#[derive(Default)]` does not respect `#[serde(default = "default_alias_types")]`, so we need to implement it manually.
impl Default for DbtContract {
    fn default() -> Self {
        Self {
            alias_types: default_alias_types(),
            enforced: false,
            checksum: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnumString, Display, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum DbtIncrementalStrategy {
    Append,
    Merge,
    #[serde(rename = "delete+insert")]
    #[strum(serialize = "delete+insert")]
    DeleteInsert,
    InsertOverwrite,
    Microbatch,
    /// replace_where (Databricks only)
    /// see https://docs.getdbt.com/reference/resource-configs/databricks-configs
    ReplaceWhere,
    #[strum(default)]
    #[serde(untagged)]
    Custom(String),
}

#[derive(Debug, Clone, Serialize, UntaggedEnumDeserialize, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
pub enum DbtUniqueKey {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnSchemaChange {
    Ignore,
    AppendNewColumns,
    Fail,
    SyncAllColumns,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnConfigurationChange {
    Apply,
    Continue,
    Fail,
    #[serde(other)]
    Unknown,
}

impl From<StringOrArrayOfStrings> for DbtUniqueKey {
    fn from(value: StringOrArrayOfStrings) -> Self {
        match value {
            StringOrArrayOfStrings::String(s) => DbtUniqueKey::Single(s),
            StringOrArrayOfStrings::ArrayOfStrings(v) => DbtUniqueKey::Multiple(v),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HardDeletes {
    Ignore,
    Invalidate,
    NewRecord,
}

// Impl try from string
impl TryFrom<String> for HardDeletes {
    type Error = Box<FsError>;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(match value.as_str() {
            "ignore" => HardDeletes::Ignore,
            "invalidate" => HardDeletes::Invalidate,
            "new_record" => HardDeletes::NewRecord,
            _ => {
                return err!(
                    ErrorCode::InvalidConfig,
                    "Invalid hard_deletes value: {}",
                    value
                );
            }
        })
    }
}

/// Constraints (model level or column level)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct Constraint {
    #[serde(rename = "type")]
    pub type_: ConstraintType,
    pub expression: Option<String>,
    pub name: Option<String>,
    // Only ForeignKey constraints accept: a relation input
    // ref(), source() etc
    pub to: Option<String>,
    /// Only ForeignKey constraints accept: a list columns in that table
    /// containing the corresponding primary or unique key.
    pub to_columns: Option<Vec<String>>,
    pub warn_unsupported: Option<bool>,
    pub warn_unenforced: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintSupport {
    Enforced,
    NotEnforced,
    NotSupported,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintType {
    #[default]
    NotNull,
    Unique,
    PrimaryKey,
    ForeignKey,
    Check,
    Custom,
}

#[derive(Debug, Clone, Serialize, UntaggedEnumDeserialize)]
#[serde(untagged)]
pub enum DbtChecksum {
    String(String),
    Object(DbtChecksumObject),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtChecksumObject {
    pub name: String,
    pub checksum: String,
}

impl Default for DbtChecksum {
    fn default() -> Self {
        Self::Object(DbtChecksumObject {
            name: "".to_string(),
            checksum: "".to_string(),
        })
    }
}

impl PartialEq for DbtChecksum {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(s1), Self::String(s2)) => s1 == s2,
            (Self::Object(o1), Self::Object(o2)) => {
                o1.name.to_lowercase() == o2.name.to_lowercase() && o1.checksum == o2.checksum
            }
            (Self::String(c1), Self::Object(o2)) => *c1 == o2.checksum,
            (Self::Object(o1), Self::String(c2)) => o1.checksum == *c2,
        }
    }
}

impl DbtChecksum {
    /// Returns the checksum string value regardless of variant.
    pub fn as_checksum_string(&self) -> &str {
        match self {
            Self::String(s) => s,
            Self::Object(o) => &o.checksum,
        }
    }

    pub fn hash(s: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(s);
        let checksum = hasher.finalize();
        Self::Object(DbtChecksumObject {
            name: "SHA256".to_string(),
            checksum: hex::encode(checksum),
        })
    }
    pub fn seed_file_hash(s: &[u8], path: &str) -> Self {
        const MAXIMUM_SEED_SIZE: usize = 1024 * 1024; // 1MB

        if s.len() > MAXIMUM_SEED_SIZE {
            // For large seeds, use path-based checksum like dbt-core
            Self::Object(DbtChecksumObject {
                name: "path".to_string(),
                checksum: path.to_string(),
            })
        } else {
            // For normal seeds, hash the content
            let mut hasher = Sha256::new();
            let utf8_string = String::from_utf8_lossy(s);
            let trimmed_string = utf8_string.trim();
            hasher.update(trimmed_string.as_bytes());
            let checksum = hasher.finalize();
            Self::Object(DbtChecksumObject {
                name: "SHA256".to_string(),
                checksum: hex::encode(checksum),
            })
        }
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct IncludeExclude {
    pub exclude: Option<StringOrArrayOfStrings>,
    pub include: Option<StringOrArrayOfStrings>,
}

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema, Default)]
pub struct Expect {
    pub rows: Option<Rows>,
    #[serde(default)]
    pub format: Formats,
    pub fixture: Option<String>,
}

#[derive(
    Debug, Serialize, Default, Deserialize, Clone, EnumString, Display, JsonSchema, PartialEq,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum Formats {
    #[default]
    Dict,
    Csv,
    Sql,
}

#[derive(Debug, Serialize, Default, Deserialize, Clone, JsonSchema)]
pub struct Given {
    pub input: Spanned<String>,
    pub rows: Option<Rows>,
    #[serde(default)]
    pub format: Formats,
    pub fixture: Option<String>,
}

#[derive(Debug, Serialize, UntaggedEnumDeserialize, Clone, JsonSchema)]
#[serde(untagged)]
pub enum Rows {
    String(String),
    List(Vec<BTreeMap<String, YmlValue>>),
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Default, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct DocsConfig {
    #[serde(default = "default_show")]
    pub show: bool,
    pub node_color: Option<String>,
}

fn default_show() -> bool {
    true
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct PersistDocsConfig {
    pub columns: Option<bool>,
    pub relation: Option<bool>,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct ScheduleConfig {
    pub cron: Option<String>,
    pub time_zone_value: Option<String>,
}

/// Schedule configuration that accepts both string and structured formats.
/// This allows users to specify schedule as either:
/// - A string: `schedule: "USING CRON 0,15,30,45 * * * * UTC"`
/// - A structured config: `schedule: { cron: "0 * * * *", time_zone_value: "UTC" }`
#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
pub enum Schedule {
    String(String),
    ScheduleConfig(ScheduleConfig),
}

impl Schedule {
    /// Convert Schedule to ScheduleConfig
    pub fn to_schedule_config(&self) -> ScheduleConfig {
        match self {
            Schedule::String(s) => ScheduleConfig {
                cron: Some(s.clone()),
                time_zone_value: None,
            },
            Schedule::ScheduleConfig(config) => config.clone(),
        }
    }
}

#[derive(UntaggedEnumDeserialize, Serialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
pub enum Hooks {
    String(String),
    ArrayOfStrings(Vec<String>),
    HookConfig(HookConfig),
    HookConfigArray(Vec<HookConfig>),
}

impl Hooks {
    pub fn extend(&mut self, other: &Self) {
        let mut new_hooks = vec![];
        match other {
            Hooks::String(s) => {
                new_hooks.push(HookConfig {
                    sql: Some(s.clone()),
                    transaction: Some(true),
                    index: None,
                });
            }
            Hooks::ArrayOfStrings(v) => {
                new_hooks.extend(v.iter().map(|s| HookConfig {
                    sql: Some(s.clone()),
                    transaction: Some(true),
                    index: None,
                }));
            }
            Hooks::HookConfig(hook_config) => {
                new_hooks.push(hook_config.clone());
            }
            Hooks::HookConfigArray(hook_configs) => {
                new_hooks.extend(hook_configs.clone());
            }
        }
        match self {
            Hooks::String(s) => {
                new_hooks.push(HookConfig {
                    sql: Some(s.clone()),
                    transaction: Some(true),
                    index: None,
                });
            }
            Hooks::ArrayOfStrings(v) => {
                new_hooks.extend(v.iter().map(|s| HookConfig {
                    sql: Some(s.clone()),
                    transaction: Some(true),
                    index: None,
                }));
            }
            Hooks::HookConfig(hook_config) => {
                new_hooks.push(hook_config.clone());
            }
            Hooks::HookConfigArray(hook_configs) => {
                new_hooks.extend(hook_configs.clone());
            }
        }
        *self = Hooks::HookConfigArray(new_hooks);
    }

    /// Compare hooks where different variants can be equal if they contain the same SQL
    /// These checks are needed so that we can conform to dbt-core/dbt-mantle
    /// when comparing hooks.
    pub fn hooks_equal(&self, other: &Self) -> bool {
        // Helper to check if a Hooks value is effectively empty
        let is_empty_hooks = |hooks: &Hooks| -> bool {
            match hooks {
                Hooks::ArrayOfStrings(vec) => vec.is_empty(),
                _ => false,
            }
        };

        // If both are the same variant and equal, return true
        if self == other {
            return true;
        }

        // Check if one is empty array and the other is something that should be considered equal
        if is_empty_hooks(self) && is_empty_hooks(other) {
            return true;
        }

        // Compare different variants by their SQL content
        match (self, other) {
            // String vs HookConfig: compare the string with the SQL field
            (Hooks::String(s), Hooks::HookConfig(config))
            | (Hooks::HookConfig(config), Hooks::String(s)) => config.sql.as_ref() == Some(s),

            // ArrayOfStrings vs HookConfigArray: compare each string with corresponding SQL field
            (Hooks::ArrayOfStrings(strings), Hooks::HookConfigArray(configs))
            | (Hooks::HookConfigArray(configs), Hooks::ArrayOfStrings(strings)) => {
                // Both must have the same length
                if strings.len() != configs.len() {
                    return false;
                }

                // Each string must match the corresponding config's SQL
                strings
                    .iter()
                    .zip(configs.iter())
                    .all(|(s, config)| config.sql.as_ref() == Some(s))
            }

            // String vs ArrayOfStrings with single element
            (Hooks::String(s), Hooks::ArrayOfStrings(vec))
            | (Hooks::ArrayOfStrings(vec), Hooks::String(s)) => {
                vec.len() == 1 && vec.first() == Some(s)
            }

            // HookConfig vs HookConfigArray with single element
            (Hooks::HookConfig(config), Hooks::HookConfigArray(vec))
            | (Hooks::HookConfigArray(vec), Hooks::HookConfig(config)) => {
                vec.len() == 1 && vec.first() == Some(config)
            }

            // String vs HookConfigArray with single element
            (Hooks::String(s), Hooks::HookConfigArray(configs))
            | (Hooks::HookConfigArray(configs), Hooks::String(s)) => {
                configs.len() == 1 && configs.first().and_then(|c| c.sql.as_ref()) == Some(s)
            }

            // ArrayOfStrings vs HookConfig - only equal if array has one element
            (Hooks::ArrayOfStrings(strings), Hooks::HookConfig(config))
            | (Hooks::HookConfig(config), Hooks::ArrayOfStrings(strings)) => {
                strings.len() == 1 && strings.first() == config.sql.as_ref()
            }

            _ => false,
        }
    }
}

// Helper function to compare hooks wrapped in Verbatim<Option<Hooks>> where None equals Some(ArrayOfStrings([]))
pub fn hooks_equal(a: &Verbatim<Option<Hooks>>, b: &Verbatim<Option<Hooks>>) -> bool {
    match (a.as_ref(), b.as_ref()) {
        (None, None) => true,
        (None, Some(b_hooks)) => {
            // Check if b_hooks is effectively empty
            matches!(b_hooks, Hooks::ArrayOfStrings(vec) if vec.is_empty())
        }
        (Some(a_hooks), None) => {
            // Check if a_hooks is effectively empty
            matches!(a_hooks, Hooks::ArrayOfStrings(vec) if vec.is_empty())
        }
        (Some(a_hooks), Some(b_hooks)) => a_hooks.hooks_equal(b_hooks),
    }
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
pub struct HookConfig {
    pub sql: Option<String>,
    pub transaction: Option<bool>,
    pub index: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct Dimension {
    pub name: String,
    #[serde(rename = "type")]
    pub dimension_type: ColumnPropertiesDimensionType,
    pub description: Option<String>,
    pub label: Option<String>,
    #[serde(default = "default_false")]
    pub is_partition: bool,
    pub type_params: Option<DimensionTypeParams>,
    pub expr: Option<String>,
    pub metadata: Option<SourceFileMetadata>,
    pub config: Option<SemanticLayerElementConfig>,
}

fn default_false() -> bool {
    false
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Default)]
pub struct DimensionTypeParams {
    pub time_granularity: Option<Granularity>,
    pub validity_params: Option<DimensionValidityParams>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct DimensionValidityParams {
    #[serde(default = "default_false")]
    pub is_start: bool,
    #[serde(default = "default_false")]
    pub is_end: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
pub struct SemanticModelDependsOn {
    pub macros: Vec<String>,
    pub nodes: Vec<String>,
}

#[derive(Default, Deserialize, Serialize, Debug, Clone, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StoreFailuresAs {
    #[default]
    Ephemeral,
    Table,
    View,
}

#[derive(Debug, Serialize, Default, Deserialize, Clone, EnumString, Display, JsonSchema)]
#[strum(serialize_all = "lowercase")]
pub enum Severity {
    #[default]
    #[serde(alias = "error", alias = "ERROR")]
    Error,
    #[serde(alias = "warn", alias = "WARN")]
    Warn,
}

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct Versions {
    pub v: YmlValue,
    pub config: Verbatim<Option<dbt_serde_yaml::Value>>,
    pub __additional_properties__: Verbatim<HashMap<String, YmlValue>>,
}

impl Versions {
    pub fn get_version(&self) -> Option<String> {
        match &self.v {
            dbt_serde_yaml::Value::String(s, _) => Some(s.to_string()),
            dbt_serde_yaml::Value::Number(n, _) => Some(n.to_string()),
            _ => None,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfoWrapper {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped_nodes: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defined_at: Option<CodeLocationWithFile>,
    pub node_info: NodeInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_name: String,
    pub unique_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_finished_at: Option<String>,
    pub node_status: String,
}

/// Get the semantic names for database, schema, and identifier
/// This function will parse the database, schema, and identifier
/// according to the dialect and quoting rules.
pub fn normalize_quoting(
    quoting: &ResolvedQuoting,
    adapter_type: AdapterType,
    database: &str,
    schema: &str,
    identifier: &str,
) -> (String, String, String, ResolvedQuoting) {
    let dialect: Dialect = Dialect::from(adapter_type);
    let (database, database_quoting) = _normalize_quote(quoting.database, &dialect, database);
    let (schema, schema_quoting) = _normalize_quote(quoting.schema, &dialect, schema);
    let (identifier, identifier_quoting) =
        _normalize_quote(quoting.identifier, &dialect, identifier);
    (
        database,
        schema,
        identifier,
        ResolvedQuoting {
            database: database_quoting,
            schema: schema_quoting,
            identifier: identifier_quoting,
        },
    )
}

pub fn normalize_quote(quoting: bool, adapter_type: AdapterType, name: &str) -> (String, bool) {
    let dialect: Dialect = Dialect::from(adapter_type);
    _normalize_quote(quoting, &dialect, name)
}

pub fn _normalize_quote(quoting: bool, dialect: &Dialect, name: &str) -> (String, bool) {
    let quoted = name.len() > 1
        && name.starts_with(dialect.quote_char())
        && name.ends_with(dialect.quote_char());

    // If the name is quoted, but the quote config is false, we need to unquote the name
    if (quoted && !quoting) && !name.is_empty() {
        (name[1..name.len() - 1].to_string(), true)
    } else {
        (name.to_string(), quoting)
    }
}

/// Normalize SQL by compacting multiple spaces and newlines into a single space.
/// This ensures consistent checksums regardless of formatting differences
/// due to multiple spaces and newlines.
pub fn normalize_sql(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut last_was_whitespace = false;

    for c in sql.chars() {
        if c.is_whitespace() {
            if !last_was_whitespace {
                result.push(' ');
                last_was_whitespace = true;
            }
        } else {
            result.push(c);
            last_was_whitespace = false;
        }
    }

    result.trim().to_string()
}

/// Merge two meta maps, with the second map's values taking precedence on key conflicts.
pub fn merge_meta(
    base_meta: Option<IndexMap<String, YmlValue>>,
    update_meta: Option<IndexMap<String, YmlValue>>,
) -> Option<IndexMap<String, YmlValue>> {
    match (base_meta, update_meta) {
        (Some(base_map), Some(update_map)) => {
            let mut merged = base_map;
            merged.extend(update_map);
            Some(merged)
        }
        (None, Some(update_map)) => Some(update_map),
        (Some(base_map), None) => Some(base_map),
        (None, None) => None,
    }
}

/// Merge two tag lists, deduplicating and sorting the result.
pub fn merge_tags(
    base_tags: Option<Vec<String>>,
    update_tags: Option<Vec<String>>,
) -> Option<Vec<String>> {
    match (base_tags, update_tags) {
        // If both are None, result is None
        (None, None) => None,
        // If either has a value (even empty), we preserve that semantic meaning
        (Some(mut base), Some(update)) => {
            base.extend(update);
            base.sort();
            base.dedup();
            Some(base)
        }
        // If only one side has a value, use it
        (Some(base), None) => Some(base),
        (None, Some(update)) => Some(update),
    }
}

pub fn conform_normalized_snapshot_raw_code_to_mantle_format(normalized_full: &str) -> String {
    // Strip snapshot tags to match dbt-mantle behavior
    // Remove everything before and including {%snapshot name%} or {%-snapshot name-%}
    let sql_without_opening = normalized_full
        .find("{%-snapshot")
        .or_else(|| normalized_full.find("{%snapshot"))
        .and_then(|start_pos| {
            // Found the opening tag, now find where it ends
            let after_tag_start = &normalized_full[start_pos..];
            after_tag_start
                .find("-%}")
                .or_else(|| after_tag_start.find("%}"))
                .map(|end_offset| {
                    let tag_end = if after_tag_start[end_offset..].starts_with("-%}") {
                        end_offset + 3
                    } else {
                        end_offset + 2
                    };
                    &normalized_full[start_pos + tag_end..]
                })
        })
        .unwrap_or(normalized_full);

    // Strip the closing endsnapshot tag ({%endsnapshot%} or {%-endsnapshot-%})
    let normalized_sql = sql_without_opening
        .strip_suffix("-%}")
        .or_else(|| sql_without_opening.strip_suffix("%}"))
        .and_then(|s| {
            // Find the start of the closing tag ({%- or {%)
            s.rfind("{%-endsnapshot")
                .or_else(|| s.rfind("{%endsnapshot"))
                .map(|pos| &s[..pos])
        })
        .unwrap_or(sql_without_opening);

    normalized_sql.to_string()
}

/// Schema refresh interval configuration.
///
/// This type represents how often to refresh source schemas from remote.
/// Default: 1 hour.
///
/// Examples:
/// - `"30m"`, `"2h"`, `"1d"` - refresh after the specified duration
/// - `"never"` - never automatically refresh (only manual sync)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaRefreshInterval {
    /// Refresh after the specified duration
    Duration(Duration), //todo validate that this deserializes as claimed
    /// Never automatically refresh
    Never,
}

impl schemars::JsonSchema for SchemaRefreshInterval {
    fn schema_name() -> String {
        "SchemaRefreshInterval".to_string()
    }

    fn json_schema(_gen: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            metadata: Some(Box::new(schemars::schema::Metadata {
                description: Some(
                    "Schema refresh interval: '30m', '2h', '1d', or 'never'".to_string(),
                ),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

impl Default for SchemaRefreshInterval {
    fn default() -> Self {
        Self::Duration(Duration::from_hours(1))
    }
}

impl SchemaRefreshInterval {
    /// Creates a new interval from hours.
    pub fn from_hours(hours: u64) -> Self {
        Self::Duration(Duration::from_hours(hours))
    }

    /// Returns the duration as seconds, or None if set to never refresh.
    pub fn as_secs(&self) -> Option<u64> {
        match self {
            Self::Duration(d) => Some(d.as_secs()),
            Self::Never => None,
        }
    }

    /// Returns the duration, or None if set to never refresh.
    pub fn as_duration(&self) -> Option<Duration> {
        match self {
            Self::Duration(d) => Some(*d),
            Self::Never => None,
        }
    }

    /// Returns true if this interval is set to never refresh.
    pub fn is_never(&self) -> bool {
        matches!(self, Self::Never)
    }
}

impl From<Duration> for SchemaRefreshInterval {
    fn from(duration: Duration) -> Self {
        Self::Duration(duration)
    }
}

impl From<SchemaRefreshInterval> for Option<Duration> {
    fn from(interval: SchemaRefreshInterval) -> Self {
        interval.as_duration()
    }
}

impl FromStr for SchemaRefreshInterval {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("never") {
            return Ok(Self::Never);
        }
        // Use humantime to parse duration strings like "30m", "2h", "1d"
        let duration = humantime::parse_duration(s).map_err(|e| {
            format!(
                "Invalid schema_refresh_interval '{}': {}. \
                 Expected format like '30m', '2h', '1d', or 'never'",
                s, e
            )
        })?;

        // Validate duration bounds
        const MIN_DURATION: Duration = Duration::from_secs(0); // 0 seconds (always refresh)
        const MAX_DURATION: Duration = Duration::from_secs(365 * 24 * 60 * 60); // 1 year

        if duration < MIN_DURATION {
            return Err(format!(
                "schema_refresh_interval '{}' is invalid. Must be non-negative",
                s
            ));
        }

        if duration > MAX_DURATION {
            return Err(format!(
                "schema_refresh_interval '{}' is too long. Maximum allowed is 1 year",
                s
            ));
        }

        Ok(Self::Duration(duration))
    }
}

impl<'de> Deserialize<'de> for SchemaRefreshInterval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl Serialize for SchemaRefreshInterval {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Duration(d) => {
                serializer.serialize_str(&humantime::format_duration(*d).to_string())
            }
            Self::Never => serializer.serialize_str("never"),
        }
    }
}

impl std::fmt::Display for SchemaRefreshInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Duration(d) => write!(f, "{}", humantime::format_duration(*d)),
            Self::Never => write!(f, "never"),
        }
    }
}

/// Configuration for schema synchronization behavior.
///
/// This can be specified at source level (default for all tables) or
/// at individual table level (overrides source-level settings).
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
pub struct SyncConfig {
    /// How often to refresh the schema from the warehouse.
    /// Examples: "30m", "2h", "1d", "never"
    pub schema_refresh_interval: Option<SchemaRefreshInterval>,
}

/// Configuration for cluster by columns.
///
/// dbt-core allows either of the variants for the `cluster_by`
/// to allow cluster on a single column or on multiple columns
#[derive(Debug, Clone, Serialize, UntaggedEnumDeserialize, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
pub enum ClusterConfig {
    String(String),
    List(Vec<String>),
}

impl ClusterConfig {
    /// Normalize the enum as a list of cluster_by fields
    pub fn fields(&self) -> Vec<&str> {
        match self {
            ClusterConfig::String(s) => vec![s.as_ref()],
            ClusterConfig::List(l) => l.iter().map(|s| s.as_ref()).collect(),
        }
    }

    /// Normalize the enum as a list of cluster_by fields
    pub fn into_fields(self) -> Vec<String> {
        match self {
            ClusterConfig::String(s) => vec![s],
            ClusterConfig::List(l) => l,
        }
    }
}

/// Configuration for partition by columns.
///
/// dbt-core allows either of the variants for the `partition_by` in the model config
/// but the bigquery-adapter throws RunTime error
/// the behaviors are tested from the latest dbt-core + bigquery-adapter as this is written
/// we're conformant to this behavior via here and via the `into_bigquery()` method
#[derive(Debug, Clone, Serialize, UntaggedEnumDeserialize, PartialEq, Eq, JsonSchema)]
#[serde(untagged)]
pub enum PartitionConfig {
    String(String),
    List(Vec<String>),
    BigqueryPartitionConfig(BigqueryPartitionConfig),
}

impl PartitionConfig {
    pub fn into_bigquery(self) -> Option<BigqueryPartitionConfig> {
        match self {
            PartitionConfig::BigqueryPartitionConfig(bq) => Some(bq),
            _ => None,
        }
    }

    pub fn as_bigquery(&self) -> Option<&BigqueryPartitionConfig> {
        match self {
            PartitionConfig::BigqueryPartitionConfig(bq) => Some(bq),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::schemas::serde::minijinja_value_to_typed_struct;

    use super::*;
    use minijinja::value::Value as MinijinjaValue;

    #[test]
    fn model_freshness_rules_eq_defaults_updates_on_to_any() {
        let base = ModelFreshnessRules {
            count: Some(1),
            period: Some(FreshnessPeriod::hour),
            updates_on: None,
        };
        let other = ModelFreshnessRules {
            count: Some(1),
            period: Some(FreshnessPeriod::hour),
            updates_on: Some(UpdatesOn::Any),
        };

        assert_eq!(base, other);
    }

    #[test]
    fn test_hooks_equal_array_of_strings_vs_hook_config_array() {
        let array_of_strings = Hooks::ArrayOfStrings(vec![
            "{{ dbt_snow_mask.apply_masking_policy('snapshots') }}".to_string(),
        ]);

        let hook_config_array = Hooks::HookConfigArray(vec![HookConfig {
            sql: Some("{{ dbt_snow_mask.apply_masking_policy('snapshots') }}".to_string()),
            transaction: Some(true),
            index: None,
        }]);

        // Test that they are equal
        assert!(array_of_strings.hooks_equal(&hook_config_array));
        assert!(hook_config_array.hooks_equal(&array_of_strings));
    }

    #[test]
    fn test_hooks_equal_string_vs_hook_config() {
        let hook_string =
            Hooks::String("{{ dbt_snow_mask.apply_masking_policy('snapshots') }}".to_string());

        let hook_config = Hooks::HookConfig(HookConfig {
            sql: Some("{{ dbt_snow_mask.apply_masking_policy('snapshots') }}".to_string()),
            transaction: Some(true),
            index: None,
        });

        // Test that they are equal
        assert!(hook_string.hooks_equal(&hook_config));
        assert!(hook_config.hooks_equal(&hook_string));
    }

    #[test]
    fn test_get_semantic_name_snowflake_simple() {
        helper(AdapterType::Snowflake, false, "xyz", false, "xyz");
    }

    #[test]
    fn test_get_semantic_name_snowflake_quoted_identifier() {
        helper(AdapterType::Snowflake, false, r#""xyz""#, true, "xyz");
    }

    #[test]
    fn test_get_semantic_name_snowflake_quoting() {
        helper(AdapterType::Snowflake, true, "xyz", true, "xyz");
    }
    #[test]
    fn test_get_semantic_name_snowflake_quoted_identifier_with_quoting() {
        helper(AdapterType::Snowflake, true, r#""xyz""#, true, r#""xyz""#);
    }

    #[test]
    fn test_get_semantic_name_snowflake_non_identifier() {
        // This will fail because `z.3` should be quoted, but this is a user error
        helper(AdapterType::Snowflake, false, "z.3", false, "z.3");
    }

    #[test]
    fn test_get_semantic_name_snowflake_reserved() {
        // This will fail because `group` is a reserved keyword, but this is a user error
        helper(AdapterType::Snowflake, false, "group", false, "group");
    }

    #[test]
    fn test_get_semantic_name_snowflake_quoted_reserved() {
        helper(AdapterType::Snowflake, false, r#""GROUP""#, true, "GROUP");
    }

    #[test]
    fn test_get_semantic_name_snowflake_quoted_reserved_with_quoting() {
        helper(
            AdapterType::Snowflake,
            true,
            r#""GROUP""#,
            true,
            r#""GROUP""#,
        );
    }

    #[test]
    fn test_get_semantic_name_postgres_simple() {
        helper(AdapterType::Postgres, false, "xyz", false, "xyz");
    }

    #[test]
    fn test_get_semantic_name_postgres_quoted_identifier() {
        helper(AdapterType::Postgres, false, r#""xyz""#, true, "xyz");
    }

    #[test]
    fn test_get_semantic_name_postgres_quoting() {
        helper(AdapterType::Postgres, true, "xyz", true, "xyz");
    }
    #[test]
    fn test_get_semantic_name_postgres_quoted_identifier_with_quoting() {
        helper(AdapterType::Postgres, true, r#""xyz""#, true, r#""xyz""#);
    }

    #[test]
    fn test_get_semantic_name_postgres_non_identifier() {
        // This will fail because `z.3` should be quoted, but this is a user error
        helper(AdapterType::Postgres, false, "z.3", false, "z.3");
    }

    #[test]
    fn test_get_semantic_name_postgres_reserved() {
        // This will fail because `group` is a reserved keyword, but this is a user error
        helper(AdapterType::Postgres, false, "group", false, "group");
    }

    #[test]
    fn test_get_semantic_name_postgres_quoted_reserved() {
        helper(AdapterType::Postgres, false, r#""GROUP""#, true, "GROUP");
    }

    #[test]
    fn test_get_semantic_name_postgres_quoted_reserved_with_quoting() {
        helper(
            AdapterType::Postgres,
            true,
            r#""GROUP""#,
            true,
            r#""GROUP""#,
        );
    }
    fn helper(
        adapter_type: AdapterType,
        quoting: bool,
        identifier: &str,
        expected_quoting: bool,
        expected_identifier: &str,
    ) {
        let result = normalize_quote(quoting, adapter_type, identifier);

        let (actual_identifier, actual_quoting) = result;
        assert_eq!(actual_identifier, expected_identifier);
        assert_eq!(actual_quoting, expected_quoting);
    }

    mod schema_refresh_interval_tests {
        use super::*;

        #[test]
        fn test_parsing() {
            // Duration variants
            assert_eq!(
                "30m".parse::<SchemaRefreshInterval>().unwrap().as_secs(),
                Some(30 * 60)
            );
            assert_eq!(
                "2h".parse::<SchemaRefreshInterval>().unwrap().as_secs(),
                Some(2 * 60 * 60)
            );
            assert_eq!(
                "1d".parse::<SchemaRefreshInterval>().unwrap().as_secs(),
                Some(24 * 60 * 60)
            );

            // Never variant (case insensitive)
            assert!("never".parse::<SchemaRefreshInterval>().unwrap().is_never());
            assert!("NEVER".parse::<SchemaRefreshInterval>().unwrap().is_never());
            assert!("Never".parse::<SchemaRefreshInterval>().unwrap().is_never());

            // Never returns None for duration
            let never: SchemaRefreshInterval = "never".parse().unwrap();
            assert_eq!(never.as_secs(), None);
            assert_eq!(never.as_duration(), None);
        }

        #[test]
        fn test_default_and_constructors() {
            // Default is 1 hour
            let default = SchemaRefreshInterval::default();
            assert_eq!(default.as_secs(), Some(60 * 60));
            assert!(!default.is_never());

            // from_hours constructor
            assert_eq!(
                SchemaRefreshInterval::from_hours(3).as_secs(),
                Some(3 * 60 * 60)
            );
        }

        #[test]
        fn test_yaml_roundtrip() {
            #[derive(Serialize, Deserialize, PartialEq, Debug)]
            struct Config {
                interval: SchemaRefreshInterval,
            }

            // Duration roundtrip
            let duration_config = Config {
                interval: "2h".parse().unwrap(),
            };
            let yaml = dbt_serde_yaml::to_string(&duration_config).unwrap();
            assert_eq!(
                dbt_serde_yaml::from_str::<Config>(&yaml).unwrap(),
                duration_config
            );

            // Never roundtrip
            let never_config = Config {
                interval: SchemaRefreshInterval::Never,
            };
            let yaml = dbt_serde_yaml::to_string(&never_config).unwrap();
            assert_eq!(
                dbt_serde_yaml::from_str::<Config>(&yaml).unwrap(),
                never_config
            );
        }

        #[test]
        fn test_conversions() {
            // Into Option<Duration>
            let duration: Option<Duration> = "1h".parse::<SchemaRefreshInterval>().unwrap().into();
            assert_eq!(duration.unwrap().as_secs(), 60 * 60);

            let none: Option<Duration> = SchemaRefreshInterval::Never.into();
            assert!(none.is_none());

            // Display
            assert_eq!(
                format!("{}", "30m".parse::<SchemaRefreshInterval>().unwrap()),
                "30m"
            );
            assert_eq!(format!("{}", SchemaRefreshInterval::Never), "never");
        }

        #[test]
        fn test_zero_ttl() {
            // 0s TTL means always refresh
            let zero_ttl = "0s".parse::<SchemaRefreshInterval>().unwrap();
            assert_eq!(zero_ttl.as_secs(), Some(0));
            assert!(!zero_ttl.is_never());

            // Verify 0s is different from "never"
            let never = SchemaRefreshInterval::Never;
            assert_ne!(zero_ttl, never);
        }

        #[test]
        fn test_validation_bounds() {
            // Test minimum duration validation (0s and above should succeed)
            assert!("-1s".parse::<SchemaRefreshInterval>().is_err());
            assert!("0s".parse::<SchemaRefreshInterval>().is_ok());
            assert!("30s".parse::<SchemaRefreshInterval>().is_ok());
            assert!("59s".parse::<SchemaRefreshInterval>().is_ok());

            // Test 1 minute (should succeed)
            assert!("1m".parse::<SchemaRefreshInterval>().is_ok());
            assert!("60s".parse::<SchemaRefreshInterval>().is_ok());

            // Test maximum duration validation (> 1 year should fail)
            assert!("366d".parse::<SchemaRefreshInterval>().is_err());
            assert!("2y".parse::<SchemaRefreshInterval>().is_err());

            // Test exactly 1 year (should succeed)
            assert!("365d".parse::<SchemaRefreshInterval>().is_ok());
            assert!("8760h".parse::<SchemaRefreshInterval>().is_ok());
        }

        #[test]
        fn test_error_messages() {
            // Test invalid format error
            let err = "invalid".parse::<SchemaRefreshInterval>().unwrap_err();
            assert!(err.contains("Invalid schema_refresh_interval"));
            assert!(err.contains("Expected format like '30m', '2h', '1d', or 'never'"));

            // Test negative duration error
            let err = "-1s".parse::<SchemaRefreshInterval>().unwrap_err();
            assert!(err.contains("Invalid schema_refresh_interval"));
            assert!(err.contains("Expected format"));

            // Test too long error
            let err = "2y".parse::<SchemaRefreshInterval>().unwrap_err();
            assert!(err.contains("too long"));
            assert!(err.contains("Maximum allowed is 1 year"));
        }
    }

    #[test]
    fn test_schedule_parses_string_format() {
        // Test parsing schedule as a plain string (the format that was failing before)
        let yaml = r#"schedule: "USING CRON 0,15,30,45 * * * * UTC""#;
        #[derive(Deserialize)]
        struct TestConfig {
            schedule: Schedule,
        }
        let config: TestConfig = dbt_serde_yaml::from_str(yaml).unwrap();

        // Verify it parsed as Schedule::String
        assert!(matches!(config.schedule, Schedule::String(_)));

        // Verify to_schedule_config() works correctly
        let schedule_config = config.schedule.to_schedule_config();
        assert_eq!(
            schedule_config.cron,
            Some("USING CRON 0,15,30,45 * * * * UTC".to_string())
        );
        assert_eq!(schedule_config.time_zone_value, None);
    }

    #[test]
    fn test_schedule_parses_struct_format() {
        // Test parsing schedule as a structured config
        let yaml = r#"
schedule:
  cron: "0 */6 * * *"
  time_zone_value: "UTC"
"#;
        #[derive(Deserialize)]
        struct TestConfig {
            schedule: Schedule,
        }
        let config: TestConfig = dbt_serde_yaml::from_str(yaml).unwrap();

        // Verify it parsed as Schedule::ScheduleConfig
        assert!(matches!(config.schedule, Schedule::ScheduleConfig(_)));

        // Verify to_schedule_config() works correctly
        let schedule_config = config.schedule.to_schedule_config();
        assert_eq!(schedule_config.cron, Some("0 */6 * * *".to_string()));
        assert_eq!(schedule_config.time_zone_value, Some("UTC".to_string()));
    }

    #[test]
    fn test_schedule_in_model_config_string_format() {
        // Test the exact YAML format from the bug report:
        // models:
        //   - name: decrypt_drivers_license_number_task
        //     config:
        //       schedule: "USING CRON 0,15,30,45 * * * * UTC"
        let yaml = r#"
models:
  - name: some_scheduled_task
    config:
      schedule: "USING CRON 0,15,30,45 * * * * UTC"
"#;
        #[derive(Deserialize)]
        struct ModelsFile {
            models: Vec<ModelEntry>,
        }
        #[derive(Deserialize)]
        struct ModelEntry {
            name: String,
            config: ModelConfigInner,
        }
        #[derive(Deserialize)]
        struct ModelConfigInner {
            schedule: Schedule,
        }

        let parsed: ModelsFile = dbt_serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed.models.len(), 1);
        assert_eq!(parsed.models[0].name, "some_scheduled_task");

        let schedule_config = parsed.models[0].config.schedule.to_schedule_config();
        assert_eq!(
            schedule_config.cron,
            Some("USING CRON 0,15,30,45 * * * * UTC".to_string())
        );
        assert_eq!(schedule_config.time_zone_value, None);
    }

    #[test]
    fn test_freshness_rules_count_as_number() {
        let yaml = r#"
count: 24
period: hour
"#;
        let rules: FreshnessRules = dbt_serde_yaml::from_str(yaml).unwrap();
        assert_eq!(rules.count, Some(24));
        assert_eq!(rules.period, Some(FreshnessPeriod::hour));
    }

    #[test]
    fn test_freshness_rules_count_as_string() {
        let yaml = r#"
count: "24"
period: hour
"#;
        let rules: FreshnessRules = dbt_serde_yaml::from_str(yaml).unwrap();
        assert_eq!(rules.count, Some(24));
        assert_eq!(rules.period, Some(FreshnessPeriod::hour));
    }

    #[test]
    fn test_bigquery_partition_config_legacy_deserialize_from_jinja_values() {
        // Test String variant
        let string_value = MinijinjaValue::from("partition_field");
        let result = minijinja_value_to_typed_struct::<PartitionConfig>(string_value).unwrap();
        assert!(matches!(result, PartitionConfig::String(s) if s == "partition_field"));

        // Test List variant
        let list_value = MinijinjaValue::from(vec!["field1", "field2"]);
        let result = minijinja_value_to_typed_struct::<PartitionConfig>(list_value).unwrap();
        assert!(
            matches!(result, PartitionConfig::List(ref list) if list == &vec!["field1".to_string(), "field2".to_string()])
        );

        // Test BigqueryPartitionConfig variant with time partitioning
        let config_json: YmlValue = dbt_serde_yaml::from_str(
            r#"
            field: "partition_date"
            data_type: "date"
            granularity: "day"
            time_ingestion_partitioning: true
        "#,
        )
        .unwrap();
        let config_value = MinijinjaValue::from_serialize(&config_json);
        let result = minijinja_value_to_typed_struct::<PartitionConfig>(config_value).unwrap();
        if let PartitionConfig::BigqueryPartitionConfig(config) = result {
            assert_eq!(config.field, "partition_date");
            assert_eq!(config.data_type, "date");
            assert!(config.time_ingestion_partitioning());
            assert_eq!(config.granularity().unwrap(), "day");
        } else {
            panic!("Expected BigqueryPartitionConfig variant");
        }
    }
}
